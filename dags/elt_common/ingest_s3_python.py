"""
Python baseline of the Postgres -> S3 (MinIO) ingestion every language implements.

COPY src TO STDOUT -> per-row step -> gzip (level 6) -> one object uploaded in
sequential 8 MiB multipart parts. Same algorithm, sizes and checksum settings
as the Go/Java/TS handlers.
"""

from __future__ import annotations

import zlib

import boto3
import psycopg
from botocore.config import Config
from psycopg import sql

from elt_common.ingest_python import FIELDS_PER_ROW, _ident
from elt_common.metrics import Metrics

PART_SIZE = 8 << 20
GZIP_LEVEL = 6  # gzip.compress defaults to 9; set explicitly to match the other languages
ROW_BUFFER = 1 << 20


class _PartUploader:
    def __init__(self, s3, bucket: str, key: str) -> None:
        self.s3, self.bucket, self.key = s3, bucket, key
        self.upload_id = s3.create_multipart_upload(Bucket=bucket, Key=key, ContentType="application/gzip")[
            "UploadId"
        ]
        self.buf = bytearray()
        self.parts: list[dict] = []
        self.bytes_out = 0

    def write(self, data: bytes) -> None:
        self.buf += data
        while len(self.buf) >= PART_SIZE:
            self._upload(bytes(self.buf[:PART_SIZE]))
            del self.buf[:PART_SIZE]

    def _upload(self, body: bytes) -> None:
        num = len(self.parts) + 1
        res = self.s3.upload_part(
            Bucket=self.bucket, Key=self.key, UploadId=self.upload_id, PartNumber=num, Body=body
        )
        self.parts.append({"PartNumber": num, "ETag": res["ETag"]})
        self.bytes_out += len(body)

    def complete(self) -> None:
        if self.buf or not self.parts:
            self._upload(bytes(self.buf))
            self.buf.clear()
        self.s3.complete_multipart_upload(
            Bucket=self.bucket, Key=self.key, UploadId=self.upload_id, MultipartUpload={"Parts": self.parts}
        )

    def abort(self) -> None:
        try:
            self.s3.abort_multipart_upload(Bucket=self.bucket, Key=self.key, UploadId=self.upload_id)
        except Exception:  # best effort
            pass


def s3_client_from_airflow(conn_id: str):
    from airflow.sdk import Connection

    c = Connection.get(conn_id)
    extra = c.extra_dejson
    return boto3.client(
        "s3",
        endpoint_url=extra.get("endpoint_url"),
        region_name=extra.get("region_name", "us-east-1"),
        aws_access_key_id=c.login,
        aws_secret_access_key=c.password,
        config=Config(
            s3={"addressing_style": "path"},
            request_checksum_calculation="when_required",
            response_checksum_validation="when_required",
        ),
    )


def ingest_s3(cfg: dict, conn_info: dict, s3) -> dict:
    m = Metrics()
    suffix = f"\t{cfg['batch_id']}\n".encode()
    rows = bytes_in = 0
    gz = zlib.compressobj(GZIP_LEVEL, zlib.DEFLATED, 31)  # wbits=31 -> gzip container
    up = _PartUploader(s3, cfg["bucket"], cfg["key"])
    try:
        with psycopg.connect(**conn_info) as src, src.cursor() as cur:
            copy_out = sql.SQL("COPY {} TO STDOUT").format(_ident(cfg["source_table"]))
            with cur.copy(copy_out) as cout:
                buf = bytearray()
                for row in cout:
                    bytes_in += len(row)
                    if not row or row[-1] != 0x0A:
                        raise ValueError("truncated row at end of stream")
                    row = bytes(row)
                    fields = row.count(b"\t") + 1
                    if fields != FIELDS_PER_ROW:
                        raise ValueError(f"row {rows + 1} has {fields} fields, want {FIELDS_PER_ROW}")
                    buf += row[:-1]
                    buf += suffix
                    rows += 1
                    if len(buf) >= ROW_BUFFER:
                        up.write(gz.compress(buf))
                        buf = bytearray()
                up.write(gz.compress(buf) + gz.flush())
        up.complete()
    except BaseException:
        up.abort()
        raise
    out = m.finish(rows, bytes_in)
    out.update(
        batch_id=cfg["batch_id"],
        language="python",
        source_table=cfg["source_table"],
        target=f"s3://{cfg['bucket']}/{cfg['key']}",
        row_count=rows,
        bytes_out=up.bytes_out,
        parts=len(up.parts),
    )
    return out
