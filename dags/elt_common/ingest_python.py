"""
Python baseline of the extract-load every language implements.

COPY src TO STDOUT -> per-row step -> COPY target FROM STDIN, in one
transaction that first truncates the target. The per-row step is identical in
every language: check the field count, append "\t<batch_id>", count the row.
"""

from __future__ import annotations

import psycopg
from psycopg import sql

from elt_common.metrics import Metrics

FIELDS_PER_ROW = 8
FLUSH_BYTES = 1 << 20


def _ident(dotted: str) -> sql.Identifier:
    return sql.Identifier(*dotted.split("."))


def ingest(cfg: dict, conn_info: dict) -> dict:
    m = Metrics()
    suffix = f"\t{cfg['batch_id']}\n".encode()
    rows = 0
    bytes_in = 0
    with psycopg.connect(**conn_info) as src, psycopg.connect(**conn_info) as dst:
        with src.cursor() as scur, dst.cursor() as dcur:
            dcur.execute(sql.SQL("TRUNCATE {}").format(_ident(cfg["target_table"])))
            copy_out = sql.SQL("COPY {} TO STDOUT").format(_ident(cfg["source_table"]))
            copy_in = sql.SQL("COPY {} FROM STDIN").format(_ident(cfg["target_table"]))
            with scur.copy(copy_out) as cout, dcur.copy(copy_in) as cin:
                buf = bytearray()
                # psycopg yields one CopyData message per row in text format.
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
                    if len(buf) >= FLUSH_BYTES:
                        cin.write(buf)
                        buf = bytearray()
                if buf:
                    cin.write(buf)
        dst.commit()
    out = m.finish(rows, bytes_in)
    out.update(
        batch_id=cfg["batch_id"],
        language="python",
        source_table=cfg["source_table"],
        target_table=cfg["target_table"],
        row_count=rows,
    )
    return out


def conn_info_from_airflow(conn_id: str) -> dict:
    from airflow.sdk import Connection

    c = Connection.get(conn_id)
    return {"host": c.host, "port": c.port, "dbname": c.schema, "user": c.login, "password": c.password}
