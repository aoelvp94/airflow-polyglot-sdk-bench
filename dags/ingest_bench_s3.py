"""
Ingestion benchmark, Postgres -> S3 (MinIO): one language per run, chosen by ``lang``.

    plan_s3_batch ─┐
    choose_language┼─> noop_<lang> ─> ingest_s3_<lang>(cfg) ─> record ─> verify_object ─> cleanup
                   └─ (other three languages skipped)

Same harness as ``ingest_bench`` with a different sink: each ``ingest_s3_<lang>``
streams ``COPY src TO STDOUT`` through the shared per-row step, gzips it
(level 6) and uploads ONE object in sequential 8 MiB multipart parts to
``s3://raw/orders/<table>/<batch>/orders.tsv.gz`` on MinIO.

``verify_object`` (not measured) streams the object back, decompresses it,
and checks the row count and that every row ends with the batch id.
``cleanup`` deletes the object.

Schedule: none. Triggered by ``make bench-s3``. max_active_runs=1.
"""

from __future__ import annotations

import re
import zlib
from datetime import timedelta

from airflow.sdk import Param, dag, task

from elt_common import LANGUAGES, S3_BENCH_BUCKET, S3_CONN_ID, SOURCE_TABLES, STUB_QUEUES, WAREHOUSE_CONN_ID
from elt_common.ingest_python import conn_info_from_airflow
from elt_common.ingest_s3_python import ingest_s3, s3_client_from_airflow
from elt_common.metrics import Metrics


@task
def plan_s3_batch(params: dict, run_id: str) -> dict:
    # Exactly the keys the Go struct / Java POJO / TS interface declare.
    safe = re.sub(r"[^A-Za-z0-9._-]", "_", run_id)
    return {
        "batch_id": run_id,
        "source_table": f"src.{params['source_table']}",
        "conn_id": WAREHOUSE_CONN_ID,
        "s3_conn_id": S3_CONN_ID,
        "bucket": S3_BENCH_BUCKET,
        "key": f"orders/{params['source_table']}/{params['lang']}/{safe}/orders.tsv.gz",
    }


@task.branch
def choose_language(params: dict) -> str:
    return f"noop_{params['lang']}"


@task
def noop_python() -> dict:
    return {**Metrics().finish(0, 0), "language": "python"}


@task
def ingest_s3_python(cfg: dict) -> dict:
    return ingest_s3(cfg, conn_info_from_airflow(cfg["conn_id"]), s3_client_from_airflow(cfg["s3_conn_id"]))


@task.stub(queue=STUB_QUEUES["go"])
def noop_go(): ...


@task.stub(queue=STUB_QUEUES["go"])
def ingest_s3_go(cfg: dict): ...


@task.stub(queue=STUB_QUEUES["java"])
def noop_java(): ...


@task.stub(queue=STUB_QUEUES["java"])
def ingest_s3_java(cfg: dict): ...


@task.stub(queue=STUB_QUEUES["ts"])
def noop_ts(): ...


@task.stub(queue=STUB_QUEUES["ts"])
def ingest_s3_ts(cfg: dict): ...


@task(trigger_rule="none_failed_min_one_success")
def record(params: dict, ti=None) -> dict:
    lang = params["lang"]
    noop = ti.xcom_pull(task_ids=f"noop_{lang}")
    result = ti.xcom_pull(task_ids=f"ingest_s3_{lang}")
    if noop is None or result is None:
        raise ValueError(f"missing XCom for {lang}: noop={noop!r} ingest={result!r}")
    return {"lang": lang, "source_table": params["source_table"], "noop": noop, "ingest": result}


@task
def verify_object(cfg: dict, recorded: dict) -> dict:
    """Stream the object back: decompress, count rows, check every row ends with the batch id."""
    s3 = s3_client_from_airflow(cfg["s3_conn_id"])
    body = s3.get_object(Bucket=cfg["bucket"], Key=cfg["key"])["Body"]
    d = zlib.decompressobj(31)
    suffix = f"\t{cfg['batch_id']}".encode()
    rows = 0
    carry = b""
    compressed = 0
    for chunk in body.iter_chunks(1 << 20):
        compressed += len(chunk)
        data = carry + d.decompress(chunk)
        lines = data.split(b"\n")
        carry = lines.pop()
        for line in lines:
            if not line.endswith(suffix):
                raise ValueError(f"row {rows + 1} does not end with the batch id")
        rows += len(lines)
    if carry + d.flush():
        raise ValueError("object does not end with a newline")
    reported = recorded["ingest"]["row_count"]
    if rows != reported:
        raise ValueError(f"object has {rows} rows, handler reported {reported}")
    if compressed != recorded["ingest"]["bytes_out"]:
        raise ValueError(f"object is {compressed} bytes, handler reported {recorded['ingest']['bytes_out']}")
    return {"rows": rows, "compressed_bytes": compressed}


@task(trigger_rule="all_done")
def cleanup(cfg: dict) -> None:
    s3_client_from_airflow(cfg["s3_conn_id"]).delete_object(Bucket=cfg["bucket"], Key=cfg["key"])


@dag(
    dag_id="ingest_bench_s3",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=30),
    tags=["benchmark", "ingestion", "s3"],
    params={
        "lang": Param("go", enum=list(LANGUAGES)),
        "source_table": Param("orders_1m", enum=list(SOURCE_TABLES)),
    },
)
def ingest_bench_s3():
    cfg = plan_s3_batch()
    branch = choose_language()
    noops = {"python": noop_python(), "go": noop_go(), "java": noop_java(), "ts": noop_ts()}
    ingests = {
        "python": ingest_s3_python(cfg),
        "go": ingest_s3_go(cfg),
        "java": ingest_s3_java(cfg),
        "ts": ingest_s3_ts(cfg),
    }
    recorded = record()
    for lang in LANGUAGES:
        branch >> noops[lang] >> ingests[lang] >> recorded
    verify_object(cfg, recorded) >> cleanup(cfg)


ingest_bench_s3()
