"""
Ingestion benchmark: one language per run, chosen by the ``lang`` param.

    plan_bench_batch ─┐
    choose_language ──┼─> noop_<lang> ─> ingest_<lang>(cfg) ─> record ─> cleanup
                      └─ (other three languages skipped)

``noop_<lang>`` returns immediately, so its task duration is the pure startup
cost of that language (coordinator spawn + runtime init + SDK handshake).
``ingest_<lang>`` runs the same COPY-based extract-load as the main pipeline,
into a per-language table ``raw.bench_<lang>``. ``record`` gathers both
results into one XCom that ``bench/run_bench.py`` reads together with the task
instance durations from the REST API. ``cleanup`` empties the bench table so
the next run starts from the same state.

Schedule: none. Triggered by ``make bench``. Runs one at a time
(max_active_runs=1) so languages never compete for CPU or the database.
"""

from __future__ import annotations

from datetime import timedelta

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import Param, dag, task

from elt_common import LANGUAGES, SOURCE_TABLES, STUB_QUEUES, WAREHOUSE_CONN_ID
from elt_common.ingest_python import conn_info_from_airflow, ingest
from elt_common.metrics import Metrics


@task
def plan_bench_batch(params: dict, run_id: str) -> dict:
    # Exactly the keys the Go struct / Java POJO / TS interface declare.
    return {
        "batch_id": run_id,
        "source_table": f"src.{params['source_table']}",
        "target_table": f"raw.bench_{params['lang']}",
        "conn_id": WAREHOUSE_CONN_ID,
    }


@task.branch
def choose_language(params: dict) -> str:
    return f"noop_{params['lang']}"


# --- Python baseline (runs in the normal Python task runner) -----------------
@task
def noop_python() -> dict:
    return {**Metrics().finish(0, 0), "language": "python"}


@task
def ingest_python(cfg: dict) -> dict:
    return ingest(cfg, conn_info_from_airflow(cfg["conn_id"]))


# --- Other languages: bodies live in the handler bundles ---------------------
@task.stub(queue=STUB_QUEUES["go"])
def noop_go(): ...


@task.stub(queue=STUB_QUEUES["go"])
def ingest_go(cfg: dict): ...


@task.stub(queue=STUB_QUEUES["java"])
def noop_java(): ...


@task.stub(queue=STUB_QUEUES["java"])
def ingest_java(cfg: dict): ...


@task.stub(queue=STUB_QUEUES["ts"])
def noop_ts(): ...


@task.stub(queue=STUB_QUEUES["ts"])
def ingest_ts(cfg: dict): ...


@task(trigger_rule="none_failed_min_one_success")
def record(params: dict, ti=None) -> dict:
    lang = params["lang"]
    noop = ti.xcom_pull(task_ids=f"noop_{lang}")
    result = ti.xcom_pull(task_ids=f"ingest_{lang}")
    if noop is None or result is None:
        raise ValueError(f"missing XCom for {lang}: noop={noop!r} ingest={result!r}")
    return {"lang": lang, "source_table": params["source_table"], "noop": noop, "ingest": result}


@task
def cleanup(cfg: dict) -> None:
    PostgresHook(postgres_conn_id=cfg["conn_id"]).run(f"TRUNCATE {cfg['target_table']}")


@dag(
    dag_id="ingest_bench",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=30),
    tags=["benchmark", "ingestion"],
    params={
        "lang": Param("go", enum=list(LANGUAGES)),
        "source_table": Param("orders_1m", enum=list(SOURCE_TABLES)),
    },
)
def ingest_bench():
    cfg = plan_bench_batch()
    branch = choose_language()
    noops = {
        "python": noop_python(),
        "go": noop_go(),
        "java": noop_java(),
        "ts": noop_ts(),
    }
    ingests = {
        "python": ingest_python(cfg),
        "go": ingest_go(cfg),
        "java": ingest_java(cfg),
        "ts": ingest_ts(cfg),
    }
    recorded = record()
    for lang in LANGUAGES:
        branch >> noops[lang] >> ingests[lang] >> recorded
    recorded >> cleanup(cfg)


ingest_bench()
