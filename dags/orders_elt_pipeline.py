"""
Orders ELT: multi-language ingestion, then dbt via Cosmos, then validation.

    plan_batch ──────┐
    choose_ingestion ┼─> ingest_{python|go|java|ts}(cfg) ─> collect_ingest_result
                     │        (one runs, three skip)                │
                     │                                              v
                     │                          dbt (Cosmos DbtTaskGroup:
                     │                           stg_orders -> fct_orders_daily, + tests)
                     │                                              │
                     └──────────────────────────────> validate_row_counts(result)

Purpose: land ``src.<source_table>`` into ``raw.orders`` with the ingestion
language chosen by the ``ingest_language`` param: ``auto`` (default, from the
INGEST_LANGUAGE env var) picks the measured winner for the source table, see
BENCHMARK.md; ``python|go|java|ts`` forces one, transform it with dbt, and
check that row counts agree end to end.

Cross-language contract: ``plan_batch`` returns ONE dict and each ingestion
stub receives it whole as ``cfg``. Each ingestion returns ONE dict
(batch_id, row_count, source_table, target_table, timings). Nothing indexes
an XCom by key across the language boundary.

Schedule: none (manual / ``make run``). Nothing runs after it.
"""

from __future__ import annotations

from datetime import timedelta

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import Param, dag, task
from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import ExecutionMode, LoadMode, TestBehavior, TestIndirectSelection
from cosmos.profiles import PostgresUserPasswordProfileMapping

from elt_common import (
    DBT_EXECUTABLE,
    DBT_PROJECT_DIR,
    DEFAULT_INGEST_LANGUAGE,
    LANGUAGES,
    SOURCE_TABLES,
    STUB_QUEUES,
    WAREHOUSE_CONN_ID,
    resolve_ingest_language,
)
from elt_common.ingest_python import conn_info_from_airflow, ingest

INGEST_TASK_IDS = [f"ingest_{lang}" for lang in LANGUAGES]


@task
def plan_batch(params: dict, run_id: str) -> dict:
    # Exactly the keys the Go struct / Java POJO / TS interface declare: the Go
    # and Java SDKs reject unknown keys when decoding into a typed input.
    return {
        "batch_id": run_id,
        "source_table": f"src.{params['source_table']}",
        "target_table": "raw.orders",
        "conn_id": WAREHOUSE_CONN_ID,
    }


@task.branch
def choose_ingestion(params: dict) -> str:
    lang = resolve_ingest_language(params["ingest_language"], params["source_table"])
    print(f"ingest_language={params['ingest_language']!r} source_table={params['source_table']!r} -> {lang}")
    return f"ingest_{lang}"


@task
def ingest_python(cfg: dict) -> dict:
    return ingest(cfg, conn_info_from_airflow(cfg["conn_id"]))


@task.stub(queue=STUB_QUEUES["go"])
def ingest_go(cfg: dict): ...


@task.stub(queue=STUB_QUEUES["java"])
def ingest_java(cfg: dict): ...


@task.stub(queue=STUB_QUEUES["ts"])
def ingest_ts(cfg: dict): ...


@task(trigger_rule="none_failed_min_one_success")
def collect_ingest_result(ti=None) -> dict:
    """Return the one ingestion result that ran, whichever language produced it."""
    results = {tid: ti.xcom_pull(task_ids=tid) for tid in INGEST_TASK_IDS}
    ran = {tid: r for tid, r in results.items() if r is not None}
    if len(ran) != 1:
        raise ValueError(f"expected exactly one ingestion result, got {sorted(ran)}")
    (result,) = ran.values()
    return result


@task
def validate_row_counts(result: dict) -> dict:
    """Source == raw (this batch) == staging == sum of the mart's order_count."""
    hook = PostgresHook(postgres_conn_id=WAREHOUSE_CONN_ID)
    batch_id = result["batch_id"]
    counts = {
        "reported_by_ingestion": result["row_count"],
        "source": hook.get_first(f"SELECT count(*) FROM {result['source_table']}")[0],
        "raw": hook.get_first(
            f"SELECT count(*) FROM {result['target_table']} WHERE batch_id = %s", parameters=(batch_id,)
        )[0],
        "stg_orders": hook.get_first("SELECT count(*) FROM analytics.stg_orders")[0],
        "fct_orders_daily": hook.get_first(
            "SELECT coalesce(sum(order_count), 0)::bigint FROM analytics.fct_orders_daily WHERE batch_id = %s",
            parameters=(batch_id,),
        )[0],
    }
    if len(set(counts.values())) != 1:
        raise ValueError(f"row counts disagree for batch {batch_id}: {counts}")
    print(f"row counts agree for batch {batch_id}: {counts}")
    return {"batch_id": batch_id, "language": result["language"], **counts}


@dag(
    dag_id="orders_elt_pipeline",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=30),
    tags=["dbt", "ingestion", "orders"],
    params={
        "ingest_language": Param(DEFAULT_INGEST_LANGUAGE, enum=["auto", *LANGUAGES]),
        "source_table": Param("orders_1m", enum=list(SOURCE_TABLES)),
    },
)
def orders_elt_pipeline():
    cfg = plan_batch()
    branch = choose_ingestion()
    ingests = [ingest_python(cfg), ingest_go(cfg), ingest_java(cfg), ingest_ts(cfg)]
    result = collect_ingest_result()
    branch >> ingests >> result

    dbt = DbtTaskGroup(
        group_id="dbt",
        project_config=ProjectConfig(
            dbt_project_path=DBT_PROJECT_DIR,
            manifest_path=f"{DBT_PROJECT_DIR}/target/manifest.json",
        ),
        profile_config=ProfileConfig(
            profile_name="orders",
            target_name="airflow",
            profile_mapping=PostgresUserPasswordProfileMapping(
                conn_id=WAREHOUSE_CONN_ID,
                profile_args={"schema": "analytics", "threads": 2},
            ),
        ),
        execution_config=ExecutionConfig(
            execution_mode=ExecutionMode.LOCAL,
            dbt_executable_path=DBT_EXECUTABLE,
            # A model's test task runs `dbt test --select <model>`. With the default
            # (eager) a test that also refs a downstream model, such as
            # assert_mart_counts_match_staging, would run in stg_orders.test before
            # fct_orders_daily exists. Buildable defers it to the last parent's test task.
            test_indirect_selection=TestIndirectSelection.BUILDABLE,
        ),
        render_config=RenderConfig(
            load_method=LoadMode.DBT_MANIFEST,
            select=["tag:orders"],
            test_behavior=TestBehavior.AFTER_EACH,
        ),
        operator_args={
            # `vars` is a templated field on Cosmos' local operators, so the
            # batch id is read from the single result dict at run time.
            "vars": {"batch_id": "{{ ti.xcom_pull(task_ids='collect_ingest_result')['batch_id'] }}"},
            "install_deps": False,
        },
    )
    result >> dbt >> validate_row_counts(result)


orders_elt_pipeline()
