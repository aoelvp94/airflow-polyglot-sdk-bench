"""Shared constants and the Python ingestion baseline for the demo Dags."""

from __future__ import annotations

import os

WAREHOUSE_CONN_ID = "pg_warehouse"
LANGUAGES = ("python", "go", "java", "ts")
SOURCE_TABLES = ("orders_1m", "orders_10m")

# Language each stub runs on. Values are [sdk] queue_to_coordinator keys.
STUB_QUEUES = {"go": "golang", "java": "java", "ts": "typescript"}

# Winner per dataset size, from BENCHMARK.md (n=6 runs each):
#   orders_1m  -> go   (wall time tied; lowest startup overhead, 15 MB peak RSS)
#   orders_10m -> java (wall time tied; least CPU: 2.7 s vs 6.2 s Go, 13.0 s Python)
INGEST_LANGUAGE_BY_TABLE = {"orders_1m": "go", "orders_10m": "java"}

# Default for orders_elt_pipeline's ingest_language param. "auto" applies the
# table above; override per deployment with INGEST_LANGUAGE, or per run with the param.
DEFAULT_INGEST_LANGUAGE = os.environ.get("INGEST_LANGUAGE", "auto")


def resolve_ingest_language(requested: str, source_table: str) -> str:
    if requested == "auto":
        return INGEST_LANGUAGE_BY_TABLE.get(source_table, "go")
    if requested not in LANGUAGES:
        raise ValueError(f"unknown ingest_language {requested!r}")
    return requested

DBT_PROJECT_DIR = "/opt/airflow/dbt/orders"
DBT_EXECUTABLE = "/opt/airflow/dbt-venv/bin/dbt"
