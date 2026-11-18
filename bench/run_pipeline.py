#!/usr/bin/env python3
"""Trigger orders_elt_pipeline once, wait, and print task states and the validation result."""

from __future__ import annotations

import argparse
import json
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).parent))
import airflow_api as api  # noqa: E402

DAG_ID = "orders_elt_pipeline"

p = argparse.ArgumentParser()
p.add_argument("--lang", default="auto")
p.add_argument("--table", default="orders_1m")
a = p.parse_args()

api.wait_for_dag(DAG_ID)
api.unpause(DAG_ID)
run_id = api.trigger(DAG_ID, {"ingest_language": a.lang, "source_table": a.table})
print(f"triggered {DAG_ID} run_id={run_id} lang={a.lang} table={a.table}", flush=True)
state = api.wait_run(DAG_ID, run_id)
for tid, ti in sorted(api.task_instances(DAG_ID, run_id).items(), key=lambda kv: kv[1].get("start_date") or "~"):
    print(f"  {tid:<45} {ti['state'] or '-':<10} {ti.get('duration') or 0:8.2f}s")
print(f"dag run state: {state}")
if state == "success":
    print("collect_ingest_result:", json.dumps(api.xcom(DAG_ID, run_id, "collect_ingest_result")))
    print("validate_row_counts:  ", json.dumps(api.xcom(DAG_ID, run_id, "validate_row_counts")))
sys.exit(0 if state == "success" else 1)
