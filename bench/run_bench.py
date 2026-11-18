#!/usr/bin/env python3
"""
Drive the `ingest_bench` Dag: one Dag run per (table, repetition, language).

For each source table:
  * one warm-up run (recorded with warmup=true, excluded from stats) so the
    source table is in the page cache for every measured run;
  * REPEATS rounds; in round r the language order is rotated by r, so no
    language always runs first or last.

Every run appends one JSON line with the task-instance timings from the REST
API and the handler-reported metrics (from the `record` task's XCom).

Usage: python3 bench/run_bench.py --repeats 3 --langs python,go,java,ts \
           --tables orders_1m,orders_10m --out results/bench_raw.jsonl
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).parent))
import airflow_api as api  # noqa: E402

DAG_ID = "ingest_bench"


def _ts(s: str | None) -> dt.datetime | None:
    return dt.datetime.fromisoformat(s.replace("Z", "+00:00")) if s else None


def _secs(a: str | None, b: str | None) -> float | None:
    ta, tb = _ts(a), _ts(b)
    return round((tb - ta).total_seconds(), 3) if ta and tb else None


def one_run(lang: str, table: str, rep: int, warmup: bool) -> dict:
    run_id = api.trigger(DAG_ID, {"lang": lang, "source_table": table})
    state = api.wait_run(DAG_ID, run_id)
    tis = api.task_instances(DAG_ID, run_id)
    row: dict = {"lang": lang, "table": table, "rep": rep, "warmup": warmup, "run_id": run_id, "state": state}
    for kind in ("noop", "ingest"):
        ti = tis[f"{kind}_{lang}"]
        row[f"{kind}_ti_state"] = ti["state"]
        row[f"{kind}_ti_duration_s"] = ti.get("duration")
        # start_date is stamped after the coordinator has begun spawning the
        # process, so TI duration hides part of the startup. queued_when ->
        # end_date includes all of it (plus executor pickup, equal for all).
        row[f"{kind}_queued_to_start_s"] = _secs(ti.get("queued_when"), ti.get("start_date"))
        row[f"{kind}_queued_to_end_s"] = _secs(ti.get("queued_when"), ti.get("end_date"))
        for f in ("queued_when", "start_date", "end_date"):
            row[f"{kind}_{f}"] = ti.get(f)
    if state == "success":
        rec = api.xcom(DAG_ID, run_id, "record")
        row["noop"] = rec["noop"]
        row["ingest"] = rec["ingest"]
    return row


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--repeats", type=int, default=3)
    p.add_argument("--langs", default="python,go,java,ts")
    p.add_argument("--tables", default="orders_1m,orders_10m")
    p.add_argument("--out", default="results/bench_raw.jsonl")
    p.add_argument("--no-warmup", action="store_true")
    p.add_argument("--rep-start", type=int, default=1, help="number of the first repetition (to extend a run)")
    a = p.parse_args()

    langs = a.langs.split(",")
    tables = a.tables.split(",")
    out = pathlib.Path(a.out)
    out.parent.mkdir(parents=True, exist_ok=True)

    api.wait_for_dag(DAG_ID)
    api.unpause(DAG_ID)

    plan: list[tuple[str, str, int, bool]] = []
    for table in tables:
        if not a.no_warmup:
            plan.append((langs[0], table, 0, True))
        for rep in range(a.rep_start, a.rep_start + a.repeats):
            k = (rep - 1) % len(langs)
            for lang in langs[k:] + langs[:k]:
                plan.append((lang, table, rep, False))

    with out.open("a") as f:
        for i, (lang, table, rep, warm) in enumerate(plan, 1):
            row = one_run(lang, table, rep, warm)
            row["recorded_at"] = dt.datetime.now(dt.UTC).isoformat(timespec="seconds")
            f.write(json.dumps(row) + "\n")
            f.flush()
            ing = row.get("ingest") or {}
            print(
                f"[{i}/{len(plan)}] {table:<10} {lang:<6} rep={rep}{' (warm-up)' if warm else ''} "
                f"state={row['state']} noop_ti={row['noop_ti_duration_s']}s "
                f"ingest_ti={row['ingest_ti_duration_s']}s handler={ing.get('handler_elapsed_s')}s "
                f"rows={ing.get('row_count')} rss={ing.get('peak_rss_mb')}MB",
                flush=True,
            )


if __name__ == "__main__":
    main()
