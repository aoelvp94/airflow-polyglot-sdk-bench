#!/usr/bin/env python3
"""
Summarize results/bench_raw.jsonl into Markdown tables: median and [min-max]
per (table, language), warm-up runs excluded.

Columns
  task e2e        ingest_<lang> queued_when -> end_date: what a Dag run pays for the task,
                  including process spawn (TI start_date is stamped mid-spawn, so the
                  TI "duration" alone hides part of the startup)
  handler         time inside the handler: connect, TRUNCATE, COPY->step->COPY, COMMIT
  overhead        task e2e - handler: executor pickup, process spawn, runtime init,
                  SDK handshake, XCom/connection fetch, result push
  rows/s          row_count / handler time
  CPU             process CPU (user+sys) spent inside the handler
  peak RSS        VmHWM of the task process (whole process, incl. runtime)
  noop e2e        noop_<lang> queued_when -> end_date: pure startup cost incl. executor pickup
  noop RSS        VmHWM of the noop task process: runtime baseline
"""

from __future__ import annotations

import json
import statistics
import sys
from collections import defaultdict

LANG_ORDER = ["python", "go", "java", "ts"]


def fmt(vals: list[float], digits: int = 2, unit: str = "") -> str:
    vals = [v for v in vals if v is not None]
    if not vals:
        return "n/a"
    med = statistics.median(vals)
    if digits == 0:
        return f"{med:,.0f}{unit} [{min(vals):,.0f}–{max(vals):,.0f}]"
    return f"{med:.{digits}f}{unit} [{min(vals):.{digits}f}–{max(vals):.{digits}f}]"


def main(path: str) -> None:
    rows = [json.loads(line) for line in open(path) if line.strip()]
    measured = [r for r in rows if not r["warmup"]]
    failed = [r for r in measured if r["state"] != "success"]
    ok = [r for r in measured if r["state"] == "success"]

    groups: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for r in ok:
        groups[(r["table"], r["lang"])].append(r)

    print(f"Source: `{path}` — {len(ok)} successful measured runs, {len(failed)} failed, "
          f"{len(rows) - len(measured)} warm-up runs excluded.\n")
    tables = sorted({t for t, _ in groups}, key=lambda t: (len(t), t))
    for table in tables:
        print(f"### {table}\n")
        print("| lang | n | task e2e (s) | handler (s) | overhead (s) | rows/s | CPU in handler (s) | peak RSS (MB) |")
        print("|---|---|---|---|---|---|---|---|")
        for lang in LANG_ORDER:
            g = groups.get((table, lang))
            if not g:
                continue
            ing = [r["ingest"] for r in g]
            print(
                f"| {lang} | {len(g)} "
                f"| {fmt([r['ingest_queued_to_end_s'] for r in g])} "
                f"| {fmt([i['handler_elapsed_s'] for i in ing])} "
                f"| {fmt([r['ingest_queued_to_end_s'] - i['handler_elapsed_s'] for r, i in zip(g, ing)])} "
                f"| {fmt([i.get('rows_per_s') for i in ing], 0)} "
                f"| {fmt([i['cpu_handler_s'] for i in ing])} "
                f"| {fmt([i['peak_rss_mb'] for i in ing], 0)} |"
            )
        print()

    print("### Startup cost (noop task, all tables pooled)\n")
    print("| lang | n | noop e2e (s) | noop TI duration (s) | process age at handler start (s) | noop process CPU total (s) | noop peak RSS (MB) |")
    print("|---|---|---|---|---|---|---|")
    for lang in LANG_ORDER:
        g = [r for r in ok if r["lang"] == lang]
        if not g:
            continue
        n = [r["noop"] for r in g]
        print(
            f"| {lang} | {len(g)} "
            f"| {fmt([r['noop_queued_to_end_s'] for r in g])} "
            f"| {fmt([r['noop_ti_duration_s'] for r in g])} "
            f"| {fmt([x['process_age_at_start_s'] for x in n])} "
            f"| {fmt([x['cpu_process_total_s'] for x in n])} "
            f"| {fmt([x['peak_rss_mb'] for x in n], 0)} |"
        )
    print()
    if failed:
        print("### Failed runs\n")
        for r in failed:
            print(f"- {r['table']} {r['lang']} rep={r['rep']} run_id=`{r['run_id']}` "
                  f"noop={r['noop_ti_state']} ingest={r['ingest_ti_state']}")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "results/bench_raw.jsonl")
