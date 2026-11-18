# Ingestion benchmark: Python vs Go vs Java vs TypeScript task SDKs

Every figure here comes from runs in this repo. The raw data is
[results/bench_raw.jsonl](results/bench_raw.jsonl) (one JSON line per Dag run), and the tables
below are generated from it by [bench/summarize.py](bench/summarize.py).

## What was measured

**The job.** An extract-load from `src.orders_{1m,10m}` (Postgres) to `raw.bench_<lang>` (same
Postgres), implemented once per language as an Airflow task. Each implementation:

1. reads the Airflow connection `pg_warehouse` through its SDK client and opens two connections;
2. `BEGIN; TRUNCATE target`;
3. streams `COPY src TO STDOUT` → per-row step → `COPY target FROM STDIN`;
4. `COMMIT`, then returns one dict (row_count, batch_id, timings, memory) as its return-value XCom.

The per-row step is the same everywhere: check that the row has 8 tab-separated fields, append
`\t<batch_id>`, count the row. Output goes to the target in ~1 MiB writes.

| lang | how it runs in Airflow | driver |
|---|---|---|
| python | plain `@task` (Task SDK task runner) | psycopg 3.3.6 `cursor.copy()` |
| go | `@task.stub(queue="golang")` → ExecutableCoordinator → Go SDK v1.0.0 bundle | pgx v5.13.0 `PgConn().CopyTo/CopyFrom` + `io.Pipe` |
| java | `@task.stub(queue="java")` → JavaCoordinator → JRE 17.0.21, airflow-sdk 1.0.0 | pgJDBC 42.7.8 `CopyManager` (`readFromCopy`/`writeToCopy`) |
| ts | `@task.stub(queue="typescript")` → NodeCoordinator → Node 22.23.5, ts-sdk 1.0.0 | pg 8 + pg-copy-streams 7, `stream.pipeline` |

Code: [elt_common/ingest_python.py](dags/elt_common/ingest_python.py),
[handlers/go/ingest.go](handlers/go/ingest.go),
[handlers/java/.../IngestTask.java](handlers/java/src/main/java/demo/IngestTask.java),
[handlers/ts/src/main.ts](handlers/ts/src/main.ts).

**The harness.** The [`ingest_bench`](dags/ingest_bench.py) Dag runs one language per Dag run,
chosen by the `lang` param: `noop_<lang>` → `ingest_<lang>` → `record` → `cleanup`.
`max_active_runs=1`, so no two measured tasks ever overlap. [bench/run_bench.py](bench/run_bench.py)
triggers the runs through the REST API:

- one **warm-up** run per table (excluded from the stats), so the source table is in the page cache;
- then the repetitions, with the language order **rotated each round** (round *r* starts
  with language *r*), so no language is always first or last after a checkpoint or cleanup.

**Metrics, kept separate as asked:**

| metric | source | what it isolates |
|---|---|---|
| **noop e2e** | `noop_<lang>`: TI `queued_when → end_date` | **startup overhead**: executor pickup + process spawn + runtime init + SDK handshake + XCom push, with no work |
| process age at handler start | inside the process: `/proc/self/stat` starttime vs `/proc/uptime` | the in-process share of startup (exec → handler entry) |
| **handler** | wall clock inside the handler | **throughput** part: connect, TRUNCATE, COPY→step→COPY, COMMIT |
| task e2e | `ingest_<lang>`: TI `queued_when → end_date` | what a Dag run actually pays for the task |
| overhead | task e2e − handler | everything around the work, on a real task |
| rows/s | row_count / handler | |
| **CPU** | `/proc/self/stat` utime+stime, handler end − handler start | CPU the language spent on the job (Postgres server CPU is not included) |
| **peak RSS** | `/proc/self/status` VmHWM at handler end | whole task process, runtime included |

Every language reads the `/proc` metrics the same way. Why `queued_when → end_date` and not
the TI "duration": for a stub task, Airflow stamps `start_date` partway through spawning the
process. A Java no-op's own process had been alive 0.41 s (median) when the handler started,
yet its TI duration was only 0.24 s. TI duration is reported alongside, but it understates
startup.

**Environment.** Apple M4 (10 cores), macOS 26.5.2, Docker Desktop with 10 CPUs / 7.7 GiB
(linuxkit kernel 7.0.14). Airflow 3.4.0 (apache/airflow:3.4.0-python3.12), LocalExecutor.
Tasks run in the scheduler container. Postgres 17 for both the source and the target, in a
separate container (`shared_buffers=1GB`, `max_wal_size=8GB`). Data comes from
[sql/seed.sql](sql/seed.sql) with `setseed(0.42)`: 1M rows = 101 MB of COPY text, 10M rows =
1.02 GB of COPY text (1,302 MB on disk).

**Commands used** (2026-11-18):

```bash
make up
```

```bash
make seed
```

```bash
make bench REPEATS=3
```

```bash
python3 bench/run_bench.py --repeats 3 --rep-start 4 --langs python,go,java,ts --tables orders_1m,orders_10m --out results/bench_raw.jsonl
```

```bash
make report
```

The first `make bench` produced reps 1–3. The `run_bench.py` command added reps 4–6, with its
own warm-up per table, so **n = 6 per language and table** (48 measured runs, 4 warm-ups, 0
failures). I extended to n=6 because rep 1 at 10M was an outlier for every language (see
caveats). The method was not changed between the two batches.

## Results

Each cell is the **median [min–max]** over n runs.

Source: `results/bench_raw.jsonl` — 48 successful measured runs, 0 failed, 4 warm-up runs excluded.

### orders_1m

| lang | n | task e2e (s) | handler (s) | overhead (s) | rows/s | CPU in handler (s) | peak RSS (MB) |
|---|---|---|---|---|---|---|---|
| python | 6 | 1.81 [1.50–2.70] | 1.47 [1.20–1.87] | 0.42 [0.22–1.05] | 691,782 [535,021–830,867] | 1.29 [1.05–1.58] | 193 [185–202] |
| go | 6 | 1.78 [1.57–4.62] | 1.56 [1.40–2.33] | 0.20 [0.11–2.35] | 640,710 [429,656–712,157] | 0.74 [0.60–1.27] | 15 [14–15] |
| java | 6 | 2.09 [1.72–2.89] | 1.62 [1.28–2.09] | 0.51 [0.44–0.81] | 619,698 [479,084–782,248] | 0.84 [0.68–0.99] | 162 [161–163] |
| ts | 6 | 1.81 [1.63–2.67] | 1.62 [1.51–2.48] | 0.18 [0.12–0.43] | 617,857 [403,205–663,040] | 0.71 [0.58–1.06] | 103 [99–109] |

### orders_10m

| lang | n | task e2e (s) | handler (s) | overhead (s) | rows/s | CPU in handler (s) | peak RSS (MB) |
|---|---|---|---|---|---|---|---|
| python | 6 | 13.85 [11.51–21.89] | 13.45 [11.26–21.29] | 0.31 [0.25–0.60] | 745,684 [469,646–888,362] | 13.04 [10.98–19.76] | 208 [196–241] |
| go | 6 | 12.95 [11.28–20.14] | 12.82 [11.12–19.73] | 0.14 [0.12–0.41] | 779,996 [506,764–899,154] | 6.23 [5.46–7.74] | 15 [14–15] |
| java | 6 | 12.57 [11.23–19.55] | 12.11 [10.88–19.04] | 0.46 [0.34–0.59] | 826,066 [525,357–919,393] | 2.66 [2.40–5.24] | 163 [162–164] |
| ts | 6 | 15.46 [10.60–21.61] | 15.22 [10.44–21.46] | 0.16 [0.12–0.59] | 681,135 [466,009–957,960] | 5.45 [3.65–7.61] | 128 [120–135] |

### Startup cost (noop task, all tables pooled)

| lang | n | noop e2e (s) | noop TI duration (s) | process age at handler start (s) | noop process CPU total (s) | noop peak RSS (MB) |
|---|---|---|---|---|---|---|
| python | 12 | 0.21 [0.16–1.68] | 0.18 [0.14–1.59] | 0.12 [0.10–1.47] | 0.10 [0.06–1.06] | 175 [171–180] |
| go | 12 | 0.15 [0.07–0.22] | 0.03 [0.02–0.06] | 0.03 [0.02–0.13] | 0.00 [0.00–0.06] | 11 [11–11] |
| java | 12 | 0.51 [0.42–0.81] | 0.24 [0.18–0.32] | 0.41 [0.28–0.58] | 0.60 [0.49–0.93] | 86 [86–91] |
| ts | 12 | 0.22 [0.12–1.28] | 0.04 [0.02–0.16] | 0.14 [0.08–0.55] | 0.06 [0.04–0.43] | 51 [51–52] |


## Reading the numbers

**1. Throughput doesn't separate the languages; Postgres does.** At 10M rows the medians are
12.1–15.2 s and every language's range spans roughly 10.5–21.5 s. The spread *within* a
language is several times the gap *between* languages, so no language is faster at moving
rows with n=6. That's expected for COPY→COPY: the server parses, checks and writes 1 GB on
both sides, and every client mostly waits on sockets.

**2. CPU separates them clearly** (10M, CPU inside the handler, median):

| python | go | java | ts |
|---|---|---|---|
| 13.04 s | 6.23 s | **2.66 s** | 5.45 s |

Python's CPU (13.0 s) is about equal to its wall time (13.5 s). It runs one core flat out,
so it is already at its limit. Any heavier per-row logic (parsing, hashing, type coercion)
would make Python the bottleneck before Postgres is. Java's JIT-compiled byte loop over
pgJDBC's per-row buffers is the cheapest. Go spends more than Java here. I didn't profile it,
but the obvious candidates are the two `io.Pipe` copies plus pgx's own buffering.

**3. Memory separates them clearly** (peak RSS of the whole task process, 10M):
Go **15 MB**, TS 128 MB, Java 163 MB, Python 208 MB. Memory is flat between 1M and 10M for
every language, so all four stream. The figures are runtime baseline plus buffers (compare
the no-op RSS: 11 / 51 / 86 / 175 MB). Python's figure includes Airflow itself, because the
task runs in the Task SDK's task process.

**4. Startup separates them, and only matters for small jobs** (no-op, queued→end, median):
Go 0.15 s, Python 0.21 s, TS 0.22 s, **Java 0.51 s**. Inside the process, the JVM needs
0.41 s from exec to handler and burns 0.60 s of CPU just starting. The others need 0.03–0.14 s.
On a real ingest task the overhead (task e2e − handler) shows the same order:
Go 0.14–0.20 s, TS 0.16–0.18 s, Python 0.31–0.42 s, Java 0.46–0.51 s.

## Decision

The trade-off is startup cost against CPU efficiency. Wall time doesn't decide it.

| dataset | winner | why (from the tables above) |
|---|---|---|
| **1M rows** | **Go** | Wall time tied (1.78 s median vs 1.81–2.09 s). Lowest per-task overhead (0.20 s vs Java 0.51 s; JVM startup is ~25% of a ~2 s task). 15 MB peak RSS vs 103–193 MB. CPU 0.74 s, essentially tied with TS (0.71 s). |
| **10M rows** | **Java** | Wall time tied (12.57 s median, the lowest, but within noise). Least CPU by far: 2.66 s vs Go 6.23 s, TS 5.45 s, Python 13.04 s. Its ~0.35 s extra startup is < 3% of the task. Costs ~150 MB more RAM than Go. |

**Python** is never the pick. It ties on wall time today but uses the most CPU and memory,
and it has no headroom for heavier per-row work. **TS** is a reasonable middle ground (low
startup, mid CPU) but wins neither size. At 10M its median was the slowest, with the widest
range.

Where the call could flip:
- **Many concurrent ingests on one worker, memory-bound**: Go at every size (15 MB vs 163 MB per task).
- **CPU-bound worker or heavier per-row transforms**: Java from ~1M rows up.
- **Many tiny tasks** (≪1M rows, seconds or less): Go or TS. JVM startup would dominate.

**How it's wired:** `orders_elt_pipeline`'s `ingest_language` param defaults to `auto`
(env `INGEST_LANGUAGE`). `auto` maps `orders_1m → go` and `orders_10m → java`
([dags/elt_common/__init__.py](dags/elt_common/__init__.py)). Any language can be forced per
run (`make run INGEST_LANG=ts`) or per deployment (`INGEST_LANGUAGE=go`).

## End-to-end pipeline runs

`orders_elt_pipeline` was run once per language. These were run on their own, not as
benchmark measurements: run once, with dbt on the same box.

```bash
make run TABLE=orders_1m
```

```bash
make run INGEST_LANG=auto TABLE=orders_10m
```

```bash
make run INGEST_LANG=python TABLE=orders_1m
```

```bash
make run INGEST_LANG=ts TABLE=orders_1m
```

| run_id | param → picked | ingest handler (s) | source = raw = stg_orders = Σ fct_orders_daily |
|---|---|---|---|
| manual__2026-11-18T05:03:03.741499+00:00 | auto/orders_1m → **go** | 3.131 | 1,000,000 (all equal) |
| manual__2026-11-18T05:04:03.482527+00:00 | auto/orders_10m → **java** | 15.639 | 10,000,000 (all equal) |
| manual__2026-11-18T05:05:16.332571+00:00 | python/orders_1m | 3.683 | 1,000,000 (all equal) |
| manual__2026-11-18T05:05:43.029169+00:00 | ts/orders_1m | 1.237 | 1,000,000 (all equal) |

All four runs succeeded, including Cosmos `dbt.stg_orders.{run,test}`,
`dbt.fct_orders_daily.{run,test}` and `validate_row_counts`. The Python run's 241 MB RSS and
0.50 s process age (vs 0.12 s in the bench) are real. In this Dag the Python task process
re-parses a Dag file that imports Cosmos. That's a cost the stub languages don't pay.

## Caveats, and what was not done

- **The rep-1 outlier at 10M.** All four languages took 18–21 s in round 1 against ~11–14 s
  afterwards. Postgres logged a WAL-triggered checkpoint from 04:46:08 to 04:47:36, writing
  22,512 buffers (17% of shared_buffers). It overlaps the Python, Go and Java rep-1 runs.
  TS rep 1 (04:47:59–04:48:18) overlapped a checkpoint that wrote only 6 buffers, so that run
  is unexplained. Insert-triggered autovacuum on `src.orders_10m` had finished at 04:40:50,
  before any run. Checkpoints also happened during later rounds without a comparable slowdown.
  I report these runs and don't exclude them. Medians are robust to them, and the min–max
  ranges show them.
- **n=6 cannot resolve wall-time differences of ~10%** at this variance, which is why the
  decision rests on CPU, memory and startup.
- **One machine, Docker Desktop on macOS**, with client and server on the same VM and no
  network latency. Over a real network, client CPU matters even less and server/network
  matters more.
- **Postgres → Postgres only.** The Postgres → S3/MinIO path in the brief was **not
  implemented or measured**. With object storage as the sink, client CPU (serialization,
  compression) would matter more, so the CPU column is the better predictor there.
- **JVM defaults.** No `-Xmx`, GC, CDS or AppCDS tuning. AppCDS or CRaC could cut Java's
  0.41 s exec-to-handler time. Go was built with `-trimpath -ldflags="-s -w"`.
- **Implementations were tuned to parity, not to the limit.** The TS handler was first
  measured pushing one buffer per row (3.19 s handler at 1M in a smoke run). It was changed
  to 1 MiB batching like the others before any benchmark run (smoke data:
  [results/smoke.jsonl](results/smoke.jsonl), [results/smoke2.jsonl](results/smoke2.jsonl),
  not used in the tables).
- Postgres server CPU is not in the CPU column. It is the same work for every language.

## Raw numbers (every run)

Generated from `results/bench_raw.jsonl` (also in [results/bench_runs.md](results/bench_runs.md)).
"e2e" = TI `queued_when → end_date`.

| table | lang | rep | warm-up | noop e2e (s) | ingest e2e (s) | ingest TI duration (s) | handler (s) | rows | CPU in handler (s) | peak RSS (MB) | recorded_at (UTC) |
|---|---|---|---|---|---|---|---|---|---|---|---|
| orders_1m | python | 0 | yes | 0.229 | 1.738 | 1.636 | 1.409 | 1,000,000 | 1.21 | 192.7 | 04:44:15 |
| orders_1m | python | 1 |  | 0.299 | 1.655 | 1.515 | 1.266 | 1,000,000 | 1.05 | 198.6 | 04:44:23 |
| orders_1m | go | 1 |  | 0.175 | 1.766 | 1.536 | 1.404 | 1,000,000 | 0.6 | 14.8 | 04:44:31 |
| orders_1m | java | 1 |  | 0.571 | 2.894 | 2.411 | 2.087 | 1,000,000 | 0.99 | 161.3 | 04:44:42 |
| orders_1m | ts | 1 |  | 0.229 | 2.112 | 1.798 | 1.686 | 1,000,000 | 0.75 | 98.8 | 04:44:52 |
| orders_1m | go | 2 |  | 0.169 | 1.793 | 1.639 | 1.563 | 1,000,000 | 0.78 | 14.7 | 04:45:01 |
| orders_1m | java | 2 |  | 0.733 | 1.718 | 1.537 | 1.278 | 1,000,000 | 0.7 | 162.3 | 04:45:09 |
| orders_1m | ts | 2 |  | 0.223 | 1.632 | 1.569 | 1.513 | 1,000,000 | 0.58 | 101.8 | 04:45:17 |
| orders_1m | python | 2 |  | 0.358 | 1.504 | 1.490 | 1.285 | 1,000,000 | 1.15 | 191.7 | 04:45:26 |
| orders_1m | java | 3 |  | 0.495 | 1.944 | 1.708 | 1.509 | 1,000,000 | 0.75 | 161.0 | 04:45:32 |
| orders_1m | ts | 3 |  | 0.214 | 1.861 | 1.735 | 1.688 | 1,000,000 | 0.61 | 102.6 | 04:45:40 |
| orders_1m | python | 3 |  | 0.164 | 2.698 | 2.679 | 1.652 | 1,000,000 | 1.44 | 194.1 | 04:45:48 |
| orders_1m | go | 3 |  | 0.132 | 1.722 | 1.687 | 1.559 | 1,000,000 | 0.6 | 14.3 | 04:45:57 |
| orders_10m | python | 0 | yes | 1.106 | 27.718 | 27.293 | 25.244 | 10,000,000 | 21.65 | 230.6 | 04:46:34 |
| orders_10m | python | 1 |  | 0.59 | 21.895 | 21.830 | 21.293 | 10,000,000 | 19.76 | 240.7 | 04:47:04 |
| orders_10m | go | 1 |  | 0.219 | 20.139 | 19.923 | 19.733 | 10,000,000 | 7.74 | 14.6 | 04:47:29 |
| orders_10m | java | 1 |  | 0.812 | 19.546 | 19.345 | 19.035 | 10,000,000 | 5.24 | 163.9 | 04:47:57 |
| orders_10m | ts | 1 |  | 0.365 | 18.41 | 18.172 | 18.092 | 10,000,000 | 6.28 | 128.6 | 04:48:22 |
| orders_10m | go | 2 |  | 0.216 | 12.002 | 11.818 | 11.767 | 10,000,000 | 5.67 | 14.5 | 04:48:41 |
| orders_10m | java | 2 |  | 0.672 | 12.655 | 12.425 | 12.143 | 10,000,000 | 2.66 | 164.0 | 04:49:01 |
| orders_10m | ts | 2 |  | 0.239 | 11.967 | 11.893 | 11.842 | 10,000,000 | 4.3 | 135.2 | 04:49:20 |
| orders_10m | python | 2 |  | 0.158 | 11.509 | 11.496 | 11.257 | 10,000,000 | 11.06 | 196.2 | 04:49:36 |
| orders_10m | java | 3 |  | 0.466 | 13.047 | 12.847 | 12.635 | 10,000,000 | 2.97 | 162.5 | 04:49:55 |
| orders_10m | ts | 3 |  | 0.211 | 10.603 | 10.515 | 10.439 | 10,000,000 | 3.65 | 124.2 | 04:50:11 |
| orders_10m | python | 3 |  | 0.2 | 13.028 | 13.005 | 12.721 | 10,000,000 | 12.25 | 222.3 | 04:50:31 |
| orders_10m | go | 3 |  | 0.072 | 12.975 | 12.933 | 12.852 | 10,000,000 | 6.23 | 14.5 | 04:50:52 |
| orders_1m | python | 0 | yes | 0.388 | 1.882 | 1.841 | 1.545 | 1,000,000 | 1.43 | 184.4 | 04:51:46 |
| orders_1m | ts | 4 |  | 0.124 | 1.715 | 1.636 | 1.556 | 1,000,000 | 0.75 | 108.9 | 04:51:55 |
| orders_1m | python | 4 |  | 0.223 | 1.972 | 1.942 | 1.653 | 1,000,000 | 1.58 | 185.3 | 04:52:03 |
| orders_1m | go | 4 |  | 0.205 | 2.475 | 2.435 | 2.327 | 1,000,000 | 1.27 | 14.8 | 04:52:12 |
| orders_1m | java | 4 |  | 0.485 | 2.525 | 2.383 | 1.842 | 1,000,000 | 0.98 | 161.7 | 04:52:22 |
| orders_1m | python | 5 |  | 0.171 | 2.328 | 2.292 | 1.869 | 1,000,000 | 1.51 | 202.2 | 04:52:33 |
| orders_1m | go | 5 |  | 0.066 | 4.623 | 3.280 | 2.272 | 1,000,000 | 0.92 | 14.7 | 04:52:44 |
| orders_1m | java | 5 |  | 0.455 | 2.237 | 2.083 | 1.734 | 1,000,000 | 0.93 | 162.4 | 04:52:53 |
| orders_1m | ts | 5 |  | 0.209 | 1.752 | 1.670 | 1.508 | 1,000,000 | 0.66 | 107.1 | 04:53:01 |
| orders_1m | go | 6 |  | 0.087 | 1.575 | 1.532 | 1.463 | 1,000,000 | 0.7 | 14.8 | 04:53:10 |
| orders_1m | java | 6 |  | 0.437 | 1.846 | 1.671 | 1.321 | 1,000,000 | 0.68 | 162.7 | 04:53:18 |
| orders_1m | ts | 6 |  | 0.141 | 2.665 | 2.569 | 2.48 | 1,000,000 | 1.06 | 102.7 | 04:53:26 |
| orders_1m | python | 6 |  | 0.204 | 1.656 | 1.633 | 1.204 | 1,000,000 | 1.14 | 185.5 | 04:53:37 |
| orders_10m | python | 0 | yes | 0.191 | 14.866 | 14.848 | 14.613 | 10,000,000 | 14.13 | 218.6 | 04:53:59 |
| orders_10m | ts | 4 |  | 1.282 | 18.777 | 18.298 | 18.188 | 10,000,000 | 6.25 | 120.2 | 04:54:30 |
| orders_10m | python | 4 |  | 0.387 | 16.634 | 16.591 | 16.358 | 10,000,000 | 15.71 | 203.7 | 04:54:55 |
| orders_10m | go | 4 |  | 0.124 | 13.975 | 13.914 | 13.841 | 10,000,000 | 6.43 | 14.6 | 04:55:15 |
| orders_10m | java | 4 |  | 0.537 | 12.483 | 12.143 | 11.89 | 10,000,000 | 2.65 | 162.3 | 04:55:34 |
| orders_10m | python | 5 |  | 1.678 | 14.671 | 14.574 | 14.179 | 10,000,000 | 13.83 | 207.7 | 04:55:59 |
| orders_10m | go | 5 |  | 0.138 | 11.277 | 11.188 | 11.122 | 10,000,000 | 5.46 | 14.6 | 04:56:16 |
| orders_10m | java | 5 |  | 0.52 | 12.405 | 12.259 | 12.068 | 10,000,000 | 2.66 | 162.1 | 04:56:34 |
| orders_10m | ts | 5 |  | 0.222 | 21.605 | 21.554 | 21.459 | 10,000,000 | 7.61 | 132.8 | 04:57:01 |
| orders_10m | go | 6 |  | 0.162 | 12.924 | 12.860 | 12.79 | 10,000,000 | 6.23 | 14.7 | 04:57:20 |
| orders_10m | java | 6 |  | 0.422 | 11.235 | 11.083 | 10.877 | 10,000,000 | 2.4 | 163.4 | 04:57:36 |
| orders_10m | ts | 6 |  | 0.151 | 12.5 | 12.419 | 12.353 | 10,000,000 | 4.65 | 126.9 | 04:57:55 |
| orders_10m | python | 6 |  | 0.182 | 11.586 | 11.543 | 11.272 | 10,000,000 | 10.98 | 207.9 | 04:58:13 |
