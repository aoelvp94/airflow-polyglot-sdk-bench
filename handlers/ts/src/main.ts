/**
 * TypeScript TaskHandlers for the Python Dags `orders_elt_pipeline` and
 * `ingest_bench`. The Python files declare the graph with
 * @task.stub(queue="typescript"); NodeCoordinator finds this packed
 * bundle.min.mjs in the `ts-task-handlers` Dag bundle and runs one Node
 * process per task instance.
 */
import { Transform } from "node:stream";
import { pipeline } from "node:stream/promises";
import { readFileSync } from "node:fs";
import { Bundle, TaskHandler, getClient, getContext } from "@apache-airflow/ts-sdk";
import pg from "pg";
import { from as copyFrom, to as copyTo } from "pg-copy-streams";

/** Exactly the keys plan_batch returns. */
interface BatchConfig {
  batch_id: string;
  source_table: string;
  target_table: string;
  conn_id: string;
}

const FIELDS_PER_ROW = 8;
const TAB = 0x09;
const NL = 0x0a;
const FLUSH_BYTES = 1 << 20;
const EMPTY = Buffer.alloc(0);

/**
 * The shared per-row step: check the field count, append "\t<batch_id>",
 * count the row. COPY chunks are not row-aligned, so a partial trailing row is
 * carried into the next chunk.
 */
class AppendBatchId extends Transform {
  rows = 0;
  bytesIn = 0;
  private carry: Buffer = Buffer.alloc(0);
  private readonly suffix: Buffer;

  constructor(batchId: string) {
    super();
    this.suffix = Buffer.from(`\t${batchId}\n`);
  }

  // Output is batched into ~1 MiB writes, like the Go/Java/Python handlers;
  // pg-copy-streams emits one chunk per row, and pushing each one would send
  // one CopyData message per row.
  private out: Buffer = Buffer.allocUnsafe(FLUSH_BYTES * 2);
  private outLen = 0;

  override _transform(chunk: Buffer, _enc: BufferEncoding, cb: (err?: Error | null) => void): void {
    this.bytesIn += chunk.length;
    const buf = this.carry.length ? Buffer.concat([this.carry, chunk]) : chunk;
    let lineStart = 0;
    let tabs = 0;
    for (let i = 0; i < buf.length; i++) {
      const b = buf[i];
      if (b === TAB) {
        tabs++;
      } else if (b === NL) {
        if (tabs + 1 !== FIELDS_PER_ROW) {
          cb(new Error(`row ${this.rows + 1} has ${tabs + 1} fields, want ${FIELDS_PER_ROW}`));
          return;
        }
        const need = i - lineStart + this.suffix.length;
        if (this.outLen + need > this.out.length) this.flushOut(need);
        this.outLen += buf.copy(this.out, this.outLen, lineStart, i);
        this.outLen += this.suffix.copy(this.out, this.outLen);
        this.rows++;
        lineStart = i + 1;
        tabs = 0;
      }
    }
    this.carry = lineStart < buf.length ? Buffer.from(buf.subarray(lineStart)) : EMPTY;
    if (this.outLen >= FLUSH_BYTES) this.flushOut(0);
    cb();
  }

  override _flush(cb: (err?: Error | null) => void): void {
    if (this.carry.length) {
      cb(new Error("truncated row at end of stream"));
      return;
    }
    this.flushOut(0);
    cb();
  }

  /** Push what is buffered; make sure the next buffer can hold `need` bytes. */
  private flushOut(need: number): void {
    if (this.outLen) this.push(this.out.subarray(0, this.outLen));
    this.out = Buffer.allocUnsafe(Math.max(FLUSH_BYTES * 2, need));
    this.outLen = 0;
  }
}

function quoteIdent(dotted: string): string {
  return dotted
    .split(".")
    .map((p) => `"${p.replaceAll('"', '""')}"`)
    .join(".");
}

async function ingest({ cfg }: { cfg: BatchConfig }) {
  const m = startMetrics();
  const conn = await getClient().getConnectionOrThrow(cfg.conn_id);
  const pgcfg = {
    host: conn.host ?? undefined,
    port: conn.port ?? undefined,
    database: conn.schema ?? undefined,
    user: conn.login ?? undefined,
    password: conn.password ?? undefined,
  };
  const src = new pg.Client(pgcfg);
  const dst = new pg.Client(pgcfg);
  await src.connect();
  await dst.connect();
  const step = new AppendBatchId(cfg.batch_id);
  try {
    await dst.query("BEGIN");
    await dst.query(`TRUNCATE ${quoteIdent(cfg.target_table)}`);
    await pipeline(
      src.query(copyTo(`COPY ${quoteIdent(cfg.source_table)} TO STDOUT`)),
      step,
      dst.query(copyFrom(`COPY ${quoteIdent(cfg.target_table)} FROM STDIN`)),
    );
    await dst.query("COMMIT");
  } catch (e) {
    await dst.query("ROLLBACK").catch(() => undefined);
    throw e;
  } finally {
    await src.end();
    await dst.end();
  }
  console.log(`ingest finished task=${getContext().taskId} rows=${step.rows}`);
  return {
    ...m.finish(step.rows, step.bytesIn),
    batch_id: cfg.batch_id,
    language: "ts",
    source_table: cfg.source_table,
    target_table: cfg.target_table,
    row_count: step.rows,
  };
}

/** Measures pure task startup: coordinator spawn + Node start + SDK handshake. */
async function noop() {
  return { ...startMetrics().finish(0, 0), language: "ts" };
}

// --- metrics, read the same way in every language from /proc/self ----------

const CLK_TCK = 100; // USER_HZ on Linux

function statFields(): string[] {
  const s = readFileSync("/proc/self/stat", "utf8");
  return s.slice(s.lastIndexOf(")") + 2).trim().split(/\s+/);
}
function procCpu(): number {
  const f = statFields();
  return (Number(f[11]) + Number(f[12])) / CLK_TCK;
}
function processAge(): number {
  const up = Number(readFileSync("/proc/uptime", "utf8").split(/\s+/)[0]);
  return up - Number(statFields()[19]) / CLK_TCK;
}
function vmHwmKb(): number {
  const line = readFileSync("/proc/self/status", "utf8")
    .split("\n")
    .find((l) => l.startsWith("VmHWM:"));
  return line ? Number(line.split(/\s+/)[1]) : 0;
}
const round3 = (v: number) => Math.round(v * 1000) / 1000;

function startMetrics() {
  const wallStart = process.hrtime.bigint();
  const cpuStart = procCpu();
  const age = processAge();
  return {
    finish(rows: number, bytesIn: number) {
      const elapsed = Number(process.hrtime.bigint() - wallStart) / 1e9;
      const cpuEnd = procCpu();
      const out: Record<string, number> = {
        handler_elapsed_s: round3(elapsed),
        process_age_at_start_s: round3(age),
        cpu_handler_s: round3(cpuEnd - cpuStart),
        cpu_process_total_s: round3(cpuEnd),
        peak_rss_mb: round3(vmHwmKb() / 1024),
        bytes_in: bytesIn,
      };
      if (elapsed > 0 && rows > 0) out.rows_per_s = Math.floor(rows / elapsed);
      return out;
    },
  };
}

const bundle = new Bundle();
bundle.register(new TaskHandler("orders_elt_pipeline", "ingest_ts", ingest));
bundle.register(new TaskHandler("ingest_bench", "ingest_ts", ingest));
bundle.register(new TaskHandler("ingest_bench", "noop_ts", noop));
await bundle.serve();
