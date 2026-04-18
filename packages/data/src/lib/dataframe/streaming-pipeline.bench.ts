// ---------------------------------------------------------------------------
// End-to-end streaming pipeline benchmark — 1M rows
//
// Three scenarios, run in order for direct comparison:
//   (c) Direct-feed IncrementalAggregation — pure aggregator, no I/O
//   (a) Current architecture: CsvSink (buffered) → CsvFile (buffered) → stream
//   (b) True streaming: StreamingCsvSink + streamingCsvRows + IncrementalAggregation
//
// Correctness is asserted against a reference JS aggregate; timing is printed.
// Run with:
//   bun test packages/data/src/lib/dataframe/streaming-pipeline.bench.ts
// ---------------------------------------------------------------------------

import { describe, it, expect, beforeAll, afterAll } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { stat } from "node:fs/promises";

import { DataFrame } from "./dataframe.ts";
import { CsvFile } from "./file-source.ts";
import { CsvSink } from "./sink.ts";
import { IncrementalAggregation } from "./incremental-aggregation.ts";
import { exprAgg } from "./logical-plan.ts";
import { col } from "./expr.ts";
import { streamingCsvRows, StreamingCsvSink } from "./csv-stream.ts";

const ROWS = 1_000_000;
const CHUNK = 1_000;
const REGIONS = ["north", "south", "east", "west"] as const;

type Row = { id: number; region: string; value: number; active: number };

// ---------------------------------------------------------------------------
// Deterministic data generation
// ---------------------------------------------------------------------------

function genRow(i: number): Row {
  const region = REGIONS[i & 3]!;
  const x = (i * 2654435761) >>> 0;
  const value = x % 10_000;
  return { id: i, region, value, active: i % 3 === 0 ? 1 : 0 };
}

function* rowGenerator(n: number): Generator<Row> {
  for (let i = 0; i < n; i++) yield genRow(i);
}

function* chunked<T>(iter: Iterable<T>, size: number): Generator<T[]> {
  let buf: T[] = [];
  for (const v of iter) {
    buf.push(v);
    if (buf.length === size) {
      yield buf;
      buf = [];
    }
  }
  if (buf.length > 0) yield buf;
}

// ---------------------------------------------------------------------------
// Reference aggregate — plain JS, computed once
// ---------------------------------------------------------------------------

interface Agg {
  region: string;
  count: number;
  sum: number;
  min: number;
  max: number;
  avg: number;
}

function referenceAgg(): Map<string, Agg> {
  const m = new Map<string, { count: number; sum: number; min: number; max: number }>();
  for (let i = 0; i < ROWS; i++) {
    const r = genRow(i);
    const acc = m.get(r.region) ?? { count: 0, sum: 0, min: Infinity, max: -Infinity };
    acc.count++;
    acc.sum += r.value;
    if (r.value < acc.min) acc.min = r.value;
    if (r.value > acc.max) acc.max = r.value;
    m.set(r.region, acc);
  }
  const out = new Map<string, Agg>();
  for (const [region, v] of m) {
    out.set(region, {
      region,
      count: v.count,
      sum: v.sum,
      min: v.min,
      max: v.max,
      avg: v.sum / v.count,
    });
  }
  return out;
}

function assertMatches(
  actual: Record<string, unknown>[],
  reference: Map<string, Agg>,
  label: string,
) {
  expect(actual, `${label}: row count`).toHaveLength(reference.size);
  for (const row of actual) {
    const ref = reference.get(row.region as string);
    expect(ref, `${label}: region '${row.region}' exists`).toBeDefined();
    expect(row.count, `${label}: count for ${row.region}`).toBe(ref!.count);
    expect(row.sum, `${label}: sum for ${row.region}`).toBe(ref!.sum);
    expect(row.min, `${label}: min for ${row.region}`).toBe(ref!.min);
    expect(row.max, `${label}: max for ${row.region}`).toBe(ref!.max);
    expect(
      Math.abs((row.avg as number) - ref!.avg),
      `${label}: avg for ${row.region}`,
    ).toBeLessThan(1e-9);
  }
}

// ---------------------------------------------------------------------------
// Timing + memory helpers
// ---------------------------------------------------------------------------

async function timeAsync<T>(
  label: string,
  fn: () => Promise<T>,
): Promise<{ result: T; ms: number }> {
  const t0 = performance.now();
  const result = await fn();
  const ms = performance.now() - t0;
  console.log(`  ${label.padEnd(38)} ${ms.toFixed(1).padStart(9)} ms`);
  return { result, ms };
}

function timeSync<T>(label: string, fn: () => T): { result: T; ms: number } {
  const t0 = performance.now();
  const result = fn();
  const ms = performance.now() - t0;
  console.log(`  ${label.padEnd(38)} ${ms.toFixed(1).padStart(9)} ms`);
  return { result, ms };
}

function rssMB(): number {
  return process.memoryUsage().rss / 1024 / 1024;
}

// ---------------------------------------------------------------------------
// Shared fixtures
// ---------------------------------------------------------------------------

let TMP = "";
let CSV_PATH_BUFFERED = "";
let CSV_PATH_STREAMING = "";
let REFERENCE: Map<string, Agg>;

beforeAll(() => {
  TMP = mkdtempSync(join(tmpdir(), "promin-stream-bench-"));
  CSV_PATH_BUFFERED = join(TMP, "rows.buffered.csv");
  CSV_PATH_STREAMING = join(TMP, "rows.streaming.csv");
  console.log(`\nbench tmp dir: ${TMP}`);
  console.log(`rows: ${ROWS.toLocaleString()}, chunk: ${CHUNK}\n`);
  const { result, ms } = timeSync("reference agg (plain JS baseline)", referenceAgg);
  REFERENCE = result;
  console.log(`  → ${ROWS.toLocaleString()} rows scanned in plain JS in ${ms.toFixed(1)}ms\n`);
});

afterAll(() => {
  rmSync(TMP, { recursive: true, force: true });
});

// ---------------------------------------------------------------------------
// (c) Direct-feed bench
// ---------------------------------------------------------------------------

describe("streaming pipeline (c) — direct-feed IncrementalAggregation", () => {
  it("aggregates 1M rows fed in 1K chunks", async () => {
    console.log("\n(c) direct-feed — no I/O");
    const rssBefore = rssMB();

    const agg = IncrementalAggregation.create({
      groupBy: ["region"],
      agg: {
        count: exprAgg({ expr: col("value"), agg: "count" }),
        sum: exprAgg({ expr: col("value"), agg: "sum" }),
        min: exprAgg({ expr: col("value"), agg: "min" }),
        max: exprAgg({ expr: col("value"), agg: "max" }),
        avg: exprAgg({ expr: col("value"), agg: "avg" }),
      },
    });

    const { ms: ingestMs } = await timeAsync("ingest 1M rows (1K chunks)", async () => {
      for (const chunk of chunked(rowGenerator(ROWS), CHUNK)) {
        await agg.ingest(chunk);
      }
    });

    const { result: rows, ms: snapMs } = await timeAsync("snapshot + collect", async () => {
      const snap = await agg.snapshot();
      return snap.collect();
    });

    console.log(
      `  rss delta                              ${(rssMB() - rssBefore).toFixed(1).padStart(9)} MB`,
    );
    console.log(
      `  TOTAL                                  ${(ingestMs + snapMs).toFixed(1).padStart(9)} ms`,
    );

    assertMatches(rows as Record<string, unknown>[], REFERENCE, "(c)");
  }, 120_000);
});

// ---------------------------------------------------------------------------
// (a) Current architecture — buffered sink + buffered load + stream
// ---------------------------------------------------------------------------

describe("streaming pipeline (a) — buffered CsvSink + buffered CsvFile", () => {
  it("write 1M-row CSV, load, stream through groupBy+agg", async () => {
    console.log("\n(a) current architecture — buffered I/O");
    const rssBefore = rssMB();

    const { ms: writeMs } = await timeAsync("CsvSink.write x 1M + end", async () => {
      const sink = CsvSink<Row>(CSV_PATH_BUFFERED);
      for (const row of rowGenerator(ROWS)) await sink.write(row);
      await sink.end();
    });
    const sz = await stat(CSV_PATH_BUFFERED);
    console.log(
      `  file size                              ${(sz.size / 1_048_576).toFixed(1).padStart(9)} MB`,
    );

    const df = DataFrame.fromFile<Row>(CsvFile<Row>(CSV_PATH_BUFFERED));

    const { result: rows, ms: aggMs } = await timeAsync(
      "load + groupBy + agg (.collect)",
      async () => {
        return df
          .groupBy("region")
          .agg({
            count: exprAgg({ expr: col("value"), agg: "count" }),
            sum: exprAgg({ expr: col("value"), agg: "sum" }),
            min: exprAgg({ expr: col("value"), agg: "min" }),
            max: exprAgg({ expr: col("value"), agg: "max" }),
            avg: exprAgg({ expr: col("value"), agg: "avg" }),
          })
          .collect();
      },
    );

    const rssDelta = rssMB() - rssBefore;
    console.log(`  rss delta                              ${rssDelta.toFixed(1).padStart(9)} MB`);
    console.log(
      `  TOTAL                                  ${(writeMs + aggMs).toFixed(1).padStart(9)} ms`,
    );

    assertMatches(rows as Record<string, unknown>[], REFERENCE, "(a)");
  }, 180_000);
});

// ---------------------------------------------------------------------------
// (b) True streaming I/O
// ---------------------------------------------------------------------------

describe("streaming pipeline (b) — StreamingCsvSink + streamingCsvRows + IncrementalAggregation", () => {
  it("write 1M-row CSV incrementally, stream rows into aggregator", async () => {
    console.log("\n(b) true streaming I/O — disk-to-aggregate");
    const rssBefore = rssMB();

    const { ms: writeMs } = await timeAsync("StreamingCsvSink.write x 1M + end", async () => {
      const sink = StreamingCsvSink<Row>(CSV_PATH_STREAMING);
      for (const row of rowGenerator(ROWS)) await sink.write(row);
      await sink.end();
    });
    const sz = await stat(CSV_PATH_STREAMING);
    console.log(
      `  file size                              ${(sz.size / 1_048_576).toFixed(1).padStart(9)} MB`,
    );

    const agg = IncrementalAggregation.create({
      groupBy: ["region"],
      agg: {
        count: exprAgg({ expr: col("value"), agg: "count" }),
        sum: exprAgg({ expr: col("value"), agg: "sum" }),
        min: exprAgg({ expr: col("value"), agg: "min" }),
        max: exprAgg({ expr: col("value"), agg: "max" }),
        avg: exprAgg({ expr: col("value"), agg: "avg" }),
      },
    });

    const { ms: pipeMs } = await timeAsync("stream file → ingest (1K chunks)", async () => {
      let buf: Row[] = [];
      for await (const row of streamingCsvRows<Row>(CSV_PATH_STREAMING)) {
        buf.push(row);
        if (buf.length === CHUNK) {
          await agg.ingest(buf);
          buf = [];
        }
      }
      if (buf.length > 0) await agg.ingest(buf);
    });

    const { result: rows, ms: snapMs } = await timeAsync("snapshot + collect", async () => {
      const snap = await agg.snapshot();
      return snap.collect();
    });

    const rssDelta = rssMB() - rssBefore;
    console.log(`  rss delta                              ${rssDelta.toFixed(1).padStart(9)} MB`);
    console.log(
      `  TOTAL                                  ${(writeMs + pipeMs + snapMs).toFixed(1).padStart(9)} ms`,
    );

    assertMatches(rows as Record<string, unknown>[], REFERENCE, "(b)");
  }, 180_000);
});
