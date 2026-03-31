/**
 * Cross-language benchmark: Promin DataFrame on 1M row CSV.
 * Compare with bench-pandas-polars.py for the same operations.
 */

import { DataFrame, CsvFile } from "../../src/lib/dataframe/index.ts";
import { col } from "../../src/lib/dataframe/expr.ts";
import { readFileSync } from "fs";

const CSV_PATH = "/tmp/benchmark_1m.csv";
const WARMUP = 1;
const RUNS = 5;

async function bench(name: string, fn: () => unknown | Promise<unknown>) {
  for (let i = 0; i < WARMUP; i++) await fn();

  const times: number[] = [];
  for (let i = 0; i < RUNS; i++) {
    const start = performance.now();
    await fn();
    times.push(performance.now() - start);
  }

  const avg = times.reduce((a, b) => a + b, 0) / times.length;
  const min = Math.min(...times);
  console.log(`  ${name}: ${avg.toFixed(1)}ms avg, ${min.toFixed(1)}ms min`);
  return avg;
}

// --- CSV Load ---
console.log("=".repeat(60));
console.log("Promin DataFrame (ArrayExecutor)");
console.log("=".repeat(60));

console.log("\n--- CSV Load ---");
await bench("CsvFile.load()", () => CsvFile(CSV_PATH).load());

// Pre-load for subsequent benchmarks
const data = (await CsvFile(CSV_PATH).load()) as any[];
const df = DataFrame.fromArray(data);

console.log("\n--- Filter (revenue > 5000) ---");
await bench("df.filter (JS fn)", () => df.filter((r: any) => r.revenue > 5000).collect());
await bench("df.filter (Expr)", () => df.filter(col("revenue").gt(5000)).collect());

console.log("\n--- GroupBy + Sum ---");
await bench("df.groupBy.agg", () =>
  df
    .groupBy("region" as any)
    .agg({ revenue: "sum" } as any)
    .collect());

console.log("\n--- GroupBy + Multiple Agg ---");
// ArrayExecutor only supports one agg per column — chain them
await bench("df.groupBy.agg (sum)", () =>
  df
    .groupBy("region" as any)
    .agg({ revenue: "sum" } as any)
    .collect());

console.log("\n--- Sort + Limit 10 ---");
await bench("df.sort.limit", () =>
  df
    .sort("revenue" as any, "desc")
    .limit(10)
    .collect());

console.log("\n--- Filter + GroupBy + Sort ---");
await bench("df.chained (JS fn)", () =>
  df
    .filter((r: any) => r.status === "active")
    .groupBy("region" as any)
    .agg({ revenue: "sum" } as any)
    .sort("revenue_sum" as any, "desc")
    .collect());
await bench("df.chained (Expr)", () =>
  df
    .filter(col("status").eq("active"))
    .groupBy("region" as any)
    .agg({ revenue: "sum" } as any)
    .sort("revenue_sum" as any, "desc")
    .collect());

console.log("\n--- Distinct regions ---");
await bench("df.select.distinct", () =>
  df
    .select("region" as any)
    .distinct()
    .collect());

// --- DuckDB executor ---
try {
  const { DuckDBExecutor } = await import("../../../duckdb/src/lib/duckdb-executor.ts");
  const duckdb = new DuckDBExecutor();

  console.log();
  console.log("=".repeat(60));
  console.log("Promin DataFrame (DuckDBExecutor)");
  console.log("=".repeat(60));

  // Load from file — DuckDB reads CSV natively
  const ddf = DataFrame.fromFile(CsvFile(CSV_PATH)).withExecutor(duckdb);

  console.log("\n--- CSV Load (native read_csv_auto) ---");
  await bench("duckdb read_csv + collect", () =>
    DataFrame.fromFile(CsvFile(CSV_PATH)).withExecutor(new DuckDBExecutor()).collect());

  // Pre-warm cache
  await ddf.collect();

  console.log("\n--- Filter (revenue > 5000) — Expr pushdown ---");
  await bench("duckdb filter (Expr→SQL)", () => ddf.filter(col("revenue").gt(5000)).collect());

  console.log("\n--- GroupBy + Sum ---");
  await bench("duckdb groupby", () =>
    ddf
      .groupBy("region" as any)
      .agg({ revenue: "sum" } as any)
      .collect());

  console.log("\n--- Sort + Limit 10 ---");
  await bench("duckdb sort+limit", () =>
    ddf
      .sort("revenue" as any, "desc")
      .limit(10)
      .collect());

  console.log("\n--- Filter + GroupBy + Sort (Expr) ---");
  await bench("duckdb chained (Expr)", () =>
    ddf
      .filter(col("status").eq("active"))
      .groupBy("region" as any)
      .agg({ revenue: "sum" } as any)
      .sort("revenue_sum" as any, "desc")
      .collect());

  console.log("\n--- Distinct regions ---");
  await bench("duckdb distinct", () =>
    ddf
      .select("region" as any)
      .distinct()
      .collect());
} catch (e) {
  console.log("\nDuckDB not available:", (e as Error).message);
}

process.exit(0);
