/**
 * Run all cross-language benchmarks and generate a markdown report.
 * Output: /tmp/benchmark-report.md (also printed to stdout)
 *
 * Usage:
 *   bun run benchmarks/cross-language/run-all.ts
 *
 * CI usage:
 *   bun run benchmarks/cross-language/run-all.ts >> $GITHUB_STEP_SUMMARY
 */

import { DataFrame, CsvFile, col } from "@promin/core";
import { writeFileSync } from "fs";
import { execSync } from "child_process";

const CSV_PATH = "/tmp/benchmark_1m.csv";
const WARMUP = 2;
const RUNS = 5;

// ---------------------------------------------------------------------------
// Generate data if not present
// ---------------------------------------------------------------------------

try {
  const { statSync } = await import("fs");
  statSync(CSV_PATH);
} catch {
  console.error("Generating 1M row CSV...");
  execSync(`bun run ${import.meta.dir}/generate-data.ts`, { stdio: "inherit" });
}

// ---------------------------------------------------------------------------
// Bench helper
// ---------------------------------------------------------------------------

type Result = { name: string; avgMs: number; minMs: number };

async function bench(name: string, fn: () => unknown | Promise<unknown>): Promise<Result> {
  for (let i = 0; i < WARMUP; i++) await fn();
  const times: number[] = [];
  for (let i = 0; i < RUNS; i++) {
    const start = performance.now();
    await fn();
    times.push(performance.now() - start);
  }
  const avgMs = times.reduce((a, b) => a + b, 0) / times.length;
  const minMs = Math.min(...times);
  return { name, avgMs, minMs };
}

// ---------------------------------------------------------------------------
// Run Promin benchmarks
// ---------------------------------------------------------------------------

const data = await CsvFile(CSV_PATH).load() as any[];
const df = DataFrame.fromArray(data);

const promin: Result[] = [];

promin.push(await bench("CSV Load", () => CsvFile(CSV_PATH).load()));
promin.push(await bench("Filter (revenue > 5000)", () => df.filter(col("revenue").gt(5000)).collect()));
promin.push(await bench("GroupBy + Sum", () => df.groupBy("region" as any).agg({ revenue: "sum" } as any).collect()));
promin.push(await bench("Sort (full)", () => df.sort("revenue" as any, "desc").collect()));
promin.push(await bench("Sort + Limit 10", () => df.sort("revenue" as any, "desc").limit(10).collect()));
promin.push(await bench("Chained (filter+groupBy+sort)", () =>
  df.filter(col("status").eq("active")).groupBy("region" as any).agg({ revenue: "sum" } as any).sort("revenue" as any, "desc").collect(),
));
promin.push(await bench("Distinct", () => df.select("region" as any).distinct().collect()));

// ---------------------------------------------------------------------------
// Run Promin AutoExecutor benchmarks
// ---------------------------------------------------------------------------

let autoResults: Result[] = [];
try {
  const { AutoExecutor } = await import("@promin/duckdb");
  const auto = new AutoExecutor({ threshold: 10_000 });
  const adf = DataFrame.fromArray(data).withExecutor(auto);
  const adfFile = DataFrame.fromFile(CsvFile(CSV_PATH)).withExecutor(auto);

  autoResults.push(await bench("CSV Load (file)", () => DataFrame.fromFile(CsvFile(CSV_PATH)).withExecutor(new (require("@promin/duckdb").AutoExecutor)()).collect()));
  autoResults.push(await bench("Filter (revenue > 5000)", () => adf.filter(col("revenue").gt(5000)).collect()));
  autoResults.push(await bench("GroupBy + Sum", () => adf.groupBy("region" as any).agg({ revenue: "sum" } as any).collect()));
  autoResults.push(await bench("Sort (full)", () => adf.sort("revenue" as any, "desc").collect()));
  autoResults.push(await bench("Sort + Limit 10", () => adf.sort("revenue" as any, "desc").limit(10).collect()));
  autoResults.push(await bench("Chained (filter+groupBy+sort)", () =>
    adf.filter(col("status").eq("active")).groupBy("region" as any).agg({ revenue: "sum" } as any).sort("revenue" as any, "desc").collect(),
  ));
  autoResults.push(await bench("Distinct", () => adf.select("region" as any).distinct().collect()));
} catch (e) {
  console.error("AutoExecutor not available:", (e as Error).message?.slice(0, 100));
}

// ---------------------------------------------------------------------------
// Run Python benchmarks (Pandas + Polars)
// ---------------------------------------------------------------------------

type PyResult = { name: string; avgMs: number };

function runPython(): { pandas: PyResult[]; polars: PyResult[] } {
  try {
    const output = execSync(
      `uv run ${import.meta.dir}/bench-pandas-polars-json.py`,
      { encoding: "utf-8", timeout: 120_000 },
    );
    return JSON.parse(output);
  } catch (e) {
    console.error("Python benchmarks failed:", (e as Error).message?.slice(0, 200));
    return { pandas: [], polars: [] };
  }
}

// Write a JSON-output version of the Python script
writeFileSync(`${import.meta.dir}/bench-pandas-polars-json.py`, `# /// script
# requires-python = ">=3.11"
# dependencies = ["pandas", "polars"]
# ///
import time, json, pandas as pd, polars as pl

CSV = "${CSV_PATH}"
W, R = 2, 5

def b(fn):
    for _ in range(W): fn()
    ts = []
    for _ in range(R):
        s = time.perf_counter()
        fn()
        ts.append((time.perf_counter() - s) * 1000)
    return sum(ts) / len(ts)

pdf = pd.read_csv(CSV)
plf = pl.read_csv(CSV)

pandas = [
    {"name": "CSV Load", "avgMs": b(lambda: pd.read_csv(CSV))},
    {"name": "Filter (revenue > 5000)", "avgMs": b(lambda: pdf[pdf["revenue"] > 5000])},
    {"name": "GroupBy + Sum", "avgMs": b(lambda: pdf.groupby("region")["revenue"].sum())},
    {"name": "Sort (full)", "avgMs": b(lambda: pdf.sort_values("revenue", ascending=False))},
    {"name": "Sort + Limit 10", "avgMs": b(lambda: pdf.sort_values("revenue", ascending=False).head(10))},
    {"name": "Chained (filter+groupBy+sort)", "avgMs": b(lambda: pdf[pdf["status"] == "active"].groupby("region")["revenue"].sum().sort_values(ascending=False))},
    {"name": "Distinct", "avgMs": b(lambda: pdf["region"].unique())},
]

polars = [
    {"name": "CSV Load", "avgMs": b(lambda: pl.read_csv(CSV))},
    {"name": "Filter (revenue > 5000)", "avgMs": b(lambda: plf.filter(pl.col("revenue") > 5000))},
    {"name": "GroupBy + Sum", "avgMs": b(lambda: plf.group_by("region").agg(pl.col("revenue").sum()))},
    {"name": "Sort (full)", "avgMs": b(lambda: plf.sort("revenue", descending=True))},
    {"name": "Sort + Limit 10", "avgMs": b(lambda: plf.sort("revenue", descending=True).head(10))},
    {"name": "Chained (filter+groupBy+sort)", "avgMs": b(lambda: plf.filter(pl.col("status") == "active").group_by("region").agg(pl.col("revenue").sum()).sort("revenue", descending=True))},
    {"name": "Distinct", "avgMs": b(lambda: plf.select("region").unique())},
]

print(json.dumps({"pandas": pandas, "polars": polars}))
`);

console.error("Running Pandas + Polars benchmarks...");
const py = runPython();

// ---------------------------------------------------------------------------
// Generate report
// ---------------------------------------------------------------------------

function fmt(ms: number): string {
  if (ms < 1) return `${(ms * 1000).toFixed(0)}µs`;
  if (ms < 1000) return `${ms.toFixed(1)}ms`;
  return `${(ms / 1000).toFixed(2)}s`;
}

/** Compare Promin vs other: promin faster → bold, promin slower → plain */
function vsOther(prominMs: number, otherMs: number): string {
  if (otherMs === 0) return "—";
  const r = otherMs / prominMs;
  if (r > 1.1) return `**${r.toFixed(1)}x faster**`;
  if (r < 0.9) return `${(1 / r).toFixed(1)}x slower`;
  return "~same";
}

const lines: string[] = [];
lines.push("## Cross-Language Benchmark Report");
lines.push("");
lines.push(`**Dataset**: 1M rows, 9 columns, ${(59).toFixed(0)}MB CSV`);
lines.push(`**Machine**: ${process.arch}, Bun ${process.versions?.bun ?? "?"}`);
lines.push(`**Date**: ${new Date().toISOString().slice(0, 10)}`);
lines.push("");
const hasAuto = autoResults.length > 0;
if (hasAuto) {
  lines.push("| Operation | Polars | Promin (Array) | Promin (Auto) | Pandas | vs Pandas | vs Polars |");
  lines.push("|---|---|---|---|---|---|---|");
} else {
  lines.push("| Operation | Polars | Promin | Pandas | vs Pandas | vs Polars |");
  lines.push("|---|---|---|---|---|---|");
}

for (let i = 0; i < promin.length; i++) {
  const p = promin[i]!;
  const a = autoResults[i];
  const pandas = py.pandas[i];
  const polars = py.polars[i];
  const pdMs = pandas?.avgMs ?? 0;
  const plMs = polars?.avgMs ?? 0;
  // Use the best Promin result for comparison
  const bestMs = a ? Math.min(p.avgMs, a.avgMs) : p.avgMs;

  if (hasAuto) {
    lines.push(
      `| ${p.name} | ${plMs ? fmt(plMs) : "—"} | ${fmt(p.avgMs)} | ${a ? fmt(a.avgMs) : "—"} | ${pdMs ? fmt(pdMs) : "—"} | ${pdMs ? vsOther(bestMs, pdMs) : "—"} | ${plMs ? vsOther(bestMs, plMs) : "—"} |`,
    );
  } else {
    lines.push(
      `| ${p.name} | ${plMs ? fmt(plMs) : "—"} | ${fmt(p.avgMs)} | ${pdMs ? fmt(pdMs) : "—"} | ${pdMs ? vsOther(p.avgMs, pdMs) : "—"} | ${plMs ? vsOther(p.avgMs, plMs) : "—"} |`,
    );
  }
}

lines.push("");
lines.push("*Lower is better. Bold = Promin wins.*");

const report = lines.join("\n");
console.log(report);
writeFileSync("/tmp/benchmark-report.md", report);
console.error("\nReport saved to /tmp/benchmark-report.md");

process.exit(0);
