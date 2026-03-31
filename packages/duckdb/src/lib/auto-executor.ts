// ---------------------------------------------------------------------------
// AutoExecutor — picks the best executor based on data size and plan
//
// Decision rules (in priority order):
//   1. File-backed source (has hint)          → DuckDB (native reading)
//   2. Small data (< 1K rows)                 → Array (overhead dominates)
//   3. Analytical ops + large data            → DuckDB (groupBy, window, distinct, union)
//   4. Sort + Limit + large data              → DuckDB (top-N heap, 154x faster at 1M)
//   5. Join + large data                      → DuckDB (SQL join optimizer)
//   6. Only pass-through ops (filter/map/...) → Array (DuckDB falls back to JS anyway)
//   7. Large data + non-trivial ops           → DuckDB
//   8. Default                                → Array
//
// Usage:
//   const executor = new AutoExecutor();
//   const df = DataFrame.fromArray(data).withExecutor(executor);
//   // Automatically picks Array for small data, DuckDB for large
// ---------------------------------------------------------------------------

import type { DataFrameExecutor, ExecutionCost, LogicalPlan } from "@promin/core";
import { ArrayExecutor } from "@promin/core";
import { DuckDBExecutor } from "./duckdb-executor.ts";

/**
 * Automatically selects ArrayExecutor or DuckDBExecutor based on data size
 * and source type. File-backed sources (with hints) prefer DuckDB for native reading.
 *
 * @example
 * ```ts
 * const executor = new AutoExecutor();
 *
 * // Small data → ArrayExecutor
 * DataFrame.fromArray(smallData).withExecutor(executor)
 *
 * // Large data → DuckDBExecutor
 * DataFrame.fromArray(bigData).withExecutor(executor)
 *
 * // File source → DuckDBExecutor (native reader)
 * DataFrame.fromFile(ParquetFile("logs.parquet")).withExecutor(executor)
 * ```
 */
export class AutoExecutor implements DataFrameExecutor {
  private arrayExecutor = new ArrayExecutor();
  private duckdbExecutor: DuckDBExecutor;
  private threshold: number;

  constructor(params?: { threshold?: number }) {
    this.threshold = params?.threshold ?? 10_000;
    this.duckdbExecutor = new DuckDBExecutor();
  }

  /**
   * Register a native loader on the underlying DuckDB executor.
   * @see DuckDBExecutor.registerLoader
   */
  registerLoader(format: string, toSql: (path: string) => string): this {
    this.duckdbExecutor.registerLoader(format, toSql);
    return this;
  }

  async execute<T>(plan: LogicalPlan): Promise<T[]> {
    const executor = this.selectExecutor(plan);
    return executor.execute<T>(plan);
  }

  executeSync?<T>(plan: LogicalPlan): T[] {
    // Sync only works with ArrayExecutor
    return this.arrayExecutor.executeSync!(plan);
  }

  supports(plan: LogicalPlan): boolean {
    return true;
  }

  estimateCost(plan: LogicalPlan): ExecutionCost {
    const executor = this.selectExecutor(plan);
    return executor.estimateCost(plan);
  }

  private selectExecutor(plan: LogicalPlan): DataFrameExecutor {
    const source = findSource(plan);
    const rowCount = source?.data.length ?? 0;
    const hasHint = !!source?.hint;
    const ops = collectOps(plan);

    // Rule 1: File-backed source → DuckDB (native reading, no JS parsing)
    if (hasHint) {
      return this.duckdbExecutor;
    }

    // Rule 2: Small data → Array (always, overhead dominates)
    if (rowCount < 1_000 && !hasHint) {
      return this.arrayExecutor;
    }

    // Rule 3: Has analytical ops (groupBy, window, distinct, union) → DuckDB
    // These are DuckDB's sweet spot — columnar engine with SQL optimizer
    const analyticalOps = new Set(["GroupBy", "Window", "Distinct", "Union"]);
    const hasAnalytical = ops.some((op) => analyticalOps.has(op));
    if (hasAnalytical && rowCount > this.threshold) {
      return this.duckdbExecutor;
    }

    // Rule 4: Sort + Limit → DuckDB (top-N heap, 154x faster at 1M rows)
    const hasSort = ops.includes("Sort");
    const hasLimit = ops.includes("Limit");
    if (hasSort && hasLimit && rowCount > this.threshold) {
      return this.duckdbExecutor;
    }

    // Rule 5: Join → DuckDB for large data (SQL join optimizer)
    if (ops.includes("Join") && rowCount > this.threshold) {
      return this.duckdbExecutor;
    }

    // Rule 6: Only pass-through ops (filter, map, withColumn, select, drop) → Array
    // DuckDB falls back to JS for these anyway, so Array is faster
    const passThrough = new Set([
      "Source",
      "Filter",
      "Map",
      "WithColumn",
      "Select",
      "Drop",
      "Rename",
      "Limit",
      "Offset",
      "Slice",
      "Reverse",
      "Sort",
    ]);
    const allPassThrough = ops.every((op) => passThrough.has(op));
    if (allPassThrough) {
      return this.arrayExecutor;
    }

    // Rule 7: Large data with any non-trivial ops → DuckDB
    if (rowCount > this.threshold) {
      return this.duckdbExecutor;
    }

    // Default → Array
    return this.arrayExecutor;
  }
}

/** Walk the plan to find the root Source node. */
function findSource(plan: LogicalPlan): (LogicalPlan & { _tag: "Source" }) | null {
  if (plan._tag === "Source") return plan;
  if ("input" in plan) return findSource((plan as any).input);
  if ("left" in plan) return findSource((plan as any).left);
  if (plan._tag === "Concat" && plan.inputs.length > 0) return findSource(plan.inputs[0]!);
  return null;
}

/** Collect all operation types in the plan tree. */
function collectOps(plan: LogicalPlan): string[] {
  const ops: string[] = [plan._tag];
  if ("input" in plan && (plan as any).input) {
    ops.push(...collectOps((plan as any).input));
  }
  if ("left" in plan && (plan as any).left) {
    ops.push(...collectOps((plan as any).left));
  }
  if ("right" in plan && (plan as any).right) {
    ops.push(...collectOps((plan as any).right));
  }
  if (plan._tag === "Concat") {
    for (const input of plan.inputs) {
      ops.push(...collectOps(input));
    }
  }
  return ops;
}
