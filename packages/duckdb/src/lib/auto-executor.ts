// ---------------------------------------------------------------------------
// AutoExecutor — picks the best executor based on data size and plan
//
// Rules:
//   - Source with hint (file-backed) + DuckDB available → DuckDB (native file reading)
//   - Source data > threshold rows → DuckDB (better for large analytical queries)
//   - Otherwise → ArrayExecutor (zero overhead for small data)
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
    // File-backed source with hint → DuckDB (native reading)
    const source = findSource(plan);
    if (source?.hint) {
      return this.duckdbExecutor;
    }

    // Large data → DuckDB
    if (source && source.data.length > this.threshold) {
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
