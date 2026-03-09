// ---------------------------------------------------------------------------
// GroupedDataFrame — result of .groupBy(), supports .agg()
// ---------------------------------------------------------------------------

import type { AggFn, LogicalPlan } from "./logical-plan.ts";
import type { DataFrameExecutor } from "./executor.ts";
import { DataFrame } from "./dataframe.ts";

export class GroupedDataFrame<T, K extends keyof T> {
  constructor(
    private readonly _plan: LogicalPlan,
    private readonly _columns: K[],
    private readonly _executor: DataFrameExecutor,
  ) {}

  /**
   * Aggregate grouped columns.
   *
   * @example
   * ```ts
   * df.groupBy("region")
   *   .agg({ revenue: "sum", orders: "count" })
   * ```
   */
  agg<Aggs extends Partial<Record<Exclude<keyof T, K>, AggFn>>>(
    aggs: Aggs,
  ): DataFrame<Pick<T, K> & Record<string, unknown>> {
    return DataFrame._fromPlan(
      {
        _tag: "GroupBy",
        input: this._plan,
        columns: this._columns as string[],
        aggs: aggs as Record<string, AggFn>,
      },
      this._executor,
    );
  }
}
