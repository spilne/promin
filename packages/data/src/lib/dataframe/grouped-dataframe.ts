// ---------------------------------------------------------------------------
// GroupedDataFrame — result of .groupBy(), supports .agg()
// ---------------------------------------------------------------------------

import type { AggFn, ExprAgg, LogicalPlan } from "./logical-plan.ts";
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
   *
   * A key naming a non-grouped column takes any {@link AggFn}; any other
   * literal key is a new output column and must be an {@link ExprAgg}
   * (`exprAgg`). A `Record<string, AggFn>` built at runtime is not checked.
   */
  agg<Aggs extends Record<string, AggFn>>(
    aggs: Aggs & {
      [P in keyof Aggs]: P extends Exclude<keyof T, K> ? AggFn : string extends P ? AggFn : ExprAgg;
    },
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
