// ---------------------------------------------------------------------------
// Logical plan — operations recorded by DataFrame, executed by backends
// ---------------------------------------------------------------------------

export type LogicalPlan =
  | SourcePlan
  | FilterPlan
  | MapPlan
  | SelectPlan
  | DropPlan
  | RenamePlan
  | WithColumnPlan
  | SortPlan
  | LimitPlan
  | OffsetPlan
  | DistinctPlan
  | GroupByPlan
  | JoinPlan
  | SlicePlan
  | WindowPlan
  | PivotPlan
  | UnpivotPlan
  | ExplodePlan
  | RollingPlan
  | CumulativePlan
  | ConcatPlan
  | UnionPlan
  | ReversePlan
  | FillNullPlan;

export interface SourcePlan {
  readonly _tag: "Source";
  readonly data: unknown[];
  /**
   * Optional async loader — when the DataFrame was created from a Frameable source.
   * The executor calls this to get data if `data` is empty.
   */
  readonly load?: () => Promise<unknown[]>;
  /**
   * Optional streaming reader — yields rows without materializing the whole
   * source. The chunked executor prefers this over `load()` for bounded-memory
   * consumption; whole-file terminals like `.collect()` still fall back to
   * `load()` (or drain the stream if only `stream` is present).
   */
  readonly stream?: () => AsyncIterable<unknown>;
  /**
   * Optional source hint — format and location for executor-native loading.
   * Example: `"csv:/path/to/file.csv"`, `"parquet:/path/to/file.parquet"`
   *
   * Executors can register handlers for known hints via `registerLoader()`.
   * If no handler matches, the executor falls back to `load()`.
   * Core never interprets this — it's opaque to the plan.
   */
  readonly hint?: string;
}

export interface FilterPlan {
  readonly _tag: "Filter";
  readonly input: LogicalPlan;
  readonly fn: (row: any) => boolean;
  /** Optional: AST for SQL-compilable filters. Set when filter uses Expr instead of raw function. */
  readonly expr?: import("./expr.ts").ExprAst;
}

export interface MapPlan {
  readonly _tag: "Map";
  readonly input: LogicalPlan;
  readonly fn: (row: any) => any;
}

export interface SelectPlan {
  readonly _tag: "Select";
  readonly input: LogicalPlan;
  readonly columns: string[];
}

export interface DropPlan {
  readonly _tag: "Drop";
  readonly input: LogicalPlan;
  readonly columns: string[];
}

export interface RenamePlan {
  readonly _tag: "Rename";
  readonly input: LogicalPlan;
  readonly mapping: Record<string, string>;
}

export interface WithColumnPlan {
  readonly _tag: "WithColumn";
  readonly input: LogicalPlan;
  readonly name: string;
  readonly fn: (row: any) => any;
  /** Optional: AST for SQL-compilable expressions. */
  readonly expr?: import("./expr.ts").ExprAst;
}

export interface SortPlan {
  readonly _tag: "Sort";
  readonly input: LogicalPlan;
  readonly by: string | { column: string; order: "asc" | "desc" }[];
  readonly order: "asc" | "desc"; // used when by is string (backward compat)
}

export interface LimitPlan {
  readonly _tag: "Limit";
  readonly input: LogicalPlan;
  readonly n: number;
}

export interface OffsetPlan {
  readonly _tag: "Offset";
  readonly input: LogicalPlan;
  readonly n: number;
}

export interface DistinctPlan {
  readonly _tag: "Distinct";
  readonly input: LogicalPlan;
  readonly by?: string;
  readonly keep?: "first" | "last";
}

export interface GroupByPlan {
  readonly _tag: "GroupBy";
  readonly input: LogicalPlan;
  readonly columns: string[];
  readonly aggs: Record<string, AggFn>;
}

export interface CustomAgg<Acc = unknown, Result = unknown> {
  readonly _tag: "custom";
  readonly init: Acc;
  readonly accumulate: (acc: Acc, value: unknown) => Acc;
  readonly finalize: (acc: Acc) => Result;
}

export interface ExprAgg {
  readonly _tag: "expr";
  readonly expr: import("./expr.ts").Expr;
  readonly agg: Exclude<AggFn, ExprAgg>;
  readonly filter?: import("./expr.ts").Expr;
}

export type AggFn =
  | "sum"
  | "count"
  | "avg"
  | "min"
  | "max"
  | "first"
  | "last"
  | "collect"
  | "median"
  | "stddev"
  | "variance"
  | "mode"
  | "countDistinct"
  | CustomAgg
  | ExprAgg;

export function percentile(q: number): CustomAgg<number[], number> {
  return {
    _tag: "custom",
    init: [],
    accumulate: (acc, val) => {
      acc.push(Number(val));
      return acc;
    },
    finalize: (acc) => {
      acc.sort((a, b) => a - b);
      const idx = Math.ceil(q * acc.length) - 1;
      return acc[Math.max(0, idx)]!;
    },
  };
}

export function exprAgg(params: {
  expr: import("./expr.ts").Expr;
  agg: Exclude<AggFn, ExprAgg>;
  filter?: import("./expr.ts").Expr;
}): ExprAgg {
  return { _tag: "expr", expr: params.expr, agg: params.agg, filter: params.filter };
}

export function reduce<Acc, Result>(
  init: Acc,
  accumulate: (acc: Acc, value: unknown) => Acc,
  finalize?: (acc: Acc) => Result,
): CustomAgg<Acc, Result> {
  return {
    _tag: "custom",
    init,
    accumulate,
    finalize: finalize ?? ((acc) => acc as unknown as Result),
  };
}

export interface JoinPlan {
  readonly _tag: "Join";
  readonly left: LogicalPlan;
  readonly right: LogicalPlan;
  readonly on: string | string[];
  readonly type: "inner" | "left" | "right" | "full" | "semi" | "anti";
}

export interface SlicePlan {
  readonly _tag: "Slice";
  readonly input: LogicalPlan;
  readonly start: number;
  readonly end?: number;
}

export type WindowFn =
  | "row_number"
  | "rank"
  | "dense_rank"
  | "lag"
  | "lead"
  | "sum"
  | "avg"
  | "min"
  | "max"
  | "running_total"
  | "first"
  | "last"
  | "ntile";

export interface WindowPlan {
  readonly _tag: "Window";
  readonly input: LogicalPlan;
  readonly name: string;
  readonly partitionBy?: string;
  readonly orderBy: string;
  readonly fn: WindowFn;
  readonly args?: { offset?: number; n?: number; default?: unknown };
}

export interface PivotPlan {
  readonly _tag: "Pivot";
  readonly input: LogicalPlan;
  readonly index: string;
  readonly columns: string;
  readonly values: string;
  readonly agg: AggFn;
}

export interface UnpivotPlan {
  readonly _tag: "Unpivot";
  readonly input: LogicalPlan;
  readonly id: string;
  readonly columns: string[];
}

export interface ExplodePlan {
  readonly _tag: "Explode";
  readonly input: LogicalPlan;
  readonly column: string;
}

export type RollingFn = "mean" | "sum" | "min" | "max" | "std";

export interface RollingPlan {
  readonly _tag: "Rolling";
  readonly input: LogicalPlan;
  readonly column: string;
  readonly window: number;
  readonly fn: RollingFn;
  readonly outputName: string;
}

export type CumulativeFn = "sum" | "prod" | "min" | "max" | "pctChange";

export interface CumulativePlan {
  readonly _tag: "Cumulative";
  readonly input: LogicalPlan;
  readonly column: string;
  readonly fn: CumulativeFn;
  readonly outputName: string;
}

export interface ConcatPlan {
  readonly _tag: "Concat";
  readonly inputs: LogicalPlan[];
}

export interface UnionPlan {
  readonly _tag: "Union";
  readonly left: LogicalPlan;
  readonly right: LogicalPlan;
}

export interface ReversePlan {
  readonly _tag: "Reverse";
  readonly input: LogicalPlan;
}

export interface FillNullPlan {
  readonly _tag: "FillNull";
  readonly input: LogicalPlan;
  readonly column: string;
  readonly method: "value" | "forward" | "backward";
  readonly value?: unknown;
}
