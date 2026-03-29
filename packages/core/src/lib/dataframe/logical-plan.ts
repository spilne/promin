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
  | ReversePlan;

export interface SourcePlan {
  readonly _tag: "Source";
  readonly data: unknown[];
  /**
   * Optional async loader — when the DataFrame was created from a Frameable source.
   * The executor calls this to get data if `data` is empty.
   */
  readonly load?: () => Promise<unknown[]>;
  /**
   * Optional DuckDB hint — SQL expression to load data natively.
   * Example: `"read_csv_auto('/path/to/file.csv')"` or `"read_parquet('/path/to/file.parquet')"`
   * DuckDB executor uses this instead of load(). Other executors ignore it and call load().
   */
  readonly duckdbSql?: string;
}

export interface FilterPlan {
  readonly _tag: "Filter";
  readonly input: LogicalPlan;
  readonly fn: (row: any) => boolean;
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
}

export interface SortPlan {
  readonly _tag: "Sort";
  readonly input: LogicalPlan;
  readonly by: string;
  readonly order: "asc" | "desc";
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

export type AggFn = "sum" | "count" | "avg" | "min" | "max" | "first" | "last" | "collect";

export interface JoinPlan {
  readonly _tag: "Join";
  readonly left: LogicalPlan;
  readonly right: LogicalPlan;
  readonly on: string;
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
