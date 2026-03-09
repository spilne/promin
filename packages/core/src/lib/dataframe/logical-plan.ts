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
  | SlicePlan;

export interface SourcePlan {
  readonly _tag: "Source";
  readonly data: unknown[];
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
