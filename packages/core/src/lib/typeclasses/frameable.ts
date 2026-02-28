// ---------------------------------------------------------------------------
// Frameable<T> — "I can produce a bounded dataset of T"
// Base capability for DataFrame sources.
// ---------------------------------------------------------------------------

import type { Codec } from "./codec.ts";

export interface FrameSchema {
  readonly columns: readonly {
    readonly name: string;
    readonly type: "string" | "number" | "boolean" | "date" | "json" | "array" | "unknown";
  }[];
}

export interface Frameable<T> {
  load(): Promise<T[]>;
  schema: FrameSchema;
  codec: Codec<T>;
  estimatedRowCount?: number;
}

export function isFrameable<T>(value: unknown): value is Frameable<T> {
  return (
    value !== null &&
    typeof value === "object" &&
    "load" in value &&
    typeof (value as any).load === "function" &&
    "schema" in value &&
    "codec" in value
  );
}

// ---------------------------------------------------------------------------
// PushdownFilterable<T> — "I can filter at the source level"
// ---------------------------------------------------------------------------

export interface Predicate {
  readonly column: string;
  readonly op:
    | "eq"
    | "neq"
    | "gt"
    | "gte"
    | "lt"
    | "lte"
    | "in"
    | "not_in"
    | "is_null"
    | "is_not_null"
    | "like";
  readonly value?: unknown;
}

export interface PushdownFilterable<T> extends Frameable<T> {
  loadFiltered(predicates: Predicate[]): Promise<T[]>;
}

export function isPushdownFilterable<T>(value: unknown): value is PushdownFilterable<T> {
  return (
    isFrameable(value) &&
    "loadFiltered" in value &&
    typeof (value as any).loadFiltered === "function"
  );
}

// ---------------------------------------------------------------------------
// ColumnSelectable<T> — "I can select specific columns at source level"
// ---------------------------------------------------------------------------

export interface ColumnSelectable<T> extends Frameable<T> {
  loadColumns(columns: string[]): Promise<Partial<T>[]>;
}

export function isColumnSelectable<T>(value: unknown): value is ColumnSelectable<T> {
  return (
    isFrameable(value) && "loadColumns" in value && typeof (value as any).loadColumns === "function"
  );
}

// ---------------------------------------------------------------------------
// SourceSortable<T> — "I can sort at the source level"
// ---------------------------------------------------------------------------

export interface SourceSortable<T> extends Frameable<T> {
  loadSorted(column: string, order: "asc" | "desc"): Promise<T[]>;
}

export function isSourceSortable<T>(value: unknown): value is SourceSortable<T> {
  return (
    isFrameable(value) && "loadSorted" in value && typeof (value as any).loadSorted === "function"
  );
}
