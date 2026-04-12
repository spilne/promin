// ---------------------------------------------------------------------------
// SqlModel — a single SQL transformation in a DAG
// ---------------------------------------------------------------------------

export type Materialization = "table" | "view" | "incremental" | "ephemeral";

export interface ExpectationDef {
  type: "not_null" | "unique" | "between" | "row_count" | "custom";
  column?: string;
  min?: number;
  max?: number;
  check?: string;
}

export interface SqlModel {
  /** Model name — used as table/view name and in dependsOn references. */
  name: string;
  /** SQL SELECT statement. References to other models use the model name directly. */
  sql: string;
  /** Models this depends on (resolved at compile time). */
  dependsOn: string[];
  /** How to materialize the result. Default: "table". */
  materialization: Materialization;
  /** Expected output schema (for documentation). */
  schema?: Record<string, string>;
  /** Data quality tests to run after materialization. */
  tests?: ExpectationDef[];
  /** Human-readable description. */
  description?: string;
  /** Tags for filtering (e.g. "staging", "mart", "daily"). */
  tags?: string[];
}

export interface SqlProject {
  /** Project name. */
  name: string;
  /** Models forming the DAG. */
  models: SqlModel[];
  /** Source tables — raw data that models read from. */
  sources?: Record<string, { schema: string; tables: string[] }>;
}

export interface SqlProjectResult {
  modelsRun: number;
  modelsSucceeded: number;
  modelsFailed: number;
  testsRun: number;
  testsPassed: number;
  testsFailed: number;
  errors: { model: string; error: string }[];
  durationMs: number;
}
