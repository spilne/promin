// ---------------------------------------------------------------------------
// SqlModel — a single SQL transformation in a DAG
// ---------------------------------------------------------------------------

/**
 * How a model's SELECT is materialized:
 * - `table` drops and re-creates the table on every run.
 * - `view` drops and re-creates the view.
 * - `incremental` creates the table on the first run, then inserts only the
 *   rows whose `uniqueKey` isn't in the table yet.
 */
export type Materialization = "table" | "view" | "incremental";

/** A data test run against a model after it is materialized. */
export interface SqlExpectationDef {
  type: "not_null" | "unique" | "between" | "row_count" | "custom";
  /** Column under test. Required for `not_null`, `unique` and `between`. */
  column?: string;
  /** Lower bound: `between` (default 0) and `row_count`. */
  min?: number;
  /** Upper bound: `between` (default unbounded) and `row_count`. */
  max?: number;
  /**
   * `custom` only: a query whose first row's first column is truthy when
   * the expectation holds, e.g. `SELECT COUNT(*) = 0 FROM fct WHERE total < 0`.
   */
  check?: string;
  /**
   * `"error"` (default): a failing test fails the model's step, so its
   * dependants don't run on bad data and the step's retry policy applies.
   * `"warn"`: the failure is recorded in the result and the run continues.
   */
  severity?: "error" | "warn";
}

export interface SqlModel {
  /** Model name — used as table/view name and in dependsOn references. */
  name: string;
  /** SQL SELECT statement. References to other models use the model name directly. */
  sql: string;
  /** Models this depends on (resolved at compile time). */
  dependsOn: string[];
  /** How to materialize the result. */
  materialization: Materialization;
  /**
   * `incremental` only (and required there): the column that identifies a
   * row. A run inserts the selected rows whose key isn't in the table yet.
   */
  uniqueKey?: string;
  /** Expected output schema (for documentation). */
  schema?: Record<string, string>;
  /** Data quality tests to run after materialization. */
  tests?: SqlExpectationDef[];
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

/** What one model's step returns. */
export interface SqlModelRunResult {
  model: string;
  materialization: Materialization;
  testsRun: number;
  testsPassed: number;
  /** Failed `severity: "warn"` tests. */
  warnings: string[];
  /** Epoch ms when the model started and finished, from the compiler's clock. */
  startedAt: number;
  finishedAt: number;
}

/** The workflow's output: every model materialized and every error-severity test passed. */
export interface SqlProjectResult {
  models: SqlModelRunResult[];
  modelsRun: number;
  testsRun: number;
  testsPassed: number;
  /** Failed `severity: "warn"` tests, across all models. */
  testsWarned: number;
  warnings: { model: string; message: string }[];
  /** From the first model's start to the last model's finish. */
  durationMs: number;
}
