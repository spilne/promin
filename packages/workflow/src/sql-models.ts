// ---------------------------------------------------------------------------
// @promin/workflow/sql-models — dbt-style SQL model DAGs as workflows.
//
// `compileSqlProject` turns a project of SQL models into a workflow: one
// step per model in dependency order, each materializing its table or view
// and running its data tests, plus a final step that sums up the run.
// ---------------------------------------------------------------------------

export type {
  SqlModel,
  SqlProject,
  SqlProjectResult,
  SqlModelRunResult,
  Materialization,
  SqlExpectationDef,
} from "./lib/sql-models/sql-model.ts";
export {
  compileSqlProject,
  SqlModelError,
  type SqlCompilerConfig,
  type SqlProjectInput,
} from "./lib/sql-models/sql-compiler.ts";
