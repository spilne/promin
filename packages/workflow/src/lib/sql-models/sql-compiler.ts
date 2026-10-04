// ---------------------------------------------------------------------------
// compileSqlProject — SqlProject → Workflow
//
// Each model becomes a workflow step that materializes it and runs its data
// tests; dependencies come from `dependsOn`, so independent models run in
// parallel. A final step sums the model results up into a
// `SqlProjectResult`. A model whose materialization or error-severity test
// fails fails its step with `SqlModelError`: its dependants don't run, the
// step's retry policy applies and the run ends failed.
// ---------------------------------------------------------------------------

import { TaggedError, succeed, tryPromise, type Eff, type Throws } from "@spilne/perfect-core";
import { workflow } from "../durable/workflow-builder.ts";
import type { Workflow } from "../durable/workflow-types.ts";
import type { StepOptions } from "../durable/step-definition.ts";
import { topologicalSort } from "../durable/workflow-dag.ts";
import { SystemWallClock, type WallClock } from "../shared/wall-clock.ts";
import type {
  SqlProject,
  SqlModel,
  SqlProjectResult,
  SqlModelRunResult,
  SqlExpectationDef,
} from "./sql-model.ts";

/** A model failed to materialize, or one of its error-severity tests failed. */
export class SqlModelError extends TaggedError("SqlModelError")<{
  readonly model: string;
  readonly failures: readonly string[];
  readonly message: string;
}>() {}

/** The compiled workflow's input. */
export interface SqlProjectInput {
  readonly date?: string;
}

export interface SqlCompilerConfig {
  /** The SQL project to compile. */
  project: SqlProject;
  /**
   * Execute a SQL statement. This is the bridge to your database.
   * Return rows for SELECT, empty array for DDL.
   */
  executeSql: (sql: string) => Promise<unknown[]>;
  /** Step options (retry, timeout, …) applied to every model step. */
  modelStepOptions?: StepOptions<SqlModelError>;
  /** Time source for the model timings. Default: `SystemWallClock`. */
  clock?: WallClock;
}

/** Name of the final step that aggregates the model results. */
const RESULT_STEP = "sql-project:result";

/** Table, view, column and schema-qualified names the compiler interpolates. */
const IDENTIFIER = /^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*$/;

type ModelStepEff = Eff<SqlModelRunResult, Throws<SqlModelError>>;

/**
 * The slice of the workflow builder the compiler uses. Model steps are added
 * in a loop, so the builder's per-step type accumulation can't be tracked.
 */
interface ModelStepBuilder {
  step(
    name: string,
    config: { dependsOn: string[] },
    fn: (ctx: { deps: Record<string, unknown> }) => Eff<unknown, Throws<SqlModelError>>,
    options?: StepOptions<SqlModelError>,
  ): ModelStepBuilder;
  build(): Workflow<SqlProjectInput, SqlProjectResult>;
}

/**
 * Compile a SqlProject into a Workflow.
 *
 * Each model becomes a workflow step that:
 * 1. Drops existing table/view (for table/view materialization), or creates
 *    the table once (incremental)
 * 2. Creates the table/view from the model's SQL, or inserts the new rows
 * 3. Runs data quality tests (if defined)
 *
 * Models run in DAG order — dependencies execute first. Independent models
 * run in parallel. Throws when the project is invalid: a dependency cycle or
 * unknown dependency, a model or column name that isn't a plain SQL
 * identifier, an incremental model without `uniqueKey`, or a test missing
 * its `column` / `check`.
 */
export function compileSqlProject(
  config: SqlCompilerConfig,
): Workflow<SqlProjectInput, SqlProjectResult> {
  const { project, executeSql } = config;
  const clock = config.clock ?? SystemWallClock;
  validateProject(project);

  const sorted = topologicalSort({
    nodes: project.models.map((m) => ({ name: m.name, dependsOn: m.dependsOn })),
    workflowId: `sql:${project.name}`,
  });
  const byName = new Map(project.models.map((m) => [m.name, m]));

  let builder = workflow<SqlProjectInput>({
    name: `sql-project:${project.name}`,
    type: "sql-model",
    metadata: { project: project.name },
  }) as unknown as ModelStepBuilder;

  for (const modelName of sorted) {
    const model = byName.get(modelName)!;
    builder = builder.step(
      model.name,
      { dependsOn: [...model.dependsOn] },
      (): ModelStepEff =>
        tryPromise(
          () => runModel({ model, executeSql, clock }),
          (error) =>
            error instanceof SqlModelError
              ? error
              : new SqlModelError({
                  model: model.name,
                  failures: [asMessage(error)],
                  message: `model "${model.name}" failed: ${asMessage(error)}`,
                }),
        ),
      config.modelStepOptions,
    );
  }

  builder = builder.step(RESULT_STEP, { dependsOn: [...sorted] }, ({ deps }) =>
    succeed(summarize(sorted.map((name) => deps[name] as SqlModelRunResult))),
  );

  return builder.build();
}

function validateProject(project: SqlProject): void {
  const problems: string[] = [];
  for (const model of project.models) {
    const where = `model "${model.name}"`;
    if (!IDENTIFIER.test(model.name)) problems.push(`${where}: name is not a SQL identifier`);
    if (model.name === RESULT_STEP) problems.push(`${where}: name is reserved`);
    if (model.materialization === "incremental") {
      if (model.uniqueKey === undefined) problems.push(`${where}: incremental needs uniqueKey`);
      else if (!IDENTIFIER.test(model.uniqueKey)) {
        problems.push(`${where}: uniqueKey is not a SQL identifier`);
      }
    } else if (!["table", "view"].includes(model.materialization)) {
      problems.push(`${where}: unknown materialization "${String(model.materialization)}"`);
    }
    for (const test of model.tests ?? []) {
      const needsColumn =
        test.type === "not_null" || test.type === "unique" || test.type === "between";
      if (needsColumn && (test.column === undefined || !IDENTIFIER.test(test.column))) {
        problems.push(`${where}: ${test.type} test needs a column that is a SQL identifier`);
      }
      if (test.type === "custom" && !test.check) {
        problems.push(`${where}: custom test needs a check query`);
      }
    }
  }
  if (problems.length > 0) {
    throw new Error(`invalid SQL project "${project.name}":\n  ${problems.join("\n  ")}`);
  }
}

async function runModel(params: {
  model: SqlModel;
  executeSql: (sql: string) => Promise<unknown[]>;
  clock: WallClock;
}): Promise<SqlModelRunResult> {
  const { model, executeSql, clock } = params;
  const startedAt = clock.currentTimeMs();

  try {
    await materialize({ model, executeSql });
  } catch (err) {
    const failure = `materialization failed: ${asMessage(err)}`;
    throw new SqlModelError({
      model: model.name,
      failures: [failure],
      message: `model "${model.name}" ${failure}`,
    });
  }

  let testsRun = 0;
  let testsPassed = 0;
  const failures: string[] = [];
  const warnings: string[] = [];
  for (const test of model.tests ?? []) {
    testsRun++;
    let failure: string | undefined;
    try {
      if (await runTest({ tableName: model.name, test, executeSql })) testsPassed++;
      else failure = `test failed: ${describeTest(test)}`;
    } catch (err) {
      failure = `test error: ${describeTest(test)}: ${asMessage(err)}`;
    }
    if (failure === undefined) continue;
    if (test.severity === "warn") warnings.push(failure);
    else failures.push(failure);
  }
  if (failures.length > 0) {
    throw new SqlModelError({
      model: model.name,
      failures,
      message: `model "${model.name}": ${failures.join("; ")}`,
    });
  }

  return {
    model: model.name,
    materialization: model.materialization,
    testsRun,
    testsPassed,
    warnings,
    startedAt,
    finishedAt: clock.currentTimeMs(),
  };
}

async function materialize(params: {
  model: SqlModel;
  executeSql: (sql: string) => Promise<unknown[]>;
}): Promise<void> {
  const { model, executeSql } = params;
  switch (model.materialization) {
    case "table":
      await executeSql(`DROP TABLE IF EXISTS ${model.name} CASCADE`);
      await executeSql(`CREATE TABLE ${model.name} AS ${model.sql}`);
      return;
    case "view":
      await executeSql(`DROP VIEW IF EXISTS ${model.name} CASCADE`);
      await executeSql(`CREATE VIEW ${model.name} AS ${model.sql}`);
      return;
    case "incremental": {
      // The model SQL is wrapped as a subquery, so it may carry its own
      // WHERE / GROUP BY / ORDER BY.
      const key = model.uniqueKey!;
      const source = `(${model.sql}) AS promin_src`;
      await executeSql(
        `CREATE TABLE IF NOT EXISTS ${model.name} AS SELECT * FROM ${source} WHERE 1=0`,
      );
      await executeSql(
        `INSERT INTO ${model.name} SELECT * FROM ${source} ` +
          `WHERE NOT EXISTS (SELECT 1 FROM ${model.name} promin_t WHERE promin_t.${key} = promin_src.${key})`,
      );
      return;
    }
  }
}

function summarize(models: readonly SqlModelRunResult[]): SqlProjectResult {
  const warnings = models.flatMap((m) =>
    m.warnings.map((message) => ({ model: m.model, message })),
  );
  const startedAt = Math.min(...models.map((m) => m.startedAt));
  const finishedAt = Math.max(...models.map((m) => m.finishedAt));
  return {
    models: [...models],
    modelsRun: models.length,
    testsRun: models.reduce((n, m) => n + m.testsRun, 0),
    testsPassed: models.reduce((n, m) => n + m.testsPassed, 0),
    testsWarned: warnings.length,
    warnings,
    durationMs: models.length === 0 ? 0 : finishedAt - startedAt,
  };
}

function describeTest(test: SqlExpectationDef): string {
  return test.type === "custom" ? `custom(${test.check})` : `${test.type}(${test.column ?? ""})`;
}

async function runTest(params: {
  tableName: string;
  test: SqlExpectationDef;
  executeSql: (sql: string) => Promise<unknown[]>;
}): Promise<boolean> {
  const { tableName, test, executeSql } = params;
  const first = (rows: unknown[]): Record<string, unknown> =>
    (rows[0] ?? {}) as Record<string, unknown>;
  switch (test.type) {
    case "not_null": {
      const rows = await executeSql(
        `SELECT COUNT(*) as cnt FROM ${tableName} WHERE ${test.column} IS NULL`,
      );
      return Number(first(rows).cnt ?? 0) === 0;
    }
    case "unique": {
      const rows = await executeSql(
        `SELECT COUNT(*) - COUNT(DISTINCT ${test.column}) as dupes FROM ${tableName}`,
      );
      return Number(first(rows).dupes ?? 0) === 0;
    }
    case "between": {
      const bounds = [`${test.column} < ${Number(test.min ?? 0)}`];
      if (test.max !== undefined) bounds.push(`${test.column} > ${Number(test.max)}`);
      const rows = await executeSql(
        `SELECT COUNT(*) as cnt FROM ${tableName} WHERE ${bounds.join(" OR ")}`,
      );
      return Number(first(rows).cnt ?? 0) === 0;
    }
    case "row_count": {
      const rows = await executeSql(`SELECT COUNT(*) as cnt FROM ${tableName}`);
      const count = Number(first(rows).cnt ?? 0);
      return (
        (test.min === undefined || count >= test.min) &&
        (test.max === undefined || count <= test.max)
      );
    }
    case "custom": {
      const rows = await executeSql(test.check!);
      return isTruthy(Object.values(first(rows))[0]);
    }
  }
}

/** SQL drivers return booleans as true / 1 / "t" / "true". */
function isTruthy(value: unknown): boolean {
  if (typeof value === "string") return ["t", "true", "1"].includes(value.toLowerCase());
  if (typeof value === "bigint") return value !== 0n;
  return Boolean(value);
}

function asMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}
