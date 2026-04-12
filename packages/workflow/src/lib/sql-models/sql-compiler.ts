// ---------------------------------------------------------------------------
// compileSqlProject — SqlProject → WorkflowDefinition
//
// Each model becomes a workflow step. Dependencies are resolved via the DAG.
// Steps execute SQL (CREATE TABLE AS / CREATE VIEW AS) and optionally
// run data quality tests.
// ---------------------------------------------------------------------------

import { Pipeline } from "@promin/core";
import { workflow } from "../durable/durable-pipeline.ts";
import type { WorkflowDefinition } from "../durable/durable-pipeline.ts";
import type { WorkflowStorage } from "../durable/workflow-storage.ts";
import { topologicalSort } from "../durable/workflow-dag.ts";
import type { SqlProject, SqlModel, SqlProjectResult, ExpectationDef } from "./sql-model.ts";

export interface SqlCompilerConfig {
  /** The SQL project to compile. */
  project: SqlProject;
  /** Workflow storage for durability. */
  storage: WorkflowStorage;
  /**
   * Execute a SQL statement. This is the bridge to your database.
   * Return rows for SELECT, empty array for DDL.
   */
  executeSql: (sql: string) => Promise<unknown[]>;
}

/**
 * Compile a SqlProject into a WorkflowDefinition.
 *
 * Each model becomes a workflow step that:
 * 1. Drops existing table/view (for table/view materialization)
 * 2. Creates table/view from the model's SQL
 * 3. Runs data quality tests (if defined)
 *
 * Models run in DAG order — dependencies execute first.
 * Independent models run in parallel.
 */
export function compileSqlProject(
  config: SqlCompilerConfig,
): WorkflowDefinition<{ date?: string }, SqlProjectResult> {
  const { project, storage, executeSql } = config;

  // Validate DAG
  const dagNodes = project.models.map((m) => ({
    name: m.name,
    dependsOn: m.dependsOn,
  }));
  topologicalSort({ nodes: dagNodes, workflowId: `sql:${project.name}` });

  // Build workflow
  let builder: any = workflow<{ date?: string }>({
    name: `sql-project:${project.name}`,
    storage,
    type: "sql-model",
    metadata: { project: project.name },
  });

  const sorted = topologicalSort({ nodes: dagNodes, workflowId: `sql:${project.name}` });

  for (const modelName of sorted) {
    const model = project.models.find((m) => m.name === modelName)!;

    if (model.dependsOn.length === 0) {
      // Root model — no dependencies
      builder = builder.step(model.name, () =>
        Pipeline.fromPromise(() => executeModel(model, executeSql)),
      );
    } else {
      // DAG model — depends on other models
      builder = builder.step(model.name, { dependsOn: model.dependsOn }, () =>
        Pipeline.fromPromise(() => executeModel(model, executeSql)),
      );
    }
  }

  return builder.build() as WorkflowDefinition<{ date?: string }, SqlProjectResult>;
}

async function executeModel(
  model: SqlModel,
  executeSql: (sql: string) => Promise<unknown[]>,
): Promise<{
  model: string;
  materialization: string;
  testsRun: number;
  testsPassed: number;
  errors: string[];
}> {
  const errors: string[] = [];

  // Materialize
  try {
    switch (model.materialization) {
      case "table":
        await executeSql(`DROP TABLE IF EXISTS ${model.name} CASCADE`);
        await executeSql(`CREATE TABLE ${model.name} AS ${model.sql}`);
        break;
      case "view":
        await executeSql(`DROP VIEW IF EXISTS ${model.name} CASCADE`);
        await executeSql(`CREATE VIEW ${model.name} AS ${model.sql}`);
        break;
      case "incremental":
        // Simple incremental: insert new rows
        await executeSql(`CREATE TABLE IF NOT EXISTS ${model.name} AS ${model.sql} WHERE 1=0`);
        await executeSql(`INSERT INTO ${model.name} ${model.sql}`);
        break;
      case "ephemeral":
        // Ephemeral models don't materialize — they're CTEs referenced by other models
        break;
    }
  } catch (err) {
    errors.push(`Materialization failed: ${err instanceof Error ? err.message : String(err)}`);
  }

  // Run tests
  let testsRun = 0;
  let testsPassed = 0;

  if (model.tests && model.materialization !== "ephemeral") {
    for (const test of model.tests) {
      testsRun++;
      try {
        const passed = await runTest(model.name, test, executeSql);
        if (passed) testsPassed++;
        else errors.push(`Test failed: ${test.type}(${test.column ?? ""})`);
      } catch (err) {
        errors.push(
          `Test error: ${test.type} — ${err instanceof Error ? err.message : String(err)}`,
        );
      }
    }
  }

  return {
    model: model.name,
    materialization: model.materialization,
    testsRun,
    testsPassed,
    errors,
  };
}

async function runTest(
  tableName: string,
  test: ExpectationDef,
  executeSql: (sql: string) => Promise<unknown[]>,
): Promise<boolean> {
  switch (test.type) {
    case "not_null": {
      const rows = await executeSql(
        `SELECT COUNT(*) as cnt FROM ${tableName} WHERE ${test.column} IS NULL`,
      );
      return Number((rows[0] as any)?.cnt ?? 0) === 0;
    }
    case "unique": {
      const rows = await executeSql(
        `SELECT COUNT(*) - COUNT(DISTINCT ${test.column}) as dupes FROM ${tableName}`,
      );
      return Number((rows[0] as any)?.dupes ?? 0) === 0;
    }
    case "between": {
      const rows = await executeSql(
        `SELECT COUNT(*) as cnt FROM ${tableName} WHERE ${test.column} < ${test.min ?? 0} OR ${test.column} > ${test.max ?? 999999999}`,
      );
      return Number((rows[0] as any)?.cnt ?? 0) === 0;
    }
    case "row_count": {
      const rows = await executeSql(`SELECT COUNT(*) as cnt FROM ${tableName}`);
      const count = Number((rows[0] as any)?.cnt ?? 0);
      return (
        (test.min === undefined || count >= test.min) &&
        (test.max === undefined || count <= test.max)
      );
    }
    default:
      return true;
  }
}
