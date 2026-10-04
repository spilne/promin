import { describe, it, expect } from "bun:test";
import { InMemoryWorkflowStorage } from "../../../index.ts";
import { createWorkflowRunner } from "../../durable/workflow-runner.ts";
import { FakeWallClock } from "../../shared/wall-clock.ts";
import { compileSqlProject } from "../sql-compiler.ts";
import type { SqlModel, SqlProject, SqlProjectResult } from "../sql-model.ts";

/** Records every statement; `answer` decides what each one returns or throws. */
function recordingDb(answer: (sql: string) => unknown[] = () => []) {
  const executed: string[] = [];
  const executeSql = async (sql: string): Promise<unknown[]> => {
    executed.push(sql);
    return answer(sql);
  };
  return { executed, executeSql };
}

const table = (name: string, extra: Partial<SqlModel> = {}): SqlModel => ({
  name,
  sql: `SELECT * FROM raw_${name}`,
  dependsOn: [],
  materialization: "table",
  ...extra,
});

async function run(project: SqlProject, executeSql: (sql: string) => Promise<unknown[]>) {
  const storage = new InMemoryWorkflowStorage();
  const runner = createWorkflowRunner({ storage });
  const wf = compileSqlProject({ project, executeSql, clock: FakeWallClock.create(1_000) });
  const outcome = await runner.runSafe({ workflow: wf, workflowId: "sql", input: {} });
  return { outcome, state: await storage.loadWorkflow("sql") };
}

describe("compileSqlProject outcomes", () => {
  it("returns a SqlProjectResult summing up every model", async () => {
    const db = recordingDb((sql) => (sql.includes("COUNT(*) as cnt") ? [{ cnt: 3 }] : []));
    const { outcome } = await run(
      {
        name: "p",
        models: [
          table("a", { tests: [{ type: "row_count", min: 1 }] }),
          table("b", { dependsOn: ["a"] }),
        ],
      },
      db.executeSql,
    );

    const result = outcome.data as SqlProjectResult;
    expect(result.modelsRun).toBe(2);
    expect(result.testsRun).toBe(1);
    expect(result.testsPassed).toBe(1);
    expect(result.testsWarned).toBe(0);
    expect(result.models.map((m) => m.model)).toEqual(["a", "b"]);
    expect(result.durationMs).toBe(0);
  });

  it("models without dependencies are independent roots of the DAG", () => {
    const wf = compileSqlProject({
      project: {
        name: "roots",
        models: [table("a"), table("b"), table("c", { dependsOn: ["a"] })],
      },
      executeSql: async () => [],
    });
    const deps = Object.fromEntries(wf.dag.steps.map((s) => [s.name, [...s.dependsOn]]));
    expect(deps["a"]).toEqual([]);
    expect(deps["b"]).toEqual([]);
    expect(deps["c"]).toEqual(["a"]);
  });

  it("a failed materialization fails the run and its dependants never run", async () => {
    const db = recordingDb((sql) => {
      if (sql.startsWith("CREATE TABLE a ")) throw new Error("relation raw_a does not exist");
      return [];
    });
    const { outcome, state } = await run(
      { name: "p", models: [table("a"), table("b", { dependsOn: ["a"] })] },
      db.executeSql,
    );

    expect(outcome.error).toBeDefined();
    expect(state?.status).toBe("failed");
    expect(state?.steps["a"]?.status).toBe("failed");
    expect(db.executed.some((s) => s.includes("CREATE TABLE b "))).toBe(false);
  });

  it("an error-severity test fails the model; a warn-severity one is recorded", async () => {
    const nulls = (sql: string) => (sql.includes("IS NULL") ? [{ cnt: 2 }] : []);

    const failing = await run(
      { name: "p", models: [table("a", { tests: [{ type: "not_null", column: "id" }] })] },
      recordingDb(nulls).executeSql,
    );
    expect(failing.state?.status).toBe("failed");
    expect(failing.state?.steps["a"]?.error).toContain("not_null(id)");

    const warning = await run(
      {
        name: "p",
        models: [table("a", { tests: [{ type: "not_null", column: "id", severity: "warn" }] })],
      },
      recordingDb(nulls).executeSql,
    );
    expect(warning.state?.status).toBe("completed");
    const result = warning.outcome.data as SqlProjectResult;
    expect(result.testsWarned).toBe(1);
    expect(result.warnings).toEqual([{ model: "a", message: "test failed: not_null(id)" }]);
  });

  it("custom tests pass only when the check query returns a truthy first column", async () => {
    const project = (check: string): SqlProject => ({
      name: "p",
      models: [table("a", { tests: [{ type: "custom", check }] })],
    });
    const answers: Record<string, unknown> = { ok: true, okText: "t", bad: false, badZero: 0 };
    const db = recordingDb((sql) => [{ passed: answers[sql] }]);

    for (const check of ["ok", "okText"]) {
      expect((await run(project(check), db.executeSql)).state?.status).toBe("completed");
    }
    for (const check of ["bad", "badZero"]) {
      expect((await run(project(check), db.executeSql)).state?.status).toBe("failed");
    }
  });

  it("incremental models wrap the SELECT and only insert rows with a new key", async () => {
    const db = recordingDb();
    const { state } = await run(
      {
        name: "p",
        models: [
          {
            name: "events",
            sql: "SELECT id, kind FROM raw_events WHERE kind <> 'noise' ORDER BY id",
            dependsOn: [],
            materialization: "incremental",
            uniqueKey: "id",
          },
        ],
      },
      db.executeSql,
    );

    expect(state?.status).toBe("completed");
    expect(db.executed).toEqual([
      "CREATE TABLE IF NOT EXISTS events AS SELECT * FROM " +
        "(SELECT id, kind FROM raw_events WHERE kind <> 'noise' ORDER BY id) AS promin_src WHERE 1=0",
      "INSERT INTO events SELECT * FROM " +
        "(SELECT id, kind FROM raw_events WHERE kind <> 'noise' ORDER BY id) AS promin_src " +
        "WHERE NOT EXISTS (SELECT 1 FROM events promin_t WHERE promin_t.id = promin_src.id)",
    ]);
  });

  it("rejects invalid projects at compile time", () => {
    const compile = (models: SqlModel[]) => () =>
      compileSqlProject({ project: { name: "bad", models }, executeSql: async () => [] });

    expect(compile([table("a; DROP TABLE users")])).toThrow(/not a SQL identifier/);
    expect(compile([table("a", { materialization: "incremental" })])).toThrow(/needs uniqueKey/);
    expect(compile([table("a", { tests: [{ type: "unique" }] })])).toThrow(/needs a column/);
    expect(compile([table("a", { tests: [{ type: "custom" }] })])).toThrow(/needs a check/);
    expect(compile([table("a", { dependsOn: ["missing"] })])).toThrow();
  });
});
