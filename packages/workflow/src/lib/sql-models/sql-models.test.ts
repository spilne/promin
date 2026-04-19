import { describe, it, expect } from "bun:test";
import { InMemoryWorkflowStorage } from "../durable/index.ts";
import { createWorkflowRunner } from "../durable/workflow-runner.ts";
import { compileSqlProject } from "./sql-compiler.ts";
import type { SqlProject } from "./sql-model.ts";

// ---------------------------------------------------------------------------
// Mock SQL executor — simulates a database with in-memory tables
// ---------------------------------------------------------------------------

function createMockDb() {
  const tables = new Map<string, unknown[]>();
  const views = new Map<string, string>();
  const executed: string[] = [];

  // Pre-populate source tables
  tables.set("orders", [
    { id: "o1", user_id: "u1", amount: 100, status: "completed" },
    { id: "o2", user_id: "u2", amount: 250, status: "pending" },
    { id: "o3", user_id: "u1", amount: 50, status: "completed" },
  ]);
  tables.set("users", [
    { id: "u1", name: "Alice", region: "US" },
    { id: "u2", name: "Bob", region: "EU" },
  ]);

  const executeSql = async (sql: string): Promise<unknown[]> => {
    executed.push(sql);

    // Handle DROP
    if (sql.startsWith("DROP TABLE") || sql.startsWith("DROP VIEW")) {
      const name = sql.match(/(?:TABLE|VIEW) IF EXISTS (\w+)/)?.[1];
      if (name) {
        tables.delete(name);
        views.delete(name);
      }
      return [];
    }

    // Handle CREATE TABLE AS
    if (sql.startsWith("CREATE TABLE")) {
      const name = sql.match(/CREATE TABLE (\w+) AS/)?.[1];
      if (name) {
        // Simulate: create table with some mock data
        tables.set(name, [{ _model: name, _created: true }]);
      }
      return [];
    }

    // Handle CREATE VIEW AS
    if (sql.startsWith("CREATE VIEW")) {
      const name = sql.match(/CREATE VIEW (\w+) AS/)?.[1];
      if (name) {
        views.set(name, sql);
        tables.set(name, [{ _model: name, _view: true }]);
      }
      return [];
    }

    // Handle SELECT COUNT for tests
    if (sql.includes("COUNT(*)") && sql.includes("IS NULL")) {
      return [{ cnt: 0 }]; // no nulls
    }
    if (sql.includes("COUNT(DISTINCT")) {
      return [{ dupes: 0 }]; // no dupes
    }
    if (sql.includes("COUNT(*)")) {
      const tableName = sql.match(/FROM (\w+)/)?.[1];
      const rows = tables.get(tableName ?? "") ?? [];
      return [{ cnt: rows.length }];
    }

    return [];
  };

  return { tables, views, executed, executeSql };
}

// ---------------------------------------------------------------------------
// Basic compilation
// ---------------------------------------------------------------------------

describe("SQL model orchestration — build analytics tables from raw data", () => {
  it("staging view feeds into a revenue fact table — linear dependency chain", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const db = createMockDb();

    const project: SqlProject = {
      name: "simple",
      models: [
        {
          name: "stg_orders",
          sql: "SELECT * FROM orders WHERE status != 'deleted'",
          dependsOn: [],
          materialization: "view",
        },
        {
          name: "fct_revenue",
          sql: "SELECT user_id, SUM(amount) as total FROM stg_orders GROUP BY user_id",
          dependsOn: ["stg_orders"],
          materialization: "table",
        },
      ],
    };

    const wf = compileSqlProject({ project, executeSql: db.executeSql });
    await runner.run({ workflow: wf, workflowId: "sql-1", input: {} });

    const state = await storage.loadWorkflow("sql-1");
    expect(state?.status).toBe("completed");
    expect(state?.steps["stg_orders"]?.status).toBe("completed");
    expect(state?.steps["fct_revenue"]?.status).toBe("completed");

    // Verify SQL was executed in order
    expect(db.executed.some((s) => s.includes("CREATE VIEW stg_orders"))).toBe(true);
    expect(db.executed.some((s) => s.includes("CREATE TABLE fct_revenue"))).toBe(true);
  });

  it("orders and users merge into revenue, then top regions — diamond dependency graph", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const db = createMockDb();

    const project: SqlProject = {
      name: "diamond",
      models: [
        { name: "stg_orders", sql: "SELECT * FROM orders", dependsOn: [], materialization: "view" },
        { name: "stg_users", sql: "SELECT * FROM users", dependsOn: [], materialization: "view" },
        {
          name: "fct_revenue",
          sql: "SELECT u.region, SUM(o.amount) FROM stg_orders o JOIN stg_users u ON o.user_id = u.id GROUP BY u.region",
          dependsOn: ["stg_orders", "stg_users"],
          materialization: "table",
        },
        {
          name: "dim_top_regions",
          sql: "SELECT * FROM fct_revenue ORDER BY total_revenue DESC LIMIT 10",
          dependsOn: ["fct_revenue"],
          materialization: "table",
        },
      ],
    };

    const wf = compileSqlProject({ project, executeSql: db.executeSql });
    await runner.run({ workflow: wf, workflowId: "sql-2", input: {} });

    const state = await storage.loadWorkflow("sql-2");
    expect(state?.status).toBe("completed");

    // All 4 models should have run
    expect(Object.keys(state?.steps ?? {}).length).toBe(4);
  });

  it("validates order table has no nulls or duplicates after build — quality gate", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const db = createMockDb();

    const project: SqlProject = {
      name: "tested",
      models: [
        {
          name: "orders_clean",
          sql: "SELECT * FROM orders",
          dependsOn: [],
          materialization: "table",
          tests: [
            { type: "not_null", column: "id" },
            { type: "unique", column: "id" },
            { type: "row_count", min: 1 },
          ],
        },
      ],
    };

    const wf = compileSqlProject({ project, executeSql: db.executeSql });
    await runner.run({ workflow: wf, workflowId: "sql-3", input: {} });

    const state = await storage.loadWorkflow("sql-3");
    expect(state?.status).toBe("completed");

    // Step result should include test info
    const result = state?.steps["orders_clean"]?.result as any;
    expect(result.testsRun).toBe(3);
    expect(result.testsPassed).toBe(3);
  });

  it("workflow name reflects the analytics project — useful for monitoring", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const db = createMockDb();

    const project: SqlProject = {
      name: "my-analytics",
      models: [{ name: "m1", sql: "SELECT 1", dependsOn: [], materialization: "table" }],
    };

    const wf = compileSqlProject({ project, executeSql: db.executeSql });
    expect(wf.name).toBe("sql-project:my-analytics");
  });

  it("lightweight staging layer uses views to avoid duplicating raw data", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const db = createMockDb();

    const project: SqlProject = {
      name: "views",
      models: [
        { name: "my_view", sql: "SELECT * FROM orders", dependsOn: [], materialization: "view" },
      ],
    };

    const wf = compileSqlProject({ project, executeSql: db.executeSql });
    await runner.run({ workflow: wf, workflowId: "sql-4", input: {} });

    expect(db.executed.some((s) => s.includes("CREATE VIEW my_view"))).toBe(true);
    expect(db.views.has("my_view")).toBe(true);
  });

  it("rebuilds table from scratch each run — drop then create for clean state", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const db = createMockDb();

    const project: SqlProject = {
      name: "recreate",
      models: [
        { name: "target", sql: "SELECT * FROM orders", dependsOn: [], materialization: "table" },
      ],
    };

    const wf = compileSqlProject({ project, executeSql: db.executeSql });
    await runner.run({ workflow: wf, workflowId: "sql-5", input: {} });

    const dropIdx = db.executed.findIndex((s) => s.includes("DROP TABLE IF EXISTS target"));
    const createIdx = db.executed.findIndex((s) => s.includes("CREATE TABLE target"));
    expect(dropIdx).toBeLessThan(createIdx);
  });

  it("re-running the same build is a no-op — safe to retry after partial failure", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const db = createMockDb();

    const project: SqlProject = {
      name: "idempotent",
      models: [
        { name: "m1", sql: "SELECT * FROM orders", dependsOn: [], materialization: "table" },
      ],
    };

    // Run twice with same workflowId — second run should just return (checkpointed)
    const wf = compileSqlProject({ project, executeSql: db.executeSql });
    await runner.run({ workflow: wf, workflowId: "sql-6", input: {} });

    const countBefore = db.executed.length;
    await runner.run({ workflow: wf, workflowId: "sql-6", input: {} }); // re-run
    const countAfter = db.executed.length;

    // No new SQL executed — step was checkpointed
    expect(countAfter).toBe(countBefore);
  });
});
