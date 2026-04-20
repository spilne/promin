/**
 * dbt-style SQL pipeline — staging views → fact tables with quality tests.
 * The compiler turns this into a durable workflow: crash-safe, parallel where possible.
 */

import { compileSqlProject, InMemoryWorkflowStorage, createWorkflowRunner } from "@promin/workflow";

const storage = new InMemoryWorkflowStorage();
const runner = createWorkflowRunner({ storage });

const project = {
  name: "analytics",
  models: [
    {
      name: "stg_orders",
      sql: "SELECT id, user_id, amount, created_at FROM raw.orders",
      dependsOn: [],
      materialization: "view" as const,
    },
    {
      name: "stg_users",
      sql: "SELECT id, name, region FROM raw.users",
      dependsOn: [],
      materialization: "view" as const,
    },
    {
      name: "fct_revenue",
      sql: `
        SELECT u.region, SUM(o.amount) as revenue, COUNT(*) as orders
        FROM stg_orders o JOIN stg_users u ON o.user_id = u.id
        GROUP BY u.region
      `,
      dependsOn: ["stg_orders", "stg_users"],
      materialization: "table" as const,
      tests: [
        { type: "not_null" as const, column: "region" },
        { type: "row_count" as const, min: 1 },
      ],
    },
  ],
};

const wf = compileSqlProject({
  project,
  executeSql: async (sql) => {
    console.log(`Executing: ${sql.slice(0, 60)}...`);
    return [];
  },
});

// stg_orders and stg_users run in parallel, then fct_revenue
const result = (await runner.run({
  workflow: wf,
  workflowId: "daily-2026-04-06",
  input: {},
})) as { modelsRun: number; testsPassed: number };
console.log(`${result.modelsRun} models, ${result.testsPassed} tests passed`);
