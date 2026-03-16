/**
 * SQL Model Runner — dbt-style analytics pipeline
 *
 * Business flow:
 * 1. Analytics team defines SQL models for an e-commerce data warehouse
 * 2. Staging models clean raw orders and users (filter deleted records, drop nulls)
 * 3. A fact table aggregates daily revenue by region and pricing plan
 * 4. Dimension tables summarize region performance and plan-level metrics
 * 5. Models are compiled into a dependency graph; independent models run in parallel
 * 6. After each model materializes, data tests verify uniqueness, nulls, and row counts
 * 7. The full pipeline runs daily on a schedule, idempotent by date
 *
 * Similar to dbt: declare SQL models with dependencies, and the system handles execution order and testing.
 */

import {
  compileSqlProject,
  InMemoryWorkflowStorage,
  type SqlProject,
} from "@promin/core";

const storage = new InMemoryWorkflowStorage();

// ---------------------------------------------------------------------------
// Analytics project — staging → facts → dimensions
// ---------------------------------------------------------------------------

const analyticsProject: SqlProject = {
  name: "ecommerce-analytics",
  sources: {
    raw: { schema: "public", tables: ["orders", "users", "products"] },
  },
  models: [
    // Staging: clean raw data
    {
      name: "stg_orders",
      sql: `
        SELECT id, user_id, product_id, amount, status, created_at
        FROM orders
        WHERE status != 'deleted' AND amount > 0
      `,
      dependsOn: [],
      materialization: "view",
      description: "Cleaned orders — no deleted, no zero-amount",
      tags: ["staging"],
      tests: [
        { type: "not_null", column: "id" },
        { type: "unique", column: "id" },
      ],
    },
    {
      name: "stg_users",
      sql: `
        SELECT id, name, email, region, plan, created_at
        FROM users
        WHERE email IS NOT NULL
      `,
      dependsOn: [],
      materialization: "view",
      tags: ["staging"],
      tests: [
        { type: "not_null", column: "id" },
        { type: "unique", column: "email" },
      ],
    },

    // Facts: aggregated business metrics
    {
      name: "fct_daily_revenue",
      sql: `
        SELECT
          DATE(o.created_at) as date,
          u.region,
          u.plan,
          COUNT(DISTINCT o.id) as order_count,
          SUM(o.amount) as revenue,
          AVG(o.amount) as avg_order_value,
          COUNT(DISTINCT o.user_id) as unique_customers
        FROM stg_orders o
        JOIN stg_users u ON o.user_id = u.id
        WHERE o.status = 'completed'
        GROUP BY DATE(o.created_at), u.region, u.plan
      `,
      dependsOn: ["stg_orders", "stg_users"],
      materialization: "table",
      tags: ["mart", "daily"],
      tests: [
        { type: "not_null", column: "date" },
        { type: "not_null", column: "region" },
        { type: "row_count", min: 1 },
        { type: "between", column: "revenue", min: 0, max: 999_999_999 },
      ],
    },

    // Dimensions: enriched lookup tables
    {
      name: "dim_regions",
      sql: `
        SELECT
          region,
          SUM(revenue) as total_revenue,
          SUM(order_count) as total_orders,
          ROUND(AVG(avg_order_value), 2) as avg_aov,
          SUM(unique_customers) as total_customers
        FROM fct_daily_revenue
        GROUP BY region
        ORDER BY total_revenue DESC
      `,
      dependsOn: ["fct_daily_revenue"],
      materialization: "table",
      tags: ["mart"],
    },
    {
      name: "dim_plan_performance",
      sql: `
        SELECT
          plan,
          SUM(revenue) as total_revenue,
          SUM(order_count) as total_orders,
          ROUND(SUM(revenue) / NULLIF(SUM(unique_customers), 0), 2) as revenue_per_customer
        FROM fct_daily_revenue
        GROUP BY plan
      `,
      dependsOn: ["fct_daily_revenue"],
      materialization: "table",
      tags: ["mart"],
    },
  ],
};

// ---------------------------------------------------------------------------
// Compile and run
// ---------------------------------------------------------------------------

// Mock SQL executor (replace with real DB in production)
async function mockExecuteSql(sql: string): Promise<unknown[]> {
  console.log(`  SQL: ${sql.trim().split("\n")[0]}...`);
  // In production: return db.execute(sql)
  if (sql.includes("COUNT(*)") && sql.includes("IS NULL")) return [{ cnt: 0 }];
  if (sql.includes("COUNT(DISTINCT")) return [{ dupes: 0 }];
  if (sql.includes("COUNT(*)")) return [{ cnt: 42 }];
  return [];
}

async function runAnalytics() {
  console.log("Compiling analytics project...\n");

  const analyticsWorkflow = compileSqlProject({
    project: analyticsProject,
    storage,
    executeSql: mockExecuteSql,
  });

  console.log(`Workflow: ${analyticsWorkflow.name}`);
  console.log(`DAG: ${analyticsWorkflow.dag.steps.map((s) => s.name).join(" → ")}\n`);

  // Run the pipeline
  const workflowId = `analytics-${new Date().toISOString().split("T")[0]}`;
  console.log(`Running ${workflowId}...\n`);

  await analyticsWorkflow.run({ workflowId, input: { date: "2026-04-02" } });

  // Check results
  const state = await storage.loadWorkflow(workflowId);
  console.log(`\nStatus: ${state?.status}`);
  for (const [name, step] of Object.entries(state?.steps ?? {})) {
    const result = step.result as any;
    const tests = result?.testsRun ? ` (${result.testsPassed}/${result.testsRun} tests passed)` : "";
    console.log(`  ${name}: ${step.status}${tests}`);
  }
}

// ---------------------------------------------------------------------------
// Schedule daily run
// ---------------------------------------------------------------------------

// In production:
// StreamPipeline.tick(86_400_000)
//   .through(trigger({
//     workflow: analyticsWorkflow,
//     toInput: () => ({ date: new Date().toISOString().split("T")[0] }),
//     toWorkflowId: () => `analytics-${new Date().toISOString().split("T")[0]}`,
//     concurrency: 1,
//     onDuplicate: "skip",
//   }))
//   .drain();

export { analyticsProject, runAnalytics };
