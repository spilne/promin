# SQL Models

dbt-style SQL transformation layer — define models as SELECT statements with dependencies, compile them to a durable workflow that handles materialization, ordering, and data quality testing.

## Main Idea

Write SQL logic, declare dependencies. `compileSqlProject` (from `@promin/workflow/sql-models`) builds a workflow with one step per model: models run in parallel where the DAG allows, results are materialized as tables or views, and quality tests run after each model. Backed by the durable workflow engine — a re-run with the same workflow id resumes from the last checkpoint.

## Examples

### Basic pipeline: staging → fact table

```typescript
import { createWorkflowRunner, InMemoryWorkflowStorage } from "@promin/workflow";
import { compileSqlProject, type SqlProject } from "@promin/workflow/sql-models";

declare const db: { query(sql: string): Promise<unknown[]> }; // any SQL client

const project: SqlProject = {
  name: "analytics",
  models: [
    {
      name: "stg_orders",
      sql: "SELECT id, user_id, amount FROM raw.orders",
      dependsOn: [],
      materialization: "view",
    },
    {
      name: "stg_users",
      sql: "SELECT id, name, tier FROM raw.users",
      dependsOn: [],
      materialization: "view",
    },
    {
      name: "fct_revenue",
      sql: `
        SELECT u.tier, SUM(o.amount) as total, COUNT(*) as orders
        FROM stg_orders o JOIN stg_users u ON o.user_id = u.id
        GROUP BY u.tier
      `,
      dependsOn: ["stg_orders", "stg_users"],
      materialization: "table",
    },
  ],
};

const wf = compileSqlProject({ project, executeSql: (sql) => db.query(sql) });
const runner = createWorkflowRunner({ storage: new InMemoryWorkflowStorage() });
const result = await runner.run({ workflow: wf, workflowId: "analytics-daily", input: {} });
console.log(`${result.modelsRun} models, ${result.testsPassed}/${result.testsRun} tests passed`);
```

Execution order: `stg_orders` and `stg_users` run in parallel, then `fct_revenue`. A last step (`sql-project:result`) sums the run up into a `SqlProjectResult` (`models`, `modelsRun`, `testsRun`, `testsPassed`, `testsWarned`, `warnings`, `durationMs`).

### Data quality tests

```typescript
import type { SqlModel } from "@promin/workflow/sql-models";

const fctRevenue: SqlModel = {
  name: "fct_revenue",
  sql: "SELECT tier, SUM(amount) AS total FROM stg_orders GROUP BY tier",
  dependsOn: ["stg_orders"],
  materialization: "table",
  tests: [
    { type: "not_null", column: "tier" },
    { type: "unique", column: "tier" },
    { type: "row_count", min: 1 },
    { type: "between", column: "total", min: 0, max: 1_000_000 },
    { type: "custom", check: "SELECT COUNT(*) = 0 FROM fct_revenue WHERE total < 0" },
    { type: "row_count", max: 10, severity: "warn" },
  ],
};
```

Tests run after the model is materialized. A `custom` test passes when the first column of its query's first row is truthy.

A failed test fails the model's step with `SqlModelError` — the same as a failed materialization — so its dependants don't run on bad data, the step's retry policy applies (`modelStepOptions`) and the run ends failed. A test with `severity: "warn"` is recorded in the result's `warnings` instead.

### Crash recovery

```typescript
import { createWorkflowRunner, InMemoryWorkflowStorage } from "@promin/workflow";
import { compileSqlProject, type SqlProject } from "@promin/workflow/sql-models";

declare const project: SqlProject;
declare const executeSql: (sql: string) => Promise<unknown[]>;

const wf = compileSqlProject({
  project,
  executeSql,
  modelStepOptions: { retry: { maxRetries: 2 } },
});
const runner = createWorkflowRunner({ storage: new InMemoryWorkflowStorage() });

// If this run crashes after stg_orders but before fct_revenue...
await runner.runSafe({ workflow: wf, workflowId: "run-1", input: {} });
// ...re-running the same workflowId (from any process on durable storage) skips the
// checkpointed models and runs only fct_revenue.
await runner.run({ workflow: wf, workflowId: "run-1", input: {} });
```

## Materializations

| Type          | Behavior                                                                  |
| ------------- | ------------------------------------------------------------------------- |
| `table`       | DROP + CREATE TABLE AS SELECT (clean rebuild)                             |
| `view`        | DROP + CREATE VIEW (no data duplication)                                  |
| `incremental` | CREATE TABLE once, then INSERT the selected rows whose `uniqueKey` is new |

An `incremental` model needs a `uniqueKey` column. Its SELECT is wrapped as a subquery, so it may have its own `WHERE`, `GROUP BY` or `ORDER BY`.

`compileSqlProject` throws when the project is invalid: a dependency cycle or unknown dependency, a model, column or key name that isn't a plain SQL identifier, an incremental model without `uniqueKey`, or a test missing its `column` / `check`.

## Use Cases

- **Analytics pipelines** — staging → fact → mart layers with dependency ordering
- **Daily/hourly rebuilds** — crash-recoverable, one workflow id per run
- **Data quality gates** — fail the pipeline if a model produces bad data
