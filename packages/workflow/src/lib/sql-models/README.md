# SQL Models

dbt-style SQL transformation layer — define models as SELECT statements with dependencies, compile to a durable workflow that handles materialization, ordering, and data quality testing.

## Main Idea

Write SQL logic, declare dependencies. The compiler builds a DAG, runs models in parallel where possible, materializes results as tables or views, and runs quality tests after each model. Backed by the durable workflow engine — partial runs resume from the last checkpoint.

## Examples

### Basic pipeline: staging → fact table

```typescript
import { compileSqlProject } from "@promin/workflow";

const project = {
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

const wf = compileSqlProject({ project, storage, executeSql: (sql) => db.query(sql) });
await wf.run({ workflowId: "analytics-daily", input: {} });
```

Execution order: `stg_orders` and `stg_users` run in parallel, then `fct_revenue`.

### Data quality tests

```typescript
{
  name: "fct_revenue",
  sql: "SELECT ...",
  dependsOn: ["stg_orders", "stg_users"],
  materialization: "table",
  tests: [
    { type: "not_null", column: "tier" },
    { type: "unique", column: "tier" },
    { type: "row_count", min: 1 },
    { type: "between", column: "total", min: 0, max: 1_000_000 },
    { type: "custom", check: "SELECT COUNT(*) = 0 FROM fct_revenue WHERE total < 0" },
  ],
}
```

Tests run after the model is materialized. Failures are reported in the result.

### Parameterized runs

```typescript
const wf = compileSqlProject({ project, storage, executeSql });

// Input is available as {{ date }} in SQL
await wf.run({ workflowId: "analytics-2026-04-06", input: { date: "2026-04-06" } });
```

### Crash recovery

```typescript
// If this crashes after stg_orders but before fct_revenue...
await wf.run({ workflowId: "run-1", input: {} });

// ...re-running with the same workflowId skips completed models
await wf.run({ workflowId: "run-1", input: {} });
// Only fct_revenue runs — stg_orders and stg_users are checkpointed
```

## Materializations

| Type          | Behavior                                       |
| ------------- | ---------------------------------------------- |
| `table`       | DROP + CREATE TABLE AS SELECT (clean rebuild)  |
| `view`        | CREATE OR REPLACE VIEW (no data duplication)   |
| `incremental` | INSERT INTO ... SELECT (append new rows)       |
| `ephemeral`   | Not materialized — inlined by dependent models |

## Use Cases

- **Analytics pipelines** — staging → fact → mart layers with dependency ordering
- **Daily/hourly rebuilds** — parameterized by date, crash-recoverable
- **Data quality gates** — fail the pipeline if a model produces bad data
- **Migration scripts** — ordered DDL execution with rollback on failure
