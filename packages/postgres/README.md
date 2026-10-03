# @promin/postgres

Postgres stores for promin: workflow storage, the distributed step queue, the durable scheduler, workflow start queue and advertisements, state machines, plus agent and eval stores. Built on [`@spilne/perfect-postgres`](https://www.npmjs.com/package/@spilne/perfect-postgres), which supplies the generic Postgres building blocks (`DrizzleDb`, `createPostgresDb`, `ensureTable`, `PgQueue`, `PgChangeStream`, `PgLeaderElection`, pgmq, rate limiting, and more).

## Install

```typescript
import { migrate, PostgresWorkflowStorage } from "@promin/postgres";
import { createPostgresDb } from "@spilne/perfect-postgres";
import { postgresDescribe } from "@promin/postgres/testing";
```

Two entrypoints:

| Entrypoint                 | What                                                                  |
| -------------------------- | --------------------------------------------------------------------- |
| `@promin/postgres`         | Workflow storage, step queue, scheduler, agent + eval stores, lookups |
| `@promin/postgres/testing` | Test container helpers                                                |

All stores accept a `DrizzleDb` (from `@spilne/perfect-postgres`), so any Postgres driver works (postgres-js, bun:sql, etc).

## Workflow Storage

Production-grade `WorkflowStorage` backed by Postgres. Integer lookup tables for status fields, `pg_advisory_lock` for distributed locking, configurable table prefix for multi-tenant DBs.

```typescript
import { createPostgresDb } from "@spilne/perfect-postgres";
import { migrate, PostgresWorkflowStorage } from "@promin/postgres";
import { workflow } from "@promin/workflow";

const db = createPostgresDb(process.env.DATABASE_URL!);

// Idempotent — safe on every startup
await migrate(db);

const storage = await PostgresWorkflowStorage.create({ db });

// Use with workflows
const result = await workflow<{ userId: string }>({ name: "onboard" })
  .stepAsync("fetch", ({ input }) => api.getUser(input.userId))
  .stepAsync("provision", ({ prev }) => api.createAccount(prev))
  .bind(storage)
  .run({ workflowId: `onboard-${userId}`, input: { userId } });
```

### Configuration

```typescript
PostgresWorkflowStorage.create({
  db, // DrizzleDb instance (required)
  tablePrefix: "wf_", // Table name prefix (default: "wf_")
  instanceId: "node-1", // Lock ownership ID (default: random UUID)
  useAdvisoryLocks: false, // row locks + fence tokens (default); `true` is deprecated (unsafe through a pool)
  defaultLockDurationMs: 30_000,
  autoSeedLookups: true, // Auto-seed status enum tables (default: true)
});
```

### Migrations

```typescript
await migrate(db, {
  migrationsTable: "__drizzle_migrations_workflows", // Isolate for multi-app DBs
  logger: { info: console.log, error: console.error },
});
```

## Durable Scheduler

Postgres-backed, distributed-safe cron scheduler. Persistent schedules, catch-up for missed runs, at-least-once tick delivery, leader election via fenced lease rows (`wf_leader_leases`, server-clock TTL), jitter, and backfill.

Implements `Streamable<ScheduleTick>` — works with `trigger()` and all perfect `Stream` combinators.

```typescript
import { createDurableScheduler, migrate } from "@promin/postgres";

await migrate(db);
const scheduler = createDurableScheduler({ db });

// Register persistent schedules
await scheduler.register({
  id: "daily-etl",
  name: "Daily ETL Pipeline",
  cron: "0 2 * * *",
  timezone: "America/New_York",
  maxCatchUp: 3,
  jitterMs: 30_000,
  metadata: { pipeline: "etl" },
});

await scheduler.register({
  id: "heartbeat",
  intervalMs: 30_000,
});

// Stream ticks into workflows (stream() returns a perfect Stream)
await scheduler
  .stream("daily-etl")
  .through(
    trigger({
      workflow: etlWorkflow,
      runner,
      storage,
      toInput: (tick) => ({ date: tick.scheduledAt.toISOString().split("T")[0] }),
      toWorkflowId: (tick) => `etl-${tick.scheduledAt.toISOString().split("T")[0]}`,
    }),
  )
  .drain()
  .run();

// Management
const next5 = await scheduler.nextFireTimes("daily-etl", 5);
await scheduler.triggerNow("daily-etl");
await scheduler.backfill("daily-etl", { from: new Date("2026-03-01"), to: new Date("2026-03-20") });
await scheduler.pause("daily-etl");
await scheduler.resume("daily-etl");
```

## Step Queue

Postgres-backed distributed step queue for workflow workers. Uses `SELECT FOR UPDATE SKIP LOCKED` so each pending task is handed to exactly one claimer, with natural load balancing across workers. A task is only handed out again after `requeueStuck` returns it to pending (dead worker, or no heartbeat within the stale timeout), so execution is at-least-once across worker crashes.

### Setup

```typescript
import { PgStepQueue } from "@promin/postgres";

const queue = new PgStepQueue({
  db, // DrizzleDb instance (required)
  workerId: "worker-1", // Identifies this worker (default: random UUID)
  namespace: "prod", // Isolate tasks by namespace (default: null = unscoped)
});

// Create the table (for dev/testing — prefer migrations for production)
await queue.ensureTable();
```

For production migrations, include the Drizzle schema:

```typescript
import { PgStepQueue } from "@promin/postgres";
export const stepQueue = PgStepQueue.schema;
```

### Enqueue tasks

```typescript
const taskId = await queue.enqueue({
  workflowId: "order-123",
  stepName: "charge",
  queue: "payments",
  input: { amount: 99.99 },
  prevResults: { validate: { ok: true } },
  priority: 8, // Higher = claimed first (default: 5)
});
```

### Claim and process tasks

```typescript
const tasks = await queue.claim({
  queues: ["payments", "notifications"],
  limit: 10,
  fairness: "strict-priority",
});

for (const task of tasks) {
  const start = Date.now();
  try {
    const result = await processStep(task);
    await queue.complete({
      taskId: task.id,
      result,
      durationMs: Date.now() - start,
    });
  } catch (err) {
    await queue.fail({
      taskId: task.id,
      error: String(err),
      durationMs: Date.now() - start,
    });
  }
}
```

### Fairness policies

Control how tasks are ordered when claiming:

| Policy              | Behavior                                                                                 |
| ------------------- | ---------------------------------------------------------------------------------------- |
| `"strict-priority"` | Highest priority first, then oldest (default)                                            |
| `"round-robin"`     | Interleave across workflows — prevents one workflow from starving others                 |
| `"weighted"`        | Priority weighted by randomness — high priority tasks are more likely but not guaranteed |

```typescript
// Round-robin across workflows
const tasks = await queue.claim({
  queues: ["default"],
  limit: 5,
  fairness: "round-robin",
});
```

### Requeue stuck tasks

Recover tasks claimed by crashed workers:

```typescript
// Requeue tasks older than 5 minutes
const requeued = await queue.requeueStuck({ staleTimeoutMs: 300_000 });

// Requeue tasks from a specific dead worker
const requeued = await queue.requeueStuck({ claimedBy: "worker-3" });
```

### Metrics

```typescript
const metrics = await queue.metrics();
// { "payments": { pending: 12, running: 3, completed: 450, failed: 2 },
//   "notifications": { pending: 0, running: 1, completed: 89, failed: 0 } }
```

## Running Tests

Requires Docker.

```bash
bun nx run @promin/postgres:test
```
