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
const next5 = await scheduler.nextFireTimes({ scheduleId: "daily-etl", count: 5 });
await scheduler.triggerNow("daily-etl");
await scheduler.backfill({
  scheduleId: "daily-etl",
  from: new Date("2026-03-01"),
  to: new Date("2026-03-20"),
});
await scheduler.pause("daily-etl");
await scheduler.resume("daily-etl");
```

## Step Queue

Postgres-backed distributed step queue for workflow workers. Uses `SELECT FOR UPDATE SKIP LOCKED` so each pending task is handed to exactly one claimer, with natural load balancing across workers. A task is only handed out again after `requeueStuck` returns it to pending (dead worker, or no heartbeat within the stale timeout), so execution is at-least-once across worker crashes: handlers must be idempotent, and `claimToken` fences every write so only the current claim can settle a task.

### Setup

```typescript
import { PgStepQueue } from "@promin/postgres";

const queue = new PgStepQueue({
  db, // DrizzleDb instance (required)
  namespace: "prod", // Isolate tasks by namespace (default: null = unscoped)
  maxDeliveries: 10, // Dead-letter a task after this many deliveries (default: 10)
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
  needs: ["payments"], // Capabilities a worker must have (default: none)
  input: { amount: 99.99 },
  prevResults: { validate: { ok: true } },
  priority: 8, // Higher = claimed first (default: 5)
  attempt: 1, // The runner's attempt number (default: 1)
});
```

Enqueue is idempotent on `(workflowId, stepName)` while a task for the pair is pending or running.

### Claim and process tasks

```typescript
const tasks = await queue.claim({
  workerId: "worker-1", // Recorded on each task; dead-worker reclaim uses it
  limit: 10,
  capabilities: ["payments"],
  stepNames: ["charge", "refund"], // Only steps this worker hosts (default: any)
  versions: ["2"], // Only these workflow versions; unversioned always pass (default: any)
});

for (const task of tasks) {
  const start = Date.now();
  const claim = { taskId: task.id, claimToken: task.claimToken };
  try {
    const result = await processStep(task);
    await queue.complete({ ...claim, result, durationMs: Date.now() - start });
  } catch (err) {
    await queue.fail({ ...claim, error: String(err), durationMs: Date.now() - start });
  }
}

// Give back a task you claimed but won't run (no delivery is counted):
await queue.release({ taskId: task.id, claimToken: task.claimToken! });
```

The step-name, version and capability filters run inside the claim query, so a worker never claims tasks it can't run and they never block the tasks behind them. Tasks are claimed highest priority first, FIFO within a priority. Tasks sharing a `(concurrencyScope, concurrencyKey)` are capped at `concurrencyLimit` running at once across every claimer: admission takes a transaction-scoped advisory lock per key and recounts the running tasks under it.

### Requeue stuck tasks

Recover tasks claimed by crashed workers. A task that has already been delivered `maxDeliveries` times is dead-lettered instead — marked `failed` with `poisoned: exceeded N deliveries` — so a task that crashes every worker stops being redelivered.

```typescript
// Requeue tasks with no heartbeat for 5 minutes
const { requeued, deadLettered } = await queue.requeueStuck({
  mode: "stale",
  olderThanMs: 300_000,
});

// Requeue every task claimed by a dead worker
await queue.requeueStuck({ mode: "worker", workerId: "worker-3" });
```

### Inspect and purge

```typescript
const task = await queue.get(taskId); // status, deliveries, claimedBy, result / error, …

// Delete completed / failed tasks older than a week
await queue.purge({ completedBefore: new Date(Date.now() - 7 * 24 * 60 * 60 * 1000) });
```

### Metrics

```typescript
const metrics = await queue.metrics({ since: new Date(Date.now() - 60 * 60 * 1000) });
// { pending, running, completed, failed, avgWaitMs, avgExecMs, p95ExecMs }
```

## Running Tests

Requires Docker.

```bash
bun nx run @promin/postgres:test
```
