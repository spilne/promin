# @promin/postgres

Postgres infrastructure for the pipeline platform. Workflow storage, message queues (pgmq + SKIP LOCKED), durable scheduler, and change data capture — all backed by Postgres.

## Install

```typescript
import { migrate, PostgresWorkflowStorage } from "@promin/postgres";
import { PgmqQueue } from "@promin/postgres/pgmq";
import { postgresDescribe } from "@promin/postgres/testing";
```

Three entrypoints:

| Entrypoint                 | What                                                     |
| -------------------------- | -------------------------------------------------------- |
| `@promin/postgres`         | Workflow storage, scheduler, PgQueue, CDC, lookups       |
| `@promin/postgres/pgmq`    | pgmq extension queues (requires `CREATE EXTENSION pgmq`) |
| `@promin/postgres/testing` | Test container helpers                                   |

All accept a `DrizzleDb` instance — works with any Postgres driver (postgres-js, bun:sql, etc).

## Workflow Storage

Production-grade `WorkflowStorage` backed by Postgres. Integer lookup tables for status fields, `pg_advisory_lock` for distributed locking, configurable table prefix for multi-tenant DBs.

```typescript
import { drizzle } from "drizzle-orm/postgres-js";
import postgres from "postgres";
import { migrate, PostgresWorkflowStorage } from "@promin/postgres";
import { workflow, Pipeline } from "@promin/core";

const sql = postgres(process.env.DATABASE_URL!);
const db = drizzle(sql);

// Idempotent — safe on every startup
await migrate(db);

const storage = await PostgresWorkflowStorage.create({ db });

// Use with workflows
const result = await workflow<{ userId: string }>({ name: "onboard", storage })
  .step("fetch", ({ input }) => api.get(`/users/${input.userId}`, UserSchema))
  .step("provision", ({ prev }) => api.post("/accounts", AccountSchema, { json: prev }))
  .run({ workflowId: `onboard-${userId}`, input: { userId } });
```

### Configuration

```typescript
PostgresWorkflowStorage.create({
  db, // DrizzleDb instance (required)
  tablePrefix: "wf_", // Table name prefix (default: "wf_")
  instanceId: "node-1", // Lock ownership ID (default: random UUID)
  useAdvisoryLocks: true, // pg_advisory_lock vs row locks (default: true)
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

## PgQueue — SKIP LOCKED Queue

Message queue using plain Postgres tables. No extensions required — works with any Postgres 9.5+. Implements `Streamable<T>`, `Sinkable<T>`, and `Acknowledgeable<T>`.

```typescript
import { PgQueue } from "@promin/postgres";

const queue = await PgQueue.create<{ userId: string }>(db, "jobs");

// Publish
await queue.publish({ userId: "u_42" });
await queue.publish({ userId: "u_43" }, { delay: 60, headers: { "x-priority": "high" } });

// Subscribe (auto-ack — pop on read)
await queue
  .subscribe()
  .take(10)
  .forEach((msg) => console.log(msg.userId));

// Subscribe with manual ack/nack
await queue.subscribeAck({ vtSeconds: 30 }).forEach(async (envelope) => {
  await processUser(envelope.value);
  await envelope.ack(); // or envelope.nack() to retry
});

// Queue management
const stats = await queue.metrics(); // { pending, processing, completed, total }
await queue.requeueDead(); // Requeue stuck messages
await queue.purge(); // Clear all messages
```

## PgmqQueue — pgmq Extension Queue

High-level typed queue backed by the [pgmq](https://github.com/pgmq/pgmq) extension. SQS-like semantics with visibility timeout, archiving, and batch operations.

Requires `CREATE EXTENSION pgmq` on your database.

```typescript
import { PgmqQueue, ReadMode } from "@promin/postgres/pgmq";

const queue = await PgmqQueue.create<{ orderId: string }>(db, "orders");

// Publish
await queue.publish({ orderId: "ord_1" });
await queue.publishBatch([{ orderId: "ord_2" }, { orderId: "ord_3" }]);

// Subscribe (auto-ack via pop)
await queue.subscribe().forEach((msg) => console.log(msg.orderId));

// Subscribe with manual ack + archive mode
await queue
  .subscribeAck({
    readMode: ReadMode.standard({ vt: 30, qty: 10 }),
    ackMode: "archive",
  })
  .forEach(async (envelope) => {
    await processOrder(envelope.value);
    await envelope.ack();
  });

// LISTEN/NOTIFY for instant wakeup (instead of polling)
await queue.enableNotify();
```

### Low-level pgmq functions

For full control, use the raw SQL functions directly:

```typescript
import * as pgmq from "@promin/postgres/pgmq";

await pgmq.createQueue(db, "my-queue");
const msgId = await pgmq.send(db, "my-queue", { data: { hello: "world" } });
const records = await pgmq.read(db, "my-queue", ReadMode.standard({ vt: 30, qty: 10 }));
await pgmq.deleteMessage(db, "my-queue", msgId);
await pgmq.archive(db, "my-queue", msgId);
```

## Durable Scheduler

Postgres-backed, distributed-safe cron scheduler. Persistent schedules, catch-up for missed runs, overlap policies, leader election via `pg_advisory_lock`, jitter, and backfill.

Implements `Streamable<ScheduleTick>` — works with `trigger()` and all StreamPipeline combinators.

```typescript
import { createDurableScheduler, migrate } from "@promin/postgres";

await migrate(db);
const scheduler = createDurableScheduler({ db });

// Register persistent schedules
await scheduler.registerAsync({
  id: "daily-etl",
  name: "Daily ETL Pipeline",
  cron: "0 2 * * *",
  timezone: "America/New_York",
  maxCatchUp: 3,
  jitterMs: 30_000,
  metadata: { pipeline: "etl" },
});

await scheduler.registerAsync({
  id: "heartbeat",
  intervalMs: 30_000,
});

// Stream ticks into workflows
scheduler
  .stream("daily-etl")
  .through(
    trigger({
      workflow: etlWorkflow,
      toInput: (tick) => ({ date: tick.scheduledAt.toISOString().split("T")[0] }),
      toWorkflowId: (tick) => `etl-${tick.scheduledAt.toISOString().split("T")[0]}`,
    }),
  )
  .drain();

// Management
const next5 = await scheduler.nextFireTimes("daily-etl", 5);
await scheduler.triggerNow("daily-etl");
await scheduler.backfill("daily-etl", { from: new Date("2026-03-01"), to: new Date("2026-03-20") });
scheduler.pause("daily-etl");
scheduler.resume("daily-etl");
```

## Step Queue

Postgres-backed distributed step queue for workflow workers. Uses `SELECT FOR UPDATE SKIP LOCKED` for exactly-once delivery and natural load balancing across workers.

### Setup

```typescript
import { PgStepQueue } from "@promin/postgres";

const queue = new PgStepQueue({
  db,                    // DrizzleDb instance (required)
  workerId: "worker-1",  // Identifies this worker (default: random UUID)
  namespace: "prod",     // Isolate tasks by namespace (default: null = unscoped)
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
  priority: 8,           // Higher = claimed first (default: 5)
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

| Policy | Behavior |
| --- | --- |
| `"strict-priority"` | Highest priority first, then oldest (default) |
| `"round-robin"` | Interleave across workflows — prevents one workflow from starving others |
| `"weighted"` | Priority weighted by randomness — high priority tasks are more likely but not guaranteed |

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

## PgChangeStream — LISTEN/NOTIFY CDC

Real-time change data capture using LISTEN/NOTIFY with a poll-based fallback for at-least-once delivery. Implements `Streamable<T>` and `Replayable<T>`.

```typescript
import { PgChangeStream } from "@promin/postgres";

const stream = new PgChangeStream<{ userId: string }>({
  db,
  sql, // Raw postgres-js client (for LISTEN)
  channel: "user_changes",
  table: "users",
  payloadColumn: "payload",
  pollIntervalMs: 5000,
});

// Install auto-NOTIFY trigger on INSERT
await stream.installTrigger();

// Subscribe — merges LISTEN (low latency) + poll (reliability)
await stream.subscribe().forEach((change) => console.log("User changed:", change.userId));

// Replay from a point in time
await stream
  .subscribeFrom({ offset: { type: "timestamp", value: Date.now() - 3600_000 } })
  .forEach(handleChange);

// Manual notify (for producers)
await stream.notify({ userId: "u_42" });

// Cleanup
await stream.removeTrigger();
```

## Running Tests

Requires Docker.

```bash
bun nx run @promin/postgres:test
```
