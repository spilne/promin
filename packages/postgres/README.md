# @ts-backend/postgres

Postgres infrastructure for the pipeline platform. Workflow storage, message queues (pgmq + SKIP LOCKED), durable scheduler, and change data capture — all backed by Postgres.

## Install

```typescript
import { migrate, PostgresWorkflowStorage } from "@ts-backend/postgres";
import { PgmqQueue } from "@ts-backend/postgres/pgmq";
import { postgresDescribe } from "@ts-backend/postgres/testing";
```

Three entrypoints:

| Entrypoint                     | What                                                     |
| ------------------------------ | -------------------------------------------------------- |
| `@ts-backend/postgres`         | Workflow storage, scheduler, PgQueue, CDC, lookups       |
| `@ts-backend/postgres/pgmq`    | pgmq extension queues (requires `CREATE EXTENSION pgmq`) |
| `@ts-backend/postgres/testing` | Test container helpers                                   |

All accept a `DrizzleDb` instance — works with any Postgres driver (postgres-js, bun:sql, etc).

## Workflow Storage

Production-grade `WorkflowStorage` backed by Postgres. Integer lookup tables for status fields, `pg_advisory_lock` for distributed locking, configurable table prefix for multi-tenant DBs.

```typescript
import { drizzle } from "drizzle-orm/postgres-js";
import postgres from "postgres";
import { migrate, PostgresWorkflowStorage } from "@ts-backend/postgres";
import { workflow, Pipeline } from "@ts-backend/core";

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
import { PgQueue } from "@ts-backend/postgres";

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
import { PgmqQueue, ReadMode } from "@ts-backend/postgres/pgmq";

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
import * as pgmq from "@ts-backend/postgres/pgmq";

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
import { createDurableScheduler, migrate } from "@ts-backend/postgres";

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

## PgChangeStream — LISTEN/NOTIFY CDC

Real-time change data capture using LISTEN/NOTIFY with a poll-based fallback for at-least-once delivery. Implements `Streamable<T>` and `Replayable<T>`.

```typescript
import { PgChangeStream } from "@ts-backend/postgres";

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
bun nx run @ts-backend/postgres:test
```
