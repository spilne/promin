# @ts-backend/postgres

Postgres infrastructure for the pipeline platform. All Postgres-related capabilities in one package — workflow storage, message queues, transport adapters, and shared utilities.

## What's inside

### Workflow Storage (implemented)

Production-grade `WorkflowStorage` backed by Postgres with:

- **Integer lookup tables** for status/type fields (not string enums) — better storage compaction and index performance
- **`pg_advisory_lock`** for workflow locking (row-based fallback available)
- **Configurable** — table prefix, instance ID, lock duration, auto-seed, logger
- **`migrate()`** — idempotent schema creation + lookup seeding
- **`workflowType` + `metadata`** — categorize and tag workflows for filtering

```typescript
import { drizzle } from "drizzle-orm/postgres-js";
import postgres from "postgres";
import { migrate, PostgresWorkflowStorage } from "@ts-backend/postgres";
import { workflow, Pipeline } from "@ts-backend/core";

// 1. Connect to Postgres
const sql = postgres(process.env.DATABASE_URL!);
const db = drizzle(sql);

// 2. Run migrations (idempotent — safe on every startup)
await migrate(db, {
  // Optional: isolate migration tracking for multi-app DBs
  migrationsTable: "__drizzle_migrations_workflows",
  logger: { info: console.log, error: console.error },
});

// 3. Create storage
const storage = await PostgresWorkflowStorage.create({ db });

// 4. Define a reusable workflow
const onboardUser = workflow<{ userId: string }>({
  name: "onboard-user",
  storage,
  type: "onboarding",
  metadata: { team: "growth" },
})
  .step("fetch-user", ({ input }) => api.get(`/users/${input.userId}`, UserSchema))
  .step("create-account", ({ prev }) =>
    api.post("/accounts", AccountSchema, { json: { name: prev.name } }),
  )
  .stepAsync("send-welcome", async ({ prev }) => {
    await mailer.send(prev.email, "Welcome!");
    return { sent: true };
  })
  .build();

// 5. Run it
const result = await onboardUser.run({
  workflowId: `onboard-${userId}`,
  input: { userId },
});

// Or use with stream triggers
await eventStream
  .through(
    trigger({
      workflow: onboardUser,
      toInput: (event) => ({ userId: event.userId }),
      toWorkflowId: (event) => `onboard-${event.userId}`,
      concurrency: 5,
      onDuplicate: "skip",
    }),
  )
  .drain();
```

### Lookup Utilities (implemented)

Type-safe enum ↔ integer ID mapping that extends core string enums:

```typescript
import type { WorkflowStatus } from "@ts-backend/core";
import { defineLookup } from "@ts-backend/postgres";

const WorkflowStatusIds = defineLookup<WorkflowStatus>({
  running: 1,
  completed: 2,
  failed: 3,
  suspended: 4,
});

WorkflowStatusIds.toId("running"); // 1
WorkflowStatusIds.toName(2); // "completed"
WorkflowStatusIds.id.running; // 1
```

Drizzle integration:

```typescript
import { createLookupTable, seedLookupEnums, validateLookupEnums } from "@ts-backend/postgres";

const statusTable = createLookupTable("my_status");
await seedLookupEnums(db, [{ lookup: MyStatusIds, table: statusTable }]);
await validateLookupEnums(db, bindings); // throws if code ↔ DB drift
```

## Roadmap

### pgmq — Message Queues (planned)

[pgmq](https://github.com/pgmq/pgmq) integration for Postgres-native message queues with SQS-like semantics:

- **`PgmqQueue<T>`** — implements `Streamable<T>` + `Sinkable<T>` + `Acknowledgeable<T>` from core
- **Visibility timeout** — messages become invisible after read, reappear if not ack'd
- **Delayed messages** — schedule messages for future delivery
- **Archiving** — processed messages moved to archive table for replay/audit
- **Batch operations** — send/read multiple messages at once

Use cases:

- **Distributed workflow workers** (Phase 3.2) — workers claim steps via `pgmq.read()` with VT
- **Durable scheduler** (Phase 2.8) — delayed messages as scheduled ticks
- **Signal delivery** — queue per workflow for external signals
- **Stream trigger source** — `StreamPipeline.fromSource(pgmqQueue)` for reactive workflows

```typescript
// Planned API
const jobQueue = await PgmqQueue.create<{ userId: string }>(db, "onboard-jobs");

// Publish
await jobQueue.publish({ userId: "u_42" });

// Subscribe as StreamPipeline (Streamable)
await StreamPipeline.fromAck(jobQueue)
  .forEach(async (envelope) => {
    await processUser(envelope.value);
    await envelope.ack();
  });

// Or use with workflow trigger
await StreamPipeline.fromAck(jobQueue)
  .through(trigger({ workflow: onboardWorkflow, ... }))
  .drain();
```

### Postgres Transport Adapter (planned)

SKIP LOCKED-based transport for lightweight distributed processing without pgmq:

- Uses regular Postgres tables as queues
- `SELECT ... FOR UPDATE SKIP LOCKED` for concurrent worker consumption
- No extension required — works with any Postgres
- Implements `Streamable` + `Acknowledgeable` typeclasses

### Postgres Change Data Capture (planned)

- `LISTEN/NOTIFY` based `Streamable<T>` for real-time change streams
- Poll-based CDC with `Replayable<T>` for at-least-once delivery

## Configuration

```typescript
interface PostgresStorageConfig {
  db: DrizzleDB; // Drizzle database instance
  tablePrefix?: string; // Default: "wf_"
  instanceId?: string; // Default: random UUID
  useAdvisoryLocks?: boolean; // Default: true
  defaultLockDurationMs?: number; // Default: 30_000
  autoSeedLookups?: boolean; // Default: true
  logger?: (msg: string) => void; // Default: no-op
}
```

## Schema

All tables are prefixed with `wf_` by default (configurable).

### Lookup tables (integer IDs)

| Table                | Values                                                                                   |
| -------------------- | ---------------------------------------------------------------------------------------- |
| `wf_workflow_status` | running=1, completed=2, failed=3, suspended=4                                            |
| `wf_step_status`     | pending=1, running=2, completed=3, failed=4, skipped=5, sleeping=6, waiting_for_signal=7 |
| `wf_step_type`       | single=1, map=2, sleep=3, signal=4                                                       |

### Core tables

| Table                    | Purpose                                                             |
| ------------------------ | ------------------------------------------------------------------- |
| `wf_workflows`           | Workflow instances with status, input, result, metadata             |
| `wf_workflow_steps`      | Step state within workflows (composite PK: workflow_id + step_name) |
| `wf_workflow_step_tasks` | Map step tasks (composite PK: workflow_id + step_name + task_index) |
| `wf_workflow_signals`    | Delivered signals (unique on workflow_id + signal_name)             |
| `wf_workflow_locks`      | Row-based lock fallback                                             |

## Testing

Integration tests use [testcontainers](https://github.com/testcontainers/testcontainers-node) with `postgres:17-alpine`:

```bash
bun nx run @ts-backend/postgres:test
```

Requires Docker running locally.
