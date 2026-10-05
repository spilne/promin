# @promin/postgres

Postgres backends for `@promin/workflow` — workflow storage, the distributed step queue, the durable scheduler and leader leases, the worker registry, workflow advertisements and the start queue, the version registry, state machines — plus agent and eval stores. Built on [`@spilne/perfect-postgres`](https://www.npmjs.com/package/@spilne/perfect-postgres), which supplies the generic Postgres building blocks (`DrizzleDb`, `createPostgresDb`, `ensureTable`, `PgQueue`, `PgChangeStream`, rate limiting and more).

| Entrypoint                 | What                                                         |
| -------------------------- | ------------------------------------------------------------ |
| `@promin/postgres`         | the stores below, `migrate`, the Drizzle schema, lookups     |
| `@promin/postgres/testing` | `PostgresTestContainer`, `postgresDescribe` (testcontainers) |

Every store takes a `DrizzleDb`, so any Postgres driver Drizzle supports works (postgres-js, `bun:sql`, node-postgres).

## What it implements

| `@promin/workflow` contract     | Class                                             |
| ------------------------------- | ------------------------------------------------- |
| `WorkflowStorage`               | `PostgresWorkflowStorage`                         |
| `StepQueue`                     | `PgStepQueue`                                     |
| `SchedulerStorage` / scheduler  | `PgSchedulerStorage`, `DurableScheduler` (facade) |
| `LeaderLeaseStore`              | `PgLeaderLeaseStore` (+ `assertPgLeaseCurrent`)   |
| `WorkerRegistry`                | `PostgresWorkerRegistry`                          |
| `WorkflowVersionRegistry`       | `PostgresWorkflowVersionRegistry`                 |
| `WorkflowAdvertisementRegistry` | `PgWorkflowAdvertisementRegistry`                 |
| `WorkflowStartQueue`            | `PgWorkflowStartQueue`                            |
| `StateMachineStorage`           | `PgStateMachineStorage`                           |

Workflow storage capabilities (see the [storage contract](../workflow/src/lib/durable/storage/README.md#capabilities)):

| Capability               |     | Capability                                     |                      |
| ------------------------ | :-: | ---------------------------------------------- | :------------------: |
| `journal`                | yes | `summaries`                                    |         yes          |
| `stepAttempts`           | yes | `countWorkflows`                               |         yes          |
| `stepCheckpoint`         | yes | `dueTimers` / `signalWakeups` / `orphanedRuns` |    yes (indexed)     |
| `compensationLedger`     | yes | `runEvents` / `stepStartedEvents`              | – (the runner polls) |
| `tripwire`, `resetSteps` | yes | `cancelStale`                                  |          –           |

## Schema and migrations

```typescript
import { migrate } from "@promin/postgres";
import { createPostgresDb } from "@spilne/perfect-postgres";

const db = createPostgresDb(process.env.DATABASE_URL!);

// Idempotent — safe on every startup.
await migrate(db, {
  migrationsTable: "__drizzle_migrations_workflows", // isolate per app in a shared database
  logger: { info: console.log, error: console.error },
});
```

The schema ships as **one baseline migration** (`drizzle/0000_baseline.sql`):
every table, serial column, primary / unique / CHECK constraint, partial
index, foreign key and lookup seed in its final shape. There is no upgrade
chain from older layouts. `schema.ts` mirrors it and a drift test builds a
database from the migration and compares every table, column, index, FK and
named CHECK against it.

## Workflow storage

```typescript
import { createWorkflowRunner, workflow } from "@promin/workflow";
import { migrate, PostgresWorkflowStorage } from "@promin/postgres";
import { createPostgresDb } from "@spilne/perfect-postgres";
import { succeed } from "@spilne/perfect-core";

const db = createPostgresDb(process.env.DATABASE_URL!);
await migrate(db);

const storage = await PostgresWorkflowStorage.create({
  db,
  namespace: "prod", // optional tenant scope (default: none)
  instanceId: "node-1", // lock owner id (default: random UUID)
  defaultLockDurationMs: 30_000,
});

const onboard = workflow<{ userId: string }>({ name: "onboard" })
  .step("provision", ({ input }) => succeed({ accountId: `acct-${input.userId}` }))
  .build();

const runner = createWorkflowRunner({ storage });
await runner.run({ workflow: onboard, workflowId: "onboard-u42", input: { userId: "u42" } });
```

- **Locks** are lease rows in `wf_workflow_locks` with a `bigserial` fence
  token. `tryLock` and `heartbeat` compute expiry with the server's `NOW()`,
  so app-server clock skew never extends or shortens a lease.
- **Fenced writes** are single statements: a `MATERIALIZED` CTE takes
  `FOR SHARE` on the live lock row (token matches, `expires_at > NOW()`), and
  every write in the statement is gated on it. A takeover cannot land between
  the check and the write; a stale or expired token writes nothing and
  rejects with `FenceTokenMismatchError`.
- **One statement per hot-path write**: `checkpointStep`, step and task
  results, suspend, terminal transitions, journal appends and completions.
  `loadWorkflow` reads the run, its steps and tasks in one statement (one
  snapshot).
- `startFreshRun`, purge, cascade cancel (recursive CTE) and idempotency-key
  reclaim each run in one transaction; stream appends serialize per stream
  with `pg_advisory_xact_lock`.

## Step queue

```typescript
import { PgStepQueue } from "@promin/postgres";
import { createPostgresDb } from "@spilne/perfect-postgres";

const db = createPostgresDb(process.env.DATABASE_URL!);
const queue = new PgStepQueue({ db, namespace: "prod", maxDeliveries: 10 });

const tasks = await queue.claim({
  workerId: "worker-1",
  limit: 10,
  capabilities: ["gpu"],
  stepNames: ["transcribe"],
  versions: ["2"],
});
for (const task of tasks) {
  await queue.complete({
    taskId: task.id,
    claimToken: task.claimToken,
    result: { ok: true },
    durationMs: 5,
  });
}
```

The full contract — routing, `release`, deliveries and dead-lettering,
`requeueStuck` modes, `get` / `purge` / `metrics` — is described in
[Distributed execution](../workflow/src/lib/distributed/README.md#step-queue-contract-v2).
Postgres specifics:

- `claim` is one transaction: it walks the pending-order partial index, locks
  up to `limit` matching rows with `FOR UPDATE SKIP LOCKED` (repeating
  `status = 'pending'` in the locking scan so a row claimed concurrently is
  rejected on recheck), and stops after `limit` rows. Step-name, version and
  capability filters are part of that query.
- Concurrency keys: admission takes `pg_advisory_xact_lock` per
  `(scope, key)` in hash order and recounts running tasks under the locks, so
  concurrent claimers with disjoint capabilities cannot both admit past the
  limit.
- `requeueStuck({ lease })` checks the leader lease with
  `assertPgLeaseCurrent` in the same transaction as the writes.
- `metrics()` with no `until` applies no upper bound, so rows the app stamped
  with its own clock are never dropped by a database-clock bound.

## Scheduler and leader leases

```typescript
import { DurableScheduler, migrate, PgLeaderLeaseStore } from "@promin/postgres";
import { LeaseLeaderElection } from "@promin/workflow/scheduler";
import { coordinatorLeaderKey } from "@promin/workflow/distributed";
import { createPostgresDb } from "@spilne/perfect-postgres";

const db = createPostgresDb(process.env.DATABASE_URL!);
await migrate(db);

const scheduler = new DurableScheduler({ db, pollIntervalMs: 1_000, namespace: "prod" });
await scheduler.register({ id: "daily-etl", cron: "0 2 * * *", timezone: "America/New_York" });

// The same lease table elects a distributed coordinator.
const election = new LeaseLeaderElection({
  store: new PgLeaderLeaseStore({ db }),
  key: coordinatorLeaderKey({ namespace: "prod" }),
  instanceId: "coordinator-a",
  ttlMs: 10_000,
});
```

Leases live in `wf_leader_leases`; acquire and refresh are one
`INSERT … ON CONFLICT DO UPDATE … WHERE` against `NOW()`, so the TTL runs on
the server clock. `commitPoll` checks the lease epoch under `FOR SHARE` in
the same transaction as its writes. Delivery semantics are described in the
[Scheduler guide](../workflow/src/lib/scheduler/README.md).

## Clocks

Every store takes an optional `clock` (`WallClock`) for the timestamps it
stamps on the app side. Workflow-lock, state-machine-lock and leader-lease
expiry are computed and compared with the server's `NOW()`, so they never
depend on app-server clock skew. Never bound a column the app stamps with
`NOW()`, or a server-stamped column with an app `Date`.

## Running tests

Requires Docker.

```bash
bun nx run @promin/postgres:test
```
