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
| `NamespaceRegistry`             | `PostgresNamespaceRegistry`                       |
| `WorkflowVersionRegistry`       | `PostgresWorkflowVersionRegistry`                 |
| `WorkflowAdvertisementRegistry` | `PgWorkflowAdvertisementRegistry`                 |
| `WorkflowStartQueue`            | `PgWorkflowStartQueue`                            |
| `StateMachineStorage`           | `PgStateMachineStorage`                           |

Agent stores (`@promin/agent` contracts) include `PostgresMemoryStore`
(`appendMessages` takes an `idempotencyKey`, so a re-run thread turn never
duplicates history), `PostgresLeaseStore` and `PostgresPendingApprovalStore`
— the durable index of tool-call approvals thread turns are suspended on,
keyed by (namespace, thread, tool call), with the decision and approver
recorded on the row. Its request, expiry and decision times come from the
database server's clock, so every replica agrees on expiry.

Workflow storage capabilities (see the [storage contract](../workflow/src/lib/durable/storage/README.md#capabilities)):

| Capability               |     | Capability                                     |                      |
| ------------------------ | :-: | ---------------------------------------------- | :------------------: |
| `journal`                | yes | `summaries`                                    |         yes          |
| `stepAttempts`           | yes | `countWorkflows`                               |         yes          |
| `stepCheckpoint`         | yes | `dueTimers` / `signalWakeups` / `orphanedRuns` |    yes (indexed)     |
| `stepProgress`           | yes | (`wf_workflows.step_progress`, jsonb)          |                      |
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

`migrate()` applies the migrations in `drizzle/` in order. `0000_baseline` is the schema as first shipped; later changes are incremental, idempotent files (`ALTER TABLE ... ADD COLUMN IF NOT EXISTS`) such as `0001_step_progress`, so a database migrated by an older version upgrades by running `migrate()` again. Add a new numbered file and a `meta/_journal.json` entry for every schema change; do not edit an applied one.

The schema is a **baseline migration** (`drizzle/0000_baseline.sql`: every
table, serial column, primary / unique / CHECK constraint, partial index,
foreign key and lookup seed) plus the incremental files after it.
`0001_step_progress` also adds, idempotently, the columns earlier changes put
into the baseline in place (`wf_workflows.compensation_origin` and the
`wf_step_queue` group and heartbeat columns). `0002_claim_lanes` adds the generated `wf_step_queue.lane` column and the
indexes `claim` probes (a one-time table rewrite for the stored column; old
writers need no change). `schema.ts` mirrors the chain and
a drift test builds a database from it and compares every table, column, index,
FK and named CHECK against it; upgrade tests apply the baseline alone and the chain through `0001`, then
the full chain.

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

- `claim` is one transaction: it finds candidate ids, locks the first `limit`
  still-pending ones with `FOR UPDATE SKIP LOCKED` (repeating `status = 'pending'`
  so a row claimed concurrently is rejected on recheck), then admits them. Step-name,
  version, capability and namespace filters are part of the candidate query.
- Candidate search costs the same however deep a throttled backlog is. A _lane_ is
  the set of pending rows sharing everything admission reads (namespace, concurrency
  scope / key / limit, group fields, fairness key; the generated `lane` column).
  Rows of a lane are admitted or throttled together, and only its first few by claim
  order can be among the next claimed. Plain order first tries a bounded window of
  the global index (enough when the head is claimable). Otherwise, and always when a
  grouped task is pending, `claim` skip-scans the lane index for the distinct lanes,
  runs the admission pre-checks once per lane, and takes each open lane's head
  (a probe per lane, bounded by `limit`); fair ranking and the group-slot cutoff are
  applied to those heads. The cost is the number of open lanes, not the backlog.
  `claimStrategy: "scan"` keeps the original per-row scan; tests compare the two on
  random queues and `scripts/pg-claim-bench.ts` measures them (50k pending, one hot
  key or group: about 1.1 s with the scan against 10 ms with lanes).
- Concurrency keys: admission takes `pg_advisory_xact_lock` per
  `(scope, key)` in hash order and recounts running tasks under the locks, so
  concurrent claimers with disjoint capabilities cannot both admit past the
  limit.
- Step groups: columns `group_name`, `group_max_concurrent`, `group_max_starts`,
  `group_window_ms`, `fairness_key` (in the baseline migration, edited in place as
  the repo does; `ensureTable` adds them to an older table with `ADD COLUMN IF NOT EXISTS`). Claim pre-checks each
  group's slots from the snapshot and keeps only as many candidates per group as it
  has slots, so a throttled backlog cannot crowd out other tasks; admission then
  takes the same advisory locks (under a `group:` key) and recounts running tasks
  and window starts under them. With a grouped task pending, candidates are ranked
  per `(group, fairness_key)` (`ROW_NUMBER` plus running count); with none, the plain
  index-ordered query is used.
- Wakeups: `new PgStepQueue({ db, wakeups })` tells a `Wakeups` backend when a task
  becomes claimable (enqueue, release, requeue, a settle that frees a slot) or
  settles; see [Wakeups](#wakeups-listennotify).
- `requeueStuck({ lease })` checks the leader lease with
  `assertPgLeaseCurrent` in the same transaction as the writes.
- `metrics()` with no `until` applies no upper bound, so rows the app stamped
  with its own clock are never dropped by a database-clock bound.

## Wakeups (LISTEN/NOTIFY)

Workers, the coordinator's step waits and the signal scanner poll; a wake makes
one of them run its next iteration now. `PgWakeups` carries wakes between
processes over `pg_notify`:

```typescript
import {
  PgWakeups,
  PgStepQueue,
  PostgresWorkflowStorage,
  postgresJsListen,
} from "@promin/postgres";

const wakeups = new PgWakeups({
  db,
  connect: postgresJsListen({ url: process.env.DATABASE_URL! }), // its own connection, only while something listens
});
const queue = new PgStepQueue({ db, wakeups });
const storage = await PostgresWorkflowStorage.create({ db, wakeups }); // deliverSignal notifies
const worker = createWorker({ stepQueue: queue, registry, wakeups });
const coordinator = createDistributedWorkflowRunner({ storage, stepQueue: queue, wakeups });
const scanner = createSignalScanner({ storage, wakeups, ...target });
```

A wake is only a latency hint: polling stays the correctness path, so a lost,
late or duplicate NOTIFY costs time, never correctness. The LISTEN connection
opens with the first subscriber and closes with the last; a lost one is reopened
with capped, jittered exponential backoff and every subscriber is woken once
after a reconnect. Topics are `steps` (a task is claimable), `results` (keyed by
task id: the awaited task settled) and `signals` (keyed by workflow id). Behind a
transaction-pooling proxy, point `connect` at a direct connection (LISTEN needs a
session). `InMemoryWakeups` (from `@promin/workflow/distributed`) does the same in
one process, and any other backend implements `notify` and `onWake`.

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

## pgvector retriever

`PgVectorRetriever` implements `@promin/agent`'s `Retriever` over a table of
embedded knowledge chunks. Its table is not part of the baseline migration,
because it needs the `vector` extension and a fixed embedding dimension;
`ensureSchema()` creates it (idempotently) instead.

```typescript
import { PgVectorRetriever } from "@promin/postgres";

const docs = new PgVectorRetriever({
  db,
  embeddings, // EmbeddingProvider; vectors must have `dimensions` entries
  dimensions: 1536,
  tableName: "kb.docs_chunks", // optional, default `agent_knowledge_chunk`
});
await docs.ensureSchema({ createExtension: true, index: "hnsw" }); // or "ivfflat" / false
await docs.addDocument({ id: "handbook", title: "Handbook", text: markdown, tags: ["hr"] });
const hits = await docs.retrieve({ query: "parental leave", topK: 5, filter: { tags: ["hr"] } });
```

Search orders by cosine distance and scores `1 - distance`. Tag filters use
array containment; metadata filters match the source metadata merged with the
chunk metadata. `createExtension` needs a role allowed to create extensions;
on managed Postgres enable `vector` once and leave it off. Values are always
bound parameters; the table name is validated as a (schema-qualified)
identifier and quoted. Its tests run on the `pgvector/pgvector:pg17` image.

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
