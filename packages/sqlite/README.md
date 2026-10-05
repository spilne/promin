# @promin/sqlite

SQLite backends for `@promin/workflow` — workflow storage, step queue, scheduler storage and leader leases, worker registry, workflow advertisements and the start queue — plus agent and eval stores and a few local primitives (`SqliteQueue`, `SqliteRateLimiter`, `SqliteThrottle`). For single-node services, desktop and edge apps, CLIs and tests: one file, no server.

Every store takes a `SqliteDatabase`: any driver with `run`, `query(...).get/all/run` and `transaction`. `bun:sqlite`'s `Database` works as is at runtime; its generic `query` signature does not unify with `SqliteDatabase` under current `bun-types`, so cast it once. Stores are built with `Store.make({ db, ... })`, which creates their tables in their final shape (`CREATE TABLE IF NOT EXISTS`); there are no migrations.

```typescript
import { Database } from "bun:sqlite";
import { createWorkflowRunner, workflow } from "@promin/workflow";
import { SqliteWorkflowStorage, type SqliteDatabase } from "@promin/sqlite";
import { succeed } from "@spilne/perfect-core";

const db = new Database("workflows.db") as unknown as SqliteDatabase;
db.run("PRAGMA journal_mode = WAL");

const storage = SqliteWorkflowStorage.make({ db });
const runner = createWorkflowRunner({ storage });

const hello = workflow<{ name: string }>({ name: "hello" })
  .step("greet", ({ input }) => succeed(`hello ${input.name}`))
  .build();

await runner.run({ workflow: hello, workflowId: "hello-1", input: { name: "ada" } });
```

## What it implements

| `@promin/workflow` contract     | Class                                 |
| ------------------------------- | ------------------------------------- |
| `WorkflowStorage`               | `SqliteWorkflowStorage`               |
| `StepQueue`                     | `SqliteStepQueue`                     |
| `SchedulerStorage`              | `SqliteSchedulerStorage`              |
| `LeaderLeaseStore`              | `SqliteLeaderLeaseStore`              |
| `WorkerRegistry`                | `SqliteWorkerRegistry`                |
| `WorkflowAdvertisementRegistry` | `SqliteWorkflowAdvertisementRegistry` |
| `WorkflowStartQueue`            | `SqliteWorkflowStartQueue`            |

There is no SQLite state-machine storage or version registry; use the
in-memory ones or Postgres / Redis.

Workflow storage capabilities: `journal`, `stepAttempts`, `stepCheckpoint`,
`compensationLedger`, `resetSteps`, `summaries`, `countWorkflows`,
`cancelStale` (one-statement stale termination for `recover()`),
`dueTimers`, `signalWakeups` and `orphanedRuns`. Not supported: `tripwire`
(a `.tripwire()` step fails with `TripwireStorageMissingError`) and the push
event capabilities (`runEvents`, `stepStartedEvents`; the runner polls).

## Notes

- **Atomicity.** Every fenced write checks the fence token inside the
  write's transaction; batches, `startFreshRun`, `checkpointStep` and purge
  are one transaction each. Fence tokens come from one shared counter row, so
  two storage instances on the same file never hand out the same token.
- **Clock.** Lock, lease and claim expiry are computed from the injected
  `clock` (`WallClock`, default `SystemWallClock`), not from SQLite's clock.
  Processes sharing a file must share a host clock (they do, on one machine).
- **Concurrency.** SQLite serializes writers. Use WAL mode and keep each
  process to one connection; several processes on one file coordinate through
  SQLite's file locks, but there is no network distribution.
- **Step queue.** `claim` runs in one transaction with routing (step names,
  versions, capabilities) applied before the claim, concurrency keys,
  `claimedBy`, deliveries and dead-lettering — the full contract from
  [Distributed execution](../workflow/src/lib/distributed/README.md#step-queue-contract-v2).
  `SqliteStepQueue` has no lease store, so `requeueStuck({ lease })` is not
  fenced by the lease.
- **Scheduler.** Leases live in `<prefix>_leases` with a single-statement
  compare-and-set on the injected clock; the epoch check runs inside the
  `commitPoll` transaction.

```typescript
import { Database } from "bun:sqlite";
import { SqliteSchedulerStorage, SqliteStepQueue, type SqliteDatabase } from "@promin/sqlite";
import { DurableScheduler } from "@promin/workflow/scheduler";

const db = new Database("app.db") as unknown as SqliteDatabase;
const stepQueue = SqliteStepQueue.make({ db, maxDeliveries: 5 });
const scheduler = new DurableScheduler({ storage: SqliteSchedulerStorage.make({ db }) });
await scheduler.register({ id: "hourly", cron: "0 * * * *" });
console.log(await stepQueue.metrics({ since: new Date(0) }));
```
