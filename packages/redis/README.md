# @promin/redis

Redis backends for `@promin/workflow`: workflow storage (`RedisWorkflowStorage`), the distributed step queue (`RedisStepQueue`), the durable scheduler (`RedisSchedulerStorage`, `RedisDurableScheduler`), leader leases (`RedisLeaderLeaseStore`) and state machine storage (`RedisStateMachineStorage`). Every store is Redis Cluster safe (see [key layout](#redis-cluster)).

Generic distributed primitives (refs, semaphores, latches, rate limiters, cache store, streams, pub/sub, queues) live in [`@spilne/perfect-redis`](https://www.npmjs.com/package/@spilne/perfect-redis). One connection can back both.

## Client

Every store takes a `RedisStoreClient`: perfect-redis's driver-agnostic `RedisClient` plus the sorted-set, set and pipeline commands the stores use for their indexes (`zadd`, `zrem`, `zrangebyscore`, `zcard`, `sadd`, `srem`, `smembers`, `scard`, `sinter`, `pipeline`).

ioredis implements all of them. Its overloads don't line up with the variadic signatures, so cast once:

```typescript
import { Redis } from "ioredis";
import type { RedisStoreClient } from "@promin/redis";

const redis = new Redis("redis://localhost:6379") as unknown as RedisStoreClient;
```

## RedisWorkflowStorage

Full `WorkflowStorage` for the `@promin/workflow` engine. Workflow state, step results, the activity journal, signals, sleeps and fenced locks live in hashes and sorted sets; every fenced write is one Lua script that compares the fence token before writing, and a lock is a key with a `PX` expiry, so an expired lock fences nothing.

| Capability                                  |     | Capability                                     |                      |
| ------------------------------------------- | :-: | ---------------------------------------------- | :------------------: |
| `journal`, `stepAttempts`, `stepCheckpoint` | yes | `summaries`, `countWorkflows`                  | yes (index records)  |
| `compensationLedger`                        | yes | `dueTimers` / `signalWakeups` / `orphanedRuns` |         yes          |
| `tripwire`, `resetSteps`                    | yes | `runEvents` / `stepStartedEvents`              | – (the runner polls) |
|                                             |     | `cancelStale`                                  |          –           |

```typescript
import { RedisWorkflowStorage, type RedisStoreClient } from "@promin/redis";

declare const redis: RedisStoreClient;

const storage = new RedisWorkflowStorage({
  redis,
  prefix: "wf",
  namespace: "prod",
  retention: {
    completedTtlMs: 7 * 24 * 60 * 60 * 1000, // expire completed workflows after 7 days
    maxRunsPerWorkflow: 5,
  },
});
```

`resetSteps` (behind `WorkflowRunner.resume`) is supported. Listing and counting read compact index records, so `countWorkflows` and `listWorkflowSummaries` never load runs.

`checkpointStep` writes a settled step's row and its attempt rows and reads back the run's status in one fenced script, so the runner spends one Redis round trip per step: a 100-step chain sends 109 commands in all, against 408 with the separate writes.

### Redis Cluster

Key layout (`<p>` is the store's `prefix`):

| Store                      | Keys                                                                                                                                                                     | Slot                       |
| -------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | -------------------------- |
| `RedisWorkflowStorage`     | `<p>:{wf:<id>}` (run hash) and `<p>:{wf:<id>}:steps:<run>`, `:tasks:…`, `:lock`, `:fence`, `:journal:…`, `:signals`, `:signal_tokens`, `:streams:…`, `:child-intents`, … | one per workflow           |
|                            | `<p>:{idx}:status:<s>`, `:name:<n>`, `:ns:<ns>`, `:children:<id>`, ordering sets, the sleep schedule                                                                     | one for all indexes        |
|                            | `<p>:wf-idempotency:<ns>:<name>:<key>`, `<p>:signal_token:<tokenId>` (single-key lookups)                                                                                | any                        |
| `RedisStepQueue`           | `{<p>}:task:<id>`, `{<p>}:pending`, running / done sets, concurrency-key sets                                                                                            | one per queue              |
| `RedisSchedulerStorage`    | `{<p>}:schedule:<id>`, per-namespace due sets, the namespace set, leader leases                                                                                          | one per scheduler prefix   |
| `RedisStateMachineStorage` | `<p>:{sm:<id>}:machine`, `:events`, `:lock`                                                                                                                              | one per machine            |
| `RedisLeaderLeaseStore`    | `<p>:{<key>}:…` (holder with `PX`, persistent epoch counter); a prefix with its own tag keeps that tag                                                                   | per lease, or the prefix's |

Every key of one workflow carries the hash tag `{wf:<workflowId>}`, so all of a workflow's keys share a slot. The cross-workflow indexes (status, name, parent and namespace sets, the ordering sorted sets, the sleep schedule) share the tag `{idx}`, so they all sit in one other slot. No script touches both slots:

- A workflow's write (step rows, status, journal, fence check) is one atomic script on its own slot.
- The index update is a second script on the `{idx}` slot. It is versioned by an `iv` counter on the workflow hash, so concurrent writers converge on the latest state whatever order their updates land in. The workflow hash is authoritative. After a crash between the two scripts, the index lags until the next write to that workflow, or until a scanner, listing or purge notices the lag and repairs it.
- The sleep schedule is updated after the journal write. `findDueSleeps` drops schedule members whose journal entry is no longer a pending sleep.
- A fenced child create commits on the parent's slot. The child's row is first written provisional: unindexed, invisible to readers, with a one-hour TTL. Then one fenced script on the parent's slot records the child in the parent's child intents, and the row is confirmed and indexed. A provisional row counts only once the parent's intent names it, so a parent that lost its lock before writing the intent never creates the child. A reader that finds a provisional row with a matching intent confirms it, so a crash after the intent loses nothing. A cascading cancel also reaches children committed this way whose rows are not indexed yet.

The other stores are Cluster-safe too:

- **Step queue**: every key starts with `{<prefix>}`, so a whole queue sits in one slot. A claim walks the shared pending set and moves tasks between sets in one script. Queues with different prefixes land in different slots. A lease store that fences `requeueStuck` must keep its keys in the queue's slot: construct it with `prefix: "{<queue prefix>}"`. The queue's constructor rejects a lease store in another slot.
- **Scheduler**: every key, leader leases included, starts with `{<prefix>}`. A fenced poll commit checks the lease epoch and writes many schedules and due sets in one script. Namespaces are tracked in a set, so `findDueAcross` and listing every namespace need no `KEYS`.
- **State machines**: each machine's keys carry the tag `{sm:<id>}`. There is no cross-machine key.
- **Leader leases**: `RedisLeaderLeaseStore` tags each lease's keys with `{<key>}`. A prefix that carries its own hash tag wins, which is how the scheduler and step queue keep leases in their slot.

## RedisStepQueue

Distributed `StepQueue`. Pending tasks sit in a priority-ordered sorted set (higher priority first, FIFO within a priority); enqueue, claim, complete, fail and requeue are Lua scripts, so each is atomic.

```typescript
import { RedisStepQueue, type RedisStoreClient } from "@promin/redis";

declare const redis: RedisStoreClient;

const queue = new RedisStepQueue({ redis, prefix: "sq", maxDeliveries: 10 });

await queue.enqueue({
  workflowId: "wf-1",
  stepName: "sendEmail",
  input: { to: "alice@example.com" },
  prevResults: {},
  needs: ["smtp"],
  priority: 8,
  // At most 2 running "send-email" tasks per tenant.
  concurrencyScope: "send-email",
  concurrencyKey: "tenant-42",
  concurrencyLimit: 2,
});

const [task] = await queue.claim({
  workerId: "mailer-1",
  limit: 10,
  capabilities: ["smtp"],
  stepNames: ["send-email"],
});
await queue.complete({
  taskId: task.id,
  claimToken: task.claimToken,
  result: { sent: true },
  durationMs: 120,
});
// Or: await queue.fail({ taskId: task.id, claimToken: task.claimToken, error: "SMTP timeout", durationMs: 5000 });

// Coordinator sweeps: stale leases, and every task of a dead worker. Tasks
// past `maxDeliveries` (default 10) are dead-lettered instead.
await queue.requeueStuck({ mode: "stale", olderThanMs: 60_000 });
await queue.requeueStuck({ mode: "worker", workerId: "mailer-1" });

// Delete settled tasks older than a day.
await queue.purge({ completedBefore: new Date(Date.now() - 24 * 60 * 60 * 1000) });
```

- **Routing**: a worker claims a task only when the task's `needs` are a subset of its `capabilities`, its step is in `stepNames` and its version in `versions` (when given). The checks run in the claim script, so tasks a worker can't run never block the ones behind them.
- **Concurrency keys**: tasks sharing `(concurrencyScope, concurrencyKey)` are capped at `concurrencyLimit` running at once, across all workers. The running count is a Redis set updated in the same script as the claim, and the slot is released on complete, fail, requeue and `release()`.
- **Claim scan**: one `claim()` examines at most `claimScanLimit` (default 1000) pending tasks while skipping ones it can't take, which bounds how long a backlog of blocked tasks can hold Redis.
- **Idempotent enqueue**: while a task for `(workflowId, stepName)` is pending or running, `enqueue()` returns its id instead of adding another.

## RedisDurableScheduler

The `@promin/workflow` `DurableScheduler` (cron, rrule, intervals, catch-up, jitter, leader election) on `RedisSchedulerStorage`. Swapping it for the Postgres scheduler is a one-line change.

```typescript
import { RedisDurableScheduler, type RedisStoreClient } from "@promin/redis";

declare const redis: RedisStoreClient;

const scheduler = new RedisDurableScheduler({ redis, prefix: "sched", pollIntervalMs: 1000 });

await scheduler.register({ id: "nightly-report", cron: "0 2 * * *", timezone: "UTC" });
```

`RedisSchedulerStorage` can also be passed to the generic `DurableScheduler` directly. Schedules are hashes; each namespace has its own due-time sorted set and fenced leader lease (one per partition when partitioned), so tenants poll independently.

## RedisStateMachineStorage

State machine persistence with transition history and TTLs for terminal or abandoned machines.

```typescript
import { stateMachine } from "@promin/workflow";
import { RedisStateMachineStorage, type RedisStoreClient } from "@promin/redis";

declare const redis: RedisStoreClient;

type OrderStates = {
  open: { context: { items: number }; transitions: { close: "closed" } };
  closed: { context: { items: number }; transitions: {} };
};

const storage = new RedisStateMachineStorage({
  redis,
  prefix: "sm",
  terminalTtlMs: 24 * 60 * 60 * 1000, // expire terminal machines after 1 day
  activeTtlMs: 60 * 60 * 1000, // expire machines idle in a non-terminal state for 1 hour
});

const order = stateMachine<OrderStates>({ name: "order", storage })
  .state("open")
  .state("closed", { terminal: true })
  .on("close", { from: "open", to: "closed" })
  .build();
```

- `transition()` is compare-and-set: it applies only while the machine is still in `from` at `expectedRevision`, and the snapshot update, revision bump and history append happen together. Otherwise it throws.
- `tryLock()` returns a token; `releaseLock()` and `extendLock()` act only while the lock still holds it, so a holder whose lock expired never frees or extends the next holder's lock.
- Terminal states registered by the state machine builder switch the keys to `terminalTtlMs`.

## Errors and connections

Stores pass driver errors through unchanged. Reconnection is up to the driver (ioredis reconnects by default). The stores never close the connection you pass in. Close it yourself on shutdown:

```typescript
import { Redis } from "ioredis";

const redis = new Redis("redis://localhost:6379");
// ... stores built on it ...
redis.disconnect(); // ioredis
```
