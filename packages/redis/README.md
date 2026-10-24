# @promin/redis

Redis backends for `@promin/workflow`: workflow storage, the distributed step queue, the durable scheduler, and state machine storage.

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

Full `WorkflowStorage` (including the activity journal and journaled suspend) for the `@promin/workflow` engine. Workflow state, step results, signals, sleeps and fenced locks live in hashes and sorted sets.

```typescript
import { RedisWorkflowStorage } from "@promin/redis";

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

### Redis Cluster

Every key of one workflow carries the hash tag `{wf:<workflowId>}`, so all of a workflow's keys share a slot. The cross-workflow indexes (status, name, parent and namespace sets, the ordering sorted sets, the sleep schedule) share the tag `{idx}`, so they all sit in one other slot. No script touches both slots:

- A workflow's write (step rows, status, journal, fence check) is one atomic script on its own slot.
- The index update is a second script on the `{idx}` slot. It is versioned by an `iv` counter on the workflow hash, so concurrent writers converge on the latest state whatever order their updates land in. The workflow hash is authoritative. After a crash between the two scripts, the index lags until the next write to that workflow, or until a scanner, listing or purge notices the lag and repairs it.
- The sleep schedule is updated after the journal write. `findDueSleeps` drops schedule members whose journal entry is no longer a pending sleep.
- A fenced child create checks the parent's fence and writes the child's row in two scripts, because they are in different slots. A parent that loses its lock between the two can still create the child. Child ids are deterministic per parent step, so the next lock holder attaches to that child.

The step queue, scheduler and state machine stores are unchanged: they do not hash-tag their keys yet.

### Migrating keys from earlier versions

Earlier versions stored keys without hash tags (`wf:<id>`, `wf:<id>:steps:1`, `wf:lock:<id>`, `wf:idx:status:running` …). Those keys are invisible to this version. To move them, stop every worker and run the migration once per prefix against the standalone instance, before moving to a cluster:

```typescript
const { workflows, keys } = await storage.migrateLegacyKeys();
```

The migration renames each workflow's keys under its tag and rebuilds the indexes from the workflow hashes. It also copies the sleep schedule and distinct-value sets, then deletes the old index keys. It scans the keyspace once, which also finds streams appended before stream ids were tracked, so purge removes them. Re-running it is a no-op. For a row that still lacks stream tracking, `purgeCompleted` falls back to a bounded `SCAN` for that workflow's stream keys.

## RedisStepQueue

Distributed `StepQueue`. Pending tasks sit in a priority-ordered sorted set (higher priority first, FIFO within a priority); enqueue, claim, complete, fail and requeue are Lua scripts, so each is atomic.

```typescript
import { RedisStepQueue } from "@promin/redis";

const queue = new RedisStepQueue({ redis, prefix: "sq", workerId: "worker-1" });

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
import { RedisDurableScheduler } from "@promin/redis";

const scheduler = new RedisDurableScheduler({ redis, prefix: "sched", pollIntervalMs: 1000 });

await scheduler.register({ id: "nightly-report", cron: "0 2 * * *", timezone: "UTC" });
```

`RedisSchedulerStorage` can also be passed to the generic `DurableScheduler` directly. Schedules are hashes; each namespace has its own due-time sorted set and fenced leader lease (one per partition when partitioned), so tenants poll independently.

## RedisStateMachineStorage

State machine persistence with transition history and TTLs for terminal or abandoned machines.

```typescript
import { stateMachine } from "@promin/workflow";
import { RedisStateMachineStorage } from "@promin/redis";

const storage = new RedisStateMachineStorage({
  redis,
  prefix: "sm",
  terminalTtlMs: 24 * 60 * 60 * 1000, // expire terminal machines after 1 day
  activeTtlMs: 60 * 60 * 1000, // expire machines idle in a non-terminal state for 1 hour
});

const order = stateMachine<OrderStates>({ name: "order", storage })
  // ...states and transitions
  .build();
```

- `transition()` is compare-and-set: it applies only while the machine is still in `from` at `expectedRevision`, and the snapshot update, revision bump and history append happen together. Otherwise it throws.
- `tryLock()` returns a token; `releaseLock()` and `extendLock()` act only while the lock still holds it, so a holder whose lock expired never frees or extends the next holder's lock.
- Terminal states registered by the state machine builder switch the keys to `terminalTtlMs`.

## Errors and connections

Stores pass driver errors through unchanged. Reconnection is up to the driver (ioredis reconnects by default). The stores never close the connection you pass in. Close it yourself on shutdown:

```typescript
redis.disconnect(); // ioredis
```
