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

const [task] = await queue.claim({ capabilities: ["smtp"], limit: 10 });
await queue.complete({
  taskId: task.id,
  claimToken: task.claimToken,
  result: { sent: true },
  durationMs: 120,
});
// Or: await queue.fail({ taskId: task.id, claimToken: task.claimToken, error: "SMTP timeout", durationMs: 5000 });

await queue.requeueStuck({ staleTimeoutMs: 60_000 });
```

- **Routing**: a worker claims a task only when the task's `needs` are a subset of its `capabilities`.
- **Concurrency keys**: tasks sharing `(concurrencyScope, concurrencyKey)` are capped at `concurrencyLimit` running at once, across all workers. The running count is a Redis set updated in the same script as the claim, and the slot is released on complete, fail, requeue, and when a claim `filter` rejects the task.
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

- `transition()` is compare-and-set: it applies only while the machine is still in `from`, and the snapshot update and history append happen together. Otherwise it throws.
- `tryLock()` / `releaseLock()` use a per-holder token, so releasing never frees a lock another instance acquired after yours expired.
- Terminal states registered by the state machine builder switch the keys to `terminalTtlMs`.

## Errors and connections

Stores pass driver errors through unchanged. Reconnection is up to the driver (ioredis reconnects by default). The stores never close the connection you pass in. Close it yourself on shutdown:

```typescript
redis.disconnect(); // ioredis
```
