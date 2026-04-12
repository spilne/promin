# @promin/redis

Redis Streams transport adapter. Consumer groups, manual ack, dead consumer reclaim — same API as any other transport.

## Usage

```typescript
import Redis from "ioredis";
import { RedisStream } from "@promin/redis";
const redis = new Redis("redis://localhost:6379");

const events = new RedisStream<UserEvent>({
  redis,
  stream: "user-events",
  group: "event-processor",
});

// Create consumer group (idempotent)
await events.ensureGroup();

// Subscribe — auto-ack after processing
await events
  .subscribe()
  .filter((e) => e.type === "signup")
  .forEach(handleSignup);

// Manual ack — for at-least-once processing
await events.subscribeAck().forEach(async (envelope) => {
  await processEvent(envelope.value);
  await envelope.ack();
  // If you don't ack, the message stays in the PEL (pending entries list)
  // and can be reclaimed by another consumer
});

// Publish
await events.publish({ type: "signup", userId: "u_42" });

// Keyed publish
await events.publish(event, { key: event.userId });

// Reclaim messages from dead consumers
const stale = await events.claimPending({ minIdleMs: 30_000, count: 100 });
for (const msg of stale) {
  await processEvent(msg.value);
  await redis.xack("user-events", "event-processor", msg.id);
}

// Stream info
const info = await events.info();
console.log(`Stream length: ${info.length}, groups: ${info.groups}`);
```

## Typeclasses Implemented

| Typeclass       | Methods                                                 |
| --------------- | ------------------------------------------------------- |
| Streamable      | `subscribe()` — auto-ack via XREADGROUP                 |
| Sinkable        | `publish(value)` — XADD                                 |
| KeyedSinkable   | `publish(value, { key })` — XADD with key field         |
| Acknowledgeable | `subscribeAck()` → `envelope.ack()` — XREADGROUP + XACK |

## Configuration

The `RedisClient` interface is driver-agnostic. Pass any Redis client that satisfies the interface — ioredis, node-redis, or Bun.RedisClient all work.

```typescript
import Redis from "ioredis";
import type { RedisClient } from "@promin/redis";

// ioredis
const redis: RedisClient = new Redis("redis://localhost:6379");

// node-redis
import { createClient } from "redis";
const nodeRedis = createClient({ url: "redis://localhost:6379" });
await nodeRedis.connect();
const redis: RedisClient = nodeRedis as unknown as RedisClient;
```

The interface covers key/value, lists, hashes, sorted sets, streams, pub/sub, scripting (eval), pipelines, and connection management (duplicate/disconnect/close). Only commands actually used by the library are included.

## Distributed Primitives

### RedisRef

Distributed atomic reference. Stores a JSON value under a single Redis key with get/set/update semantics.

```typescript
import { RedisRef } from "@promin/redis";

const counter = RedisRef.make({ redis, key: "my:counter", initial: 0 });
await counter.setAsync(42);
const value = await counter.getAsync(); // 42
await counter.updateAsync((n) => n + 1); // 43
```

Note: `updateAsync` uses read-modify-write without WATCH/MULTI, so it is not atomic across multiple processes.

### RedisSignal

Distributed shared mutable value. Same API as RedisRef (get/set/update) but implements the `Signal<T>` interface.

```typescript
import { RedisSignal } from "@promin/redis";

const config = RedisSignal.make({ redis, key: "app:config", initial: { maxRetries: 3 } });
await config.setAsync({ maxRetries: 5 });
const current = await config.getAsync();
await config.updateAsync((c) => ({ ...c, maxRetries: c.maxRetries + 1 }));
```

### RedisSemaphore

Distributed counting semaphore. Limits concurrent access to a shared resource across processes. Uses BRPOP on a dedicated connection for blocking acquire.

```typescript
import { RedisSemaphore } from "@promin/redis";

const sem = await RedisSemaphore.make({ redis, key: "db:pool", permits: 5, timeoutMs: 10_000 });
await sem.acquire();
try { await queryDatabase(); }
finally { await sem.release(); }

// Or use the convenience wrapper:
await sem.withPermitAsync(() => queryDatabase());
```

### RedisChannel

Bounded distributed channel with capacity backpressure. Uses Lua scripts for atomic send operations.

```typescript
import { RedisChannel } from "@promin/redis";

const ch = RedisChannel.make<string>({ redis, key: "work:chan", capacity: 32 });
await ch.sendAsync("task-1");
await ch.sendAsync("task-2");
// Throws "Channel is full" when at capacity
// Throws "Channel is closed" after closeAsync()
await ch.closeAsync();
```

### RedisQueue

Distributed bounded FIFO queue with backpressure. Items are pushed left (LPUSH) and popped right (BRPOP) for FIFO order. `takeAsync` blocks on a dedicated connection.

```typescript
import { RedisQueue } from "@promin/redis";

const jobs = RedisQueue.make<{ id: string }>({ redis, key: "work:jobs", capacity: 100, timeoutMs: 30_000 });
await jobs.offerAsync({ id: "j-1" });
const next = await jobs.takeAsync(); // blocks until available
const size = await jobs.sizeAsync();
await jobs.shutdownAsync();
```

### RedisDeferred

Distributed one-shot synchronization. One process waits, another resolves or rejects. Multiple concurrent awaiters are supported.

```typescript
import { RedisDeferred } from "@promin/redis";

const gate = RedisDeferred.make<{ host: string }>({ redis, key: "init:config", timeoutMs: 30_000 });

// Process A (waiter):
const config = await gate.awaitAsync(); // blocks until resolved

// Process B (resolver):
await gate.succeedAsync({ host: "db.internal" });

// Or fail it:
await gate.failAsync(new Error("config unavailable"));
const done = await gate.isDoneAsync(); // true
```

### RedisLatch

Distributed countdown latch. Multiple processes count down; waiters unblock when the count reaches zero.

```typescript
import { RedisLatch } from "@promin/redis";

const latch = await RedisLatch.make({ redis, key: "init:latch", count: 3 });

// Workers call countDown when ready:
await latch.countDownAsync();

// Coordinator waits for all workers:
await latch.awaitAsync(); // unblocks when count hits 0
const remaining = await latch.remainingAsync();
```

### RedisBarrier

Distributed barrier. All parties call `awaitAsync`; the last one to arrive unblocks all others.

```typescript
import { RedisBarrier } from "@promin/redis";

const barrier = RedisBarrier.make({ redis, key: "sync:barrier", parties: 4 });

// Each process calls awaitAsync — blocks until all 4 have arrived
await barrier.awaitAsync();
const arrived = await barrier.arrivedAsync();
```

### RedisSingleflight

Distributed singleflight / request coalescing. When multiple processes call `doAsync` with the same key, only one executes the function. The rest block and receive the same result.

```typescript
import { RedisSingleflight } from "@promin/redis";

const sf = RedisSingleflight.make({ redis, prefix: "sf", timeoutMs: 10_000 });

// Both calls resolve to the same result; fetchUser is called once
const [a, b] = await Promise.all([
  sf.doAsync("user:1", () => fetchUser(1)),
  sf.doAsync("user:1", () => fetchUser(1)),
]);
```

### RedisThrottle

Distributed sliding-window throttle. Allows N operations per time window using atomic Lua scripts with sorted sets. Automatically waits when the window is full.

```typescript
import { RedisThrottle } from "@promin/redis";

const throttle = RedisThrottle.make({ redis, key: "api:github", permits: 60, windowMs: 60_000 });
await throttle.acquireAsync(); // blocks until a slot opens
const ok = await throttle.tryAcquireAsync(); // non-blocking, returns false if full

// Per-resource throttling:
await throttle.acquireAsync("user:42");
await throttle.withPermitAsync(() => callApi(), "user:42");
```

### RedisRateLimiter

Distributed sliding-window rate limiter. Unlike RedisThrottle, it throws `RateLimitExceeded` immediately instead of waiting.

```typescript
import { RedisRateLimiter } from "@promin/redis";

const limiter = RedisRateLimiter.make({ redis, key: "api:limit", limit: 100, windowMs: 60_000 });
try {
  await limiter.acquireAsync(); // throws RateLimitExceeded if over limit
} catch (e) {
  console.log(`Retry after ${e.retryAfterMs}ms`);
}
const ok = await limiter.tryAcquireAsync(); // returns false instead of throwing
const remaining = await limiter.remainingAsync(); // slots left in current window
```

## Caching

### RedisCacheStore

Key-value cache with TTL, key prefix namespacing, and bulk operations.

```typescript
import { RedisCacheStore } from "@promin/redis";

const cache = new RedisCacheStore<User>({ redis, prefix: "users:", ttlMs: 300_000 });

await cache.set("u_42", { name: "Alice" });
await cache.set("u_43", { name: "Bob" }, 60_000); // override TTL per entry

const user = await cache.get("u_42"); // User | undefined
const exists = await cache.has("u_42"); // boolean
await cache.delete("u_42");

const count = await cache.size();
await cache.clear(); // deletes all keys with the prefix
```

## Workflow Storage

### RedisWorkflowStorage

Full `WorkflowStorage` and `StepAttemptStorage` implementation for the `@promin/workflow` engine. Stores workflow state, step results, task tracking, signals, and lock management in Redis hashes and sorted sets.

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

Supports: `createWorkflow`, `loadWorkflow`, `updateStep`, `completeWorkflow`, `failWorkflow`, `cancelWorkflow`, `listWorkflows`, `sendSignal`, `consumeSignal`, lock acquisition with heartbeat, and step attempt recording.

### RedisStepQueue

Distributed step dispatch queue with priority scheduling. Uses sorted sets for pending tasks (higher priority = dequeued first, FIFO within same priority) and Lua scripts for atomic claim operations.

```typescript
import { RedisStepQueue } from "@promin/redis";

const queue = new RedisStepQueue(redis, { prefix: "sq", workerId: "worker-1" });

const taskId = await queue.enqueue({
  workflowId: "wf-1", stepName: "sendEmail", queue: "email",
  input: { to: "alice@example.com" }, prevResults: {}, priority: 8,
});

const tasks = await queue.claim({ queues: ["email"], limit: 10 });
await queue.complete({ taskId, result: { sent: true }, durationMs: 120 });
// Or: await queue.fail({ taskId, error: "SMTP timeout", durationMs: 5000 });

const stuck = await queue.requeueStuck({ staleTimeoutMs: 60_000 });
const stats = await queue.metrics(); // { email: { pending, running, completed, failed } }
```

### RedisStateMachineStorage

State machine persistence with transition history, optimistic locking, and TTL for terminal/stuck states.

```typescript
import { RedisStateMachineStorage } from "@promin/redis";

const storage = new RedisStateMachineStorage(redis, {
  prefix: "sm",
  terminalTtlMs: 24 * 60 * 60 * 1000, // expire terminal states after 1 day
  activeTtlMs: 60 * 60 * 1000, // expire stuck machines after 1 hour
});
storage.registerTerminalStates(["completed", "failed", "cancelled"]);

await storage.create({ id: "order-1", name: "order", initial: "pending", context: {} });
await storage.transition({ id: "order-1", from: "pending", to: "paid", event: "payment", context: { amount: 99 } });
const state = await storage.load("order-1");
const events = await storage.loadEvents("order-1");

// Optimistic locking for concurrent transitions:
const locked = await storage.tryLock("order-1", 5000);
await storage.releaseLock("order-1");
```

## Error Handling

All Redis primitives propagate errors from the underlying driver. Common failure modes:

- **Connection lost**: Operations reject with the driver's connection error. Reconnection behavior depends on your driver (ioredis auto-reconnects by default; node-redis does not).
- **Timeout on blocking operations**: `RedisSemaphore.acquire()`, `RedisDeferred.awaitAsync()`, `RedisQueue.takeAsync()`, and `RedisLatch.awaitAsync()` all use BRPOP with a timeout. They throw a descriptive timeout error (e.g., `"Semaphore acquire timeout after 30000ms"`).
- **Capacity exceeded**: `RedisChannel.sendAsync()` throws `"Channel is full"` and `RedisQueue.offerAsync()` throws `"Queue is full"` when at capacity.
- **Rate limiting**: `RedisRateLimiter.acquireAsync()` throws `RateLimitExceeded` with a `retryAfterMs` field.
- **State conflicts**: `RedisStateMachineStorage.transition()` throws when the current state does not match the expected `from` state.

## Cleanup

Primitives that use `duplicate()` for blocking operations (semaphore, deferred, queue, latch, barrier, singleflight) automatically close the duplicated connection when the operation completes.

For the top-level Redis connection, call either `disconnect()` or `close()` depending on your driver:

```typescript
// ioredis
redis.disconnect();

// node-redis / Bun
redis.close();
```

For workflow and stream resources, call their own disconnect/shutdown methods before closing the Redis connection:

```typescript
await events.disconnect?.();   // RedisStream — if applicable
await queue.shutdownAsync();   // RedisQueue
redis.disconnect();
```
