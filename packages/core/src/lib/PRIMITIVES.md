# Concurrency Primitives

In-process concurrency building blocks. All have distributed Redis-backed equivalents in `@promin/redis`.

## Synchronization

### Semaphore

Shared concurrency limiter. Wraps `Effect.Semaphore` to limit concurrent access to a resource.

```ts
import { PipelineSemaphore } from "@promin/core";

const apiLimit = PipelineSemaphore.make(10);
const result = await Effect.runPromise(apiLimit.withPermit(Effect.promise(() => fetch("/api"))));
```

**Methods:** `make(permits)`, `withPermit(effect)`, `availablePermits`

---

### Latch

Countdown latch -- block until N events have occurred.

```ts
import { PipelineLatch } from "@promin/core";

const latch = PipelineLatch.make({ count: 3 });
await latch.countDownAsync(); // 2 remaining
await latch.countDownAsync(); // 1 remaining
await latch.countDownAsync(); // 0 -- awaitors unblock
```

**Methods:** `make({ count })`, `countDown`, `countDownBy(n)`, `await`, `remaining`, `countDownAsync()`, `awaitAsync()`, `remainingAsync()`

---

### Barrier

Blocks all parties until every one has arrived, then releases them all at once.

```ts
import { PipelineBarrier } from "@promin/core";

const barrier = PipelineBarrier.make({ parties: 3 });
// Each concurrent fiber calls:
await barrier.awaitAsync(); // blocks until 3 fibers have arrived
```

**Methods:** `make({ parties })`, `await`, `arrived`, `awaitAsync()`, `arrivedAsync()`

---

### Deferred

One-shot synchronization. One fiber waits, another completes it with a value or error.

```ts
import { PipelineDeferred } from "@promin/core";

const gate = PipelineDeferred.make<string>();
// Fiber A waits:
const value = await gate.awaitAsync();
// Fiber B completes:
await gate.succeedAsync("ready");
```

**Methods:** `make()`, `await()`, `succeed(value)`, `fail(error)`, `isDone`, `awaitAsync()`, `succeedAsync(value)`, `failAsync(error)`, `isDoneAsync()`

---

## Data

### Ref

Atomic mutable reference for coordination between concurrent pipelines.

```ts
import { PipelineRef } from "@promin/core";

const counter = PipelineRef.make(0);
await counter.updateAsync((n) => n + 1);
const value = await counter.getAsync(); // 1
```

**Methods:** `make(initial)`, `get`, `set(value)`, `update(fn)`, `updateAndGet(fn)`, `modify(fn)`, `value` (sync), `getAsync()`, `setAsync(value)`, `updateAsync(fn)`, `updateAndGetAsync(fn)`

---

### Signal

Shared mutable value with change notifications. Subscribers receive a stream of updates.

```ts
import { PipelineSignal } from "@promin/core";

const config = PipelineSignal.make({ maxRetries: 3 });
await config.setAsync({ maxRetries: 5 }); // notifies subscribers
const stream = config.changes(); // StreamPipeline of values
```

**Methods:** `make(initial)`, `get`, `set(value)`, `update(fn)`, `getAsync()`, `setAsync(value)`, `updateAsync(fn)`, `changes()`

---

### Channel

Multi-producer, single-consumer channel with close semantics. Consumers drain as a `StreamPipeline`.

```ts
import { PipelineChannel } from "@promin/core";

const ch = PipelineChannel.make<string>(16);
await ch.sendAsync("hello");
await ch.closeAsync();
const items = await ch.toStream().collect();
```

**Methods:** `make(capacity?)`, `send(item)`, `close()`, `isClosed`, `sendAsync(item)`, `closeAsync()`, `toStream()`

---

### Queue

Bounded queue with backpressure. Bridges Effect's `Queue` into `StreamPipeline`.

```ts
import { PipelineQueue } from "@promin/core";

const queue = PipelineQueue.make<number>(100);
await queue.offerAsync(42);
const next = await queue.takeAsync(); // 42
```

**Methods:** `make(capacity)`, `makeUnbounded()`, `offer(item)`, `offerAll(items)`, `take()`, `shutdown()`, `size`, `offerAsync(item)`, `offerAllAsync(items)`, `takeAsync()`, `shutdownAsync()`, `sizeAsync()`, `toStream()`

---

### PubSub

Broadcast to multiple subscribers. Each subscriber sees every message independently.

```ts
import { PipelinePubSub } from "@promin/core";

const events = PipelinePubSub.make<string>(100);
const sub = events.subscribe(); // StreamPipeline
await events.publishAsync("event-1");
await events.shutdownAsync();
```

**Methods:** `make(capacity)`, `makeUnbounded()`, `publish(value)`, `shutdown()`, `publishAsync(value)`, `shutdownAsync()`, `subscribe()`

---

## Flow Control

### Throttle

Time-based concurrency limiter: N permits per sliding time window.

```ts
import { PipelineThrottle } from "@promin/core";

const throttle = PipelineThrottle.make({ permits: 5, windowMs: 1000 });
await throttle.withPermitAsync(() => fetch("/api"));
```

**Methods:** `make({ permits, windowMs })`, `acquire`, `tryAcquire`, `withPermit(effect)`, `withPermitPipeline(pipeline)`, `remaining`, `nextSlotIn`, `acquireAsync(resource?)`, `tryAcquireAsync(resource?)`, `withPermitAsync(fn, resource?)`, `remainingAsync()`, `nextSlotInAsync()`

---

### RateLimiter

Multi-strategy rate limiter: sliding window, fixed window, or token bucket. Fails with `RateLimitExceeded` when the limit is hit.

```ts
import { PipelineRateLimiter } from "@promin/core";

const limiter = PipelineRateLimiter.make({
  limit: 100,
  windowMs: 60_000,
  strategy: "token-bucket",
});
await limiter.withLimitAsync(() => callApi());
```

**Methods:** `make({ limit, windowMs, strategy? })`, `acquire`, `tryAcquire`, `withLimit(effect)`, `withLimitPipeline(pipeline)`, `remaining`, `resetAt`, `acquireAsync(resource?)`, `tryAcquireAsync(resource?)`, `withLimitAsync(fn, resource?)`, `remainingAsync(resource?)`, `resetAtAsync()`

---

### Singleflight

Request deduplication. Concurrent calls with the same key share one execution -- no caching after settlement.

```ts
import { PipelineSingleflight } from "@promin/core";

const sf = PipelineSingleflight.make();
const [a, b] = await Promise.all([
  sf.doAsync("user:1", () => fetchUser(1)),
  sf.doAsync("user:1", () => fetchUser(1)),
]); // fetchUser called once, both get same result
```

**Methods:** `make()`, `doEffect(key, effect)`, `do(key, pipeline)`, `doAsync(key, fn)`

---

### CircuitBreaker

Fail fast when a downstream dependency is broken. States: closed -> open (after N failures) -> half-open (after cooldown) -> closed (on success).

```ts
import { CircuitBreaker } from "@promin/core";

const breaker = new CircuitBreaker({
  failureThreshold: 5,
  resetTimeoutMs: 30_000,
});
const result = Effect.runPromise(breaker.protect(callDownstream));
```

**Methods:** `new CircuitBreaker(config)`, `protect(effect)`, `currentState`, `reset()`

**Config:** `failureThreshold`, `resetTimeoutMs`, `isFailure?(error)`

---

## Resources

### Cache

Single-value cache with TTL. First call executes, subsequent calls return cached until expiry.

```ts
import { PipelineCache } from "@promin/core";

const tokenCache = new PipelineCache<string>(55 * 60 * 1000);
const getToken = pipeline.cached(tokenCache);
await getToken.runPromise(); // hits API
await getToken.runPromise(); // returns cached
```

**Methods:** `new PipelineCache(ttlMs)`, `wrap(effect)`, `current`, `invalidate()`, `isFresh`

---

### CacheStore

Key-value cache interface with TTL, LRU eviction, and multi-layer support.

```ts
import { MemoryCache, layered } from "@promin/core";

const l1 = new MemoryCache({ ttlMs: 10_000, maxSize: 1000 });
const cache = layered(l1, redisCacheStore);

const user = await cache.getOrCompute("user:42", () => fetchUser("42"));
```

**Classes:** `MemoryCache`, `LayeredCache`

**MemoryCache methods:** `get(key)`, `set(key, value, ttlMs?)`, `delete(key)`, `has(key)`, `clear()`, `size()`

**LayeredCache methods:** all `CacheStore` methods + `getOrCompute(key, compute)`

---

### Pool

Reusable pool of N resources with automatic acquire/release.

```ts
import { PipelinePool } from "@promin/core";

const pool = PipelinePool.make({
  acquire: () => createConnection(),
  release: (conn) => conn.close(),
  size: 10,
});
const result = await pool.useAsync((conn) => conn.query("SELECT 1"));
```

**Methods:** `make({ acquire, release, size })`, `use(fn)` (Effect), `useAsync(fn)` (Promise), `size`
