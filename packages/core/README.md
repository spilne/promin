# @ts-backend/core

Generic Pipeline for structural concurrency, built on [Effect](https://effect.website). A chainable, lazily-evaluated facade over Effect that provides 80% of the patterns teams need without requiring Effect expertise.

## Quick Start

```typescript
import { Pipeline, StreamPipeline } from "@ts-backend/core";

// Simple async action with retry
const result = await Pipeline.fn(() => callService())
  .map((data) => transform(data))
  .retry({ maxRetries: 3, jitter: true })
  .timeout(5_000)
  .runPromise();

// Stream processing with backpressure
const messages: AsyncIterable<KafkaMessage> = consumer.subscribe("topic");

await StreamPipeline.fromAsyncIterable(messages, (e) => new StreamError({ cause: e }))
  .filter((msg) => msg.topic === "events")
  .parAsyncMap(10, (msg) => enrichFromDb(msg.payload))
  .groupWithin(500, 1_000)
  .tapAsync((batch) => db.bulkInsert(batch))
  .drain();
```

## Pipeline API

### Construction

| Method                                       | Description                           |
| -------------------------------------------- | ------------------------------------- |
| `Pipeline.succeed(value)`                    | Create a successful pipeline          |
| `Pipeline.fail(error)`                       | Create a failed pipeline              |
| `Pipeline.fn(f)`                             | Shorthand for `Pipeline.fromPromise`  |
| `Pipeline.fromPromise(fn)`                   | Wrap a Promise-returning function     |
| `Pipeline.from(effect, options?)`            | Wrap a raw Effect                     |
| `Pipeline.sleep(ms)`                         | Sleep then succeed with void          |
| `Pipeline.scoped({ acquire, release, use })` | Resource management with auto-cleanup |

### Transform

| Method                           | Description                                                          |
| -------------------------------- | -------------------------------------------------------------------- |
| `.map(fn)`                       | Sync transform                                                       |
| `.mapAsync(fn)`                  | Async transform (Promise)                                            |
| `.flatMap(fn)`                   | Chain dependent pipelines                                            |
| `.flatMapAsync(fn)`              | Chain a dependent async action (Promise, no Pipeline wrapper needed) |
| `.tap(fn)` / `.tapAsync(fn)`     | Side-effects (blocking)                                              |
| `.tapAsyncFork(fn)`              | Fire-and-forget async side-effect (non-blocking, forked fiber)       |
| `.tapFork(fn)`                   | Fire-and-forget pipeline side-effect (non-blocking, forked fiber)    |
| `.tapPipeline(fn)`               | Side-effect pipeline (blocking, with retry/timeout)                  |
| `.mapError(fn)`                  | Transform the error type                                             |
| `.filter({ predicate, orFail })` | Conditional filtering                                                |
| `.delay(ms)`                     | Delay execution                                                      |
| `.when(condition)`               | Conditional execution                                                |

### Resilience

| Method                                                | Description                                                     |
| ----------------------------------------------------- | --------------------------------------------------------------- |
| `.concurrently(...others)`                            | Run in parallel with others — returns flat tuple of all results |
| `.race(...others)`                                    | Race against one or more pipelines — first to succeed wins      |
| `.retry(n)` or `.retry(policy?)`                      | Retry typed errors (supports jitter, maxDelayMs, timeBudgetMs)  |
| `.retryAll(policy?)`                                  | Retry all outcomes (errors + defects + success values)          |
| `.timeout(ms)`                                        | Total timeout                                                   |
| `.withPermit(semaphore)`                              | Shared concurrency limiter                                      |
| `.withCircuitBreaker(breaker)`                        | Fail fast when downstream is broken                             |
| `.cached(cache)`                                      | Memoize with TTL                                                |
| `.repeat({ times, intervalMs? })`                     | Repeat N times                                                  |
| `.supervised({ restart, maxRestarts?, intervalMs? })` | Long-lived process with auto-restart on failure                 |

### Error Handling

| Method                | Description                                                 |
| --------------------- | ----------------------------------------------------------- |
| `.tapError(fn)`       | Side-effect on error                                        |
| `.tapCause(fn)`       | Inspect full cause (typed errors + defects + interruptions) |
| `.orElse(fallback)`   | Recover with value                                          |
| `.orElsePipeline(fn)` | Recover with another pipeline                               |
| `.catch(tag, fn)`     | Catch specific error types                                  |
| `.onInterrupt(fn)`    | Cleanup only on interruption                                |

### Polling

| Method                                    | Description                 |
| ----------------------------------------- | --------------------------- |
| `.pollUntil({ until, intervalMs?, ... })` | Fixed-interval polling      |
| `.pollUntilWithBackoff({ until, ... })`   | Exponential backoff polling |

### Combinators (static)

| Method                                         | Description                       |
| ---------------------------------------------- | --------------------------------- |
| `Pipeline.all(a, b, c)`                        | Parallel — all must succeed       |
| `Pipeline.allSettled(a, b, c)`                 | Parallel — never short-circuits   |
| `Pipeline.validate(a, b, c)`                   | Parallel — accumulates ALL errors |
| `Pipeline.race(a, b)`                          | First to succeed wins             |
| `Pipeline.fallback(a, b)`                      | Sequential fallback               |
| `Pipeline.hedged(p, { hedgeDelayMs })`         | Tail-latency optimization         |
| `Pipeline.forEach(items, fn, { concurrency })` | Bounded concurrency               |

### Observability

| Method                 | Description                          |
| ---------------------- | ------------------------------------ |
| `.withSpan(name)`      | Annotate with tracing span           |
| `.withTag(key, value)` | Annotate current span with attribute |

### Terminals

| Method              | Description                       |
| ------------------- | --------------------------------- |
| `.runPromise()`     | Execute, throw on error           |
| `.runSafe()`        | Execute, return `{ data, error }` |
| `.runEither()`      | Execute, return `Either<T, E>`    |
| `.toEffect()`       | Escape hatch to raw Effect        |
| `.toEffectStream()` | Escape hatch to raw Effect Stream |
| `.finally(fn)`      | Sync cleanup                      |

## StreamPipeline API

### Construction

| Method                                            | Description                            |
| ------------------------------------------------- | -------------------------------------- |
| `StreamPipeline.from(stream)`                     | Wrap a raw Effect Stream               |
| `StreamPipeline.fromIterable(items)`              | From sync iterable                     |
| `StreamPipeline.fromAsyncIterable(iter, onError)` | From async generator                   |
| `StreamPipeline.fromPipeline(pipeline)`           | Bridge a Pipeline result into a stream |
| `StreamPipeline.tick(intervalMs)`                 | Emit 0, 1, 2, ... at a fixed interval  |
| `StreamPipeline.unfold(seed, fn)`                 | Generate stream from seed (pagination) |
| `StreamPipeline.iterate(initial, fn)`             | Generate by repeated application       |
| `StreamPipeline.empty()`                          | Empty stream                           |
| `StreamPipeline.mergeAll(...streams)`             | Merge multiple streams                 |

### Transform

| Method                        | Description                                                     |
| ----------------------------- | --------------------------------------------------------------- |
| `.map(fn)`                    | Sync transform                                                  |
| `.mapAsync(fn)`               | Async transform                                                 |
| `.mapEffect(fn)`              | Transform using an Effect                                       |
| `.mapAccumulate(initial, fn)` | Stateful map with accumulator                                   |
| `.filter(fn, action?)`        | Filter items (`"keep"` or `"drop"`, default: keep when true)    |
| `.filterAsync(fn, action?)`   | Filter by async predicate (`"keep"` or `"drop"`)                |
| `.filterMap(fn)`              | Filter + transform in one pass (return `undefined` to drop)     |
| `.unNone()`                   | Drop `undefined`/`null` values, narrow type to `NonNullable<T>` |
| `.tap(fn)` / `.tapAsync(fn)`  | Side-effects (blocking)                                         |
| `.tapAsyncFork(fn)`           | Fire-and-forget async side-effect (non-blocking)                |
| `.take(n)`                    | Take first N items                                              |
| `.takeWhile(fn)`              | Take while predicate is true                                    |
| `.drop(n)`                    | Skip first N items                                              |
| `.dropWhile(fn)`              | Skip while predicate is true                                    |
| `.dedupe()`                   | Emit only when value changes                                    |
| `.distinctBy(fn)`             | Deduplicate by key function                                     |
| `.flatMap(fn)`                | Flat-map into sub-streams                                       |
| `.switchMap(fn)`              | Flat-map, cancel previous inner stream on new item              |
| `.concat(other)`              | Append another stream after this one completes                  |
| `.zipWithIndex()`             | Pair each item with its zero-based index                        |

### Parallel & Batching

| Method                                   | Description                                 |
| ---------------------------------------- | ------------------------------------------- |
| `.parAsyncMap(concurrency, fn)`          | Parallel async transform (ordered)          |
| `.parAsyncMapUnordered(concurrency, fn)` | Parallel async transform (completion order) |
| `.mapAsyncRetry(fn, policy?)`            | Async transform with per-item retry         |
| `.groupWithin(maxSize, maxWaitMs)`       | Time-or-size bounded batches                |
| `.grouped(size)`                         | Fixed-size batches                          |
| `.sliding(size)`                         | Sliding window                              |
| `.buffer(capacity)`                      | Decouple producer/consumer speeds           |
| `.scan(initial, fn)`                     | Running accumulator                         |
| `.debounce(ms)`                          | Emit only after quiet period                |
| `.metered(ms)`                           | Enforce max emission rate                   |

### Combination

| Method                      | Description                              |
| --------------------------- | ---------------------------------------- |
| `.merge(other)`             | Interleave with another stream           |
| `.zipWith(other, fn)`       | Combine two streams element-by-element   |
| `.interleave(other)`        | Round-robin alternation                  |
| `.broadcastThrough(...fns)` | Fan out to multiple parallel pipelines   |
| `.observe(fn)`              | Parallel side-effect sink (non-blocking) |
| `.through(pipe)`            | Reusable transformation                  |
| `.pauseWhen(ref)`           | Pause/resume based on a boolean ref      |

### Resilience

| Method            | Description                             |
| ----------------- | --------------------------------------- |
| `.retry(policy?)` | Retry entire stream on error            |
| `.timeout(ms)`    | Fail if stream doesn't complete in time |

### Terminals

| Method                     | Description                       |
| -------------------------- | --------------------------------- |
| `.forEach(fn)`             | Process each item                 |
| `.collect()`               | Collect all items into array      |
| `.collectFirst(predicate)` | Find first matching item          |
| `.collectWhile(predicate)` | Collect while predicate holds     |
| `.runFirst()`              | Get first item (`T \| undefined`) |
| `.reduce(initial, fn)`     | Fold to a single value            |
| `.drain()`                 | Consume all, discard values       |
| `.finally(fn)`             | Sync cleanup on end               |
| `.onFinalize(fn)`          | Async cleanup on end              |
| `.interruptOn(signal)`     | Stop on AbortSignal               |
| `.interruptAfter(ms)`      | Stop after duration               |
| `.toStream()`              | Escape hatch to raw Effect Stream |

## Primitives

All primitives have both Effect-returning methods (for Pipeline composition) and `*Async` Promise-returning methods (no Effect import needed).

| Primitive                                          | Description                                 | Sync accessor          |
| -------------------------------------------------- | ------------------------------------------- | ---------------------- |
| `PipelineSemaphore.make(permits)`                  | Shared concurrency limiter                  | —                      |
| `new CircuitBreaker(config)`                       | Fail fast on downstream failure             | `.currentState`        |
| `new PipelineCache<T>(ttlMs)`                      | Single-value cache with TTL                 | `.current`, `.isFresh` |
| `PipelineQueue.make<T>(capacity)`                  | Bounded queue with backpressure             | —                      |
| `PipelineRef.make(initial)`                        | Atomic mutable reference                    | `.value`               |
| `PipelineDeferred.make<T>()`                       | One-shot synchronization                    | —                      |
| `PipelineChannel.make<T>(capacity?)`               | Multi-producer channel with close semantics | `.isClosed`            |
| `PipelineSignal.make<T>(initial)`                  | Shared value with change notifications      | —                      |
| `PipelinePubSub.make<T>(capacity)`                 | Broadcast to multiple subscribers           | —                      |
| `PipelinePool.make<R>({ acquire, release, size })` | Reusable resource pool                      | —                      |

### Primitive async methods (no Effect import needed)

```typescript
// PipelineRef
const ref = PipelineRef.make(0);
await ref.setAsync(10);
await ref.updateAsync((n) => n + 1);
const value = await ref.getAsync();
expect(ref.value).toBe(11); // sync for tests

// PipelineQueue
const queue = PipelineQueue.make<Job>(100);
await queue.offerAsync(job);
const next = await queue.takeAsync();

// PipelineChannel
const ch = PipelineChannel.make<Job>();
await ch.sendAsync(job);
await ch.closeAsync();

// PipelineDeferred
const gate = PipelineDeferred.make<Config>();
await gate.succeedAsync(config);
const value = await gate.awaitAsync();

// PipelineSignal
const sig = PipelineSignal.make(defaultConfig);
await sig.setAsync(newConfig);
const current = await sig.getAsync();

// PipelinePubSub
const ps = PipelinePubSub.make<Event>(100);
await ps.publishAsync(event);

// PipelinePool
const pool = PipelinePool.make({ acquire: () => connect(), release: (c) => c.close(), size: 5 });
const result = await pool.useAsync((conn) => conn.query("SELECT 1"));
```

## Further Reading

- **[COMPARISON.md](./COMPARISON.md)** — Side-by-side code comparison: Pipeline vs Promise vs raw Effect (23-79% line reduction)
- **[PATTERNS.md](./PATTERNS.md)** — 20 real-world patterns: saga, circuit breaker, rate-limited migration, competitive redundancy, stream fan-out, worker pools, supervised consumers, and more
