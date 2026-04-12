# StreamPipeline

`StreamPipeline<T, E>` is a chainable, lazily-evaluated streaming pipeline. Nothing executes until you call a terminal — `.forEach()`, `.collect()`, `.reduce()`, or `.drain()`. Backpressure is built-in: the producer only advances when the consumer is ready.

Adjacent pure operators (`.map()`, `.filter()`, `.tap()`) are automatically fused into a single `mapChunks` call — typically 2-3x faster for chained pure operations.

## Creating streams

```typescript
import { StreamPipeline } from "@promin/core";

// From an array
const stream = StreamPipeline.fromIterable([1, 2, 3, 4, 5]);

// From an async source (Kafka, WebSocket, file reader...)
const events = StreamPipeline.fromAsyncIterable(kafkaMessages, (e) => new KafkaError(e));

// From a Pipeline result — fetch once, stream the items
const videos = StreamPipeline.fromPipeline(api.get("/videos", VideosSchema)).flatMap((list) =>
  StreamPipeline.fromIterable(list),
);

// Periodic ticks — poll every 5 seconds
const metrics = StreamPipeline.tick(5_000).mapAsync(() => fetchMetrics());
```

## Transform and filter

```typescript
await StreamPipeline.fromIterable(users)
  .filter((u) => u.active) // keep active users
  .map((u) => u.email) // extract emails
  .tap((email) => console.log(email)) // side effect (all 3 fused into one pass)
  .collect();
// => ["alice@example.com", "bob@example.com"]
```

Filter + map in one step:

```typescript
const emails = await StreamPipeline.fromIterable(users)
  .filterMap((u) => (u.active ? u.email : undefined))
  .collect();
```

## Parallel async transforms

Process items concurrently with bounded parallelism:

```typescript
// Enrich 10 items at a time, preserving input order
const enriched = await StreamPipeline.fromIterable(videoIds)
  .parAsyncMap(10, async (id) => {
    const video = await fetchVideo(id);
    const stats = await fetchStats(id);
    return { ...video, ...stats };
  })
  .collect();
```

Unordered — results arrive as they complete (faster when order doesn't matter):

```typescript
await StreamPipeline.fromIterable(urls)
  .parAsyncMapUnordered(20, (url) => fetch(url).then((r) => r.json()))
  .forEach((data) => db.insert(data));
```

## Batching and windowing

```typescript
// Fixed-size batches
await stream
  .grouped(100) // batches of 100
  .tapAsync((batch) => db.bulkInsert(batch))
  .drain();

// Time + size window — whichever comes first
await webhookQueue
  .toStream()
  .groupWithin(500, 2_000) // 500 items or 2 seconds
  .tapAsync((batch) => db.bulkInsert(batch))
  .drain();
```

## Deduplication

```typescript
// Drop consecutive duplicates
await stream.dedupe().collect();
// [1, 1, 2, 2, 1] => [1, 2, 1]

// Drop duplicates by key (keeps first seen)
await stream.distinctBy((event) => event.id).drain();
```

## Fan-out with broadcastThrough

Send every item through multiple pipelines simultaneously:

```typescript
await eventStream
  .broadcastThrough(
    // Analytics: batch and write every 5s
    (s) => s.groupWithin(100, 5_000).tapAsync((batch) => analytics.write(batch)),

    // Alerts: filter anomalies, notify immediately
    (s) => s.filter((e) => e.severity === "critical").tapAsync((e) => pager.alert(e)),

    // Archive: write everything to S3
    (s) => s.grouped(1000).tapAsync((batch) => s3.put(batch)),
  )
  .drain();
```

## Flow control

```typescript
// Take first 10 items then stop
await stream.take(10).collect();

// Stop after 30 seconds
await stream.interruptAfter(30_000).drain();

// Stop on AbortSignal (e.g., SIGTERM)
const controller = new AbortController();
await stream.interruptOn(controller.signal).drain();

// Pause when downstream is overloaded
const paused = PipelineRef.make(false);
await stream
  .pauseWhen(paused)
  .tapAsync((item) => process(item))
  .drain();
```

## Error handling

```typescript
// Fallback value on error
await stream.orElse(defaultValue).collect();

// Retry the entire stream on failure
await stream.retry({ maxRetries: 3, baseDelayMs: 1_000 }).drain();

// Side-effect on error
await stream.tapError((err) => logger.error("Stream failed", err)).drain();

// Timeout — fail if no item arrives within 30s
await stream.timeout(30_000).drain();
```

## Cleanup

```typescript
await stream
  .tapAsync((item) => process(item))
  .onFinalize(async () => {
    await db.disconnect();
    console.log("Stream shut down cleanly");
  })
  .drain();
```

## Terminals

| Terminal            | Returns                   | Description                     |
| ------------------- | ------------------------- | ------------------------------- |
| `.forEach(fn)`      | `Promise<void>`           | Process each item               |
| `.collect()`        | `Promise<T[]>`            | Collect all items into an array |
| `.reduce(init, fn)` | `Promise<U>`              | Fold into a single value        |
| `.drain()`          | `Promise<void>`           | Consume all, discard values     |
| `.runFirst()`       | `Promise<T \| undefined>` | Take first item                 |
| `.collectWhile(fn)` | `Promise<T[]>`            | Collect while predicate holds   |

## When to use StreamPipeline vs Pipeline

- **Pipeline** — single async action (fetch, compute, call API). One input, one output.
- **StreamPipeline** — sequence of items over time (events, rows, messages). Many inputs, processed one at a time with backpressure.

Use `StreamPipeline.fromPipeline()` to bridge: fetch a result with Pipeline, then stream over it.

## API Reference

### Creation

| Method | Description | Returns |
|--------|-------------|---------|
| `StreamPipeline.fromIterable(items)` | Create from a sync iterable | `StreamPipeline<T, never>` |
| `StreamPipeline.fromAsyncIterable(iter, onError)` | Create from an async iterable | `StreamPipeline<T, E>` |
| `StreamPipeline.fromPipeline(pipeline)` | Bridge a Pipeline result into a stream | `StreamPipeline<T, E>` |
| `StreamPipeline.from(stream)` | Create from a raw Effect Stream | `StreamPipeline<T, E>` |
| `StreamPipeline.empty()` | Create an empty stream | `StreamPipeline<T, E>` |
| `StreamPipeline.tick(intervalMs)` | Emit tick count at fixed intervals | `StreamPipeline<number, never>` |
| `StreamPipeline.fixedDelay(ms)` | Emit after delay between completions | `StreamPipeline<number, never>` |
| `StreamPipeline.unfold(seed, fn)` | Generate by applying async fn to seed | `StreamPipeline<T, never>` |
| `StreamPipeline.iterate(initial, fn)` | Generate by applying fn to previous value | `StreamPipeline<T, never>` |
| `StreamPipeline.repeatEval(fn)` | Repeatedly evaluate an async function | `StreamPipeline<T, never>` |
| `StreamPipeline.range(start, end)` | Emit integers [start, end) | `StreamPipeline<number, never>` |
| `StreamPipeline.fromSource(source, params?)` | Create from a Streamable typeclass | `StreamPipeline<T, never>` |
| `StreamPipeline.fromAck(source, params?)` | Create with manual acknowledgement | `StreamPipeline<Envelope<T>, never>` |

### Transform

| Method | Description | Returns |
|--------|-------------|---------|
| `.map(fn)` | Transform each item (fused) | `StreamPipeline<U, E>` |
| `.mapAsync(fn)` | Async transform via Promise | `StreamPipeline<U, E>` |
| `.mapEffect(fn)` | Transform via Effect | `StreamPipeline<U, E \| E2>` |
| `.filter(fn, action?)` | Keep or drop items by predicate (fused) | `StreamPipeline<T, E>` |
| `.filterAsync(fn, action?)` | Async filter via Promise | `StreamPipeline<T, E>` |
| `.filterMap(fn)` | Filter + map in one pass (fused) | `StreamPipeline<U, E>` |
| `.unNone()` | Drop null/undefined values (fused) | `StreamPipeline<NonNullable<T>, E>` |
| `.tap(fn)` | Sync side-effect (fused) | `StreamPipeline<T, E>` |
| `.tapAsync(fn)` | Async side-effect, awaits before continuing | `StreamPipeline<T, E>` |
| `.tapAsyncFork(fn)` | Non-blocking background side-effect | `StreamPipeline<T, E>` |
| `.flatMap(fn)` | Flat-map into sub-streams | `StreamPipeline<U, E \| E2>` |
| `.switchMap(fn)` | Flat-map, cancel previous on new item | `StreamPipeline<U, E \| E2>` |
| `.concat(other)` | Append another stream after this one | `StreamPipeline<T, E>` |
| `.zipWithIndex()` | Pair each item with its index | `StreamPipeline<[T, number], E>` |
| `.zipWithPrevious()` | Pair with previous item | `StreamPipeline<[T \| undefined, T], E>` |
| `.mapAccumulate(initial, fn)` | Stateful map with accumulator | `StreamPipeline<U, E>` |
| `.scan(initial, fn)` | Running accumulator, emits intermediates | `StreamPipeline<U, E>` |
| `.dedupe()` | Drop consecutive duplicates | `StreamPipeline<T, E>` |
| `.distinctBy(fn)` | Deduplicate by key (keeps first) | `StreamPipeline<T, E>` |
| `.changes(eq?)` | Skip consecutive equal items | `StreamPipeline<T, E>` |
| `.through(pipe)` | Apply reusable stream transformation | `StreamPipeline<U, E2>` |

### Async

| Method | Description | Returns |
|--------|-------------|---------|
| `.parAsyncMap(concurrency, fn)` | Bounded parallel async, preserves order | `StreamPipeline<U, E>` |
| `.parAsyncMapUnordered(concurrency, fn)` | Bounded parallel async, completion order | `StreamPipeline<U, E>` |
| `.mapAsyncRetry(fn, policy?)` | Async transform with per-item retry | `StreamPipeline<U, E>` |

### Batching

| Method | Description | Returns |
|--------|-------------|---------|
| `.grouped(size)` | Fixed-size batches | `StreamPipeline<T[], E>` |
| `.groupWithin(maxSize, maxWaitMs)` | Time + size window batching | `StreamPipeline<T[], E>` |
| `.sliding(size)` | Sliding window | `StreamPipeline<T[], E>` |
| `.mapChunks(fn)` | Transform entire chunks for max throughput | `StreamPipeline<U, E>` |
| `.rechunk(size)` | Re-chunk to specific size | `StreamPipeline<T, E>` |
| `.buffer(capacity)` | Decouple producer/consumer | `StreamPipeline<T, E>` |

### Fan-out

| Method | Description | Returns |
|--------|-------------|---------|
| `.broadcastThrough(...fns)` | Fan out to multiple parallel pipelines | `StreamPipeline<unknown, E>` |
| `.observe(fn)` | Non-blocking parallel side-effect consumer | `StreamPipeline<T, E>` |
| `.merge(other)` | Interleave items from two streams | `StreamPipeline<T, E>` |
| `StreamPipeline.mergeAll(...streams)` | Merge multiple streams | `StreamPipeline<T, E>` |
| `.zipWith(other, fn)` | Combine two streams element-by-element | `StreamPipeline<V, E \| E2>` |
| `.interleave(other)` | Alternate items round-robin | `StreamPipeline<T, E>` |
| `.combineLatest(other)` | Emit on either side, pair with latest | `StreamPipeline<[T, U], E \| E2>` |
| `.withLatest(other)` | Enrich with latest from side stream | `StreamPipeline<[T, U], E \| E2>` |
| `.exhaustMap(fn)` | Flat-map, ignore new while previous active | `StreamPipeline<U, E \| E2>` |

### Flow Control

| Method | Description | Returns |
|--------|-------------|---------|
| `.take(n)` | Take first N items then stop | `StreamPipeline<T, E>` |
| `.takeWhile(fn)` | Take while predicate is true | `StreamPipeline<T, E>` |
| `.takeUntil(signal)` | Take until another stream emits | `StreamPipeline<T, E \| E2>` |
| `.drop(n)` | Skip first N items | `StreamPipeline<T, E>` |
| `.dropWhile(fn)` | Skip while predicate is true | `StreamPipeline<T, E>` |
| `.debounce(ms)` | Emit after quiet period | `StreamPipeline<T, E>` |
| `.metered(ms)` | Max 1 item per interval | `StreamPipeline<T, E>` |
| `.spaced(ms)` | Fixed delay between items | `StreamPipeline<T, E>` |
| `.sample(intervalMs)` | Emit latest at fixed intervals | `StreamPipeline<T, E>` |
| `.audit(ms)` | Emit latest value per time window | `StreamPipeline<T, E>` |
| `.pauseWhen(ref, pollMs?)` | Pause/resume via boolean ref | `StreamPipeline<T, E>` |
| `.interruptOn(signal)` | Stop on AbortSignal | `StreamPipeline<T, E>` |
| `.interruptAfter(ms)` | Stop after duration | `StreamPipeline<T, E>` |
| `.repeat()` | Infinitely repeat stream output | `StreamPipeline<T, E>` |
| `.repeatN(n)` | Repeat stream N times | `StreamPipeline<T, E>` |

### Error Handling

| Method | Description | Returns |
|--------|-------------|---------|
| `.orElse(fallback)` | Recover with fallback value and stop | `StreamPipeline<T, never>` |
| `.tapError(fn)` | Side-effect on error | `StreamPipeline<T, E>` |
| `.tapAnyError(fn)` | Side-effect on typed errors and defects | `StreamPipeline<T, E>` |
| `.trapError(...classes)` | Pull defect types into typed channel | `StreamPipeline<T, E \| InstanceType<...>>` |
| `.retry(policy?)` | Retry entire stream with exponential backoff | `StreamPipeline<T, E>` |
| `.timeout(ms)` | Fail if stream doesn't complete in time | `StreamPipeline<T, E>` |
| `.handleErrorWith(fn)` | Replace error with a fallback stream | `StreamPipeline<T, E>` |
| `.attempt()` | Wrap items in Right, error in Left | `StreamPipeline<Right<T> \| Left<E>, never>` |
| `.attemptCause()` | Wrap items in Right, any failure in Left | `StreamPipeline<Right<T> \| Left<Cause<E>>, never>` |

### Terminals

| Method | Description | Returns |
|--------|-------------|---------|
| `.forEach(fn)` | Process each item | `Promise<void>` |
| `.collect()` | Collect all items into an array | `Promise<T[]>` |
| `.reduce(initial, fn)` | Fold into a single value | `Promise<U>` |
| `.drain()` | Consume all, discard values | `Promise<void>` |
| `.runFirst()` | Get first item | `Promise<T \| undefined>` |
| `.collectFirst(predicate)` | First item matching predicate | `Promise<T \| undefined>` |
| `.collectWhile(predicate)` | Collect while predicate holds | `Promise<T[]>` |
| `.to(sink, params?)` | Publish to a Sinkable | `Promise<void>` |
| `.statefulMap(params)` | Flink-style keyed state processing | `StreamPipeline<U, E>` |
| `.finally(fn)` | Sync cleanup on stream end | `StreamPipeline<T, E>` |
| `.onFinalize(fn)` | Async cleanup on stream end | `StreamPipeline<T, E>` |
| `.toStream()` | Escape hatch to raw Effect Stream | `Stream<T, E>` |
