# StreamPipeline

`StreamPipeline<T, E>` is a chainable, lazily-evaluated streaming pipeline. Nothing executes until you call a terminal — `.forEach()`, `.collect()`, `.reduce()`, or `.drain()`. Backpressure is built-in: the producer only advances when the consumer is ready.

Adjacent pure operators (`.map()`, `.filter()`, `.tap()`) are automatically fused into a single `mapChunks` call — typically 2-3x faster for chained pure operations.

## Creating streams

```typescript
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
