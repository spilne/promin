# @promin/core

Resilient async primitives and stream processing. Built on [Effect](https://effect.website).

## Modules

### [Pipeline](src/lib/pipeline.ts) — Async actions with resilience

Chainable, lazily-evaluated wrapper over Effect. Retry, timeout, circuit breaker, caching, polling — without Effect expertise.

```typescript
import { Pipeline } from "@promin/core";

const result = await Pipeline.fn(() => callService())
  .map((data) => transform(data))
  .retry({ maxRetries: 3, jitter: true })
  .timeout(5_000)
  .runPromise();
```

### [StreamPipeline](src/lib/stream-pipeline.ts) — Stream processing with backpressure

Parallel transforms, batching, deduplication, windowing. Automatic operator fusion.

```typescript
import { StreamPipeline } from "@promin/core";

await StreamPipeline.fromAsyncIterable(messages, onError)
  .filter((msg) => msg.topic === "events")
  .parAsyncMap(10, (msg) => enrichFromDb(msg.payload))
  .groupWithin(500, 1_000)
  .forEach((batch) => db.bulkInsert(batch));
```

### Additional Modules

| Module                                  | Description                                                               |
| --------------------------------------- | ------------------------------------------------------------------------- |
| [RawStream](src/lib/raw-stream.ts)      | Zero-overhead stream for hot paths (~4x faster than StreamPipeline)       |
| [Stream Pipes](src/lib/stream-pipes.ts) | Reusable `through()` transforms: CSV, JSONL, XML, base64, length-prefixed |
| [Primitives](src/lib/PRIMITIVES.md)     | Concurrency primitives: Semaphore, Latch, Barrier, Channel, Queue, PubSub |

## Related Packages

| Package                                   | Description                                            |
| ----------------------------------------- | ------------------------------------------------------ |
| [@promin/workflow](../workflow/README.md) | Durable workflows, distributed workers, state machines |
| [@promin/data](../data/README.md)         | DataFrame, data quality, profiling, diff, contracts    |
| [@promin/topology](../topology/README.md) | Stateful stream processing: windows, joins, shuffle    |
