# @promin/core

TypeScript toolkit for resilient async operations, durable workflows, stream processing, and analytics. Built on [Effect](https://effect.website).

## Modules

### [Pipeline](src/lib/pipeline.ts) — Async actions with resilience

Chainable, lazily-evaluated wrapper over Effect. Retry, timeout, circuit breaker, caching, polling — without Effect expertise.

```typescript
const result = await Pipeline.fn(() => callService())
  .map((data) => transform(data))
  .retry({ maxRetries: 3, jitter: true })
  .timeout(5_000)
  .runPromise();
```

### [StreamPipeline](src/lib/stream-pipeline.ts) — Stream processing with backpressure

Parallel transforms, batching, deduplication, windowing. Automatic operator fusion.

```typescript
await StreamPipeline.fromAsyncIterable(messages, onError)
  .filter((msg) => msg.topic === "events")
  .parAsyncMap(10, (msg) => enrichFromDb(msg.payload))
  .groupWithin(500, 1_000)
  .forEach((batch) => db.bulkInsert(batch));
```

### [StreamTopology](src/lib/stream-topology/README.md) — Stateful stream processing

Kafka Streams / Flink-style: keyed state, time windows, joins, deduplication, checkpointing, distributed shuffle.

```typescript
const topology = StreamTopology.source(clicks)
  .keyBy((e) => e.userId)
  .shuffle() // repartition for multi-instance
  .tumbling(60_000)
  .count()
  .to(output);
```

### [Durable Workflows](src/lib/durable/README.md) — Crash-recoverable execution

DAG-based workflows with compensation (sagas), map steps, sleep/signal, retry, and visual editor schema.

```typescript
const orderFlow = workflow({ name: "process-order", storage })
  .stepAsync("validate", async ({ input }) => validate(input))
  .stepAsync("charge", async ({ prev }) => charge(prev), {
    compensate: async ({ result }) => refund(result),
  })
  .stepAsync("ship", async ({ prev }) => ship(prev))
  .build();
```

### [Distributed Workers](src/lib/distributed/README.md) — Multi-machine step execution

Coordinator + workers via Postgres queues. Route steps to specialized hardware (GPU, AI). Same workflow definition works in-process and distributed.

```typescript
const coordinator = createCoordinator({ storage, stepQueue, routing: { transcribe: "gpu" } });
await coordinator.submit({ workflow: processVideo, workflowId: "v1", input });
```

### [DataFrame](src/lib/dataframe/README.md) — Lazy analytics

Composable DataFrame with expression builder, window functions, joins, and pluggable executors (Array or DuckDB).

```typescript
const result = await DataFrame.fromRows(sales)
  .filter(col("amount").gt(lit(100)))
  .groupBy("region")
  .agg({ total: { column: "amount", fn: "sum" } })
  .sort("total", "desc")
  .collect();
```

### Additional Modules

| Module                                    | Description                                                               |
| ----------------------------------------- | ------------------------------------------------------------------------- |
| [RawStream](src/lib/raw-stream.ts)        | Zero-overhead stream for hot paths (~4x faster than StreamPipeline)       |
| [Stream Pipes](src/lib/stream-pipes.ts)   | Reusable `through()` transforms: CSV, JSONL, XML, base64, length-prefixed |
| [Data Profiler](src/lib/data-profiler/)   | Column statistics, correlations, quality warnings                         |
| [Data Diff](src/lib/data-diff/)           | Row-level change detection between datasets                               |
| [Data Quality](src/lib/data-quality/)     | Declarative expectations and validation suites                            |
| [Data Contracts](src/lib/data-contracts/) | Schema + expectations + SLA definitions                                   |
| [SQL Models](src/lib/sql-models/)         | dbt-style SQL model DAG with materialization                              |
| [Scheduler](src/lib/scheduler/)           | Cron + rrule-based workflow scheduling                                    |
