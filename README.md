# Promin

TypeScript platform for pipelines, workflows, streaming, and data processing. Built on Effect, Bun, and Postgres.

## Packages

| Package | Description |
|---|---|
| **[@promin/core](./packages/core/)** | Pipeline, StreamPipeline, DataFrame, durable workflows, stream topology |
| **[@promin/http](./packages/http/)** | HTTP client with retry, streaming (SSE/NDJSON), circuit breaker |
| **[@promin/duckdb](./packages/duckdb/)** | DuckDB executor for DataFrame — SQL compilation, file sources |
| **[@promin/kafka](./packages/kafka/)** | Kafka transport adapter (Partitionable, Acknowledgeable) |
| **[@promin/postgres](./packages/postgres/)** | Postgres workflow storage, step queue (SKIP LOCKED), durable scheduler |
| **[@promin/redis](./packages/redis/)** | Redis stream transport adapter |
| **[@promin/container](./packages/container/)** | Container step executor (Docker, K8s, local process) |

## Quick Start

```bash
bun install
```

```typescript
import { Pipeline, StreamPipeline, DataFrame, workflow, col } from "@promin/core";

// Pipeline — composable async operations with retry, timeout, concurrency
const result = await Pipeline.fromPromise(() => fetch("/api/data"))
  .map(r => r.json())
  .retry(3)
  .timeout(5_000)
  .runPromise();

// StreamPipeline — streaming with automatic operator fusion
await StreamPipeline.fromAsyncIterable(events)
  .map(transform)       // ┐
  .filter(isValid)       // ├ fused into single mapChunks (2x faster)
  .tap(log)              // ┘
  .parAsyncMap(10, enrich)
  .groupWithin(500, 1_000)
  .forEach(batch => db.bulkInsert(batch));

// DataFrame — analytics with pluggable executors
const topRegions = await DataFrame.fromArray(sales)
  .filter(col("revenue").gt(1000))
  .groupBy("region")
  .agg({ revenue: "sum" })
  .sort("revenue", "desc")
  .limit(10)
  .collect();

// Durable workflow — survives crashes, supports signals
const kyc = workflow<KycInput>({ name: "kyc", storage })
  .stepAsync("validate", async ({ input }) => validate(input))
  .stepAsync("submit-check", async ({ input }) => submitCheck(input))
  .waitForSignal<CheckResult>("result", { signalName: "check-done", timeoutMs: 30 * 60_000 })
  .stepAsync("decide", async ({ prev }) => prev.passed ? approve() : reject())
  .build();

await kyc.runSafe({ workflowId: "kyc-123", input });
const status = await kyc.getStatus("kyc-123");
```

## Development

```bash
bun install          # install dependencies
bun run test         # unit tests
bun run bench        # cross-language benchmarks (vs Pandas/Polars)
bun run bench:all    # all benchmark suites
```

### Commands

| Command | Description |
|---|---|
| `bun run test` | Unit tests (core, http, kafka, postgres) |
| `bun run test:integration` | Integration tests (Kafka, Redis, Postgres) |
| `bun run bench` | Cross-language benchmarks (Promin vs Pandas vs Polars) |
| `bun run bench:all` | All benchmarks (stream, pipeline, dataframe, topology, cross-language) |
| `bun nx run-many -t typecheck` | Typecheck all packages |
| `bun nx run-many -t lint` | Lint all packages |

## Architecture

```
@promin/core (no native deps)
  Pipeline<T,E>          — composable async operations
  StreamPipeline<T,E>    — streaming with operator fusion
  RawStream<T>           — zero-overhead stream (no Effect)
  DataFrame<T>           — lazy analytics with pluggable executors
  workflow()             — durable workflows with DAG, signals, sleep
  StreamTopology         — stateful stream processing (windows, joins)
  Distributed            — coordinator + workers via Postgres SKIP LOCKED

@promin/duckdb (optional, adds DuckDB)
  DuckDBExecutor         — compiles DataFrame plans to SQL
  AutoExecutor           — smart routing: Array for small, DuckDB for large

@promin/http
  HttpClient             — retry, circuit breaker, SSE/NDJSON streaming

@promin/kafka, @promin/redis, @promin/postgres
  Transport adapters implementing Streamable/Sinkable/Partitionable
```

## Technology Stack

| Purpose | Library |
|---|---|
| Runtime | Bun |
| Language | TypeScript |
| FP/Concurrency | Effect |
| Validation | Zod |
| Monorepo | Nx |
| Linting | oxlint |
| Formatting | oxfmt |
| Testing | bun:test |
| Benchmarking | mitata |
