# Promin

TypeScript toolkit for resilient async operations, durable workflows, stream processing, and analytics. Built on [perfect](https://github.com/spilne/perfect), Bun, and Postgres.

## Why Promin?

Production services need retry, timeout, circuit breakers, backpressure, crash recovery, and observability. Most TypeScript tools solve one of these — Promin composes them all:

- **Durable workflows** — DAG-based steps that survive crashes. Compensation (sagas), signals, sleep. Not just a job queue.
- **DataFrame** — lazy analytics with expression builder. Array executor for small data, DuckDB for large. Not just `Array.filter().map()`.

All of these compose on [perfect](https://github.com/spilne/perfect) (`@spilne/perfect-core`) — the effect runtime that provides retry, timeout, circuit breakers, streams with backpressure, and concurrency primitives. A workflow step can be a perfect `Eff` with retry. A DataFrame query can run inside a durable step. One type system, one runtime.

## Packages

| Package                                                    | Description                                                                                       |
| ---------------------------------------------------------- | ------------------------------------------------------------------------------------------------- |
| **[@promin/workflow](./packages/workflow/)**               | Durable workflows, distributed workers, state machines, scheduler, SQL models                     |
| **[@promin/data](./packages/data/)**                       | DataFrame, data quality, profiling, diff, contracts                                               |
| **[@promin/duckdb](./packages/duckdb/)**                   | DuckDB executor for DataFrame — SQL compilation, file sources                                     |
| **[@promin/postgres](./packages/postgres/)**               | Postgres workflow storage, step queue (SKIP LOCKED), durable scheduler, leader leases, registries |
| **[@promin/redis](./packages/redis/)**                     | Redis workflow storage, step queue, scheduler, state machines (Cluster-safe)                      |
| **[@promin/sqlite](./packages/sqlite/)**                   | SQLite workflow storage, step queue, scheduler, leader leases                                     |
| **[@promin/workflow-remote](./packages/workflow-remote/)** | Workflow storage and worker RPC over HTTP                                                         |
| **[@promin/container](./packages/container/)**             | Container step executor (Docker, K8s, local process)                                              |

The effect runtime, streams and concurrency primitives come from [perfect](https://github.com/spilne/perfect) (`@spilne/perfect-core`); HTTP client, Kafka transport, and stateful stream topology live there too: `@spilne/perfect-http`, `@spilne/perfect-kafka`, `@spilne/perfect-topology`.

## Quick Start

```bash
bun install
```

```typescript
import { TaggedError, tryPromise } from "@spilne/perfect-core";
import { col, DataFrame } from "@promin/data";
import { createWorkflowRunner, InMemoryWorkflowStorage, workflow } from "@promin/workflow";

class FetchError extends TaggedError("FetchError")<{ message: string }>() {}
class TimeoutError extends TaggedError("TimeoutError")<{ message: string }>() {}

// perfect — typed async effects with retry and timeout
const data = await tryPromise(
  () => fetch("https://api.example.com/data").then((r) => r.json()),
  (e) => new FetchError({ message: String(e) }),
)
  .retry({ times: 3, backoff: "exponential" })
  .timeoutFail(5_000, () => new TimeoutError({ message: "slow" }))
  .orDie()
  .run();

// DataFrame — analytics with pluggable executors
declare const sales: { region: string; revenue: number }[];
const topRegions = await DataFrame.fromArray(sales)
  .filter(col("revenue").gt(1000))
  .groupBy("region")
  .agg({ revenue: "sum" })
  .sort("revenue", "desc")
  .limit(10)
  .collect();

// Durable workflow — survives crashes, waits for signals
declare function submitCheck(input: { userId: string }): Promise<{ checkId: string }>;
const kyc = workflow<{ userId: string }>({ name: "kyc" })
  .stepAsync("submit-check", ({ input }) => submitCheck(input))
  .waitForSignal<{ passed: boolean }>("result", {
    signalName: "check-done",
    timeoutMs: 30 * 60_000,
  })
  .stepAsync("decide", async ({ prev }) => (prev.passed ? "approved" : "rejected"))
  .build();

const runner = createWorkflowRunner({ storage: new InMemoryWorkflowStorage() });
const handle = await runner.start({
  workflow: kyc,
  workflowId: "kyc-123",
  input: { userId: "u1" },
});
await handle.signal({ signalName: "check-done", payload: { passed: true } });
const status = await runner.getStatus({ workflowId: "kyc-123" });
console.log(data, topRegions, status?.state);
```

## Development

```bash
bun install          # install dependencies
bun run test         # unit tests
bun run bench        # cross-language benchmarks (vs Pandas/Polars)
bun run bench:all    # all benchmark suites
```

### Commands

| Command                        | Description                                                             |
| ------------------------------ | ----------------------------------------------------------------------- |
| `bun run test`                 | Tests (workflow, data, postgres, redis; store tests use testcontainers) |
| `bun run bench`                | Cross-language benchmarks (Promin vs Pandas vs Polars)                  |
| `bun run bench:all`            | All benchmarks (dataframe, workflow, cross-language)                    |
| `bun nx run-many -t typecheck` | Typecheck all packages                                                  |
| `bun nx run-many -t lint`      | Lint all packages                                                       |

## Architecture

```
@spilne/perfect-core (external)
  Eff<A,S>, Stream<A,S>  — effect runtime, streams, retry, concurrency primitives

@promin/workflow
  workflow()             — durable workflows: DAG steps, journaled steps, signals, sleep, sagas
  /distributed           — distributed runner + workers over a step queue
  /scheduler             — cron / RRULE / interval schedules, fenced leader leases

@promin/data (no native deps)
  DataFrame<T>           — lazy analytics with pluggable executors

@promin/duckdb (optional, adds DuckDB)
  DuckDBExecutor         — compiles DataFrame plans to SQL
  AutoExecutor           — smart routing: Array for small, DuckDB for large

@promin/postgres, @promin/redis, @promin/sqlite, @promin/workflow-remote
  Workflow storage, step queue and scheduler backends (and HTTP transport)
```

## Technology Stack

| Purpose        | Library                          |
| -------------- | -------------------------------- |
| Runtime        | Bun                              |
| Language       | TypeScript                       |
| FP/Concurrency | perfect (`@spilne/perfect-core`) |
| Validation     | Zod                              |
| Monorepo       | Nx                               |
| Linting        | oxlint                           |
| Formatting     | oxfmt                            |
| Testing        | bun:test                         |
| Benchmarking   | mitata                           |

## Documentation

- **[Docs](./documentation/)** — full documentation (build with `bun run docs`)
- **[Examples](./examples/)** — real-world scenarios, ordered simple → advanced
- **[Glossary](./documentation/content/docs/reference/glossary.mdx)** — Promin concepts mapped to Temporal, Airflow, Spark, Pandas/Polars
- **[perfect](https://github.com/spilne/perfect)** — the effect runtime promin is built on, with its own guide and examples
