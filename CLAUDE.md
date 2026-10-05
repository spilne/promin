# CLAUDE.md

Project-level instructions for AI agents working on this codebase.

## Project Overview

**Nx monorepo** with **Bun** runtime, **perfect** (`@spilne/perfect-core`) for effects, streams and concurrency, and **Zod** validation. Linting via **oxlint**, formatting via **oxfmt**.

### Directory Layout

```
packages/
  workflow/            # Durable workflows, DAG steps, distributed workers, scheduler, state machines
                       #   (subpaths: ./distributed ./scheduler ./discovery ./sql-models ./storage-kit ./testing ./dev)
  data/                # Lazy DataFrame, expression builder, data quality, profiling, diff
  duckdb/              # DuckDB executor for analytical queries
  postgres/            # Postgres workflow storage, step queue, scheduler, leader leases, registries
  redis/               # Redis workflow storage, step queue, scheduler, state machines (Cluster-safe)
  sqlite/              # SQLite workflow storage, step queue, scheduler, leases; rate limiter, throttle, queue
  container/           # Docker / Kubernetes step execution
  agent/               # Durable AI agent loops and tool orchestration
  evals/               # Agent evaluation framework
  workflow-remote/     # HTTP/RPC adapters for remote workflow storage and workers
  zorya/               # Self-hostable workflow platform server + dashboard
  zorya-client/        # Zorya client SDK
```

## TypeScript Conventions

### Prefer Objects for Multiple Parameters

When a function takes **2 or more parameters**, use a single options/params object instead of positional arguments.

```typescript
// ❌ BAD
function searchVideos(query: string, order: string, videoDuration?: string) { ... }

// ✅ GOOD
function searchVideos(params: { query: string; order: string; videoDuration?: string }) { ... }
```

### Naming Conventions

- **Files**: kebab-case with suffix (`workflow-runner.ts`, `stream-pipeline.ts`)
- **Classes**: PascalCase. Lifecycle services are interfaces built by a
  `create*` factory (`createWorkflowRunner` returns a `WorkflowRunner`); their
  `Default*` classes stay internal. Concrete stores, queues, registries and
  schedulers are classes built with `new` (`InMemoryWorkflowStorage`,
  `DurableScheduler`), with no parallel factory.
- **Interfaces**: PascalCase, no `I` prefix (`WorkflowRunner`, not `IWorkflowRunner`)
- **Functions**: camelCase, prefix `create` for factories
- **Constants**: UPPER_SNAKE_CASE
- **Schemas**: PascalCase with `Schema` suffix (`GenerateRequestSchema`)
- **Types**: Inferred from schemas via `z.infer<typeof Schema>`

## Testing

- **Framework**: `bun:test` (`describe`, `it`, `expect`)
- **Mocking**: Manual mock implementations of interfaces (no mocking library)
- **Pattern**: Arrange-Act-Assert with helper factories for mocks
- **Location**: `*.test.ts` files live in a `tests/` subfolder beside the
  source they cover (e.g. `src/lib/durable/tests/foo.test.ts` tests
  `src/lib/durable/foo.ts`). Bench files (`*.bench.ts`) and type-fixture
  files (`*.type-fixture.ts`) stay co-located with source.

### Time-sensitive code: use `WallClock`, never `Date.now()` or `setTimeout`

Any new subsystem that does time math — duration tracking, deadline
checks, retry backoff, heartbeats, periodic polls, expiry windows —
takes a `clock?: WallClock` config field, defaults to `SystemWallClock`, and
funnels every time read or scheduled callback through it. `WallClock`
lives in `@promin/workflow` (inside workflow, import it relatively from
`src/lib/shared/wall-clock.ts`); it is a callback-based clock and is not
perfect's fiber-level `Clock` service:

```ts
import { SystemWallClock, type WallClock } from "@promin/workflow";

export interface FooConfig {
  // ...
  /** Time source. Default: `SystemWallClock`. Tests pass a `FakeWallClock`. */
  clock?: WallClock;
}

export class Foo {
  private readonly clock: WallClock;
  constructor(config: FooConfig) {
    this.clock = config.clock ?? SystemWallClock;
  }

  async run() {
    const start = this.clock.currentTimeMs(); // NOT Date.now()
    await something();
    const duration = this.clock.currentTimeMs() - start;

    // Interval + timeout go through the clock too so FakeWallClock.advance(ms)
    // can drive them deterministically in tests.
    const handle = this.clock.setInterval(() => tick(), 1_000);
    await new Promise<void>((r) => this.clock.setTimeout(() => r(), 500));
    handle.clear();
  }
}
```

Tests swap in `FakeWallClock` and call `clock.advance(ms)` to both move time
and fire any due callbacks, synchronously:

```ts
import { FakeWallClock } from "@promin/workflow";

const clock = FakeWallClock.create(0);
const foo = new Foo({ clock });
const done = foo.run();

// Wait until the code under test has parked on a clock timer, then advance.
while (clock.pendingCount() === 0) await new Promise((r) => setTimeout(r, 0));
clock.advance(500);
await done;
```

Never sleep a fixed amount of real time before `advance()`: under load the
code may not have registered its timer yet, `advance()` fires nothing, and
the test hangs or flakes. Wait on `clock.pendingCount()` or another
observable condition (a storage row, a status) with event-loop yields, and
bound the wait by an iteration count rather than by wall time. Real timers
belong only in tests that are about real timers.

When time sensitivity crosses a client/server boundary, compare
timestamps from the same clock: a column the app stamps with its clock
(e.g. `completed_at`) must not be bounded by the database's `NOW()`, and
a column the database stamps (e.g. `created_at` defaulting to `NOW()`)
must not be bounded by an app-side `new Date()` — the two clocks skew by
milliseconds or more. When no bound is needed, leave the window open
rather than picking either clock. See `PgStepQueue.metrics`. Expiry that
several processes must agree on — workflow locks, state-machine locks,
leader leases — is computed and compared on the database server (`NOW()`
in Postgres, `PX` keys in Redis), never with an app-side `Date`; only the
single-host stores (in-memory, SQLite) use the injected `WallClock` for it.

## Running Typecheck

Use nx to run typecheck (this is what CI does):

```bash
bun nx run @promin/workflow:typecheck
bun nx run @promin/data:typecheck
```

## Key Libraries

| Purpose        | Library                          |
| -------------- | -------------------------------- |
| Runtime        | Bun                              |
| Validation     | Zod                              |
| FP/Concurrency | perfect (`@spilne/perfect-core`) |
| Monorepo       | Nx                               |
| Linting        | oxlint                           |
| Formatting     | oxfmt                            |
