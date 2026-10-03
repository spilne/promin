# CLAUDE.md

Project-level instructions for AI agents working on this codebase.

## Project Overview

**Nx monorepo** with **Bun** runtime, **Effect** for functional programming, and **Zod** validation. Linting via **oxlint**, formatting via **oxfmt**.

### Directory Layout

```
packages/
  core/                # Effect-based Pipeline<T,E> and StreamPipeline<T,E> primitives
  postgres/            # Postgres state backend, step queue, change streams
  redis/               # Redis state backend and cache store
  duckdb/              # DuckDB executor for analytical queries
  container/           # DI container
  integration/         # Integration tests (testcontainers: Redis, Postgres)
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
- **Classes**: PascalCase, prefix `Default` for implementations (`DefaultWorkflowRunner`)
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
    const start = this.clock.currentTimeMs();  // NOT Date.now()
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

// Hand off a microtask so async internals reach their scheduled callbacks.
await Promise.resolve();
clock.advance(500);
await done;
```

When time sensitivity crosses a client/server boundary (e.g. Postgres
`created_at` vs app-side `until`), prefer letting the server clock decide
— embed `NOW()` directly in the SQL rather than binding a client-side
`new Date()` that can skew by a few ms. See `PgStepQueue.metrics` for
the pattern.

## Running Typecheck

Use nx to run typecheck (this is what CI does):

```bash
bun nx run @promin/core:typecheck
bun nx run @promin/workflow:typecheck
```

## Key Libraries

| Purpose | Library |
|---------|---------|
| Runtime | Bun |
| Validation | Zod |
| FP/Concurrency | Effect |
| Monorepo | Nx |
| Linting | oxlint |
| Formatting | oxfmt |
