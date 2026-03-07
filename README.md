# TypeScript Backend

Effect-based pipeline and HTTP client libraries built with Nx monorepo and Bun runtime.

## Pre-requisites

### Install Bun

See the official installation guide: [Bun Installation](https://bun.sh/docs/installation)

## Libraries

- **[@promin/core](./packages/core/)** - Effect-based `Pipeline<T,E>` and `StreamPipeline<T,E>` primitives for composable, retry-aware data processing
- **[@promin/http](./packages/http/)** - Effect-based HTTP client with chainable pipelines, streaming (SSE/NDJSON), retry, polling, and parallel execution

## Development

```bash
bun install
```

### Typecheck

```bash
bun nx run-many -t typecheck
```

### Test

```bash
bun nx run-many -t test
```

### Lint & Format

```bash
bun nx run-many -t lint
bun nx run-many -t format
```

### Full CI

```bash
bun nx run-many -t ci
```

## Technology Stack

- **Runtime:** Bun
- **Language:** TypeScript 5.9
- **FP/Concurrency:** Effect
- **HTTP Client:** @effect/platform
- **Validation:** Zod
- **Build Tool:** Nx
- **Testing:** Bun test
- **Linting:** oxlint
- **Formatting:** oxfmt
