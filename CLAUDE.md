# CLAUDE.md

Project-level instructions for AI agents working on this codebase.

## Project Overview

**Nx monorepo** with **Bun** runtime, **Effect** for functional programming, and **Zod** validation. Linting via **oxlint**, formatting via **oxfmt**.

### Directory Layout

```
libs/
  pipeline/            # Effect-based Pipeline<T,E> and StreamPipeline<T,E> primitives
  http/                # Effect-based HTTP client with pipelines, streaming, retry
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

- **Files**: kebab-case with suffix (`http-client.ts`, `stream-pipeline.ts`)
- **Classes**: PascalCase, prefix `Default` for implementations (`DefaultHttpClient`)
- **Interfaces**: PascalCase, no `I` prefix (`HttpClient`, not `IHttpClient`)
- **Functions**: camelCase, prefix `create` for factories
- **Constants**: UPPER_SNAKE_CASE
- **Schemas**: PascalCase with `Schema` suffix (`GenerateRequestSchema`)
- **Types**: Inferred from schemas via `z.infer<typeof Schema>`

## Testing

- **Framework**: `bun:test` (`describe`, `it`, `expect`)
- **Mocking**: Manual mock implementations of interfaces (no mocking library)
- **Pattern**: Arrange-Act-Assert with helper factories for mocks
- **Location**: Co-located `*.test.ts` files next to source

## Running Typecheck

Use nx to run typecheck (this is what CI does):

```bash
bun nx run @ts-backend/core:typecheck
bun nx run @ts-backend/http:typecheck
```

## Key Libraries

| Purpose | Library |
|---------|---------|
| Runtime | Bun |
| Validation | Zod |
| FP/Concurrency | Effect |
| HTTP Client | @effect/platform |
| Monorepo | Nx |
| Linting | oxlint |
| Formatting | oxfmt |
