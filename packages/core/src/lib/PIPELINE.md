# Pipeline

`Pipeline<T, E>` is a lazy, composable async action. It wraps Effect with a fluent API — retry, timeout, circuit breaker, caching, and parallel composition without Effect expertise.

## Why not just Promises?

Promises work for simple cases. But real production code needs resilience, and Promises don't compose it well:

**Retry** — You write a for-loop with try/catch, handle backoff, decide which errors to retry. Every call site re-invents this.

```typescript
import { Pipeline } from "@promin/core";

// Promise: 15 lines of retry logic
let result;
for (let i = 0; i < 3; i++) {
  try {
    result = await fetchUser(id);
    break;
  } catch (e) {
    if (i === 2) throw e;
    await sleep(500 * Math.pow(2, i));
  }
}

// Pipeline: 1 line
const result = await Pipeline.fn(() => fetchUser(id))
  .retry({ maxRetries: 3, baseDelayMs: 500 })
  .runPromise();
```

**Timeout + retry** — With Promises, do you timeout each attempt or the whole chain? You need nested `AbortController` + `setTimeout` + retry loop. With Pipeline, they compose:

```typescript
// Each attempt retries up to 3x, but the entire operation times out at 10s
const result = await Pipeline.fn(() => fetchUser(id))
  .retry(3)
  .timeout(10_000)
  .runPromise();
```

**Parallel with per-branch resilience** — `Promise.all` fails on the first error. You need `Promise.allSettled` + manual result unwrapping. With Pipeline:

```typescript
const [user, videos, stats] = await Pipeline.all(
  fetchUser(id).retry(3),
  fetchVideos(id).orElse([]), // fallback to empty on failure
  fetchStats(id).cached(statsCache), // serve from cache for 5min
)
  .timeout(10_000)
  .runPromise();
```

**Cancellation** — When you `Promise.race` two fetches, the loser keeps running. Pipeline cancels it:

```typescript
// Loser is immediately cancelled — no wasted resources
const fastest = await Pipeline.race(callOpenAI(prompt), callAnthropic(prompt)).runPromise();
```

**Error typing** — Promises have `catch(e: unknown)`. Pipeline errors are typed and matchable:

```typescript
const { data, error } = await fetchUser(id).runSafe();
if (error) {
  switch (error._tag) {
    case "NotFound":
      return null;
    case "RateLimit":
      await sleep(error.retryAfterMs);
      break;
  }
}
```

**Composition** — "fetch user, then fetch their posts" is nested callbacks with Promises. Pipeline chains:

```typescript
const posts = await fetchUser(id)
  .flatMap((user) => fetchPosts(user.id))
  .map((posts) => posts.filter((p) => p.published))
  .runPromise();
```

## Why not raw Effect?

Effect is powerful but has a steep learning curve. Pipeline gives you 80% of the patterns without:

- Learning the Effect type system (`Effect<A, E, R>`, layers, services)
- Managing fibers manually
- Understanding the `pipe` / `gen` / `Do` notation styles
- Dealing with `Layer`, `Context`, and dependency injection

If you need the full power, `.toEffect()` drops down to raw Effect at any point.

## API Reference

### Creation

| Method | Description | Returns |
|--------|-------------|---------|
| `Pipeline.fn(fn)` | Create from a Promise-returning function | `Pipeline<T, never>` |
| `Pipeline.fromPromise(fn)` | Create from a Promise-returning function | `Pipeline<T, never>` |
| `Pipeline.from(effect, options?)` | Create from a raw Effect | `Pipeline<T, E>` |
| `Pipeline.succeed(value)` | Create a pipeline that succeeds immediately | `Pipeline<T, never>` |
| `Pipeline.fail(error)` | Create a pipeline that fails immediately | `Pipeline<never, E>` |
| `Pipeline.sleep(ms)` | Sleep for a duration | `Pipeline<void, never>` |
| `Pipeline.scoped({ acquire, release, use })` | Resource acquisition with guaranteed release | `Pipeline<T, E>` |

### Transform

| Method | Description | Returns |
|--------|-------------|---------|
| `.map(fn)` | Transform the success value | `Pipeline<U, E>` |
| `.mapAsync(fn)` | Async transform via Promise | `Pipeline<U, E>` |
| `.mapError(fn)` | Transform the error type | `Pipeline<T, E2>` |
| `.flatMap(fn)` | Chain a dependent Pipeline | `Pipeline<U, E \| E2>` |
| `.flatMapAsync(fn)` | Chain a dependent async action | `Pipeline<U, E>` |
| `.tap(fn)` | Sync side-effect without changing value | `Pipeline<T, E>` |
| `.tapAsync(fn)` | Async side-effect, awaits before continuing | `Pipeline<T, E>` |
| `.tapAsyncFork(fn)` | Non-blocking background side-effect | `Pipeline<T, E>` |
| `.tapFork(fn)` | Fork a side-effect Pipeline in background | `Pipeline<T, E>` |
| `.tapPipeline(fn)` | Run a side-effect Pipeline, await completion | `Pipeline<T, E \| E2>` |
| `.filter({ predicate, orFail })` | Fail if predicate returns false | `Pipeline<T, E \| E2>` |
| `.delay(ms)` | Delay execution by fixed duration | `Pipeline<T, E>` |
| `.when(condition)` | Conditionally execute | `Pipeline<T \| undefined, E>` |

### Resilience

| Method | Description | Returns |
|--------|-------------|---------|
| `.retry(policyOrMaxRetries?)` | Retry on typed errors | `Pipeline<T, E>` |
| `.retryAll(policy?)` | Retry on all outcomes (errors, defects, success values) | `Pipeline<T, E>` |
| `.timeout(ms)` | Total timeout including retries | `Pipeline<T, E \| TimeoutError>` |
| `.race(...others)` | First to succeed wins, losers cancelled | `Pipeline<T, E>` |
| `.concurrently(...others)` | Run in parallel, return all results as tuple | `Pipeline<[T, ...], E \| ...>` |
| `.withPermit(semaphore)` | Acquire semaphore permit before running | `Pipeline<T, E>` |
| `.withCircuitBreaker(breaker)` | Fail fast when circuit is open | `Pipeline<T, E \| CircuitOpenError>` |
| `.cached(cache)` | Return cached value if fresh | `Pipeline<T, E>` |
| `.cachedBy(store, key, options?)` | Cache by key in a CacheStore | `Pipeline<T, E>` |
| `.repeat({ times, intervalMs? })` | Repeat N times with optional interval | `Pipeline<T, E>` |
| `.supervised(params?)` | Long-lived process that restarts on failure | `Pipeline<void, never>` |

### Error Handling

| Method | Description | Returns |
|--------|-------------|---------|
| `.orElse(fallback)` | Recover with a constant fallback value | `Pipeline<T, never>` |
| `.catch(tag, fn)` | Catch specific error tag and recover | `Pipeline<T, Exclude<E, { _tag }>>` |
| `.handleError(fn)` | Map any error to a value | `Pipeline<T \| U, never>` |
| `.handleErrorWith(fn)` | Recover by running a different Pipeline | `Pipeline<T \| U, E2>` |
| `.handleErrorAsync(fn)` | Recover with an async function | `Pipeline<T \| U, never>` |
| `.recover(predicate, fn)` | Partial recovery — matching errors only | `Pipeline<T \| U, E>` |
| `.recoverWith(predicate, fn)` | Partial recovery via Pipeline | `Pipeline<T \| U, E \| E2>` |
| `.recoverAsync(predicate, fn)` | Partial recovery via async function | `Pipeline<T \| U, E>` |
| `.redeem(onError, onSuccess)` | Transform both error and success channels | `Pipeline<B, never>` |
| `.redeemWith(onError, onSuccess)` | Transform both channels via Pipeline | `Pipeline<B, E2>` |
| `.redeemAsync(onError, onSuccess)` | Transform both channels via async | `Pipeline<B, never>` |
| `.tapError(fn)` | Side-effect on error | `Pipeline<T, E>` |
| `.tapCause(fn)` | Side-effect on full Cause | `Pipeline<T, E>` |
| `.tapAnyError(fn)` | Side-effect on typed errors and defects | `Pipeline<T, E>` |
| `.trapError(...classes)` | Pull defect types into typed error channel | `Pipeline<T, E \| InstanceType<...>>` |

### Polling

| Method | Description | Returns |
|--------|-------------|---------|
| `.pollUntil({ until, intervalMs?, ... })` | Repeat until condition is met | `Pipeline<T, E \| PollTimeoutError>` |
| `.pollUntilWithBackoff({ until, ... })` | Poll with exponential backoff | `Pipeline<T, E \| PollTimeoutError>` |

### Static Combinators

| Method | Description | Returns |
|--------|-------------|---------|
| `Pipeline.all(...pipelines)` | Parallel execution, all must succeed | `Pipeline<[...], E>` |
| `Pipeline.allSettled(...pipelines)` | Parallel, returns Either for each | `Pipeline<[Either<T,E>...], never>` |
| `Pipeline.validate(...pipelines)` | Parallel, accumulates all errors | `Pipeline<[...], E>` |
| `Pipeline.race(...pipelines)` | First to succeed wins | `Pipeline<T, E>` |
| `Pipeline.fallback(...pipelines)` | Try in order, first success wins | `Pipeline<T, E>` |
| `Pipeline.hedged(pipeline, { hedgeDelayMs })` | Start backup after delay, first wins | `Pipeline<T, E>` |
| `Pipeline.forEach(items, fn, { concurrency })` | Map over items with bounded concurrency | `Pipeline<U[], E>` |

### Observability

| Method | Description | Returns |
|--------|-------------|---------|
| `.withSpan(name)` | Annotate with a tracing span | `Pipeline<T, E>` |
| `.withTag(key, value)` | Add span attribute | `Pipeline<T, E>` |

### Execution (Terminals)

| Method | Description | Returns |
|--------|-------------|---------|
| `.runPromise()` | Execute and return Promise | `Promise<T>` |
| `.runSafe(options?)` | Execute, return `{ data, error }` — never throws | `Promise<{ data: T, error: null } \| { data: null, error: E }>` |
| `.runEither()` | Execute and return Either | `Promise<Either<T, E>>` |
| `.finally(fn)` | Run cleanup on success or failure | `Pipeline<T, E>` |
| `.onInterrupt(fn)` | Run cleanup on interruption only | `Pipeline<T, E>` |
| `.toEffect()` | Escape hatch to raw Effect | `Effect<T, E>` |
| `.toEffectStream()` | Convert to single-element Stream | `Stream<T, E>` |
