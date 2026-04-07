# Pipeline

`Pipeline<T, E>` is a lazy, composable async action. It wraps Effect with a fluent API — retry, timeout, circuit breaker, caching, and parallel composition without Effect expertise.

## Why not just Promises?

Promises work for simple cases. But real production code needs resilience, and Promises don't compose it well:

**Retry** — You write a for-loop with try/catch, handle backoff, decide which errors to retry. Every call site re-invents this.

```typescript
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
