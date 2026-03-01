# Pipeline vs Promise vs raw Effect — Code Comparison

Side-by-side comparison showing how Pipeline reduces boilerplate vs writing the same logic with Promises or raw Effect.

---

## 1. Fetch with retry and timeout

### Promise (17 lines)

```typescript
async function fetchWithRetry(url: string, retries = 3, baseDelay = 250, timeout = 5000) {
  for (let attempt = 0; attempt <= retries; attempt++) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeout);
    try {
      const res = await fetch(url, { signal: controller.signal });
      clearTimeout(timer);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      return await res.json();
    } catch (err) {
      clearTimeout(timer);
      if (attempt === retries) throw err;
      await new Promise((r) => setTimeout(r, baseDelay * 2 ** attempt));
    }
  }
}
const data = await fetchWithRetry("/api/data");
```

### Raw Effect (21 lines)

```typescript
const data = await Effect.runPromise(
  httpRequest({ url: "/api/data", schema: DataSchema }).pipe(
    Effect.retry(
      Schedule.intersect(
        Schedule.jittered(Schedule.exponential(Duration.millis(250), 2)),
        Schedule.recurs(3),
      ).pipe(
        Schedule.whileInput(
          (e: HttpClientError) =>
            e._tag === "HttpTimeoutError" ||
            e._tag === "HttpNetworkError" ||
            (e._tag === "HttpStatusError" && e.isRetryable),
        ),
      ),
    ),
    Effect.timeoutFail({
      duration: Duration.millis(5000),
      onTimeout: () => new HttpTimeoutError({ url: "/api/data", timeoutMs: 5000, message: "..." }),
    }),
  ),
);
```

### Pipeline (5 lines)

```typescript
const data = await api
  .get("/api/data", DataSchema)
  .retry({ maxRetries: 3, jitter: true })
  .timeout(5_000)
  .runPromise();
```

---

## 2. Parallel fetch with fallback

### Promise (6 lines)

```typescript
const [usersResult, statsResult] = await Promise.allSettled([
  fetch("/users").then((r) => r.json()),
  fetch("/stats").then((r) => r.json()),
]);
const users = usersResult.status === "fulfilled" ? usersResult.value : [];
const stats = statsResult.status === "fulfilled" ? statsResult.value : { total: 0 };
```

### Raw Effect (11 lines)

```typescript
const [users, stats] = await Effect.runPromise(
  Effect.all(
    [
      httpRequest({ url: "/users", schema: UsersSchema }),
      httpRequest({ url: "/stats", schema: StatsSchema }).pipe(
        Effect.orElse(() => Effect.succeed({ total: 0 })),
      ),
    ],
    { concurrency: "unbounded" },
  ),
);
```

### Pipeline (4 lines)

```typescript
const [users, stats] = await Pipeline.all(
  api.get("/users", UsersSchema),
  api.get("/stats", StatsSchema).orElse({ total: 0 }),
).runPromise();
```

---

## 3. Bounded concurrency over N items

### Promise (14 lines)

```typescript
async function mapWithConcurrency<T, U>(
  items: T[],
  fn: (item: T) => Promise<U>,
  concurrency: number,
) {
  const results: U[] = [];
  for (let i = 0; i < items.length; i += concurrency) {
    const batch = items.slice(i, i + concurrency);
    const batchResults = await Promise.all(batch.map(fn));
    results.push(...batchResults);
  }
  return results;
}
const enriched = await mapWithConcurrency(videoIds, (id) => fetchVideo(id), 5);
```

### Raw Effect (6 lines)

```typescript
const enriched = await Effect.runPromise(
  Effect.all(
    videoIds.map((id) => Effect.promise(() => fetchVideo(id))),
    { concurrency: 5 },
  ),
);
```

### Pipeline (3 lines)

```typescript
const enriched = await StreamPipeline.fromIterable(videoIds)
  .parAsyncMap(5, (id) => fetchVideo(id))
  .collect();
```

---

## 4. Submit job → poll until done → download result

### Promise (13 lines)

```typescript
const job = await fetch("/reports", {
  method: "POST",
  body: JSON.stringify({ type: "monthly" }),
}).then((r) => r.json());
let status = job;
for (let i = 0; i < 30; i++) {
  if (status.status === "completed" || status.status === "failed") break;
  await new Promise((r) => setTimeout(r, 2000));
  status = await fetch(`/reports/${job.id}`).then((r) => r.json());
}
if (status.status === "failed") throw new Error(status.error);
if (status.status !== "completed") throw new Error("Polling timed out");
const report = await fetch(status.downloadUrl).then((r) => r.text());
```

### Raw Effect (19 lines)

```typescript
const report = await Effect.runPromise(
  httpRequest({ url: "/reports", method: "POST", json: { type: "monthly" }, schema: JobSchema }).pipe(
    Effect.flatMap((job) => {
      const poll = httpRequest({ url: `/reports/${job.id}`, schema: JobSchema });
      return Effect.gen(function* () {
        let result = yield* poll;
        let attempt = 0;
        while (result.status !== "completed" && result.status !== "failed" && attempt < 30) {
          yield* Effect.sleep(Duration.seconds(2));
          result = yield* poll;
          attempt++;
        }
        if (result.status === "failed") return yield* Effect.die(new Error(result.error));
        if (result.status !== "completed") return yield* Effect.fail(new PollTimeoutError({ ... }));
        return yield* httpRequestText({ url: result.downloadUrl! });
      });
    }),
  ),
);
```

### Pipeline (10 lines)

```typescript
const report = await api
  .post("/reports", JobSchema, { json: { type: "monthly" } })
  .flatMap((job) =>
    api.get(`/reports/${job.id}`, JobSchema).pollUntil({
      until: (j) => j.status === "completed" || j.status === "failed",
      intervalMs: 2_000,
    }),
  )
  .flatMapAsync((job) => fetch(job.downloadUrl!).then((r) => r.text()))
  .runPromise();
```

---

## 5. Circuit breaker + retry + semaphore

### Promise (40+ lines)

```typescript
// You'd need to implement circuit breaker and semaphore from scratch.
// No standard library support. Most teams use a third-party package
// like cockatiel, opossum, or bottleneck — each with different APIs.
```

### Raw Effect (15 lines)

```typescript
const semaphore = Effect.unsafeMakeSemaphore(10);
// Circuit breaker requires manual Ref-based implementation (~50 lines)
const result = await Effect.runPromise(
  semaphore.withPermits(1)(
    httpRequest({ url: "/ai/generate", method: "POST", schema, json: prompt }).pipe(
      Effect.retry(
        Schedule.intersect(
          Schedule.jittered(Schedule.exponential(Duration.millis(100), 2)),
          Schedule.recurs(3),
        ),
      ),
      Effect.timeoutFail({ duration: Duration.seconds(30), onTimeout: () => ... }),
    ),
  ),
);
```

### Pipeline (10 lines)

```typescript
const aiLimit = PipelineSemaphore.make(10);
const aiBreaker = new CircuitBreaker({ failureThreshold: 5, resetTimeoutMs: 30_000 });

const result = await api
  .post("/ai/generate", Schema, { json: prompt })
  .withPermit(aiLimit)
  .withCircuitBreaker(aiBreaker)
  .retry({ maxRetries: 3, jitter: true })
  .catch("CircuitOpenError", () => fallbackResponse)
  .runPromise();
```

---

## 6. Stream: bulk export with batching and parallel enrichment

### Promise (13 lines)

```typescript
const batch: Item[] = [];
for await (const line of response.body) {
  const item = JSON.parse(line);
  batch.push(item);
  if (batch.length >= 500) {
    const enriched = await Promise.all(batch.splice(0).map((i) => enrichFromDb(i.id)));
    await db.bulkInsert(enriched);
  }
}
if (batch.length > 0) {
  const enriched = await Promise.all(batch.map((i) => enrichFromDb(i.id)));
  await db.bulkInsert(enriched);
}
```

### Raw Effect (10 lines)

```typescript
await Effect.runPromise(
  Stream.runDrain(
    httpStreamNDJSON({ url: "/export", schema: ItemSchema }).pipe(
      Stream.mapEffect((item) => Effect.promise(() => enrichFromDb(item.id)), { concurrency: 10 }),
      Stream.groupedWithin(500, Duration.seconds(1)),
      Stream.map(Chunk.toArray),
      Stream.tap((batch) => Effect.promise(() => db.bulkInsert(batch))),
    ),
  ),
);
```

### Pipeline (6 lines)

```typescript
await api
  .postNDJSON("/export", ItemSchema, { json: query })
  .parAsyncMap(10, (item) => enrichFromDb(item.id))
  .groupWithin(500, 1_000)
  .tapAsync((batch) => db.bulkInsert(batch))
  .drain();
```

---

## 7. Fetch list → stream over items → enrich in parallel

### Promise (7 lines)

```typescript
const ids = await fetch("/video-ids").then((r) => r.json());
const enriched = [];
for (let i = 0; i < ids.length; i += 5) {
  const batch = ids.slice(i, i + 5);
  const results = await Promise.all(batch.map((id) => fetchVideo(id)));
  enriched.push(...results);
}
```

### Raw Effect (10 lines)

```typescript
const enriched = await Effect.runPromise(
  httpRequest({ url: "/video-ids", schema: IdsSchema }).pipe(
    Effect.flatMap((ids) =>
      Effect.all(
        ids.map((id) => Effect.promise(() => fetchVideo(id))),
        { concurrency: 5 },
      ),
    ),
  ),
);
```

### Pipeline (4 lines)

```typescript
const enriched = await StreamPipeline.fromPipeline(api.get("/video-ids", IdsSchema))
  .flatMap((ids) => StreamPipeline.fromIterable(ids))
  .parAsyncMap(5, (id) => fetchVideo(id))
  .collect();
```

---

## Line Count Summary

| Pattern                             | Promise | Raw Effect | Pipeline | Reduction vs Promise |
| ----------------------------------- | :-----: | :--------: | :------: | :------------------: |
| Fetch + retry + timeout             |   17    |     21     |    5     |       **71%**        |
| Parallel fetch + fallback           |    6    |     11     |    4     |       **33%**        |
| Bounded concurrency                 |   14    |     6      |    3     |       **79%**        |
| Submit → poll → download            |   13    |     19     |    10    |       **23%**        |
| Circuit breaker + retry + semaphore |   40+   |     15     |    10    |       **75%**        |
| Stream bulk export                  |   13    |     10     |    6     |       **54%**        |
| Fetch list → stream → enrich        |    7    |     10     |    4     |       **43%**        |

Pipeline is not a replacement for Effect — it's a facade. The escape hatch (`.toEffect()`) is always there when you need the full power.
