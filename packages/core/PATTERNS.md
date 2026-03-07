# Real-World Pipeline Patterns

Advanced composition patterns using `@promin/core` for complex async workflows.

---

## 1. Fan-out/fan-in with per-branch resilience

Fetch a user, then in parallel: fetch their videos (retry 3x), analytics (fallback to empty), and subscription (cached). Merge into a dashboard object.

```typescript
const subCache = new PipelineCache<Subscription>(5 * 60 * 1000);

const dashboard = await api
  .get("/users/1", UserSchema)
  .flatMap((user) =>
    Pipeline.all(
      api.get(`/users/${user.id}/videos`, VideosSchema).retry(3),
      api.get(`/users/${user.id}/analytics`, AnalyticsSchema).orElse({ views: 0, subs: 0 }),
      api.get(`/users/${user.id}/subscription`, SubSchema).cached(subCache),
    ).map(([videos, analytics, subscription]) => ({
      user,
      videos,
      analytics,
      subscription,
    })),
  )
  .timeout(10_000)
  .runPromise();
```

**What happens:**

- User fetch fails → entire pipeline fails
- Videos fetch fails 3x → pipeline fails
- Analytics fetch fails → silently replaced with `{ views: 0, subs: 0 }`
- Subscription → served from cache for 5 min, no API call
- Any branch exceeds 10s → entire pipeline cancelled, all fibers interrupted

---

## 2. Saga with compensating actions

Multi-step order fulfillment: charge payment → reserve inventory → schedule shipping. If any step fails, undo the previous steps.

```typescript
const order = await Pipeline.fn(() => chargePayment(orderId, amount))
  .flatMap((payment) =>
    Pipeline.fn(() => reserveInventory(orderId, items))
      .mapError((e) => ({ ...e, compensate: () => refundPayment(payment.id) }))
      .flatMap((reservation) =>
        Pipeline.fn(() => scheduleShipping(orderId, reservation.warehouseId)).mapError((e) => ({
          ...e,
          compensate: async () => {
            await releaseInventory(reservation.id);
            await refundPayment(payment.id);
          },
        })),
      ),
  )
  .tapError(async (e) => {
    if ("compensate" in e) await e.compensate();
  })
  .retry({ maxRetries: 2, jitter: true })
  .runPromise();
```

**What happens:**

- Payment charged → inventory reserved → shipping scheduled → success
- Shipping fails → inventory released + payment refunded → retried from scratch
- After 2 retries still failing → error propagated with compensation already done

---

## 3. Rate-limited bulk migration with progress tracking

Migrate 100k users from legacy API with shared rate limit, progress tracking, and streaming output.

```typescript
const legacyLimit = PipelineSemaphore.make(20); // legacy API allows 20 concurrent
const progress = PipelineRef.make({ migrated: 0, failed: 0, total: userIds.length });

await StreamPipeline.fromIterable(userIds)
  .parAsyncMap(50, async (id) => {
    // 50 fibers, but only 20 can hit legacy API at once
    return Pipeline.fn(() => legacyApi.getUser(id))
      .withPermit(legacyLimit)
      .retry({ maxRetries: 3, jitter: true, maxDelayMs: 5_000 })
      .runPromise();
  })
  .tapAsync(async (user) => {
    await newDb.upsertUser(user);
    await progress.updateAsync((p) => ({ ...p, migrated: p.migrated + 1 }));
  })
  .grouped(100)
  .tapAsync(async (batch) => {
    const p = await progress.getAsync();
    console.log(`Progress: ${p.migrated}/${p.total} migrated, ${p.failed} failed`);
  })
  .drain();
```

---

## 4. Competitive redundancy — fastest AI provider wins

Hit 3 AI providers in parallel, take the first response, cancel the rest. Each provider has its own circuit breaker. Fall back to a cheaper model if all fail.

```typescript
const openaiBreaker = new CircuitBreaker({ failureThreshold: 3, resetTimeoutMs: 60_000 });
const anthropicBreaker = new CircuitBreaker({ failureThreshold: 3, resetTimeoutMs: 60_000 });
const geminiBreaker = new CircuitBreaker({ failureThreshold: 3, resetTimeoutMs: 60_000 });

const response = await Pipeline.race(
  api
    .post("/ai/openai", Schema, { json: prompt })
    .withCircuitBreaker(openaiBreaker)
    .timeout(10_000),
  api
    .post("/ai/anthropic", Schema, { json: prompt })
    .withCircuitBreaker(anthropicBreaker)
    .timeout(10_000),
  api
    .post("/ai/gemini", Schema, { json: prompt })
    .withCircuitBreaker(geminiBreaker)
    .timeout(10_000),
)
  .orElsePipeline(() =>
    // All premium providers failed or circuit-open — fall back to cheap model
    api.post("/ai/cheap-model", Schema, { json: prompt }).retry(2),
  )
  .runPromise();
```

**What happens:**

- 3 requests fire simultaneously, each with its own circuit breaker
- First response wins, other 2 are interrupted (TCP connections closed)
- If OpenAI is circuit-open, it fails instantly (no wasted request) — Anthropic and Gemini still race
- If all 3 fail → cheap model used as fallback with 2 retries

---

## 5. YouTube transcription pipeline — submit → poll → parse → store

Complete transcription workflow with proxy circuit breaker, polling with backoff, and fire-and-forget analytics.

```typescript
const proxyBreaker = new CircuitBreaker({
  failureThreshold: 5,
  resetTimeoutMs: 30_000,
  isFailure: (err) => err._tag === "HttpStatusError" && err.status >= 500,
});

const transcription = await api
  .post("/proxy/transcribe", JobSchema, { json: { videoId } })
  .withCircuitBreaker(proxyBreaker)
  .retry({ maxRetries: 2, jitter: true })
  .flatMap((job) =>
    api.get(`/proxy/jobs/${job.id}`, JobSchema).pollUntilWithBackoff({
      until: (j) => j.status === "completed" || j.status === "failed",
      initialIntervalMs: 1_000,
      maxIntervalMs: 10_000,
      maxAttempts: 60,
    }),
  )
  .filter({
    predicate: (job) => job.status === "completed",
    orFail: (job) => new TranscriptionError({ message: job.error ?? "Unknown error" }),
  })
  .flatMapAsync((job) => fetchAndParseTranscript(job.resultUrl!))
  .tapAsync((transcript) => db.saveTranscript(videoId, transcript))
  .tapAsyncFork((transcript) => analytics.trackTranscription(videoId, transcript.wordCount))
  .timeout(5 * 60_000)
  .runPromise();
```

---

## 6. Real-time webhook processing with non-blocking analytics

Process YouTube webhook events with backpressure, deduplication, and fire-and-forget analytics that doesn't slow down the main processing pipeline.

```typescript
const webhookQueue = PipelineQueue.make<WebhookEvent>(1000);

// Webhook handler (producer) — blocks if queue is full (backpressure)
app.post("/webhooks/youtube", async (c) => {
  const event = c.req.valid("json");
  await webhookQueue.offerAsync(event);
  return c.json({ ok: true });
});

// Background consumer
await webhookQueue
  .toStream()
  .dedupe() // skip duplicate webhook deliveries
  .parAsyncMap(10, async (event) => {
    const channel = await db.getChannel(event.channelId);
    return { ...event, channelName: channel.name, tier: channel.tier };
  })
  .filter((e) => e.tier !== "free") // only process paid channels
  .groupWithin(50, 2_000) // batch: 50 events or 2s, whichever first
  .tapAsync((batch) => db.bulkInsertEvents(batch))
  .tapAsyncFork((batch) => analytics.trackBatch(batch)) // non-blocking — don't wait for analytics
  .onFinalize(async () => {
    await db.disconnect();
    console.log("Consumer shut down cleanly");
  })
  .drain();
```

---

## 7. Stale-while-revalidate cache with background refresh

Serve stale data immediately, refresh in background using `tapFork`. If cache is empty, wait for fresh data.

```typescript
const videoCache = new PipelineCache<VideoDetails>(60_000); // 1 min TTL

function getVideoDetails(videoId: string) {
  return api
    .get(`/videos/${videoId}`, VideoSchema)
    .cached(videoCache)
    .tapFork(() =>
      // If cache was fresh, this is a no-op (cached() returns instantly).
      // If cache expired, this re-fetches in background for next caller.
      api.get(`/videos/${videoId}`, VideoSchema).cached(videoCache),
    )
    .retry(2);
}

const details = await getVideoDetails("abc123").runPromise();
```

---

## 8. Parallel validation with accumulated errors

Validate a complex form — collect ALL errors instead of stopping at the first.

```typescript
class ValidationError extends Data.TaggedError("ValidationError")<{
  readonly field: string;
  readonly message: string;
}> {}

const validateTitle = (title: string) =>
  title.length > 0 && title.length <= 100
    ? Pipeline.succeed(title)
    : Pipeline.fail(new ValidationError({ field: "title", message: "Title must be 1-100 chars" }));

const validateTags = (tags: string[]) =>
  tags.length <= 30
    ? Pipeline.succeed(tags)
    : Pipeline.fail(new ValidationError({ field: "tags", message: "Max 30 tags" }));

const validateThumbnail = (url: string) =>
  Pipeline.fn(() => fetch(url, { method: "HEAD" }))
    .filter({
      predicate: (res) => res.ok,
      orFail: () => new ValidationError({ field: "thumbnail", message: "Invalid thumbnail URL" }),
    })
    .map(() => url);

// Accumulates ALL validation errors — doesn't stop at the first
const [title, tags, thumbnail] = await Pipeline.validate(
  validateTitle(input.title),
  validateTags(input.tags),
  validateThumbnail(input.thumbnailUrl),
).runPromise();
```

---

## 9. Stream fan-out with broadcastThrough

Process a single event stream through multiple parallel pipelines using `broadcastThrough`. Each item goes to analytics, alerts, and archive simultaneously.

```typescript
await api
  .getSSE("/events/firehose")
  .broadcastThrough(
    // Analytics: batch and write every 5s
    (s) => s.groupWithin(100, 5_000).tapAsync((batch) => analytics.writeBatch(batch)),

    // Alerts: filter anomalies, notify immediately
    (s) =>
      s.filter((e) => e.event === "anomaly").tapAsync((e) => slack.notify(`Anomaly: ${e.data}`)),

    // Archive: write everything to S3
    (s) =>
      s
        .grouped(1000)
        .tapAsync((batch) => s3.putObject(`events/${Date.now()}.json`, JSON.stringify(batch))),
  )
  .drain();
```

---

## 10. Dynamic configuration with live reload

Feature flags that affect stream processing behavior, updated in real-time via `PipelineSignal`.

```typescript
const config = PipelineSignal.make<AppConfig>({
  enrichmentConcurrency: 5,
  batchSize: 100,
  enableAiScoring: false,
});

// Admin endpoint to update config live
app.post("/admin/config", async (c) => {
  const newConfig = c.req.valid("json");
  await config.updateAsync((prev) => ({ ...prev, ...newConfig }));
  return c.json({ ok: true });
});

// Processing pipeline reads config on each batch
await webhookQueue
  .toStream()
  .groupWithin(100, 2_000)
  .tapAsync(async (batch) => {
    const cfg = await config.getAsync();
    const enriched = await StreamPipeline.fromIterable(batch)
      .parAsyncMap(cfg.enrichmentConcurrency, (item) => enrichItem(item))
      .collect();

    if (cfg.enableAiScoring) {
      await StreamPipeline.fromIterable(enriched)
        .parAsyncMap(3, (item) => aiScore(item))
        .tapAsync((scored) => db.updateScore(scored.id, scored.score))
        .drain();
    }

    await db.bulkInsert(enriched);
  })
  .drain();
```

---

## 11. Hedged request with circuit breaker per-region

Multi-region API with hedged requests for tail latency, each region having its own circuit breaker.

```typescript
const usBreaker = new CircuitBreaker({ failureThreshold: 3, resetTimeoutMs: 15_000 });
const euBreaker = new CircuitBreaker({ failureThreshold: 3, resetTimeoutMs: 15_000 });
const apBreaker = new CircuitBreaker({ failureThreshold: 3, resetTimeoutMs: 15_000 });

const usApi = api.withOverrides({ baseUrl: "https://us.api.example.com" });
const euApi = api.withOverrides({ baseUrl: "https://eu.api.example.com" });
const apApi = api.withOverrides({ baseUrl: "https://ap.api.example.com" });

const data = await Pipeline.race(
  Pipeline.hedged(usApi.get("/data", Schema).withCircuitBreaker(usBreaker), { hedgeDelayMs: 200 }),
  Pipeline.hedged(euApi.get("/data", Schema).withCircuitBreaker(euBreaker), { hedgeDelayMs: 300 }),
  apApi.get("/data", Schema).withCircuitBreaker(apBreaker),
)
  .timeout(5_000)
  .runPromise();
```

**What happens:**

- US primary fires immediately; US backup fires after 200ms if primary hasn't responded
- EU primary fires immediately; EU backup fires after 300ms
- AP fires once (no hedge — furthest region)
- First response from any wins; all others interrupted
- If US region is down → circuit opens → instantly skipped on next calls

---

## 12. Channel-based worker pool

Distribute work across N worker pipelines using a channel, with graceful shutdown.

```typescript
const jobChannel = PipelineChannel.make<Job>(500);
const results = PipelineRef.make<ProcessedJob[]>([]);

// Spawn N workers consuming from the same channel
const workers = Array.from({ length: 4 }, (_, i) =>
  jobChannel
    .toStream()
    .tapAsync(async (job) => {
      const result = await processJob(job);
      await results.updateAsync((r) => [...r, result]);
    })
    .onFinalize(async () => console.log(`Worker ${i} shut down`))
    .drain(),
);

// Producer: feed jobs
for (const job of jobs) {
  await jobChannel.sendAsync(job);
}
await jobChannel.closeAsync(); // signal workers to stop

// Wait for all workers to finish
await Promise.all(workers);

const allResults = await results.getAsync();
console.log(`Processed ${allResults.length} jobs across 4 workers`);
```

---

## 13. Supervised background consumer

Long-running queue consumer that auto-restarts on failure. Uses `supervised` for resilience.

```typescript
const queue = PipelineQueue.make<IngestEvent>(5000);

// This runs forever. If it crashes, it restarts after 2s. Max 100 restarts.
await Pipeline.fn(async () => {
  await queue
    .toStream()
    .parAsyncMap(10, (event) => processEvent(event))
    .groupWithin(100, 2_000)
    .tapAsync((batch) => db.bulkInsert(batch))
    .drain();
})
  .supervised({ restart: "on-failure", maxRestarts: 100, intervalMs: 2_000 })
  .runPromise();
```

---

## 14. Connection pool with automatic lifecycle

Use `PipelinePool` to manage a pool of database connections with automatic acquire/release.

```typescript
const dbPool = PipelinePool.make({
  acquire: () => pg.connect(),
  release: (conn) => conn.end(),
  size: 10,
});

// Each request acquires a connection, runs the query, auto-releases
app.get("/users/:id", async (c) => {
  const user = await dbPool.useAsync((conn) =>
    conn.query("SELECT * FROM users WHERE id = $1", [c.req.param("id")]),
  );
  return c.json(user.rows[0]);
});

// Works with Pipeline too
const stats = await Pipeline.fn(() =>
  dbPool.useAsync((conn) => conn.query("SELECT count(*) FROM users")),
)
  .retry(2)
  .runPromise();
```

---

## 15. Internal event bus with PubSub

Decouple services using an in-process event bus. Multiple consumers independently process the same events.

```typescript
const eventBus = PipelinePubSub.make<DomainEvent>(1000);

// Consumer 1: update search index
eventBus
  .subscribe()
  .filter((e) => e.type === "video.published" || e.type === "video.updated")
  .groupWithin(50, 1_000)
  .tapAsync((batch) => searchIndex.bulkUpdate(batch))
  .drain();

// Consumer 2: send notifications
eventBus
  .subscribe()
  .filter((e) => e.type === "video.published")
  .tapAsync((e) => notificationService.notifySubscribers(e.channelId))
  .drain();

// Producer: any service publishes events
await eventBus.publishAsync({ type: "video.published", videoId, channelId });
```

---

## 16. Fetch list → stream over items with parallel enrichment

Use `StreamPipeline.fromPipeline` to bridge an API response into a streaming pipeline.

```typescript
const enrichedVideos = await StreamPipeline.fromPipeline(
  api.get("/channels/123/video-ids", VideoIdsSchema),
)
  .flatMap((ids) => StreamPipeline.fromIterable(ids))
  .parAsyncMap(5, async (id) => {
    const [video, stats] = await Pipeline.all(
      api.get(`/videos/${id}`, VideoSchema),
      api.get(`/videos/${id}/stats`, StatsSchema).orElse({ views: 0 }),
    ).runPromise();
    return { ...video, ...stats };
  })
  .filter((v) => v.views > 1000)
  .collect();
```

---

## 17. Sliding window anomaly detection

Use `.sliding()` to compute moving averages and detect anomalies in a metrics stream.

```typescript
await api
  .getSSE("/metrics/cpu")
  .map((event) => parseFloat(event.data))
  .sliding(10) // window of last 10 readings
  .map((window) => {
    const avg = window.reduce((a, b) => a + b, 0) / window.length;
    const latest = window[window.length - 1];
    return { avg, latest, spike: latest > avg * 2 };
  })
  .filter((m) => m.spike)
  .tapAsync((m) => alerting.fire(`CPU spike: ${m.latest.toFixed(1)}% (avg ${m.avg.toFixed(1)}%)`))
  .drain();
```

---

## 18. Non-blocking telemetry with observe

Use `.observe()` to run analytics in parallel without slowing down the main processing pipeline. The observer batches and writes independently.

```typescript
await api
  .postNDJSON("/export", ItemSchema, { json: query })
  .observe((s) =>
    // Observer runs in background — batches items and writes to analytics
    s.groupWithin(100, 5_000).tapAsync((batch) => analytics.writeBatch(batch)),
  )
  // Main pipeline continues at full speed, unblocked by analytics writes
  .parAsyncMap(10, (item) => enrichFromDb(item.id))
  .tapAsync((enriched) => db.upsert(enriched))
  .drain();
```

---

## 19. Conditional notifications with `.when()`

Skip expensive operations when conditions aren't met — no `if` statement needed in the chain.

```typescript
const result = await api
  .get(`/users/${userId}`, UserSchema)
  .tapAsync((user) => db.recordLastSeen(user.id))
  .flatMap((user) =>
    Pipeline.fn(() => sendWelcomeEmail(user.email))
      .when(() => user.isNewUser && user.emailVerified)
      .map(() => user),
  )
  .runPromise();
```

---

## 20. Pausable stream processing

Use `.pauseWhen()` for flow control — pause processing when downstream can't keep up, resume when ready.

```typescript
const paused = PipelineRef.make(false);

// Monitor downstream health — pause if DB is overloaded
setInterval(async () => {
  const dbLoad = await db.getConnectionPoolUsage();
  await paused.setAsync(dbLoad > 0.9); // pause at 90% pool usage
}, 5_000);

// Processing pipeline automatically pauses/resumes
await webhookQueue
  .toStream()
  .pauseWhen(paused)
  .parAsyncMap(10, (event) => processEvent(event))
  .groupWithin(100, 2_000)
  .tapAsync((batch) => db.bulkInsert(batch))
  .drain();
```
