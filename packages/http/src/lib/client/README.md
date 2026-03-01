# @ts-backend/http/client

Effect-based HTTP client with chainable pipelines, Zod validation, retries, polling, streaming, and parallel composition.

Everything is **lazy** — nothing executes until you call `.runPromise()`, `.runSafe()`, `.forEach()`, or `.collect()`.

## Quick start

```ts
import { DefaultHttpClient, HttpPipeline } from "@ts-backend/http/client";
import { z } from "zod";

const api = new DefaultHttpClient({
  baseUrl: "https://api.example.com",
  headers: { Authorization: "Bearer token" },
  timeoutMs: 10_000,
});

const UserSchema = z.object({ id: z.number(), name: z.string() });

const user = await api.get("/users/1", UserSchema).runPromise();
```

## Table of contents

- [DefaultHttpClient — creating a client](#defaulthttpclient)
- [Mocking in tests](#mocking-in-tests)
- [Request methods](#request-methods)
- [Pipeline chaining](#pipeline-chaining)
- [Retry](#retry)
- [Polling](#polling)
- [Streaming (SSE, NDJSON, raw)](#streaming)
- [Parallel requests](#parallel-requests)
- [Racing and fallbacks](#racing-and-fallbacks)
- [Error handling](#error-handling)
- [Cancellation and cleanup](#cancellation-and-cleanup)
- [Safe execution](#safe-execution)
- [withOverrides — derive clients from a shared base](#withoverrides--derive-clients-from-a-shared-base)
- [Effect escape hatch](#effect-escape-hatch)
- [Error types](#error-types)
- [Complex real-world scenarios](#complex-real-world-scenarios)

---

## DefaultHttpClient

Create a pre-configured client. All requests inherit the base URL, headers, and timeout.

```ts
const api = new DefaultHttpClient({
  baseUrl: "https://api.example.com",
  headers: {
    Authorization: "Bearer token",
    "X-Api-Key": "key",
  },
  timeoutMs: 10_000,
});
```

You can also use it without config for one-off requests:

```ts
const api = new DefaultHttpClient();
const data = await api.get("https://some-url.com/data", DataSchema).runPromise();
```

---

## Mocking in tests

`DefaultHttpClient` implements the `HttpClient` interface. Accept the interface in your services, use `MockHttpClient` in tests.

### Service pattern

```ts
import type { HttpClient } from "@ts-backend/http/client";

// Depend on the interface — not the implementation
export class DefaultMyService implements MyService {
  constructor(
    private readonly http: HttpClient,
    loggerFactory: LoggerFactory,
  ) {
    this.logger = loggerFactory.getLogger("MyService");
  }
}
```

### Basic mocking

```ts
import { MockHttpClient } from "@ts-backend/http/client/testing";
import { noopLoggerFactory } from "@ts-backend/test";

const http = new MockHttpClient()
  .on("GET", "/users/1", { id: 1, name: "Alice" })
  .on("POST", "/users", { id: 2, name: "Created" });

const service = new DefaultMyService(http, noopLoggerFactory);
```

### Failure responses

```ts
const http = new MockHttpClient()
  .on("GET", "/users/1", MockHttpClient.fail(404))
  .on("POST", "/users", MockHttpClient.fail(500, "internal error"));
```

### Dynamic responses — depends on request

The handler receives a `RecordedCall` with `json`, `body`, `headers`, and `tag`:

```ts
// Match on JSON body
const http = new MockHttpClient().onFn("POST", "/search", (call) => {
  const query = (call.json as any)?.query;
  return query === "typescript" ? { results: ["ts-lib"] } : { results: [] };
});

// Match on headers (e.g., auth check)
const http = new MockHttpClient().onFn("GET", "/users", (call) => {
  if (!call.headers?.["Authorization"]) return MockHttpClient.fail(401);
  return { id: 1, name: "Authenticated" };
});
```

### Ordered responses — for retry / polling tests

```ts
// Simulate flaky endpoint: fail twice, then succeed
const http = new MockHttpClient().onSequence("GET", "/flaky", [
  MockHttpClient.fail(500),
  MockHttpClient.fail(500),
  { id: 1, name: "Recovered" },
]);

// Simulate polling: running → running → completed
const http = new MockHttpClient().onSequence("GET", "/jobs/1", [
  { status: "running" },
  { status: "running" },
  { status: "completed", result: "done" },
]);

// Works with .pollUntil() / .retry() — each call consumes the next response
const result = await http
  .get("/jobs/1", JobSchema)
  .pollUntil({ until: (j) => j.status === "completed", intervalMs: 10 })
  .runPromise();
```

### Streaming mocks

```ts
const http = new MockHttpClient()
  .onSSE("/events", [
    { event: "delta", data: "hello" },
    { event: "done", data: "[DONE]" },
  ])
  .onNDJSON("/export", [
    { id: 1, name: "Alice" },
    { id: 2, name: "Bob" },
  ])
  .onStream("/file", "raw text content");

const events = await http.getSSE("/events").collect();
const items = await http.getNDJSON("/export", ItemSchema).collect();
```

### Asserting calls

```ts
const http = new MockHttpClient().on("GET", "/users/1", { id: 1, name: "Alice" });

await http.get("/users/1", UserSchema).runPromise();
await http.post("/users", UserSchema, { json: { name: "New" } }).runPromise();

// Was this route called?
expect(http.calledWith("GET", "/users/1")).toBe(true);

// How many times?
expect(http.calledTimes("GET", "/users/1")).toBe(1);

// Called with this exact JSON body?
expect(http.calledWithJson("POST", "/users", { name: "New" })).toBe(true);

// Get all calls for a route
const postCalls = http.callsFor("POST", "/users");
expect(postCalls[0].json).toEqual({ name: "New" });

// Last call
expect(http.lastCall?.method).toBe("POST");

// All recorded calls (method, path, json, body, headers, tag)
expect(http.calls).toHaveLength(2);
```

### Reset between tests

```ts
http.resetCalls(); // clear calls, keep routes
http.reset(); // clear everything
```

---

## Request methods

Every method returns an `HttpPipeline<T>` — a lazy, chainable object.

```ts
// GET with Zod schema validation
const user = await api.get("/users/1", UserSchema).runPromise();

// POST with JSON body
const created = await api
  .post("/users", UserSchema, { json: { name: "Alice", email: "alice@example.com" } })
  .runPromise();

// PUT
const updated = await api.put("/users/1", UserSchema, { json: { name: "Bob" } }).runPromise();

// PATCH
const patched = await api.patch("/users/1", UserSchema, { json: { name: "Charlie" } }).runPromise();

// DELETE
const deleted = await api
  .delete("/users/1", SuccessSchema, { json: { reason: "requested" } })
  .runPromise();
```

### Unvalidated variants

When you don't need Zod validation:

```ts
// Raw JSON (returns unknown)
const raw = await api.getJson("/debug/info").runPromise();

// Raw text
const html = await api.getText("/health").runPromise();

// POST returning raw JSON
const result = await api.postJson("/webhook", { json: payload }).runPromise();
```

### Multipart upload

```ts
const result = await api
  .postMultipart("/upload", UploadResultSchema, {
    file: new File(["content"], "doc.pdf", { type: "application/pdf" }),
    fields: { description: "My document", folder: "inbox" },
  })
  .runPromise();

// Custom file field name (default is "file")
const result = await api
  .postMultipart("/upload", UploadResultSchema, {
    file: imageBlob,
    fileField: "image",
  })
  .runPromise();
```

### Per-request overrides

```ts
const user = await api
  .get("/users/1", UserSchema, {
    headers: { "X-Custom": "value" }, // merged with client defaults
    timeoutMs: 5_000, // overrides client default
  })
  .runPromise();
```

### Full control via `.request()`

```ts
const result = await api
  .request({
    path: "/custom",
    method: "OPTIONS",
    schema: CustomSchema,
    headers: { "X-Special": "true" },
    acceptStatus: (s) => s === 200 || s === 204,
  })
  .runPromise();
```

---

## Pipeline chaining

### `.map()` — transform the result

```ts
const userName = await api
  .get("/users/1", UserSchema)
  .map((user) => user.name.toUpperCase())
  .runPromise();
// "ALICE"
```

### `.flatMap()` — chain dependent requests

The next request can use the result of the previous one:

```ts
const posts = await api
  .get("/users/1", UserSchema)
  .flatMap((user) => api.get(`/users/${user.id}/posts`, PostsSchema))
  .runPromise();
```

Multi-step chains:

```ts
const thumbnail = await api
  .get("/channels/UC123", ChannelSchema)
  .flatMap((channel) => api.get(`/videos/${channel.latestVideoId}`, VideoSchema))
  .flatMap((video) => api.get(`/thumbnails/${video.thumbnailId}`, ThumbnailSchema))
  .runPromise();
```

### `.tap()` — side-effects without changing the value

```ts
const user = await api
  .get("/users/1", UserSchema)
  .tap((user) => logger.info("Fetched user", { userId: user.id }))
  .runPromise();
```

### `.tapAsync()` / `.mapAsync()` — async transforms

For async side-effects and transforms that return Promises:

```ts
// Async side-effect — awaits before continuing
const user = await api
  .get("/users/1", UserSchema)
  .tapAsync((user) => cache.set(user.id, user))
  .runPromise();

// Async transform — resolved value becomes the new pipeline value
const profile = await api
  .get("/users/1", UserSchema)
  .mapAsync((user) => db.getProfile(user.id))
  .runPromise();
```

### `.filter()` — validate the result with a predicate

```ts
const activeUser = await api
  .get("/users/1", UserSchema)
  .filter({
    predicate: (user) => user.status === "active",
    orFail: (user) =>
      new HttpStatusError({
        url: "/users/1",
        status: 403,
        body: `User ${user.id} is not active`,
        message: "User is not active",
      }),
  })
  .runPromise();
```

---

## Retry

Exponential backoff with smart defaults (only retries 5xx, 429, timeouts, and network errors):

```ts
// Default: 3 retries, 250ms base delay
const data = await api.get("/flaky-endpoint", DataSchema).retry().runPromise();

// Custom policy
const data = await api
  .get("/flaky-endpoint", DataSchema)
  .retry({
    maxRetries: 5,
    baseDelayMs: 500,
  })
  .runPromise();

// Custom retry predicate
const data = await api
  .get("/flaky-endpoint", DataSchema)
  .retry({
    maxRetries: 3,
    when: (error) => error._tag === "HttpStatusError" && error.status === 503,
  })
  .runPromise();
```

### `.timeout()` — cap the entire pipeline

Applies to the whole chain including retries:

```ts
const data = await api
  .get("/slow-endpoint", DataSchema)
  .retry({ maxRetries: 10, baseDelayMs: 1_000 })
  .timeout(30_000)
  .runPromise();
```

---

## Polling

### `.pollUntil()` — fixed interval

Repeats the request until a predicate is satisfied:

```ts
const completedJob = await api
  .get(`/jobs/${jobId}`, JobSchema)
  .pollUntil({
    until: (job) => job.status === "completed" || job.status === "failed",
    intervalMs: 2_000,
    maxAttempts: 30,
    maxDurationMs: 120_000,
  })
  .runPromise();

if (completedJob.status === "failed") {
  throw new Error(`Job failed: ${completedJob.error}`);
}
```

### `.pollUntilWithBackoff()` — exponential backoff

Starts fast, slows down over time:

```ts
const result = await api
  .get(`/exports/${exportId}`, ExportSchema)
  .pollUntilWithBackoff({
    until: (r) => r.ready === true,
    initialIntervalMs: 500, // first interval
    maxIntervalMs: 10_000, // cap at 10s
    maxAttempts: 20,
    maxDurationMs: 300_000, // 5 min total budget
  })
  .runPromise();
```

### Poll + transform

```ts
const downloadUrl = await api
  .get(`/jobs/${jobId}`, JobSchema)
  .pollUntil({
    until: (job) => job.status === "completed",
    intervalMs: 1_000,
  })
  .map((job) => job.downloadUrl!)
  .runPromise();
```

---

## Streaming

Streaming methods return `HttpStreamPipeline<T>` — a chainable, lazy stream.
Nothing executes until you call a terminal (`.forEach()`, `.collect()`, `.reduce()`, `.drain()`).
Backpressure is handled naturally — the producer only advances when the consumer is ready.

### SSE (Server-Sent Events)

```ts
// Stream SSE events from a POST (common for AI/LLM APIs)
await api
  .postSSE("/v1/chat/completions", {
    json: { model: "claude-3", messages: [{ role: "user", content: "Hello" }], stream: true },
  })
  .filter((event) => event.event === "delta")
  .map((event) => JSON.parse(event.data).text)
  .forEach((token) => process.stdout.write(token));

// GET SSE
const events = await api.getSSE("/events/subscribe").collect();
```

Each SSE event is a typed object:

```ts
interface SSEvent {
  event: string; // "message" by default
  data: string; // supports multi-line data
  id?: string;
  retry?: number;
}
```

### NDJSON (newline-delimited JSON)

Each line is parsed and validated against a Zod schema:

```ts
const items = await api
  .postNDJSON("/export", ItemSchema, { json: { format: "ndjson" } })
  .filter((item) => item.score > 0.5)
  .collect();

// Or process one-by-one
await api.getNDJSON("/feed", EventSchema).forEach((event) => processEvent(event));
```

### Raw text stream

```ts
const fullText = await api.getStream("/large-file").reduce("", (acc, chunk) => acc + chunk);
```

### Stream operators

```ts
await api
  .getSSE("/events")
  .filter((e) => e.event === "update") // keep only matching events
  .map((e) => JSON.parse(e.data)) // transform each chunk
  .mapAsync((e) => enrichFromDb(e.id)) // async transform per chunk
  .tap((data) => logger.debug("got", data)) // sync side-effect per chunk
  .tapAsync((data) => saveToCache(data)) // async side-effect per chunk
  .take(100) // stop after 100 items
  .takeWhile((e) => e.type !== "end") // stop on condition
  .drop(5) // skip first 5
  .dedupe() // skip consecutive duplicates
  .forEach((data) => handle(data)); // process each
```

### Parallel & batching (fs2-style)

```ts
// parAsyncMap — concurrent transform with bounded parallelism, preserves order
await api
  .getNDJSON("/export", VideoSchema)
  .parAsyncMap(10, (video) => enrichFromApi(video.id))
  .forEach((enriched) => index(enriched));

// parAsyncMapUnordered — same but results arrive in completion order (higher throughput)
await api
  .getNDJSON("/export", VideoSchema)
  .parAsyncMapUnordered(20, (video) => fetchDetails(video.id))
  .forEach((details) => index(details));

// groupWithin — micro-batch by count or time (whichever comes first)
await api
  .getNDJSON("/firehose", RowSchema)
  .groupWithin(500, 1_000) // up to 500 items or every 1s
  .tapAsync((batch) => db.bulkInsert(batch))
  .drain();

// grouped — fixed-size batches
await stream
  .grouped(100)
  .tapAsync((batch) => processBatch(batch))
  .drain();

// scan — running accumulator, emits every intermediate result
await api
  .getNDJSON("/videos", VideoSchema)
  .scan({ sum: 0, count: 0 }, (acc, v) => ({ sum: acc.sum + v.views, count: acc.count + 1 }))
  .map((acc) => acc.sum / acc.count)
  .forEach((avg) => gauge.set(avg));

// merge — interleave two streams
await api
  .getSSE("/events/channel-1")
  .merge(api.getSSE("/events/channel-2"))
  .forEach((event) => handle(event));

// mergeAll — interleave N streams
await HttpStreamPipeline.mergeAll(
  api.getSSE("/events/ch-1"),
  api.getSSE("/events/ch-2"),
  api.getSSE("/events/ch-3"),
).forEach((event) => handle(event));

// through — reusable stream transformations (like fs2 Pipe)
const withMetrics = (s: HttpStreamPipeline<SSEvent>) =>
  s.tap((e) => counter.inc({ type: e.event }));

await api.getSSE("/events").through(withMetrics).forEach(handle);
```

### Stream terminals

| Terminal            | Returns         | Description                                   |
| ------------------- | --------------- | --------------------------------------------- |
| `.forEach(fn)`      | `Promise<void>` | Process each chunk, resolves when stream ends |
| `.collect()`        | `Promise<T[]>`  | Collect all chunks into an array              |
| `.reduce(init, fn)` | `Promise<U>`    | Fold over all chunks to a single value        |
| `.drain()`          | `Promise<void>` | Consume the stream, discard values            |

### Stream to raw Effect Stream

```ts
import { Stream } from "effect";

const effectStream = api.getSSE("/events").toStream();
// Now use any Effect Stream combinator
```

---

## Parallel requests

### `Pipeline.all()` — run in parallel, all must succeed

```ts
import { Pipeline } from "@ts-backend/http/client";

const [users, posts, stats] = await Pipeline.all(
  api.get("/users", UsersSchema),
  api.get("/posts", PostsSchema),
  api.get("/stats", StatsSchema),
).runPromise();
```

### `Pipeline.allSettled()` — run in parallel, never short-circuits

Returns `Either` for each result so you can inspect successes and failures individually:

```ts
import { Either } from "effect";

const [usersResult, postsResult] = await Pipeline.allSettled(
  api.get("/users", UsersSchema),
  api.get("/posts", PostsSchema),
).runPromise();

const users = Either.isRight(usersResult) ? usersResult.right : [];
const posts = Either.isRight(postsResult) ? postsResult.right : [];
```

### Parallel with individual retry

Each pipeline retries independently before the parallel join:

```ts
const [users, recommendations] = await Pipeline.all(
  api.get("/users", UsersSchema).retry(3),
  api.get("/recommendations", RecsSchema).retry({ maxRetries: 5, baseDelayMs: 500 }),
).runPromise();
```

---

## Racing and fallbacks

### `Pipeline.race()` — first to succeed wins (static)

```ts
const data = await Pipeline.race(
  api.get("https://us-east.api.com/data", DataSchema),
  api.get("https://eu-west.api.com/data", DataSchema),
).runPromise();
```

### `.race()` — first to succeed wins (instance)

```ts
const data = await api
  .get("/primary", DataSchema)
  .race(api.get("/fallback", DataSchema))
  .runPromise();
```

### `Pipeline.fallback()` — try in order, first success wins

Sequential — no wasted requests:

```ts
const data = await Pipeline.fallback(
  api.get("/primary-source", DataSchema),
  api.get("/secondary-source", DataSchema),
  api.get("/cache/stale", DataSchema),
).runPromise();
```

---

## Error handling

### `.tapError()` — log without changing the error

```ts
const user = await api
  .get("/users/1", UserSchema)
  .tapError((error) => logger.warn("Request failed", { error }))
  .retry()
  .runPromise();
```

### `.orElse()` — fallback value

```ts
const user = await api.get("/users/1", UserSchema).orElse({ id: 0, name: "Unknown" }).runPromise();
// Never throws — returns the fallback on any error.
```

### `.orElsePipeline()` — fallback to a different pipeline

Use this for conditional error recovery:

```ts
const user = await api
  .get("/users/1", UserSchema)
  .orElsePipeline((error) => {
    if (error._tag === "HttpStatusError" && error.status === 404) {
      return api.post("/users", UserSchema, { json: { name: "New User" } });
    }
    return api.get("/users/default", UserSchema);
  })
  .runPromise();
```

### `.catch()` — handle all errors of a specific type

The callback returns a fallback value. It handles **all** errors matching the tag:

```ts
const user = await api
  .get("/users/1", UserSchema)
  .catch("HttpStatusError", (error) => ({ id: 0, name: `error-${error.status}` }))
  .runPromise();
```

For conditional recovery (e.g., only 404), use `.orElsePipeline()` instead.

---

## Cancellation and cleanup

### Fiber interruption aborts the fetch

When an Effect fiber is interrupted (via `.timeout()`, `Effect.race`, or `Fiber.interrupt`),
the underlying `fetch` is immediately aborted via a scoped `AbortController`. No leaked
connections, no waiting for timeouts.

```ts
import { Effect, Fiber } from "effect";

const fiber = Effect.runFork(api.get("/slow", Schema).toEffect());

// Cancel from anywhere — fetch is aborted immediately
await Effect.runPromise(Fiber.interrupt(fiber));
```

### `.interruptOn(signal)` — cancel streams from an AbortSignal

For streams consumed via `.forEach()` / `.collect()` (which return Promises, not Effects),
there's no fiber handle to interrupt from outside. Use `.interruptOn()` to bridge
Hono's disconnect signal into the stream:

```ts
app.get("/events", async (c) => {
  await api
    .getSSE("/upstream/events")
    .interruptOn(c.req.raw.signal) // stop when client disconnects
    .forEach((event) => sendToClient(event));
});
```

### `.finally()` — cleanup regardless of outcome

Runs on success, error, or interruption. Works on both pipeline types:

```ts
// Release resources after pipeline completes
const data = await api
  .get("/data", DataSchema)
  .tap((data) => acquireLock(data.id))
  .finally(() => releaseLock())
  .runPromise();

// Cleanup streaming resources
await api
  .postSSE("/events", { json: body })
  .finally(() => logger.info("Stream ended"))
  .forEach((event) => process(event));
```

---

## Safe execution

### `.runSafe()` — never throws, returns `{ data, error }`

```ts
const { data, error } = await api.get("/users/1", UserSchema).runSafe();

if (error) {
  switch (error._tag) {
    case "HttpNetworkError":
      logger.error("Network issue", { cause: error.cause });
      break;
    case "HttpTimeoutError":
      logger.error("Timed out", { timeoutMs: error.timeoutMs });
      break;
    case "HttpStatusError":
      logger.error("Bad status", { status: error.status, body: error.body });
      break;
    case "HttpParseError":
      logger.error("Parse failed", { cause: error.cause });
      break;
    case "PollTimeoutError":
      logger.error("Poll exhausted", { attempts: error.attempts });
      break;
  }
  return;
}

// data is fully typed as the Zod-inferred type
console.log(data.name);
```

### `.runEither()` — returns `Either<T, HttpClientError>`

For composing with Effect's `Either` utilities:

```ts
import { Either } from "effect";

const result = await api.get("/users/1", UserSchema).runEither();

Either.match(result, {
  onLeft: (error) => logger.warn(error._tag),
  onRight: (user) => console.log(user.name),
});
```

---

## `withOverrides` — derive clients from a shared base

Create one `DefaultHttpClient` with common settings, then derive service-specific clients:

```ts
// main.ts — shared client
const http = new DefaultHttpClient({
  headers: { "User-Agent": "api-ts" },
  timeoutMs: 10_000,
  middleware: [loggingMiddleware],
});

// Each service gets its own base URL / timeout / headers
const aiClient = http.withOverrides({
  baseUrl: config.AI_PROXY_BASE_URI,
  headers: { "X-Feature": "ai-generate" },
  timeoutMs: 60_000,
});

const ytClient = http.withOverrides({
  baseUrl: config.YT_VIEWS_BASE_URI,
});
```

How merging works:

- `baseUrl` / `timeoutMs` — replaced
- `headers` — shallow-merged (override adds/replaces, parent keys preserved)
- `middleware` — concatenated (parent first, then override)

---

## Effect escape hatch

Drop down to raw Effect when you need full power:

```ts
import { Effect } from "effect";

const effect = api.get("/users/1", UserSchema).toEffect();

// Now you can use any Effect combinator
const result = effect.pipe(
  Effect.tap((user) => Effect.log(`Fetched ${user.name}`)),
  Effect.withSpan("fetchUser"),
);

await Effect.runPromise(result);
```

Build pipelines back from effects:

```ts
import { createHttpPipeline } from "@ts-backend/http/client";

const pipeline = createHttpPipeline(someEffect);
const user = await pipeline.retry().runPromise();
```

---

## Error types

All errors extend `Data.TaggedError` — they have structural equality, serialization,
and a `_tag` discriminator for pattern matching:

| Error              | `_tag`               | Key properties                                                                      |
| ------------------ | -------------------- | ----------------------------------------------------------------------------------- |
| `HttpNetworkError` | `"HttpNetworkError"` | `url`, `cause`, `message`                                                           |
| `HttpTimeoutError` | `"HttpTimeoutError"` | `url`, `timeoutMs`, `message`                                                       |
| `HttpStatusError`  | `"HttpStatusError"`  | `url`, `status`, `body`, `message`, `isRetryable`, `isServerError`, `isClientError` |
| `HttpParseError`   | `"HttpParseError"`   | `url`, `cause`, `message`                                                           |
| `PollTimeoutError` | `"PollTimeoutError"` | `attempts`, `lastResult`, `message`                                                 |

All are part of the `HttpClientError` union type.

### When each error is raised

```
Request lifecycle:

  ┌─────────────┐
  │  fetch()    │──── DNS failure, connection refused, ──→ HttpNetworkError
  │             │     socket hang up, fetch aborted
  └──────┬──────┘
         │
         │ timeout fires before response arrives
         │────────────────────────────────────────→ HttpTimeoutError
         │
  ┌──────▼──────┐
  │  Response   │──── status < 200 or status >= 300 ──→ HttpStatusError
  │  received   │     (unless acceptStatus overrides)
  └──────┬──────┘
         │
  ┌──────▼──────┐
  │  Parse JSON │──── response.json() throws ─────────→ HttpParseError
  │             │     (invalid JSON body)
  └──────┬──────┘
         │
  ┌──────▼──────┐
  │  Validate   │──── schema.safeParse() fails ───────→ HttpParseError
  │  (Zod etc.) │     (valid JSON, wrong shape)
  └──────┬──────┘
         │
         ▼
      Success<T>
```

**Streaming adds one more point:**

```
  ┌──────────────┐
  │  Read chunk  │──── reader.read() throws ──────────→ HttpNetworkError
  │  from stream │     (connection dropped mid-stream)
  └──────────────┘
```

**NDJSON streaming adds:**

```
  ┌──────────────┐
  │  Parse line  │──── JSON.parse(line) throws ───────→ HttpParseError
  │  + validate  │     or schema.safeParse() fails
  └──────────────┘
```

**Polling adds:**

```
  ┌──────────────┐
  │  Poll loop   │──── maxAttempts exhausted ─────────→ PollTimeoutError
  │              │     or maxDurationMs exceeded
  └──────────────┘
```

**What does NOT raise an error:**

- Fiber interruption (timeout, race loser, `Fiber.interrupt`) — this is an **interruption**, not an error. The fetch is aborted cleanly via the scoped `AbortController`. Use `Effect.runPromiseExit` + `Exit.isInterrupted` to observe it.
- `.orElse()`, `.catch()`, `.orElsePipeline()` — these **recover** from errors, converting them to success values.

---

## Custom response parsers

The library is validation-library-agnostic. All `schema` parameters accept a `ResponseParser<T>` — any object with a `safeParse` method:

```ts
interface ResponseParser<T> {
  safeParse(data: unknown): { success: true; data: T } | { success: false; error: unknown };
}
```

### Zod (works out of the box)

Zod schemas satisfy `ResponseParser<T>` natively — no wrapper needed:

```ts
import { z } from "zod";

const UserSchema = z.object({ id: z.number(), name: z.string() });

// UserSchema IS a ResponseParser<User> — just pass it directly
const user = await api.get("/users/1", UserSchema).runPromise();
```

### Valibot

Valibot uses a standalone `safeParse(schema, data)` function, so a one-line adapter is needed:

```ts
import * as v from "valibot";
import type { ResponseParser } from "@ts-backend/http/client";

function valibot<T>(schema: v.BaseSchema<unknown, T, any>): ResponseParser<T> {
  return { safeParse: (data) => v.safeParse(schema, data) };
}

const UserSchema = v.object({ id: v.number(), name: v.string() });
const user = await api.get("/users/1", valibot(UserSchema)).runPromise();
```

### ArkType

```ts
import { type } from "arktype";
import type { ResponseParser } from "@ts-backend/http/client";

function arktype<T>(schema: type.Any): ResponseParser<T> {
  return {
    safeParse: (data) => {
      const result = schema(data);
      return result instanceof type.errors
        ? { success: false, error: result.summary }
        : { success: true, data: result as T };
    },
  };
}

const UserSchema = type({ id: "number", name: "string" });
const user = await api.get("/users/1", arktype<User>(UserSchema)).runPromise();
```

### Plain function

```ts
import type { ResponseParser } from "@ts-backend/http/client";

function parser<T>(validate: (data: unknown) => T): ResponseParser<T> {
  return {
    safeParse: (data) => {
      try {
        return { success: true, data: validate(data) };
      } catch (error) {
        return { success: false, error };
      }
    },
  };
}

const user = await api
  .get(
    "/users/1",
    parser((data: unknown) => {
      const obj = data as Record<string, unknown>;
      if (typeof obj.id !== "number" || typeof obj.name !== "string") throw new Error("bad shape");
      return { id: obj.id, name: obj.name };
    }),
  )
  .runPromise();
```

---

## Complex real-world scenarios

### 1. Authenticated API with token refresh

```ts
const authApi = new DefaultHttpClient({
  baseUrl: "https://auth.example.com",
  timeoutMs: 5_000,
});

const dataApi = new DefaultHttpClient({
  baseUrl: "https://api.example.com",
  timeoutMs: 10_000,
});

const TokenSchema = z.object({ accessToken: z.string(), expiresIn: z.number() });
const DataSchema = z.object({ items: z.array(z.string()) });

const data = await authApi
  .post("/oauth/token", TokenSchema, {
    json: { grantType: "client_credentials", clientId: "...", clientSecret: "..." },
  })
  .flatMap((token) =>
    dataApi.get("/protected/data", DataSchema, {
      headers: { Authorization: `Bearer ${token.accessToken}` },
    }),
  )
  .retry({ maxRetries: 2 })
  .runPromise();
```

### 2. Fan-out: fetch user then load all related data in parallel

```ts
const dashboard = await api
  .get("/users/me", UserSchema)
  .flatMap((user) =>
    Pipeline.all(
      api.get(`/users/${user.id}/videos`, VideosSchema),
      api.get(`/users/${user.id}/analytics`, AnalyticsSchema),
      api.get(`/users/${user.id}/notifications`, NotificationsSchema),
    ).map(([videos, analytics, notifications]) => ({
      user,
      videos,
      analytics,
      notifications,
    })),
  )
  .runPromise();
```

### 3. Submit job, poll for completion, download result

```ts
const JobSchema = z.object({
  id: z.string(),
  status: z.enum(["pending", "running", "completed", "failed"]),
  downloadUrl: z.string().optional(),
  error: z.string().optional(),
});

const report = await api
  .post("/reports", JobSchema, {
    json: { type: "monthly", channelId: "UC123" },
  })
  .flatMap((job) =>
    api.get(`/reports/${job.id}`, JobSchema).pollUntilWithBackoff({
      until: (j) => j.status === "completed" || j.status === "failed",
      initialIntervalMs: 500,
      maxIntervalMs: 5_000,
      maxAttempts: 60,
      maxDurationMs: 300_000,
    }),
  )
  .flatMap((job) =>
    job.status === "failed"
      ? Pipeline.fromPromise(() => {
          throw new Error(`Report generation failed: ${job.error}`);
        })
      : api.getText(job.downloadUrl!),
  )
  .runPromise();
```

### 4. Streaming AI completion with cancellation

```ts
app.post("/api/chat", async (c) => {
  const body = c.req.valid("json");
  const chunks: string[] = [];

  await aiApi
    .postSSE("/v1/chat/completions", {
      json: { model: "claude-3", messages: body.messages, stream: true },
    })
    .filter((e) => e.event !== "done")
    .map((e) => JSON.parse(e.data).choices[0]?.delta?.content ?? "")
    .filter((text) => text.length > 0)
    .interruptOn(c.req.raw.signal)
    .finally(() => logger.info("AI stream ended"))
    .forEach((text) => chunks.push(text));

  return c.json({ response: chunks.join("") });
});
```

### 5. Streaming proxy — pipe upstream SSE to downstream client

```ts
app.get("/api/events/:channelId", async (c) => {
  const channelId = c.req.param("channelId");

  return new Response(
    new ReadableStream({
      async start(controller) {
        const encoder = new TextEncoder();

        await api
          .getSSE(`/internal/events/${channelId}`)
          .interruptOn(c.req.raw.signal)
          .finally(() => controller.close())
          .forEach((event) => {
            controller.enqueue(encoder.encode(`event: ${event.event}\ndata: ${event.data}\n\n`));
          });
      },
    }),
    { headers: { "Content-Type": "text/event-stream", "Cache-Control": "no-cache" } },
  );
});
```

### 6. NDJSON bulk export with progress tracking

```ts
let processed = 0;

const results = await api
  .postNDJSON("/export/videos", VideoSchema, {
    json: { channelId: "UC123", format: "ndjson" },
    timeoutMs: 300_000,
  })
  .tap(() => {
    processed++;
    if (processed % 100 === 0) logger.info(`Processed ${processed} videos`);
  })
  .filter((video) => video.viewCount > 1000)
  .collect();

logger.info(`Export complete: ${results.length} high-view videos out of ${processed} total`);
```

### 7. Multi-region with hedged requests

Hit the primary region, but if it's slow, fire a backup after 200ms:

```ts
const data = await Pipeline.hedged(api.get("/data", DataSchema), {
  hedgeDelayMs: 200,
}).runPromise();
```

### 8. Resilient aggregation — partial failures are OK

```ts
import { Either } from "effect";

const videoIds = ["vid1", "vid2", "vid3", "vid4", "vid5"];

const results = await Pipeline.allSettled(
  ...videoIds.map((id) => api.get(`/videos/${id}`, VideoSchema).retry(2)),
).runPromise();

const videos = results.filter(Either.isRight).map((r) => r.right);

const failures = results.filter(Either.isLeft).map((r) => r.left);

logger.info(`Loaded ${videos.length}/${videoIds.length} videos, ${failures.length} failed`);
```

### 9. Webhook with retry and structured error logging

```ts
const WebhookResponseSchema = z.object({ received: z.boolean() });

const sendWebhook = (event: unknown) =>
  api
    .post("/webhooks/youtube", WebhookResponseSchema, { json: event })
    .tapError((error) => {
      logger.error("Webhook delivery failed", { error });
    })
    .retry({ maxRetries: 5, baseDelayMs: 1_000 })
    .timeout(60_000)
    .runPromise();
```

### 10. Batch processing with concurrency control

```ts
import { Effect } from "effect";

const channelIds = ["UC1", "UC2", "UC3" /* ...100 more */];

const effects = channelIds.map((id) =>
  api.get(`/channels/${id}/stats`, ChannelStatsSchema).retry({ maxRetries: 2 }).toEffect(),
);

const results = await Effect.runPromise(
  Effect.all(effects, { concurrency: 10 }), // max 10 in flight
);
```

### 11. Composing pipelines as reusable building blocks

```ts
// Define reusable pipeline factories in your service
function getUser(userId: string) {
  return api.get(`/users/${userId}`, UserSchema).retry({ maxRetries: 2 });
}

function getUserPosts(userId: string) {
  return api.get(`/users/${userId}/posts`, PostsSchema).retry({ maxRetries: 2 });
}

function getUserDashboard(userId: string) {
  return getUser(userId).flatMap((user) =>
    Pipeline.all(getUserPosts(userId), api.get(`/users/${userId}/stats`, StatsSchema)).map(
      ([posts, stats]) => ({ user, posts, stats }),
    ),
  );
}

// Use them anywhere
const dashboard = await getUserDashboard("user-123").runPromise();
```

### 12. Conditional branching based on first response

```ts
const FeatureFlagSchema = z.object({
  useNewApi: z.boolean(),
  maxBatchSize: z.number(),
});

const results = await api
  .get("/config/feature-flags", FeatureFlagSchema)
  .flatMap((flags) => {
    const endpoint = flags.useNewApi ? "/v2/search" : "/v1/search";

    return api
      .post(endpoint, SearchResultsSchema, {
        json: { query: "typescript", limit: flags.maxBatchSize },
      })
      .retry({ maxRetries: flags.useNewApi ? 1 : 3 });
  })
  .runPromise();
```

---

## Pluggable transport layer

The transport layer is the only part of the stack that touches the network. Everything
above it — URL resolution, header merging, Zod validation, retry, streaming, pipelines —
is generic orchestration that works with any transport.

### Why a separate transport?

1. **Swappable runtimes** — The default `FetchTransport` uses the global `fetch` (Bun's
   built-in). A custom transport can slot in `@effect/platform`'s HttpClient, undici, or
   anything else that can produce a `Response` — without touching consumer code.

2. **Observability** — An `@effect/platform` transport gets OpenTelemetry tracing on every
   outbound request for free. With raw `fetch`, you'd have to instrument manually.

3. **Transport-level testing** — Inject a custom transport that returns canned `Response`
   objects to test the full pipeline (status checks, JSON parsing, Zod validation) without
   a running server. This complements `MockHttpClient`, which mocks at a higher level.

4. **Separation of concerns** — Abort-controller management, signal combining, and timeout
   handling live inside the transport. Higher layers only deal with "I have a Response, now
   what?"

### Architecture

```
┌───────────────────────────────────────────────────────┐
│  HttpClient / HttpPipeline / HttpStreamPipeline        │  ← Our API
│  Zod validation, chaining, .runPromise()               │
├───────────────────────────────────────────────────────┤
│  HttpTransport interface                               │  ← Pluggable seam
│  execute(options) → Effect<Response, HttpClientError>  │
├───────────────────────────────────────────────────────┤
│  FetchTransport (default)  │  YourCustomTransport      │  ← Implementations
│  global fetch              │  @effect/platform, etc.   │
└───────────────────────────────────────────────────────┘
```

### Connection pooling

Bun's `fetch` (used by `FetchTransport`) maintains an implicit per-host keep-alive
connection pool. It works, but you have zero control over max connections, idle timeout,
or pool isolation. A transport backed by `NodeHttpClient.layerUndici` (Node only) would
expose explicit pool settings, but on Bun the pool behaviour is the same regardless of
transport.

### Using `EffectPlatformTransport`

The built-in `@effect/platform` transport gives you OpenTelemetry tracing on every
outbound request automatically:

```ts
import { DefaultHttpClient, EffectPlatformTransport } from "@ts-backend/http/client";

const api = new DefaultHttpClient({
  baseUrl: "https://api.example.com",
  transport: new EffectPlatformTransport(),
});

// All requests now get OTel spans: "http.client GET", "http.client POST", etc.
const user = await api.get("/users/1", UserSchema).runPromise();
```

### Custom transport layer (testing)

```ts
import { Layer } from "effect";
import * as PlatformHttpClient from "@effect/platform/HttpClient";

const testLayer = Layer.succeed(PlatformHttpClient.HttpClient, myMockPlatformClient);
const api = new DefaultHttpClient({
  transport: new EffectPlatformTransport({ layer: testLayer }),
});
```

### Writing your own transport

```ts
import { DefaultHttpClient, type HttpTransport } from "@ts-backend/http/client";

class MyTransport implements HttpTransport {
  execute(options) {
    // Map options → your HTTP backend → Response
    // Map their errors → our HttpClientError types
  }
}

const api = new DefaultHttpClient({
  baseUrl: "https://api.example.com",
  transport: new MyTransport(),
});
```

### What does NOT change when swapping transport

- Public API — `.get()`, `.post()`, `.retry()`, `.runPromise()`, etc.
- Zod validation (we do NOT adopt `@effect/schema`)
- Our error types (`HttpStatusError`, etc.) — transports map to these at the boundary
- Streaming, polling, combinators — all stay identical
- `MockHttpClient` — doesn't use transport at all

---

## TODO

### Other improvements

- [x] ~~**Request interceptors**~~ — shipped as `middleware` on `HttpClientConfig` (`onRequest` / `onResponse` / `onError` hooks with `tag` support)
- [x] ~~**Circuit breaker**~~ — available via `@ts-backend/core`: `.withCircuitBreaker(breaker)`
- [x] ~~**Cache**~~ — available via `@ts-backend/core`: `.cached(cache)`
- [x] ~~**Concurrency limiter**~~ — available via `@ts-backend/core`: `.withPermit(semaphore)`
- [x] ~~**Batching**~~ — available via `@ts-backend/core` StreamPipeline: `.groupWithin(size, ms)`
- [x] ~~**Pluggable transport**~~ — `HttpTransport` interface + `FetchTransport` default. Pass `transport` in `HttpClientConfig` to swap backends.
- [x] ~~**`@effect/platform` transport**~~ — `EffectPlatformTransport` wraps `@effect/platform`'s HttpClient. Gets OTel tracing for free. Pass custom `Layer` for test mocking.
- [x] ~~**Multipart upload**~~ — `api.postMultipart(path, schema, { file, fields })` builds FormData internally.
- [ ] **Metrics** — automatic request count, latency histogram, error rate per `DefaultHttpClient` instance (integrate with existing Prometheus registry from `@ts-backend/http`)
