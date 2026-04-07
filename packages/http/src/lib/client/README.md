# @promin/http/client

Effect-based HTTP client with Zod validation, streaming (SSE, NDJSON), polling, and pluggable transports.

Returns `HttpPipeline<T>` — a lazy Pipeline with all Pipeline combinators (retry, timeout, circuit breaker, race, parallel). See the [Pipeline docs](/docs/guide/pipeline) for chaining, error handling, and resilience patterns.

## Why not just fetch?

Standard HTTP clients (`fetch`, `axios`, `ky`) leave you to solve the same problems on every call site:

- **Retry** — you write a retry loop, handle backoff, decide which errors to retry. Every call site re-invents this.
- **Timeout** — `fetch` has `signal: AbortSignal.timeout()` but it doesn't compose with retry (do you timeout the whole chain or each attempt?).
- **Validation** — you get `any` back from `.json()`. Zod parsing is manual per endpoint.
- **Streaming** — SSE and NDJSON require manual `ReadableStream` parsing with error handling. No backpressure, no operators.
- **Polling** — you write `while` loops with `setTimeout`. Backoff, max attempts, and cancellation are DIY.
- **Composition** — "fetch user, then fetch their posts" requires nested `try/catch` blocks. Parallel fetches need `Promise.all` with manual error aggregation.
- **Testing** — mocking `fetch` globally is fragile. Per-test overrides are verbose.
- **Cancellation** — aborting in-flight requests when a component unmounts or a race loser finishes requires manual `AbortController` plumbing.

This client solves all of these with a single chainable API. Every request returns a lazy pipeline — nothing executes until you call a terminal. Retry, timeout, validation, and streaming compose naturally:

```ts
// With fetch: ~40 lines of retry loop + timeout + validation + error handling
// With @promin/http:
const user = await api.get("/users/1", UserSchema).retry(3).timeout(5_000).runPromise();
```

## Quick start

```ts
import { DefaultHttpClient } from "@promin/http/client";
import { z } from "zod";

const api = new DefaultHttpClient({
  baseUrl: "https://api.example.com",
  headers: { Authorization: "Bearer token" },
  timeoutMs: 10_000,
});

const UserSchema = z.object({ id: z.number(), name: z.string() });

// GET with Zod validation + retry
const user = await api.get("/users/1", UserSchema).retry(3).runPromise();

// POST with JSON body
const created = await api.post("/users", UserSchema, { json: { name: "Alice" } }).runPromise();
```

## Request methods

```ts
api.get("/path", Schema); // GET + validate
api.post("/path", Schema, { json: body }); // POST + validate
api.put("/path", Schema, { json: body }); // PUT + validate
api.patch("/path", Schema, { json: body }); // PATCH + validate
api.delete("/path", Schema); // DELETE + validate
api.getJson("/path"); // GET → unknown (no validation)
api.getText("/path"); // GET → string
api.postMultipart("/upload", Schema, { file }); // Multipart upload
```

## Polling

```ts
// Poll until job completes — fixed interval
const job = await api
  .get(`/jobs/${jobId}`, JobSchema)
  .pollUntil({
    until: (j) => j.status === "completed" || j.status === "failed",
    intervalMs: 2_000,
    maxAttempts: 30,
  })
  .runPromise();

// Exponential backoff polling
const result = await api
  .get(`/exports/${id}`, ExportSchema)
  .pollUntilWithBackoff({
    until: (r) => r.ready,
    initialIntervalMs: 500,
    maxIntervalMs: 10_000,
    maxAttempts: 20,
  })
  .runPromise();
```

## Streaming

Streaming methods return `HttpStreamPipeline<T>` — lazy, backpressured, with all StreamPipeline operators.

### SSE (Server-Sent Events)

```ts
await api
  .postSSE("/v1/chat/completions", {
    json: { model: "claude-3", messages, stream: true },
  })
  .filter((e) => e.event === "delta")
  .map((e) => JSON.parse(e.data).text)
  .forEach((token) => process.stdout.write(token));
```

### NDJSON (newline-delimited JSON)

```ts
const items = await api
  .postNDJSON("/export", ItemSchema, { json: { format: "ndjson" } })
  .parAsyncMap(10, (item) => enrichFromDb(item.id))
  .groupWithin(500, 1_000)
  .tapAsync((batch) => db.bulkInsert(batch))
  .drain();
```

### Stream cancellation

```ts
// Stop when client disconnects (Hono example)
app.get("/events", async (c) => {
  await api
    .getSSE("/upstream/events")
    .interruptOn(c.req.raw.signal)
    .forEach((event) => sendToClient(event));
});
```

## Mocking

```ts
import { MockHttpClient } from "@promin/http/client";

const http = new MockHttpClient();
http.on("GET", "/users/1", { id: 1, name: "Alice" });
http.on("POST", "/users", { id: 2, name: "Bob" });

const user = await http.get("/users/1", UserSchema).runPromise();
```

Services depend on the `HttpClient` interface, not the implementation:

```ts
export class UserService {
  constructor(private http: HttpClient) {}

  getUser(id: string) {
    return this.http.get(`/users/${id}`, UserSchema).retry(2);
  }
}

// Production
new UserService(new DefaultHttpClient({ baseUrl }));

// Test
new UserService(new MockHttpClient().on("GET", "/users/1", mockUser));
```

## Derived clients

Create per-service clients from a shared base:

```ts
const http = new DefaultHttpClient({
  headers: { "User-Agent": "my-app" },
  timeoutMs: 10_000,
});

const aiClient = http.withOverrides({
  baseUrl: "https://ai.example.com",
  timeoutMs: 60_000,
});

const dataClient = http.withOverrides({
  baseUrl: "https://data.example.com",
});
```

## Custom validation libraries

Any object with `safeParse(data) → { success, data } | { success, error }` works:

```ts
// Zod — works natively
api.get("/users/1", z.object({ id: z.number() }));

// Valibot — one-line adapter
const valibot = <T>(schema: v.BaseSchema<unknown, T, any>) => ({
  safeParse: (data: unknown) => v.safeParse(schema, data),
});

api.get("/users/1", valibot(v.object({ id: v.number() })));
```

## Pluggable transport

Default uses `fetch`. Swap to `@effect/platform` for OpenTelemetry tracing:

```ts
import { EffectPlatformTransport } from "@promin/http/client";

const api = new DefaultHttpClient({
  baseUrl: "https://api.example.com",
  transport: new EffectPlatformTransport(), // OTel spans on every request
});
```

## Error types

| Error              | When                                            |
| ------------------ | ----------------------------------------------- |
| `HttpNetworkError` | DNS failure, connection refused, socket hang up |
| `HttpTimeoutError` | Timeout fires before response                   |
| `HttpStatusError`  | Non-2xx status code                             |
| `HttpParseError`   | Invalid JSON or Zod validation failure          |
| `PollTimeoutError` | Poll maxAttempts or maxDurationMs exceeded      |

All errors have `_tag` for pattern matching via `.catch()`, `.orElsePipeline()`, or `switch`.
