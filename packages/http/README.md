# @promin/http

Resilient HTTP client — retry, polling, SSE, NDJSON streaming.

## Install

```bash
bun add @promin/http
```

## Quick Example

```typescript
import { DefaultHttpClient } from "@promin/http/client";
import { z } from "zod";

const api = new DefaultHttpClient({
  baseUrl: "https://api.example.com",
  headers: { Authorization: "Bearer token" },
  timeoutMs: 10_000,
});

const UserSchema = z.object({ id: z.number(), name: z.string() });

// GET with Zod validation + retry + timeout
const user = await api
  .get("/users/1", UserSchema)
  .retry(3)
  .timeout(5_000)
  .runPromise();
```

Every request returns a lazy pipeline — nothing executes until you call a terminal. Retry, timeout, validation, and streaming compose naturally through Pipeline combinators.

## Features

- **Zod validation** — response schemas validated automatically
- **Retry** — configurable retries with exponential backoff
- **Timeout** — per-request and per-attempt timeouts
- **SSE streaming** — Server-Sent Events with typed event parsing
- **NDJSON streaming** — newline-delimited JSON with backpressure
- **Polling** — declarative long-polling with backoff and cancellation
- **Composition** — chain, race, and parallelize requests with Pipeline API
- **Pluggable transports** — swap fetch implementation for testing

## Documentation

Full docs and examples: [packages/http](https://github.com/spilne/promin/tree/main/packages/http)
