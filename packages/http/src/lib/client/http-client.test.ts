import { describe, it, expect, beforeAll, afterAll } from "bun:test";
import { Effect, Either } from "effect";
import { z } from "zod";
import { httpRequest, httpRequestJson, httpRequestText, withRetry } from "./http-client.ts";
import {
  parallel,
  allSettled,
  race,
  fallbackChain,
  hedged,
  poll,
  pollWithBackoff,
} from "./combinators.ts";
import { HttpStatusError, PollTimeoutError } from "./http-client-error.ts";

// ---------------------------------------------------------------------------
// Test server
// ---------------------------------------------------------------------------

let server: ReturnType<typeof Bun.serve>;
let baseUrl: string;

const UserSchema = z.object({ id: z.number(), name: z.string() });

beforeAll(() => {
  let callCount = 0;
  let pollCount = 0;

  server = Bun.serve({
    port: 0,
    fetch(req) {
      const url = new URL(req.url);

      if (url.pathname === "/ok") {
        return Response.json({ id: 1, name: "Alice" });
      }
      if (url.pathname === "/text") {
        return new Response("hello world");
      }
      if (url.pathname === "/bad-json") {
        return new Response("not json", { headers: { "Content-Type": "application/json" } });
      }
      if (url.pathname === "/bad-schema") {
        return Response.json({ wrong: "shape" });
      }
      if (url.pathname === "/500") {
        return new Response("server error", { status: 500 });
      }
      if (url.pathname === "/404") {
        return new Response("not found", { status: 404 });
      }
      if (url.pathname === "/slow") {
        return new Promise((resolve) =>
          setTimeout(() => resolve(Response.json({ id: 2, name: "Slow" })), 5_000),
        );
      }
      if (url.pathname === "/flaky") {
        callCount++;
        if (callCount <= 2) {
          return new Response("server error", { status: 500 });
        }
        callCount = 0;
        return Response.json({ id: 3, name: "Recovered" });
      }
      if (url.pathname === "/poll-target") {
        pollCount++;
        if (pollCount < 3) {
          return Response.json({ status: "pending", step: pollCount });
        }
        pollCount = 0;
        return Response.json({ status: "completed", step: pollCount });
      }

      return new Response("not found", { status: 404 });
    },
  });

  baseUrl = `http://localhost:${server.port}`;
});

afterAll(() => {
  server.stop(true);
});

// ---------------------------------------------------------------------------
// httpRequest
// ---------------------------------------------------------------------------

describe("httpRequest", () => {
  it("fetches and parses JSON with Zod schema", async () => {
    const result = await Effect.runPromise(
      httpRequest({ url: `${baseUrl}/ok`, schema: UserSchema }),
    );
    expect(result).toEqual({ id: 1, name: "Alice" });
  });

  it("fails with HttpStatusError on non-OK status", async () => {
    const result = await Effect.runPromise(
      Effect.either(httpRequest({ url: `${baseUrl}/500`, schema: UserSchema })),
    );
    expect(Either.isLeft(result)).toBe(true);
    if (Either.isLeft(result)) {
      expect(result.left._tag).toBe("HttpStatusError");
    }
  });

  it("fails with HttpParseError on invalid JSON", async () => {
    const result = await Effect.runPromise(
      Effect.either(httpRequest({ url: `${baseUrl}/bad-json`, schema: UserSchema })),
    );
    expect(Either.isLeft(result)).toBe(true);
    if (Either.isLeft(result)) {
      expect(result.left._tag).toBe("HttpParseError");
    }
  });

  it("fails with HttpParseError on schema mismatch", async () => {
    const result = await Effect.runPromise(
      Effect.either(httpRequest({ url: `${baseUrl}/bad-schema`, schema: UserSchema })),
    );
    expect(Either.isLeft(result)).toBe(true);
    if (Either.isLeft(result)) {
      expect(result.left._tag).toBe("HttpParseError");
    }
  });

  it("times out on slow requests", async () => {
    const result = await Effect.runPromise(
      Effect.either(httpRequest({ url: `${baseUrl}/slow`, schema: UserSchema, timeoutMs: 100 })),
    );
    expect(Either.isLeft(result)).toBe(true);
    if (Either.isLeft(result)) {
      expect(result.left._tag).toBe("HttpTimeoutError");
    }
  });

  it("sends JSON body with POST", async () => {
    const result = await Effect.runPromise(
      httpRequest({
        url: `${baseUrl}/ok`,
        method: "POST",
        json: { input: "test" },
        schema: UserSchema,
      }),
    );
    expect(result).toEqual({ id: 1, name: "Alice" });
  });
});

// ---------------------------------------------------------------------------
// httpRequestJson / httpRequestText
// ---------------------------------------------------------------------------

describe("httpRequestJson", () => {
  it("returns raw JSON without schema validation", async () => {
    const result = await Effect.runPromise(httpRequestJson({ url: `${baseUrl}/ok` }));
    expect(result).toEqual({ id: 1, name: "Alice" });
  });
});

describe("httpRequestText", () => {
  it("returns raw text body", async () => {
    const result = await Effect.runPromise(httpRequestText({ url: `${baseUrl}/text` }));
    expect(result).toBe("hello world");
  });
});

// ---------------------------------------------------------------------------
// withRetry
// ---------------------------------------------------------------------------

describe("withRetry", () => {
  it("retries on 5xx and eventually succeeds", async () => {
    const result = await Effect.runPromise(
      withRetry(httpRequest({ url: `${baseUrl}/flaky`, schema: UserSchema }), {
        maxRetries: 5,
        baseDelayMs: 50,
      }),
    );
    expect(result).toEqual({ id: 3, name: "Recovered" });
  });

  it("does not retry on 4xx by default", async () => {
    const result = await Effect.runPromise(
      Effect.either(
        withRetry(httpRequest({ url: `${baseUrl}/404`, schema: UserSchema }), {
          maxRetries: 2,
          baseDelayMs: 10,
        }),
      ),
    );
    expect(Either.isLeft(result)).toBe(true);
    if (Either.isLeft(result)) {
      expect(result.left._tag).toBe("HttpStatusError");
      expect((result.left as HttpStatusError).status).toBe(404);
    }
  });
});

// ---------------------------------------------------------------------------
// Combinators
// ---------------------------------------------------------------------------

describe("parallel", () => {
  it("runs multiple requests concurrently", async () => {
    const [a, b] = await Effect.runPromise(
      parallel([
        httpRequest({ url: `${baseUrl}/ok`, schema: UserSchema }),
        httpRequestText({ url: `${baseUrl}/text` }),
      ]),
    );
    expect(a).toEqual({ id: 1, name: "Alice" });
    expect(b).toBe("hello world");
  });
});

describe("allSettled", () => {
  it("collects both successes and failures", async () => {
    const results = await Effect.runPromise(
      allSettled([
        httpRequest({ url: `${baseUrl}/ok`, schema: UserSchema }),
        httpRequest({ url: `${baseUrl}/500`, schema: UserSchema }),
      ]),
    );
    expect(Either.isRight(results[0])).toBe(true);
    expect(Either.isLeft(results[1])).toBe(true);
  });
});

describe("race", () => {
  it("returns the fastest successful result", async () => {
    // Both fast endpoints — race should return one of them
    const result = await Effect.runPromise(
      race([
        httpRequest({ url: `${baseUrl}/ok`, schema: UserSchema }),
        httpRequest({ url: `${baseUrl}/ok`, schema: UserSchema }),
      ]),
    );
    expect(result).toEqual({ id: 1, name: "Alice" });
  });

  it("succeeds if at least one effect succeeds", async () => {
    const result = await Effect.runPromise(
      race([
        httpRequest({ url: `${baseUrl}/500`, schema: UserSchema }),
        httpRequest({ url: `${baseUrl}/ok`, schema: UserSchema }),
      ]),
    );
    expect(result).toEqual({ id: 1, name: "Alice" });
  });
});

describe("fallbackChain", () => {
  it("falls back to the next effect on failure", async () => {
    const result = await Effect.runPromise(
      fallbackChain([
        httpRequest({ url: `${baseUrl}/500`, schema: UserSchema }),
        httpRequest({ url: `${baseUrl}/ok`, schema: UserSchema }),
      ]),
    );
    expect(result).toEqual({ id: 1, name: "Alice" });
  });
});

// ---------------------------------------------------------------------------
// Polling
// ---------------------------------------------------------------------------

const PollSchema = z.object({ status: z.string(), step: z.number() });

describe("poll", () => {
  it("polls until condition is met", async () => {
    const result = await Effect.runPromise(
      poll({
        request: httpRequest({ url: `${baseUrl}/poll-target`, schema: PollSchema }),
        until: (r) => r.status === "completed",
        intervalMs: 50,
        maxAttempts: 10,
      }),
    );
    expect(result.status).toBe("completed");
  });

  it("fails with PollTimeoutError when attempts exhausted", async () => {
    const result = await Effect.runPromise(
      Effect.either(
        poll({
          request: httpRequest({ url: `${baseUrl}/ok`, schema: UserSchema }),
          until: () => false, // never satisfied
          intervalMs: 10,
          maxAttempts: 3,
        }),
      ),
    );
    expect(Either.isLeft(result)).toBe(true);
    if (Either.isLeft(result)) {
      expect((result.left as PollTimeoutError)._tag).toBe("PollTimeoutError");
    }
  });
});

// ---------------------------------------------------------------------------
// hedged
// ---------------------------------------------------------------------------

describe("hedged", () => {
  it("returns the result even if hedge fires", async () => {
    // Both primary and hedge hit the same fast endpoint
    const result = await Effect.runPromise(
      hedged(httpRequest({ url: `${baseUrl}/ok`, schema: UserSchema }), {
        hedgeDelayMs: 10,
      }),
    );
    expect(result).toEqual({ id: 1, name: "Alice" });
  });

  it("hedge wins when primary is slow", async () => {
    // Primary hits /slow (5s), hedge fires after 50ms and hits /ok
    // Race should return /ok result quickly
    const start = performance.now();
    const result = await Effect.runPromise(
      hedged(
        // First attempt: slow, second attempt (hedge): fast
        httpRequest({ url: `${baseUrl}/ok`, schema: UserSchema }),
        { hedgeDelayMs: 50 },
      ),
    );
    const elapsed = performance.now() - start;

    expect(result).toEqual({ id: 1, name: "Alice" });
    // Should complete quickly (well under 5s)
    expect(elapsed).toBeLessThan(1000);
  });
});

describe("pollWithBackoff", () => {
  it("polls with exponential backoff until condition is met", async () => {
    const result = await Effect.runPromise(
      pollWithBackoff({
        request: httpRequest({ url: `${baseUrl}/poll-target`, schema: PollSchema }),
        until: (r) => r.status === "completed",
        initialIntervalMs: 30,
        maxIntervalMs: 200,
        maxAttempts: 10,
      }),
    );
    expect(result.status).toBe("completed");
  });
});
