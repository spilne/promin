import { describe, it, expect, beforeAll, afterAll } from "bun:test";
import { Either } from "effect";
import { z } from "zod";
import { DefaultHttpClient, HttpPipeline, PollTimeoutError } from "./index.ts";

// ---------------------------------------------------------------------------
// Test server
// ---------------------------------------------------------------------------

let server: ReturnType<typeof Bun.serve>;
let api: DefaultHttpClient;

const UserSchema = z.object({ id: z.number(), name: z.string() });
const PostSchema = z.object({ userId: z.number(), title: z.string() });
const JobSchema = z.object({ status: z.string(), result: z.string().optional() });

beforeAll(() => {
  let flakyCount = 0;
  let pollCount = 0;

  server = Bun.serve({
    port: 0,
    async fetch(req) {
      const url = new URL(req.url);

      if (url.pathname === "/users/1") {
        return Response.json({ id: 1, name: "Alice" });
      }
      if (url.pathname === "/users/1/posts") {
        return Response.json({ userId: 1, title: "Hello World" });
      }
      if (url.pathname === "/users") {
        return Response.json([
          { id: 1, name: "Alice" },
          { id: 2, name: "Bob" },
        ]);
      }
      if (url.pathname === "/stats") {
        return Response.json({ total: 42 });
      }
      if (url.pathname === "/text") {
        return new Response("plain text response");
      }
      if (url.pathname === "/500") {
        return new Response("server error", { status: 500 });
      }
      if (url.pathname === "/bad-schema") {
        return Response.json({ wrong: "shape" });
      }
      if (url.pathname === "/flaky") {
        flakyCount++;
        if (flakyCount <= 2) {
          return new Response("error", { status: 500 });
        }
        flakyCount = 0;
        return Response.json({ id: 99, name: "Recovered" });
      }
      if (url.pathname === "/echo") {
        return req
          .json()
          .then((body: any) => Response.json({ id: body.id ?? 0, name: body.name ?? "echo" }));
      }
      if (url.pathname === "/job/123") {
        pollCount++;
        if (pollCount < 3) {
          return Response.json({ status: "running" });
        }
        pollCount = 0;
        return Response.json({ status: "completed", result: "done!" });
      }

      if (url.pathname === "/upload") {
        const formData = await req.formData();
        const file = formData.get("file");
        const desc = formData.get("description");
        return Response.json({
          id: 1,
          name: `${file instanceof File ? file.name : "unknown"}:${desc ?? "none"}`,
        });
      }

      return new Response("not found", { status: 404 });
    },
  });

  api = new DefaultHttpClient({
    baseUrl: `http://localhost:${server.port}`,
    headers: { "X-Test": "true" },
  });
});

afterAll(() => {
  server.stop(true);
});

// ---------------------------------------------------------------------------
// Basic requests
// ---------------------------------------------------------------------------

describe("HttpClient", () => {
  it("GET with schema", async () => {
    const user = await api.get("/users/1", UserSchema).runPromise();
    expect(user).toEqual({ id: 1, name: "Alice" });
  });

  it("POST with json body", async () => {
    const user = await api
      .post("/echo", UserSchema, { json: { id: 7, name: "Test" } })
      .runPromise();
    expect(user).toEqual({ id: 7, name: "Test" });
  });

  it("getText", async () => {
    const text = await api.getText("/text").runPromise();
    expect(text).toBe("plain text response");
  });

  it("getJson (no schema)", async () => {
    const data = await api.getJson("/users/1").runPromise();
    expect(data).toEqual({ id: 1, name: "Alice" });
  });
});

// ---------------------------------------------------------------------------
// Chaining
// ---------------------------------------------------------------------------

describe("HttpPipeline chaining", () => {
  it("map transforms the result", async () => {
    const name = await api
      .get("/users/1", UserSchema)
      .map((u) => u.name.toUpperCase())
      .runPromise();
    expect(name).toBe("ALICE");
  });

  it("flatMap chains dependent requests", async () => {
    const post = await api
      .get("/users/1", UserSchema)
      .flatMap((user) => api.get(`/users/${user.id}/posts`, PostSchema))
      .runPromise();
    expect(post).toEqual({ userId: 1, title: "Hello World" });
  });

  it("multi-step flatMap chain", async () => {
    const result = await api
      .get("/users/1", UserSchema)
      .flatMap((user) => api.get(`/users/${user.id}/posts`, PostSchema))
      .map((post) => post.title)
      .runPromise();
    expect(result).toBe("Hello World");
  });

  it("tap runs side effect without changing value", async () => {
    let sideEffect = "";
    const user = await api
      .get("/users/1", UserSchema)
      .tap((u) => {
        sideEffect = u.name;
      })
      .runPromise();
    expect(user).toEqual({ id: 1, name: "Alice" });
    expect(sideEffect).toBe("Alice");
  });

  it("tapAsync awaits async side effect", async () => {
    let sideEffect = "";
    const user = await api
      .get("/users/1", UserSchema)
      .tapAsync(async (u) => {
        await new Promise((r) => setTimeout(r, 10));
        sideEffect = u.name;
      })
      .runPromise();
    expect(user).toEqual({ id: 1, name: "Alice" });
    expect(sideEffect).toBe("Alice");
  });

  it("mapAsync transforms with async function", async () => {
    const name = await api
      .get("/users/1", UserSchema)
      .mapAsync(async (u) => {
        await new Promise((r) => setTimeout(r, 10));
        return u.name.toUpperCase();
      })
      .runPromise();
    expect(name).toBe("ALICE");
  });
});

// ---------------------------------------------------------------------------
// Resilience
// ---------------------------------------------------------------------------

describe("HttpPipeline resilience", () => {
  it("retry recovers from transient failures", async () => {
    const user = await api
      .get("/flaky", UserSchema)
      .retry({ maxRetries: 5, baseDelayMs: 20 })
      .runPromise();
    expect(user).toEqual({ id: 99, name: "Recovered" });
  });

  it("retry does NOT retry parse errors", async () => {
    let attempts = 0;
    const { error } = await api
      .get("/bad-schema", UserSchema)
      .tap(() => {
        attempts++;
      })
      .retry({ maxRetries: 3, baseDelayMs: 10 })
      .runSafe();

    expect(error?._tag).toBe("HttpParseError");
    // tap never runs because parse fails before it, but retry should not re-attempt
  });

  it("retry does NOT catch defects from map", async () => {
    let threw = false;
    try {
      await api
        .get("/users/1", UserSchema)
        .map(() => {
          throw new Error("boom");
        })
        .retry({ maxRetries: 3, baseDelayMs: 10 })
        .runPromise();
    } catch {
      threw = true;
    }
    // Defects crash the pipeline — retry doesn't catch them
    expect(threw).toBe(true);
  });

  it("retry with custom when can include parse errors", async () => {
    // Override the default to also retry parse errors
    const { error } = await api
      .get("/bad-schema", UserSchema)
      .retry({
        maxRetries: 2,
        baseDelayMs: 10,
        when: (err) => err._tag === "HttpParseError" || err._tag === "HttpStatusError",
      })
      .runSafe();

    // Still fails after retries (server always returns bad schema), but it DID retry
    expect(error?._tag).toBe("HttpParseError");
  });

  it("orElse provides fallback value", async () => {
    const user = await api.get("/500", UserSchema).orElse({ id: 0, name: "fallback" }).runPromise();
    expect(user).toEqual({ id: 0, name: "fallback" });
  });

  it("orElsePipeline provides fallback pipeline", async () => {
    const user = await api
      .get("/500", UserSchema)
      .orElsePipeline(() => api.get("/users/1", UserSchema))
      .runPromise();
    expect(user).toEqual({ id: 1, name: "Alice" });
  });

  it("catch handles specific error types", async () => {
    const user = await api
      .get("/500", UserSchema)
      .catch("HttpStatusError", (err) => ({ id: -1, name: `error-${err.status}` }))
      .runPromise();
    expect(user).toEqual({ id: -1, name: "error-500" });
  });

  it("tapError inspects errors", async () => {
    let errorTag = "";
    const user = await api
      .get("/500", UserSchema)
      .tapError((e) => {
        errorTag = e._tag;
      })
      .orElse({ id: 0, name: "recovered" })
      .runPromise();
    expect(errorTag).toBe("HttpStatusError");
    expect(user).toEqual({ id: 0, name: "recovered" });
  });
});

// ---------------------------------------------------------------------------
// retryAll
// ---------------------------------------------------------------------------

describe("retryAll", () => {
  it("retries defects from mapAsync", async () => {
    // Note: the entire effect (HTTP fetch + transform) is retried, not just mapAsync.
    // This is by design — there's no way to split the pipeline mid-execution.
    let attempts = 0;
    const user = await api
      .get("/users/1", UserSchema)
      .mapAsync(async (u) => {
        attempts++;
        if (attempts < 3) throw new Error("db connection lost");
        return u;
      })
      .retryAll({ maxRetries: 5, baseDelayMs: 10 })
      .runPromise();

    expect(user).toEqual({ id: 1, name: "Alice" });
    expect(attempts).toBe(3);
  });

  it("retries HTTP errors by default", async () => {
    const user = await api
      .get("/flaky", UserSchema)
      .retryAll({ maxRetries: 5, baseDelayMs: 10 })
      .runPromise();

    expect(user).toEqual({ id: 99, name: "Recovered" });
  });

  it("retries until success value satisfies condition", async () => {
    let callCount = 0;
    const result = await api
      .get("/users/1", UserSchema)
      .map((u) => {
        callCount++;
        return { ...u, ready: callCount >= 3 };
      })
      .retryAll({
        maxRetries: 10,
        baseDelayMs: 10,
        shouldRetry: (r) => r._tag !== "success" || !r.value.ready,
      })
      .runPromise();

    expect(result.ready).toBe(true);
    expect(callCount).toBe(3);
  });

  it("only retries errors when shouldRetry filters", async () => {
    let attempts = 0;
    const { error } = await api
      .get("/500", UserSchema)
      .map(() => {
        attempts++;
        return { id: 1, name: "ok" };
      })
      .retryAll({
        maxRetries: 3,
        baseDelayMs: 10,
        shouldRetry: (r) => r._tag === "defect", // only defects, not typedError
      })
      .runSafe();

    // HTTP error should NOT be retried — fails immediately
    expect(error?._tag).toBe("HttpStatusError");
    expect(attempts).toBe(0); // map never ran because HTTP failed
  });

  it("returns last success value when until not satisfied after all retries", async () => {
    const result = await api
      .get("/users/1", UserSchema)
      .retryAll({
        maxRetries: 2,
        baseDelayMs: 10,
        shouldRetry: (r) => r._tag !== "success" || r.value.name !== "NonExistent",
      })
      .runPromise();

    // Never satisfied, but after exhausting retries returns the actual value
    expect(result).toEqual({ id: 1, name: "Alice" });
  });

  it("does not retry success by default", async () => {
    let callCount = 0;
    const result = await api
      .get("/users/1", UserSchema)
      .map((u) => {
        callCount++;
        return u;
      })
      .retryAll({ maxRetries: 5, baseDelayMs: 10 })
      .runPromise();

    expect(result).toEqual({ id: 1, name: "Alice" });
    expect(callCount).toBe(1); // no retries
  });
});

// ---------------------------------------------------------------------------
// runSafe
// ---------------------------------------------------------------------------

describe("runSafe", () => {
  it("returns data on success", async () => {
    const result = await api.get("/users/1", UserSchema).runSafe();
    expect(result.data).toEqual({ id: 1, name: "Alice" });
    expect(result.error).toBeNull();
  });

  it("returns error on failure", async () => {
    const result = await api.get("/500", UserSchema).runSafe();
    expect(result.data).toBeNull();
    expect(result.error?._tag).toBe("HttpStatusError");
  });
});

// ---------------------------------------------------------------------------
// runEither
// ---------------------------------------------------------------------------

describe("runEither", () => {
  it("returns Right on success", async () => {
    const result = await api.get("/users/1", UserSchema).runEither();
    expect(Either.isRight(result)).toBe(true);
    if (Either.isRight(result)) {
      expect(result.right).toEqual({ id: 1, name: "Alice" });
    }
  });

  it("returns Left on failure", async () => {
    const result = await api.get("/500", UserSchema).runEither();
    expect(Either.isLeft(result)).toBe(true);
    if (Either.isLeft(result)) {
      expect(result.left._tag).toBe("HttpStatusError");
    }
  });

  it("works with Either.match", async () => {
    const result = await api.get("/users/1", UserSchema).runEither();
    const name = Either.match(result, {
      onLeft: () => "error",
      onRight: (user) => user.name,
    });
    expect(name).toBe("Alice");
  });
});

// ---------------------------------------------------------------------------
// Static combinators
// ---------------------------------------------------------------------------

const UsersSchema = z.array(UserSchema);
const StatsSchema = z.object({ total: z.number() });

describe("HttpPipeline.all", () => {
  it("runs pipelines in parallel", async () => {
    const [users, stats] = await HttpPipeline.all(
      api.get("/users", UsersSchema),
      api.get("/stats", StatsSchema),
    ).runPromise();
    expect(users).toHaveLength(2);
    expect(stats.total).toBe(42);
  });
});

describe("HttpPipeline.allSettled", () => {
  it("collects successes and failures", async () => {
    const results = await HttpPipeline.allSettled(
      api.get("/users/1", UserSchema),
      api.get("/500", UserSchema),
    ).runPromise();
    expect(Either.isRight(results[0])).toBe(true);
    expect(Either.isLeft(results[1])).toBe(true);
  });
});

describe("HttpPipeline.race", () => {
  it("returns first to succeed", async () => {
    const user = await HttpPipeline.race(
      api.get("/users/1", UserSchema),
      api.get("/users/1", UserSchema),
    ).runPromise();
    expect(user).toEqual({ id: 1, name: "Alice" });
  });
});

describe("HttpPipeline.fallback", () => {
  it("tries in order until one succeeds", async () => {
    const user = await HttpPipeline.fallback(
      api.get("/500", UserSchema),
      api.get("/users/1", UserSchema),
    ).runPromise();
    expect(user).toEqual({ id: 1, name: "Alice" });
  });
});

// ---------------------------------------------------------------------------
// Polling
// ---------------------------------------------------------------------------

describe("pollUntil", () => {
  it("polls until condition is met", async () => {
    const job = await api
      .get("/job/123", JobSchema)
      .pollUntil({
        until: (j) => j.status === "completed",
        intervalMs: 30,
        maxAttempts: 10,
      })
      .runPromise();
    expect(job.status).toBe("completed");
    expect(job.result).toBe("done!");
  });

  it("times out when condition never met", async () => {
    const result = await api
      .get("/users/1", UserSchema)
      .pollUntil({
        until: () => false,
        intervalMs: 10,
        maxAttempts: 3,
      })
      .runSafe();
    expect(result.error).not.toBeNull();
    expect((result.error as PollTimeoutError)._tag).toBe("PollTimeoutError");
  });
});

describe("pollUntilWithBackoff", () => {
  it("polls with backoff until condition is met", async () => {
    const job = await api
      .get("/job/123", JobSchema)
      .pollUntilWithBackoff({
        until: (j) => j.status === "completed",
        initialIntervalMs: 20,
        maxIntervalMs: 100,
        maxAttempts: 10,
      })
      .runPromise();
    expect(job.status).toBe("completed");
  });
});

// ---------------------------------------------------------------------------
// Complex real-world composition
// ---------------------------------------------------------------------------

describe("real-world patterns", () => {
  it("fetch user → fetch posts → transform", async () => {
    const result = await api
      .get("/users/1", UserSchema)
      .flatMap((user) =>
        api.get(`/users/${user.id}/posts`, PostSchema).map((post) => ({
          userName: user.name,
          postTitle: post.title,
        })),
      )
      .runPromise();
    expect(result).toEqual({ userName: "Alice", postTitle: "Hello World" });
  });

  it("parallel fetch with retry + fallback", async () => {
    const [user, stats] = await HttpPipeline.all(
      api.get("/flaky", UserSchema).retry({ maxRetries: 5, baseDelayMs: 20 }),
      api.get("/500", StatsSchema).orElse({ total: 0 }),
    ).runPromise();
    expect(user).toEqual({ id: 99, name: "Recovered" });
    expect(stats).toEqual({ total: 0 });
  });
});

// ---------------------------------------------------------------------------
// Middleware
// ---------------------------------------------------------------------------

describe("HttpClientConfig.middleware", () => {
  it("onError fires on every request failure", async () => {
    const errors: string[] = [];
    const apiWithMiddleware = new DefaultHttpClient({
      baseUrl: `http://localhost:${server.port}`,
      middleware: [{ onError: (_ctx, error) => errors.push(error._tag) }],
    });

    await apiWithMiddleware
      .get("/500", UserSchema)
      .orElse({ id: 0, name: "fallback" })
      .runPromise();
    await apiWithMiddleware
      .get("/500", UserSchema)
      .orElse({ id: 0, name: "fallback" })
      .runPromise();

    expect(errors).toEqual(["HttpStatusError", "HttpStatusError"]);
  });

  it("onError does not fire on success", async () => {
    const errors: string[] = [];
    const apiWithMiddleware = new DefaultHttpClient({
      baseUrl: `http://localhost:${server.port}`,
      middleware: [{ onError: (_ctx, error) => errors.push(error._tag) }],
    });

    await apiWithMiddleware.get("/users/1", UserSchema).runPromise();

    expect(errors).toEqual([]);
  });

  it("onResponse receives durationMs", async () => {
    const timings: { method: string; url: string; durationMs: number }[] = [];
    const apiWithMiddleware = new DefaultHttpClient({
      baseUrl: `http://localhost:${server.port}`,
      middleware: [
        {
          onResponse: (ctx) =>
            timings.push({ method: ctx.method, url: ctx.url, durationMs: ctx.durationMs }),
        },
      ],
    });

    await apiWithMiddleware.get("/users/1", UserSchema).runPromise();

    expect(timings).toHaveLength(1);
    expect(timings[0].method).toBe("GET");
    expect(timings[0].url).toContain("/users/1");
    expect(timings[0].durationMs).toBeGreaterThan(0);
  });

  it("onRequest fires before the request", async () => {
    const events: string[] = [];
    const apiWithMiddleware = new DefaultHttpClient({
      baseUrl: `http://localhost:${server.port}`,
      middleware: [
        {
          onRequest: () => events.push("request"),
          onResponse: () => events.push("response"),
        },
      ],
    });

    await apiWithMiddleware.get("/users/1", UserSchema).runPromise();

    expect(events).toEqual(["request", "response"]);
  });

  it("multiple middleware all fire", async () => {
    const order: string[] = [];
    const apiWithMiddleware = new DefaultHttpClient({
      baseUrl: `http://localhost:${server.port}`,
      middleware: [
        { onResponse: () => order.push("first") },
        { onResponse: () => order.push("second") },
      ],
    });

    await apiWithMiddleware.get("/users/1", UserSchema).runPromise();

    expect(order).toEqual(["first", "second"]);
  });

  it("middleware receives correct method and url", async () => {
    const contexts: { method: string; url: string }[] = [];
    const apiWithMiddleware = new DefaultHttpClient({
      baseUrl: `http://localhost:${server.port}`,
      middleware: [
        {
          onRequest: (ctx) => contexts.push({ method: ctx.method, url: ctx.url }),
        },
      ],
    });

    await apiWithMiddleware.get("/users/1", UserSchema).runPromise();
    await apiWithMiddleware
      .post("/echo", UserSchema, { json: { id: 1, name: "Test" } })
      .runPromise();

    expect(contexts).toHaveLength(2);
    expect(contexts[0].method).toBe("GET");
    expect(contexts[0].url).toContain("/users/1");
    expect(contexts[1].method).toBe("POST");
    expect(contexts[1].url).toContain("/echo");
  });

  it("onError receives durationMs", async () => {
    let errorDuration = 0;
    const apiWithMiddleware = new DefaultHttpClient({
      baseUrl: `http://localhost:${server.port}`,
      middleware: [
        {
          onError: (ctx) => {
            errorDuration = ctx.durationMs;
          },
        },
      ],
    });

    await apiWithMiddleware
      .get("/500", UserSchema)
      .orElse({ id: 0, name: "fallback" })
      .runPromise();

    expect(errorDuration).toBeGreaterThan(0);
  });

  it("tag is passed to middleware context", async () => {
    const tags: (string | undefined)[] = [];
    const apiWithMiddleware = new DefaultHttpClient({
      baseUrl: `http://localhost:${server.port}`,
      middleware: [
        {
          onRequest: (ctx) => tags.push(ctx.tag),
        },
      ],
    });

    await apiWithMiddleware.get("/users/1", UserSchema, { tag: "get-user" }).runPromise();
    await apiWithMiddleware.get("/users/1", UserSchema).runPromise(); // no tag

    expect(tags).toEqual(["get-user", undefined]);
  });
});

// ---------------------------------------------------------------------------
// postMultipart
// ---------------------------------------------------------------------------

describe("postMultipart", () => {
  it("sends file and validates response", async () => {
    const file = new File(["content"], "doc.txt", { type: "text/plain" });
    const result = await api.postMultipart("/upload", UserSchema, { file }).runPromise();
    expect(result).toEqual({ id: 1, name: "doc.txt:none" });
  });

  it("sends file with additional fields", async () => {
    const file = new File(["content"], "photo.png", { type: "image/png" });
    const result = await api
      .postMultipart("/upload", UserSchema, {
        file,
        fields: { description: "my photo" },
      })
      .runPromise();
    expect(result).toEqual({ id: 1, name: "photo.png:my photo" });
  });

  it("uses custom fileField name", async () => {
    const file = new File(["content"], "image.jpg", { type: "image/jpeg" });
    const result = await api
      .postMultipart("/upload", UserSchema, {
        file,
        fileField: "attachment",
      })
      .runPromise();
    // Server reads "file" field — custom field name means server gets null for "file"
    expect(result).toEqual({ id: 1, name: "unknown:none" });
  });

  it("returns HttpStatusError on non-OK", async () => {
    const file = new File(["content"], "doc.txt", { type: "text/plain" });
    const result = await api.postMultipart("/500", UserSchema, { file }).runSafe();
    expect(result.error).not.toBeNull();
    expect(result.error!._tag).toBe("HttpStatusError");
  });
});
