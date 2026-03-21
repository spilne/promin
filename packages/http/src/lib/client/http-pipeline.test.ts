import { describe, it, expect, beforeAll, afterAll } from "bun:test";
import { Either } from "effect";
import { z } from "zod";
import { DefaultHttpClient, HttpPipeline, PollTimeoutError } from "./index.ts";
import { MockHttpClient } from "./testing.ts";

// ---------------------------------------------------------------------------
// Schemas
// ---------------------------------------------------------------------------

const UserSchema = z.object({ id: z.number(), name: z.string() });
const PostSchema = z.object({ userId: z.number(), title: z.string() });
const JobSchema = z.object({ status: z.string(), result: z.string().optional() });
const UsersSchema = z.array(UserSchema);
const StatsSchema = z.object({ total: z.number() });

// ---------------------------------------------------------------------------
// MockHttpClient setup — no server, no network
// ---------------------------------------------------------------------------

function createMock() {
  return new MockHttpClient()
    .on("GET", "/users/1", { id: 1, name: "Alice" })
    .on("GET", "/users/1/posts", { userId: 1, title: "Hello World" })
    .on("GET", "/users", [
      { id: 1, name: "Alice" },
      { id: 2, name: "Bob" },
    ])
    .on("GET", "/stats", { total: 42 })
    .on("GET", "/text", "plain text response")
    .on("GET", "/500", MockHttpClient.fail(500))
    .on("GET", "/bad-schema", { wrong: "shape" })
    .onSequence("GET", "/flaky", [
      MockHttpClient.fail(500),
      MockHttpClient.fail(500),
      { id: 99, name: "Recovered" },
    ])
    .onFn("POST", "/echo", (call) => call.json ?? { id: 0, name: "echo" })
    .onSequence("GET", "/job/123", [
      { status: "running" },
      { status: "running" },
      { status: "completed", result: "done!" },
    ]);
}

// ---------------------------------------------------------------------------
// Basic requests
// ---------------------------------------------------------------------------

describe("HttpClient", () => {
  it("GET with schema", async () => {
    const api = createMock();
    const user = await api.get("/users/1", UserSchema).runPromise();
    expect(user).toEqual({ id: 1, name: "Alice" });
  });

  it("POST with json body", async () => {
    const api = createMock();
    const user = await api
      .post("/echo", UserSchema, { json: { id: 7, name: "Test" } })
      .runPromise();
    expect(user).toEqual({ id: 7, name: "Test" });
  });

  it("getText", async () => {
    const api = createMock();
    const text = await api.getText("/text").runPromise();
    expect(text).toBe("plain text response");
  });

  it("getJson (no schema)", async () => {
    const api = createMock();
    const data = await api.getJson("/users/1").runPromise();
    expect(data).toEqual({ id: 1, name: "Alice" });
  });
});

// ---------------------------------------------------------------------------
// Chaining
// ---------------------------------------------------------------------------

describe("HttpPipeline chaining", () => {
  it("map transforms the result", async () => {
    const api = createMock();
    const name = await api
      .get("/users/1", UserSchema)
      .map((u) => u.name.toUpperCase())
      .runPromise();
    expect(name).toBe("ALICE");
  });

  it("flatMap chains dependent requests", async () => {
    const api = createMock();
    const post = await api
      .get("/users/1", UserSchema)
      .flatMap((user) => api.get(`/users/${user.id}/posts`, PostSchema))
      .runPromise();
    expect(post).toEqual({ userId: 1, title: "Hello World" });
  });

  it("multi-step flatMap chain", async () => {
    const api = createMock();
    const result = await api
      .get("/users/1", UserSchema)
      .flatMap((user) => api.get(`/users/${user.id}/posts`, PostSchema))
      .map((post) => post.title)
      .runPromise();
    expect(result).toBe("Hello World");
  });

  it("tap runs side effect without changing value", async () => {
    const api = createMock();
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
    const api = createMock();
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
    const api = createMock();
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
    const api = createMock();
    const user = await api
      .get("/flaky", UserSchema)
      .retry({ maxRetries: 5, baseDelayMs: 10 })
      .runPromise();
    expect(user).toEqual({ id: 99, name: "Recovered" });
  });

  it("retry does NOT retry parse errors", async () => {
    const api = createMock();
    const { error } = await api
      .get("/bad-schema", UserSchema)
      .retry({ maxRetries: 3, baseDelayMs: 10 })
      .runSafe();

    expect(error?._tag).toBe("HttpParseError");
  });

  it("retry does NOT catch defects from map", async () => {
    const api = createMock();
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
    expect(threw).toBe(true);
  });

  it("retry with custom when can include parse errors", async () => {
    const api = createMock();
    const { error } = await api
      .get("/bad-schema", UserSchema)
      .retry({
        maxRetries: 2,
        baseDelayMs: 10,
        when: (err) => err._tag === "HttpParseError" || err._tag === "HttpStatusError",
      })
      .runSafe();

    expect(error?._tag).toBe("HttpParseError");
  });

  it("orElse provides fallback value", async () => {
    const api = createMock();
    const user = await api.get("/500", UserSchema).orElse({ id: 0, name: "fallback" }).runPromise();
    expect(user).toEqual({ id: 0, name: "fallback" });
  });

  it("orElsePipeline provides fallback pipeline", async () => {
    const api = createMock();
    const user = await api
      .get("/500", UserSchema)
      .orElsePipeline(() => api.get("/users/1", UserSchema))
      .runPromise();
    expect(user).toEqual({ id: 1, name: "Alice" });
  });

  it("catch handles specific error types", async () => {
    const api = createMock();
    const user = await api
      .get("/500", UserSchema)
      .catch("HttpStatusError", (err) => ({ id: -1, name: `error-${err.status}` }))
      .runPromise();
    expect(user).toEqual({ id: -1, name: "error-500" });
  });

  it("tapError inspects errors", async () => {
    const api = createMock();
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
    const api = createMock();
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
    const api = createMock();
    const user = await api
      .get("/flaky", UserSchema)
      .retryAll({ maxRetries: 5, baseDelayMs: 10 })
      .runPromise();

    expect(user).toEqual({ id: 99, name: "Recovered" });
  });

  it("retries until success value satisfies condition", async () => {
    const api = createMock();
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
    const api = createMock();
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
        shouldRetry: (r) => r._tag === "defect",
      })
      .runSafe();

    expect(error?._tag).toBe("HttpStatusError");
    expect(attempts).toBe(0);
  });

  it("returns last success value when until not satisfied after all retries", async () => {
    const api = createMock();
    const result = await api
      .get("/users/1", UserSchema)
      .retryAll({
        maxRetries: 2,
        baseDelayMs: 10,
        shouldRetry: (r) => r._tag !== "success" || r.value.name !== "NonExistent",
      })
      .runPromise();

    expect(result).toEqual({ id: 1, name: "Alice" });
  });

  it("does not retry success by default", async () => {
    const api = createMock();
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
    expect(callCount).toBe(1);
  });
});

// ---------------------------------------------------------------------------
// runSafe
// ---------------------------------------------------------------------------

describe("runSafe", () => {
  it("returns data on success", async () => {
    const api = createMock();
    const result = await api.get("/users/1", UserSchema).runSafe();
    expect(result.data).toEqual({ id: 1, name: "Alice" });
    expect(result.error).toBeNull();
  });

  it("returns error on failure", async () => {
    const api = createMock();
    const result = await api.get("/500", UserSchema).runSafe();
    expect(result.data).toBeNull();
    expect(result.error?._tag).toBe("HttpStatusError");
  });

  it("catchAll catches defects from map", async () => {
    const api = createMock();
    const result = await api
      .get("/users/1", UserSchema)
      .map(() => {
        throw new Error("boom");
      })
      .runSafe({ catchAll: true });

    expect(result.data).toBeNull();
    expect(result.error).toBeInstanceOf(Error);
    expect((result.error as Error).message).toBe("boom");
  });

  it("without catchAll, defects still throw", async () => {
    const api = createMock();
    let threw = false;
    try {
      await api
        .get("/users/1", UserSchema)
        .map(() => {
          throw new Error("boom");
        })
        .runSafe();
    } catch {
      threw = true;
    }
    expect(threw).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// runEither
// ---------------------------------------------------------------------------

describe("runEither", () => {
  it("returns Right on success", async () => {
    const api = createMock();
    const result = await api.get("/users/1", UserSchema).runEither();
    expect(Either.isRight(result)).toBe(true);
    if (Either.isRight(result)) {
      expect(result.right).toEqual({ id: 1, name: "Alice" });
    }
  });

  it("returns Left on failure", async () => {
    const api = createMock();
    const result = await api.get("/500", UserSchema).runEither();
    expect(Either.isLeft(result)).toBe(true);
    if (Either.isLeft(result)) {
      expect(result.left._tag).toBe("HttpStatusError");
    }
  });

  it("works with Either.match", async () => {
    const api = createMock();
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

describe("HttpPipeline.all", () => {
  it("runs pipelines in parallel", async () => {
    const api = createMock();
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
    const api = createMock();
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
    const api = createMock();
    const user = await HttpPipeline.race(
      api.get("/users/1", UserSchema),
      api.get("/users/1", UserSchema),
    ).runPromise();
    expect(user).toEqual({ id: 1, name: "Alice" });
  });
});

describe("HttpPipeline.fallback", () => {
  it("tries in order until one succeeds", async () => {
    const api = createMock();
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
    const api = createMock();
    const job = await api
      .get("/job/123", JobSchema)
      .pollUntil({
        until: (j) => j.status === "completed",
        intervalMs: 10,
        maxAttempts: 10,
      })
      .runPromise();
    expect(job.status).toBe("completed");
    expect(job.result).toBe("done!");
  });

  it("times out when condition never met", async () => {
    const api = createMock();
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
    const api = createMock();
    const job = await api
      .get("/job/123", JobSchema)
      .pollUntilWithBackoff({
        until: (j) => j.status === "completed",
        initialIntervalMs: 10,
        maxIntervalMs: 50,
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
    const api = createMock();
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
    const api = createMock();
    const [user, stats] = await HttpPipeline.all(
      api.get("/flaky", UserSchema).retry({ maxRetries: 5, baseDelayMs: 10 }),
      api.get("/500", StatsSchema).orElse({ total: 0 }),
    ).runPromise();
    expect(user).toEqual({ id: 99, name: "Recovered" });
    expect(stats).toEqual({ total: 0 });
  });

  it("regex route matching", async () => {
    const api = new MockHttpClient().on("GET", /\/users\/\d+/, { id: 1, name: "Any User" });

    const u1 = await api.get("/users/1", UserSchema).runPromise();
    const u2 = await api.get("/users/999", UserSchema).runPromise();
    expect(u1.name).toBe("Any User");
    expect(u2.name).toBe("Any User");
    expect(api.calledTimes("GET", /\/users\/\d+/)).toBe(2);
  });
});

// ---------------------------------------------------------------------------
// Middleware — needs real HTTP server (DefaultHttpClient behavior)
// ---------------------------------------------------------------------------

let server: ReturnType<typeof Bun.serve>;

beforeAll(() => {
  server = Bun.serve({
    port: 0,
    async fetch(req) {
      const url = new URL(req.url);
      if (url.pathname === "/users/1") return Response.json({ id: 1, name: "Alice" });
      if (url.pathname === "/500") return new Response("server error", { status: 500 });
      if (url.pathname === "/echo")
        return req
          .json()
          .then((body: any) => Response.json({ id: body.id ?? 0, name: body.name ?? "echo" }));
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
});

afterAll(() => {
  server.stop(true);
});

function realClient(overrides?: Parameters<typeof DefaultHttpClient.prototype.withOverrides>[0]) {
  const base = new DefaultHttpClient({ baseUrl: `http://localhost:${server.port}` });
  return overrides ? base.withOverrides(overrides) : base;
}

describe("HttpClientConfig.middleware", () => {
  it("onError fires on every request failure", async () => {
    const errors: string[] = [];
    const api = realClient({ middleware: [{ onError: (_ctx, error) => errors.push(error._tag) }] });

    await api.get("/500", UserSchema).orElse({ id: 0, name: "fallback" }).runPromise();
    await api.get("/500", UserSchema).orElse({ id: 0, name: "fallback" }).runPromise();

    expect(errors).toEqual(["HttpStatusError", "HttpStatusError"]);
  });

  it("onError does not fire on success", async () => {
    const errors: string[] = [];
    const api = realClient({ middleware: [{ onError: (_ctx, error) => errors.push(error._tag) }] });

    await api.get("/users/1", UserSchema).runPromise();
    expect(errors).toEqual([]);
  });

  it("onResponse receives durationMs", async () => {
    const timings: { method: string; url: string; durationMs: number }[] = [];
    const api = realClient({
      middleware: [
        {
          onResponse: (ctx) =>
            timings.push({ method: ctx.method, url: ctx.url, durationMs: ctx.durationMs }),
        },
      ],
    });

    await api.get("/users/1", UserSchema).runPromise();
    expect(timings).toHaveLength(1);
    expect(timings[0].method).toBe("GET");
    expect(timings[0].durationMs).toBeGreaterThan(0);
  });

  it("onRequest fires before the request", async () => {
    const events: string[] = [];
    const api = realClient({
      middleware: [
        { onRequest: () => events.push("request"), onResponse: () => events.push("response") },
      ],
    });

    await api.get("/users/1", UserSchema).runPromise();
    expect(events).toEqual(["request", "response"]);
  });

  it("multiple middleware all fire", async () => {
    const order: string[] = [];
    const api = realClient({
      middleware: [
        { onResponse: () => order.push("first") },
        { onResponse: () => order.push("second") },
      ],
    });

    await api.get("/users/1", UserSchema).runPromise();
    expect(order).toEqual(["first", "second"]);
  });

  it("tag is passed to middleware context", async () => {
    const tags: (string | undefined)[] = [];
    const api = realClient({ middleware: [{ onRequest: (ctx) => tags.push(ctx.tag) }] });

    await api.get("/users/1", UserSchema, { tag: "get-user" }).runPromise();
    await api.get("/users/1", UserSchema).runPromise();

    expect(tags).toEqual(["get-user", undefined]);
  });
});

// ---------------------------------------------------------------------------
// postMultipart — needs real HTTP server
// ---------------------------------------------------------------------------

describe("postMultipart", () => {
  it("sends file and validates response", async () => {
    const api = realClient();
    const file = new File(["content"], "doc.txt", { type: "text/plain" });
    const result = await api.postMultipart("/upload", UserSchema, { file }).runPromise();
    expect(result).toEqual({ id: 1, name: "doc.txt:none" });
  });

  it("sends file with additional fields", async () => {
    const api = realClient();
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
    const api = realClient();
    const file = new File(["content"], "image.jpg", { type: "image/jpeg" });
    const result = await api
      .postMultipart("/upload", UserSchema, {
        file,
        fileField: "attachment",
      })
      .runPromise();
    expect(result).toEqual({ id: 1, name: "unknown:none" });
  });

  it("returns HttpStatusError on non-OK", async () => {
    const api = realClient();
    const file = new File(["content"], "doc.txt", { type: "text/plain" });
    const result = await api.postMultipart("/500", UserSchema, { file }).runSafe();
    expect(result.error).not.toBeNull();
    expect(result.error!._tag).toBe("HttpStatusError");
  });
});
