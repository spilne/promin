import { describe, it, expect } from "bun:test";
import { z } from "zod";
import { MockHttpClient, createMockHttpClient } from "../testing.ts";
import { identityParser } from "../http-pipeline.ts";

const UserSchema = z.object({ id: z.number(), name: z.string() });

// ---------------------------------------------------------------------------
// Basic responses
// ---------------------------------------------------------------------------

describe("MockHttpClient", () => {
  it("returns {} by default", async () => {
    const client = new MockHttpClient();
    const result = await client.get("/anything", identityParser).runPromise();
    expect(result).toEqual({});
  });

  it("respondWith sets default for all requests", async () => {
    const client = new MockHttpClient().respondWith({ id: 1, name: "Default" });
    const result = await client.get("/anything", UserSchema).runPromise();
    expect(result).toEqual({ id: 1, name: "Default" });
  });

  it(".on() registers per-route responses", async () => {
    const client = new MockHttpClient()
      .on("GET", "/users/1", { id: 1, name: "Alice" })
      .on("GET", "/users/2", { id: 2, name: "Bob" });

    const alice = await client.get("/users/1", UserSchema).runPromise();
    const bob = await client.get("/users/2", UserSchema).runPromise();

    expect(alice).toEqual({ id: 1, name: "Alice" });
    expect(bob).toEqual({ id: 2, name: "Bob" });
  });

  it("unmatched routes return default", async () => {
    const client = new MockHttpClient()
      .respondWith({ id: 0, name: "Fallback" })
      .on("GET", "/users/1", { id: 1, name: "Alice" });

    const fallback = await client.get("/unknown", UserSchema).runPromise();
    expect(fallback).toEqual({ id: 0, name: "Fallback" });
  });

  it("matches on method + path", async () => {
    const client = new MockHttpClient()
      .on("GET", "/users", { id: 1, name: "GET" })
      .on("POST", "/users", { id: 2, name: "POST" });

    const get = await client.get("/users", UserSchema).runPromise();
    const post = await client.post("/users", UserSchema, { json: {} }).runPromise();

    expect(get.name).toBe("GET");
    expect(post.name).toBe("POST");
  });

  it("rejects response that does not match schema", async () => {
    const client = new MockHttpClient().on("GET", "/users/1", { wrong: "shape" });

    const { error } = await client.get("/users/1", UserSchema).runSafe();
    expect(error?._tag).toBe("HttpParseError");
  });
});

// ---------------------------------------------------------------------------
// Failure responses
// ---------------------------------------------------------------------------

describe("MockHttpClient.fail", () => {
  it("returns HttpStatusError", async () => {
    const client = new MockHttpClient().on("GET", "/404", MockHttpClient.fail(404));

    const { error } = await client.get("/404", UserSchema).runSafe();
    expect(error?._tag).toBe("HttpStatusError");
    expect((error as any).status).toBe(404);
  });

  it("fail with body", async () => {
    const client = new MockHttpClient().on(
      "GET",
      "/err",
      MockHttpClient.fail(500, "internal error"),
    );

    const { error } = await client.get("/err", UserSchema).runSafe();
    expect((error as any).status).toBe(500);
    expect((error as any).body).toBe("internal error");
  });
});

// ---------------------------------------------------------------------------
// Dynamic responses
// ---------------------------------------------------------------------------

describe("onFn — dynamic responses", () => {
  it("response depends on request body", async () => {
    const client = new MockHttpClient().onFn("POST", "/search", (call) => {
      const query = (call.json as any)?.query;
      return query === "typescript" ? { results: ["ts-lib"], count: 1 } : { results: [], count: 0 };
    });

    const ts = (await client
      .post("/search", identityParser, { json: { query: "typescript" } })
      .runPromise()) as any;
    const empty = (await client
      .post("/search", identityParser, { json: { query: "nothing" } })
      .runPromise()) as any;

    expect(ts.results).toEqual(["ts-lib"]);
    expect(empty.results).toEqual([]);
  });

  it("dynamic handler can return errors", async () => {
    const client = new MockHttpClient().onFn("GET", "/users", (call) => {
      if (call.headers?.["Authorization"] === undefined) {
        return MockHttpClient.fail(401);
      }
      return { id: 1, name: "Authenticated" };
    });

    const { error } = await client.get("/users", UserSchema).runSafe();
    expect(error?._tag).toBe("HttpStatusError");

    const { data } = await client
      .get("/users", UserSchema, { headers: { Authorization: "Bearer tok" } })
      .runSafe();
    expect((data as any).name).toBe("Authenticated");
  });
});

// ---------------------------------------------------------------------------
// Ordered responses (for retry/polling)
// ---------------------------------------------------------------------------

describe("onSequence — ordered responses", () => {
  it("returns responses in order, then repeats last", async () => {
    const client = new MockHttpClient().onSequence("GET", "/jobs/1", [
      { status: "running" },
      { status: "running" },
      { status: "completed", result: "done" },
    ]);

    const r1 = (await client.get("/jobs/1", identityParser).runPromise()) as any;
    const r2 = (await client.get("/jobs/1", identityParser).runPromise()) as any;
    const r3 = (await client.get("/jobs/1", identityParser).runPromise()) as any;
    const r4 = (await client.get("/jobs/1", identityParser).runPromise()) as any;

    expect(r1.status).toBe("running");
    expect(r2.status).toBe("running");
    expect(r3.status).toBe("completed");
    expect(r4.status).toBe("completed");
  });

  it("simulates flaky endpoint: fail then succeed", async () => {
    const client = new MockHttpClient().onSequence("GET", "/flaky", [
      MockHttpClient.fail(500),
      MockHttpClient.fail(500),
      { id: 1, name: "Recovered" },
    ]);

    const { error: e1 } = await client.get("/flaky", UserSchema).runSafe();
    expect(e1?._tag).toBe("HttpStatusError");

    const { error: e2 } = await client.get("/flaky", UserSchema).runSafe();
    expect(e2?._tag).toBe("HttpStatusError");

    const { data } = await client.get("/flaky", UserSchema).runSafe();
    expect((data as any).name).toBe("Recovered");
  });

  it("works with pollUntil", async () => {
    const client = new MockHttpClient().onSequence("GET", "/jobs/1", [
      { status: "running" },
      { status: "running" },
      { status: "completed", result: "done" },
    ]);

    const JobSchema = z.object({ status: z.string(), result: z.string().optional() });

    const result = await client
      .get("/jobs/1", JobSchema)
      .pollUntil({
        until: (job) => job.status === "completed",
        intervalMs: 10,
        maxAttempts: 10,
      })
      .runPromise();

    expect(result.status).toBe("completed");
    expect(result.result).toBe("done");
    expect(client.calledTimes("GET", "/jobs/1")).toBe(3);
  });
});

// ---------------------------------------------------------------------------
// Call recording
// ---------------------------------------------------------------------------

describe("call recording", () => {
  it("tracks method, path, json, headers, tag", async () => {
    const client = new MockHttpClient()
      .on("GET", "/users/1", { id: 1, name: "Alice" })
      .on("POST", "/users", { id: 2, name: "New" });

    await client.get("/users/1", UserSchema).runPromise();
    await client
      .post("/users", UserSchema, {
        json: { name: "New" },
        headers: { "X-Custom": "val" },
        tag: "create-user",
      })
      .runPromise();

    expect(client.calls).toHaveLength(2);
    expect(client.calls[0]).toEqual({ method: "GET", path: "/users/1" });
    expect(client.calls[1]).toEqual({
      method: "POST",
      path: "/users",
      json: { name: "New" },
      headers: { "X-Custom": "val" },
      tag: "create-user",
    });
  });

  it("records body when sent", async () => {
    const client = new MockHttpClient().respondWith({ id: 1, name: "OK" });
    await client.post("/upload", UserSchema, { body: "raw-data" }).runPromise();
    expect(client.calls[0].body).toBe("raw-data");
  });

  it("omits undefined fields", async () => {
    const client = new MockHttpClient().respondWith({ id: 1, name: "OK" });
    await client.get("/simple", UserSchema).runPromise();

    const call = client.calls[0];
    expect(call).toEqual({ method: "GET", path: "/simple" });
    expect("json" in call).toBe(false);
    expect("body" in call).toBe(false);
    expect("headers" in call).toBe(false);
    expect("tag" in call).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// Assertion helpers
// ---------------------------------------------------------------------------

describe("assertion helpers", () => {
  it("calledWith checks method + path", async () => {
    const client = new MockHttpClient().on("GET", "/users/1", { id: 1, name: "Alice" });
    await client.get("/users/1", UserSchema).runPromise();

    expect(client.calledWith("GET", "/users/1")).toBe(true);
    expect(client.calledWith("POST", "/users/1")).toBe(false);
    expect(client.calledWith("GET", "/users/2")).toBe(false);
  });

  it("calledTimes counts calls per route", async () => {
    const client = new MockHttpClient()
      .on("GET", "/users/1", { id: 1, name: "Alice" })
      .on("GET", "/users/2", { id: 2, name: "Bob" });

    await client.get("/users/1", UserSchema).runPromise();
    await client.get("/users/1", UserSchema).runPromise();
    await client.get("/users/2", UserSchema).runPromise();

    expect(client.calledTimes("GET", "/users/1")).toBe(2);
    expect(client.calledTimes("GET", "/users/2")).toBe(1);
    expect(client.calledTimes("POST", "/users")).toBe(0);
  });

  it("calledWithJson matches on body content", async () => {
    const client = new MockHttpClient().on("POST", "/users", { id: 1, name: "Alice" });
    await client
      .post("/users", UserSchema, { json: { name: "Alice", role: "admin" } })
      .runPromise();

    expect(client.calledWithJson("POST", "/users", { name: "Alice", role: "admin" })).toBe(true);
    expect(client.calledWithJson("POST", "/users", { name: "Bob" })).toBe(false);
  });

  it("callsFor returns filtered calls", async () => {
    const client = new MockHttpClient()
      .on("GET", "/a", { id: 1, name: "A" })
      .on("POST", "/b", { id: 2, name: "B" });

    await client.get("/a", UserSchema).runPromise();
    await client.post("/b", UserSchema, { json: { x: 1 } }).runPromise();
    await client.get("/a", UserSchema).runPromise();

    const aCalls = client.callsFor("GET", "/a");
    expect(aCalls).toHaveLength(2);

    const bCalls = client.callsFor("POST", "/b");
    expect(bCalls).toHaveLength(1);
    expect(bCalls[0].json).toEqual({ x: 1 });
  });

  it("lastCall returns the most recent call", async () => {
    const client = new MockHttpClient()
      .on("GET", "/first", { id: 1, name: "First" })
      .on("GET", "/second", { id: 2, name: "Second" });

    expect(client.lastCall).toBeUndefined();

    await client.get("/first", UserSchema).runPromise();
    await client.get("/second", UserSchema).runPromise();

    expect(client.lastCall?.path).toBe("/second");
  });
});

// ---------------------------------------------------------------------------
// Reset
// ---------------------------------------------------------------------------

describe("reset", () => {
  it("resetCalls clears calls but keeps routes", async () => {
    const client = new MockHttpClient().on("GET", "/users/1", { id: 1, name: "Alice" });

    await client.get("/users/1", UserSchema).runPromise();
    expect(client.calls).toHaveLength(1);

    client.resetCalls();
    expect(client.calls).toHaveLength(0);

    const result = await client.get("/users/1", UserSchema).runPromise();
    expect(result).toEqual({ id: 1, name: "Alice" });
  });

  it("reset clears everything", async () => {
    const client = new MockHttpClient()
      .respondWith({ id: 99, name: "Custom" })
      .on("GET", "/users/1", { id: 1, name: "Alice" });

    await client.get("/users/1", UserSchema).runPromise();
    client.reset();

    const result = await client.get("/users/1", identityParser).runPromise();
    expect(result).toEqual({});
  });
});

// ---------------------------------------------------------------------------
// Convenience methods
// ---------------------------------------------------------------------------

describe("convenience methods", () => {
  it("all HTTP methods delegate to request()", async () => {
    const client = new MockHttpClient().respondWith({ id: 1, name: "OK" });

    await client.get("/a", UserSchema).runPromise();
    await client.post("/b", UserSchema, { json: {} }).runPromise();
    await client.put("/c", UserSchema, { json: {} }).runPromise();
    await client.patch("/d", UserSchema, { json: {} }).runPromise();
    await client.delete("/e", UserSchema).runPromise();

    expect(client.calls.map((c) => c.method)).toEqual(["GET", "POST", "PUT", "PATCH", "DELETE"]);
  });

  it("getText works", async () => {
    const client = new MockHttpClient().on("GET", "/health", "OK");
    const text = await client.getText("/health").runPromise();
    expect(text).toBe("OK");
  });

  it("getText with Effect.suspend works with retry/sequence", async () => {
    const client = new MockHttpClient().onSequence("GET", "/health", ["starting", "ready"]);

    const r1 = await client.getText("/health").runPromise();
    const r2 = await client.getText("/health").runPromise();

    expect(r1).toBe("starting");
    expect(r2).toBe("ready");
  });

  it("getJson/postJson delegate to request", async () => {
    const client = new MockHttpClient().respondWith({ status: "ok" });
    const json = await client.getJson("/info").runPromise();
    expect(json).toEqual({ status: "ok" });
    expect(client.calls[0].method).toBe("GET");
  });
});

// ---------------------------------------------------------------------------
// Streaming mocks
// ---------------------------------------------------------------------------

describe("SSE mocking", () => {
  it("onSSE returns registered events", async () => {
    const client = new MockHttpClient().onSSE("/events", [
      { event: "delta", data: "hello" },
      { event: "done", data: "[DONE]" },
    ]);

    const events = await client.getSSE("/events").collect();
    expect(events).toHaveLength(2);
    expect(events[0].data).toBe("hello");
  });

  it("postSSE uses same route map", async () => {
    const client = new MockHttpClient().onSSE("/chat", [{ event: "delta", data: "hi" }]);
    const events = await client.postSSE("/chat", { json: {} }).collect();
    expect(events).toHaveLength(1);
  });

  it("unregistered SSE path returns empty stream", async () => {
    const client = new MockHttpClient();
    const events = await client.getSSE("/unknown").collect();
    expect(events).toHaveLength(0);
  });
});

describe("NDJSON mocking", () => {
  it("onNDJSON returns registered items", async () => {
    const client = new MockHttpClient().onNDJSON("/export", [
      { id: 1, name: "Alice" },
      { id: 2, name: "Bob" },
    ]);

    const items = await client.getNDJSON("/export", UserSchema).collect();
    expect(items).toHaveLength(2);
    expect(items[0]).toEqual({ id: 1, name: "Alice" });
  });
});

describe("raw stream mocking", () => {
  it("onStream returns registered text", async () => {
    const client = new MockHttpClient().onStream("/file", "hello world");
    const chunks = await client.getStream("/file").collect();
    expect(chunks.join("")).toBe("hello world");
  });
});

// ---------------------------------------------------------------------------
// Pipeline chaining on mock
// ---------------------------------------------------------------------------

describe("pipeline chaining on mock", () => {
  it("map, flatMap, retry all work", async () => {
    const client = new MockHttpClient()
      .on("GET", "/users/1", { id: 1, name: "Alice" })
      .on("GET", "/users/1/posts", { userId: 1, title: "Hello" });

    const PostSchema = z.object({ userId: z.number(), title: z.string() });

    const result = await client
      .get("/users/1", UserSchema)
      .flatMap((user) => client.get(`/users/${user.id}/posts`, PostSchema))
      .map((post) => post.title)
      .runPromise();

    expect(result).toBe("Hello");
  });
});

// ---------------------------------------------------------------------------
// Schema validation
// ---------------------------------------------------------------------------

describe("schema validation", () => {
  it("validates response through schema", async () => {
    const client = new MockHttpClient().on("GET", "/users/1", { id: 1, name: "Alice" });
    const result = await client.get("/users/1", UserSchema).runPromise();
    expect(result).toEqual({ id: 1, name: "Alice" });
  });

  it("rejects response that does not match schema", async () => {
    const client = new MockHttpClient().on("GET", "/users/1", { wrong: "shape" });
    const { error } = await client.get("/users/1", UserSchema).runSafe();
    expect(error?._tag).toBe("HttpParseError");
  });

  it("strips extra fields via schema", async () => {
    const client = new MockHttpClient().on("GET", "/users/1", {
      id: 1,
      name: "Alice",
      extra: "field",
    });
    const result = await client.get("/users/1", UserSchema).runPromise();
    expect(result).toEqual({ id: 1, name: "Alice" });
    expect((result as any).extra).toBeUndefined();
  });

  it("skips validation with identityParser", async () => {
    const client = new MockHttpClient().on("GET", "/raw", { anything: true });
    const result = await client.get("/raw", identityParser).runPromise();
    expect(result).toEqual({ anything: true });
  });
});

// ---------------------------------------------------------------------------
// createMockHttpClient helper
// ---------------------------------------------------------------------------

describe("createMockHttpClient", () => {
  it("returns a MockHttpClient instance", async () => {
    const client = createMockHttpClient().on("GET", "/test", { id: 1, name: "Test" });
    const result = await client.get("/test", UserSchema).runPromise();
    expect(result).toEqual({ id: 1, name: "Test" });
  });
});

// ---------------------------------------------------------------------------
// postMultipart
// ---------------------------------------------------------------------------

describe("postMultipart via MockHttpClient", () => {
  it("records FormData body in calls", async () => {
    const client = new MockHttpClient().on("POST", "/upload", { id: 1, name: "uploaded" });
    const file = new File(["content"], "doc.txt", { type: "text/plain" });

    const result = await client
      .postMultipart("/upload", UserSchema, { file, fields: { tag: "test" } })
      .runPromise();

    expect(result).toEqual({ id: 1, name: "uploaded" });
    expect(client.calledWith("POST", "/upload")).toBe(true);
    expect(client.lastCall?.body).toBeInstanceOf(FormData);

    const formData = client.lastCall?.body as FormData;
    expect(formData.get("file")).toBeInstanceOf(File);
    expect(formData.get("tag")).toBe("test");
  });

  it("uses custom fileField name", async () => {
    const client = new MockHttpClient().on("POST", "/upload", { id: 1, name: "ok" });
    const file = new File(["data"], "photo.png", { type: "image/png" });

    await client.postMultipart("/upload", UserSchema, { file, fileField: "image" }).runPromise();

    const formData = client.lastCall?.body as FormData;
    expect(formData.get("image")).toBeInstanceOf(File);
    expect(formData.get("file")).toBeNull();
  });
});
