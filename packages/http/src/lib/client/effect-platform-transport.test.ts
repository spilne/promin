import { describe, it, expect, beforeAll, afterAll } from "bun:test";
import { z } from "zod";
import { EffectPlatformTransport } from "./effect-platform-transport.ts";
import { DefaultHttpClient } from "./http-pipeline.ts";

// ---------------------------------------------------------------------------
// Test server
// ---------------------------------------------------------------------------

let server: ReturnType<typeof Bun.serve>;
let baseUrl: string;

const UserSchema = z.object({ id: z.number(), name: z.string() });

beforeAll(() => {
  server = Bun.serve({
    port: 0,
    async fetch(req) {
      const url = new URL(req.url);

      if (url.pathname === "/ok") {
        return Response.json({ id: 1, name: "Alice" });
      }
      if (url.pathname === "/text") {
        return new Response("hello world");
      }
      if (url.pathname === "/echo") {
        const body = (await req.json()) as { id?: number; name?: string };
        return Response.json({ id: body.id ?? 0, name: body.name ?? "echo" });
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
      if (url.pathname === "/500") {
        return new Response("server error", { status: 500 });
      }
      if (url.pathname === "/slow") {
        return new Promise((resolve) =>
          setTimeout(() => resolve(Response.json({ id: 2, name: "Slow" })), 5_000),
        );
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
// Tests
// ---------------------------------------------------------------------------

describe("EffectPlatformTransport", () => {
  const transport = new EffectPlatformTransport();

  describe("integrated with DefaultHttpClient", () => {
    it("GET with schema validation", async () => {
      const api = new DefaultHttpClient({ baseUrl, transport });
      const user = await api.get("/ok", UserSchema).runPromise();
      expect(user).toEqual({ id: 1, name: "Alice" });
    });

    it("POST with JSON body", async () => {
      const api = new DefaultHttpClient({ baseUrl, transport });
      const user = await api
        .post("/echo", UserSchema, { json: { id: 7, name: "Test" } })
        .runPromise();
      expect(user).toEqual({ id: 7, name: "Test" });
    });

    it("POST with FormData body", async () => {
      const api = new DefaultHttpClient({ baseUrl, transport });
      const file = new File(["content"], "doc.txt", { type: "text/plain" });
      const formData = new FormData();
      formData.append("file", file);
      formData.append("description", "my file");

      const result = await api.post("/upload", UserSchema, { body: formData }).runPromise();
      expect(result).toEqual({ id: 1, name: "doc.txt:my file" });
    });

    it("getText returns raw text", async () => {
      const api = new DefaultHttpClient({ baseUrl, transport });
      const text = await api.getText("/text").runPromise();
      expect(text).toBe("hello world");
    });

    it("returns HttpStatusError on non-OK", async () => {
      const api = new DefaultHttpClient({ baseUrl, transport });
      const result = await api.get("/500", UserSchema).runSafe();
      expect(result.error).not.toBeNull();
      expect(result.error!._tag).toBe("HttpStatusError");
    });

    it("returns HttpTimeoutError on timeout", async () => {
      const api = new DefaultHttpClient({ baseUrl, transport, timeoutMs: 100 });
      const result = await api.get("/slow", UserSchema).runSafe();
      expect(result.error).not.toBeNull();
      expect(result.error!._tag).toBe("HttpTimeoutError");
    });

    it("returns HttpNetworkError on connection refused", async () => {
      const api = new DefaultHttpClient({
        baseUrl: "http://localhost:1",
        transport,
      });
      const result = await api.get("/anything", UserSchema).runSafe();
      expect(result.error).not.toBeNull();
      expect(result.error!._tag).toBe("HttpNetworkError");
    });

    it("retry works with platform transport", async () => {
      let callCount = 0;
      const retryServer = Bun.serve({
        port: 0,
        fetch() {
          callCount++;
          if (callCount <= 2) return new Response("error", { status: 500 });
          return Response.json({ id: 99, name: "Recovered" });
        },
      });

      try {
        const api = new DefaultHttpClient({
          baseUrl: `http://localhost:${retryServer.port}`,
          transport,
        });
        const user = await api
          .get("/flaky", UserSchema)
          .retry({ maxRetries: 3, baseDelayMs: 10 })
          .runPromise();
        expect(user).toEqual({ id: 99, name: "Recovered" });
      } finally {
        retryServer.stop(true);
      }
    });

    it("postMultipart works with platform transport", async () => {
      const api = new DefaultHttpClient({ baseUrl, transport });
      const file = new File(["content"], "photo.png", { type: "image/png" });
      const result = await api
        .postMultipart("/upload", UserSchema, {
          file,
          fields: { description: "my photo" },
        })
        .runPromise();
      expect(result).toEqual({ id: 1, name: "photo.png:my photo" });
    });
  });
});
