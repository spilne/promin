import { describe, it, expect, beforeAll, afterAll } from "bun:test";
import { z } from "zod";
import { DefaultHttpClient } from "./index.ts";

// ---------------------------------------------------------------------------
// Test server with streaming endpoints
// ---------------------------------------------------------------------------

let server: ReturnType<typeof Bun.serve>;
let api: DefaultHttpClient;

beforeAll(() => {
  server = Bun.serve({
    port: 0,
    async fetch(req) {
      const url = new URL(req.url);

      // SSE endpoint
      if (url.pathname === "/events") {
        const encoder = new TextEncoder();
        const stream = new ReadableStream({
          async start(controller) {
            controller.enqueue(encoder.encode("event: delta\ndata: hello\n\n"));
            controller.enqueue(encoder.encode("event: delta\ndata: world\n\n"));
            controller.enqueue(encoder.encode("event: done\ndata: [DONE]\n\n"));
            controller.close();
          },
        });

        return new Response(stream, {
          headers: { "Content-Type": "text/event-stream" },
        });
      }

      // SSE with multi-line data and IDs
      if (url.pathname === "/events-complex") {
        const encoder = new TextEncoder();
        const stream = new ReadableStream({
          async start(controller) {
            controller.enqueue(
              encoder.encode("id: 1\nevent: update\ndata: line1\ndata: line2\n\n"),
            );
            controller.enqueue(
              encoder.encode(": this is a comment\nid: 2\nevent: update\ndata: line3\n\n"),
            );
            controller.close();
          },
        });

        return new Response(stream, {
          headers: { "Content-Type": "text/event-stream" },
        });
      }

      // NDJSON endpoint
      if (url.pathname === "/ndjson") {
        const lines = [
          JSON.stringify({ id: 1, name: "Alice" }),
          JSON.stringify({ id: 2, name: "Bob" }),
          JSON.stringify({ id: 3, name: "Charlie" }),
          "",
        ].join("\n");

        return new Response(lines, {
          headers: { "Content-Type": "application/x-ndjson" },
        });
      }

      // Raw streaming text
      if (url.pathname === "/stream-text") {
        const encoder = new TextEncoder();
        const stream = new ReadableStream({
          async start(controller) {
            controller.enqueue(encoder.encode("chunk1"));
            controller.enqueue(encoder.encode("chunk2"));
            controller.enqueue(encoder.encode("chunk3"));
            controller.close();
          },
        });

        return new Response(stream);
      }

      // Chunked SSE (chunks split across events)
      if (url.pathname === "/events-chunked") {
        const encoder = new TextEncoder();
        const stream = new ReadableStream({
          async start(controller) {
            // Split an event across two chunks
            controller.enqueue(encoder.encode("event: msg\nda"));
            controller.enqueue(encoder.encode("ta: split-value\n\n"));
            controller.close();
          },
        });

        return new Response(stream, {
          headers: { "Content-Type": "text/event-stream" },
        });
      }

      // POST stream echo — returns the body back as streaming chunks
      if (url.pathname === "/stream-echo" && req.method === "POST") {
        const body = await req.text();
        const encoder = new TextEncoder();
        const stream = new ReadableStream({
          start(controller) {
            controller.enqueue(encoder.encode(`received:${body}`));
            controller.close();
          },
        });
        return new Response(stream);
      }

      return new Response("not found", { status: 404 });
    },
  });

  api = new DefaultHttpClient({ baseUrl: `http://localhost:${server.port}` });
});

afterAll(() => {
  server.stop(true);
});

// ---------------------------------------------------------------------------
// SSE streaming
// ---------------------------------------------------------------------------

describe("SSE streaming", () => {
  it("parses SSE events", async () => {
    const events = await api.getSSE("/events").collect();

    expect(events).toHaveLength(3);
    expect(events[0]).toEqual({ event: "delta", data: "hello" });
    expect(events[1]).toEqual({ event: "delta", data: "world" });
    expect(events[2]).toEqual({ event: "done", data: "[DONE]" });
  });

  it("filters SSE events by type", async () => {
    const deltas = await api
      .getSSE("/events")
      .filter((e) => e.event === "delta")
      .collect();

    expect(deltas).toHaveLength(2);
    expect(deltas[0].data).toBe("hello");
    expect(deltas[1].data).toBe("world");
  });

  it("maps SSE event data", async () => {
    const texts = await api
      .getSSE("/events")
      .filter((e) => e.event === "delta")
      .map((e) => e.data)
      .collect();

    expect(texts).toEqual(["hello", "world"]);
  });

  it("handles multi-line data and IDs", async () => {
    const events = await api.getSSE("/events-complex").collect();

    expect(events).toHaveLength(2);
    expect(events[0]).toEqual({ event: "update", data: "line1\nline2", id: "1" });
    expect(events[1]).toEqual({ event: "update", data: "line3", id: "2" });
  });

  it("handles events split across chunks", async () => {
    const events = await api.getSSE("/events-chunked").collect();

    expect(events).toHaveLength(1);
    expect(events[0]).toEqual({ event: "msg", data: "split-value" });
  });

  it("takes only first N events", async () => {
    const events = await api.getSSE("/events").take(1).collect();

    expect(events).toHaveLength(1);
    expect(events[0].data).toBe("hello");
  });

  it("takeWhile stops on condition", async () => {
    const events = await api
      .getSSE("/events")
      .takeWhile((e) => e.data !== "[DONE]")
      .collect();

    expect(events).toHaveLength(2);
  });

  it("forEach processes each event", async () => {
    const collected: string[] = [];
    await api
      .getSSE("/events")
      .filter((e) => e.event === "delta")
      .forEach((e) => collected.push(e.data));

    expect(collected).toEqual(["hello", "world"]);
  });

  it("reduce folds over events", async () => {
    const concatenated = await api
      .getSSE("/events")
      .filter((e) => e.event === "delta")
      .reduce("", (acc, e) => acc + e.data);

    expect(concatenated).toBe("helloworld");
  });
});

// ---------------------------------------------------------------------------
// NDJSON streaming
// ---------------------------------------------------------------------------

const ItemSchema = z.object({ id: z.number(), name: z.string() });

describe("NDJSON streaming", () => {
  it("parses and validates each line", async () => {
    const items = await api.getNDJSON("/ndjson", ItemSchema).collect();

    expect(items).toHaveLength(3);
    expect(items[0]).toEqual({ id: 1, name: "Alice" });
    expect(items[2]).toEqual({ id: 3, name: "Charlie" });
  });

  it("filters items", async () => {
    const items = await api
      .getNDJSON("/ndjson", ItemSchema)
      .filter((item) => item.id > 1)
      .collect();

    expect(items).toHaveLength(2);
    expect(items[0].name).toBe("Bob");
  });

  it("maps items", async () => {
    const names = await api
      .getNDJSON("/ndjson", ItemSchema)
      .map((item) => item.name)
      .collect();

    expect(names).toEqual(["Alice", "Bob", "Charlie"]);
  });
});

// ---------------------------------------------------------------------------
// Raw text streaming
// ---------------------------------------------------------------------------

describe("raw text streaming", () => {
  it("streams raw text chunks", async () => {
    const chunks = await api.getStream("/stream-text").collect();

    // Chunks may be merged by the transport, so check concatenated result
    const full = chunks.join("");
    expect(full).toBe("chunk1chunk2chunk3");
  });

  it("collects via reduce", async () => {
    const full = await api.getStream("/stream-text").reduce("", (acc, chunk) => acc + chunk);

    expect(full).toBe("chunk1chunk2chunk3");
  });
});

// ---------------------------------------------------------------------------
// finally
// ---------------------------------------------------------------------------

describe("HttpStreamPipeline.finally", () => {
  it("runs cleanup on success", async () => {
    let cleaned = false;
    await api
      .getSSE("/events")
      .finally(() => {
        cleaned = true;
      })
      .drain();

    expect(cleaned).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// Real-world pattern: AI streaming completion
// ---------------------------------------------------------------------------

describe("real-world: AI streaming", () => {
  it("collects streamed tokens into full response", async () => {
    const fullText = await api
      .getSSE("/events")
      .filter((e) => e.event === "delta")
      .map((e) => e.data)
      .reduce("", (acc, token) => acc + token);

    expect(fullText).toBe("helloworld");
  });

  it("processes SSE with tap for logging", async () => {
    const logged: string[] = [];

    const tokens = await api
      .getSSE("/events")
      .filter((e) => e.event === "delta")
      .tap((e) => logged.push(`received: ${e.data}`))
      .map((e) => e.data)
      .collect();

    expect(tokens).toEqual(["hello", "world"]);
    expect(logged).toEqual(["received: hello", "received: world"]);
  });
});

// ---------------------------------------------------------------------------
// postStream
// ---------------------------------------------------------------------------

describe("postStream", () => {
  it("streams raw text from a POST request", async () => {
    const chunks = await api.postStream("/stream-echo", { json: { hello: "world" } }).collect();

    const full = chunks.join("");
    expect(full).toContain("received:");
    expect(full).toContain("hello");
  });
});

// ---------------------------------------------------------------------------
// tapAsync / mapAsync on streams
// ---------------------------------------------------------------------------

describe("stream tapAsync / mapAsync", () => {
  it("tapAsync awaits async side effect per chunk", async () => {
    const logged: string[] = [];

    await api
      .getSSE("/events")
      .filter((e) => e.event === "delta")
      .tapAsync(async (e) => {
        await new Promise((r) => setTimeout(r, 5));
        logged.push(e.data);
      })
      .drain();

    expect(logged).toEqual(["hello", "world"]);
  });

  it("mapAsync transforms each chunk with async function", async () => {
    const results = await api
      .getSSE("/events")
      .filter((e) => e.event === "delta")
      .mapAsync(async (e) => {
        await new Promise((r) => setTimeout(r, 5));
        return e.data.toUpperCase();
      })
      .collect();

    expect(results).toEqual(["HELLO", "WORLD"]);
  });
});

// ---------------------------------------------------------------------------
// fs2-style combinators
// ---------------------------------------------------------------------------

describe("parAsyncMap", () => {
  it("processes items concurrently while preserving order", async () => {
    const order: number[] = [];
    const items = await api
      .getNDJSON("/ndjson", ItemSchema)
      .parAsyncMap(3, async (item) => {
        // Simulate varying async work
        await new Promise((r) => setTimeout(r, Math.random() * 10));
        order.push(item.id);
        return { ...item, enriched: true };
      })
      .collect();

    expect(items).toHaveLength(3);
    // Order is preserved despite concurrent execution
    expect(items[0].name).toBe("Alice");
    expect(items[2].name).toBe("Charlie");
    expect(items.every((i) => (i as any).enriched)).toBe(true);
  });
});

describe("parAsyncMapUnordered", () => {
  it("processes items concurrently in completion order", async () => {
    const items = await api
      .getNDJSON("/ndjson", ItemSchema)
      .parAsyncMapUnordered(3, async (item) => {
        await new Promise((r) => setTimeout(r, Math.random() * 10));
        return item.name;
      })
      .collect();

    expect(items).toHaveLength(3);
    // All items present, order may vary
    expect(items.sort()).toEqual(["Alice", "Bob", "Charlie"]);
  });
});

describe("groupWithin", () => {
  it("batches items by count", async () => {
    const batches = await api
      .getNDJSON("/ndjson", ItemSchema)
      .groupWithin(2, 10_000) // max 2 items per batch, long timeout
      .collect();

    // 3 items → [batch of 2, batch of 1]
    expect(batches).toHaveLength(2);
    expect(batches[0]).toHaveLength(2);
    expect(batches[1]).toHaveLength(1);
  });
});

describe("grouped", () => {
  it("splits into fixed-size batches", async () => {
    const batches = await api.getNDJSON("/ndjson", ItemSchema).grouped(2).collect();

    expect(batches).toHaveLength(2);
    expect(batches[0]).toHaveLength(2);
    expect(batches[1]).toHaveLength(1);
    expect(batches[0][0].name).toBe("Alice");
    expect(batches[0][1].name).toBe("Bob");
    expect(batches[1][0].name).toBe("Charlie");
  });
});

describe("scan", () => {
  it("emits running accumulator", async () => {
    const results = await api
      .getNDJSON("/ndjson", ItemSchema)
      .scan(0, (acc, item) => acc + item.id)
      .collect();

    // ids: 1, 2, 3 → running sum: 1, 3, 6
    expect(results).toEqual([0, 1, 3, 6]); // includes initial value
  });
});

describe("merge", () => {
  it("interleaves two streams", async () => {
    const stream1 = api.getSSE("/events").filter((e) => e.event === "delta");
    const stream2 = api.getSSE("/events").filter((e) => e.event === "delta");

    const events = await stream1.merge(stream2).collect();

    // Both streams contribute — exact count is 4 (2 deltas × 2 streams)
    expect(events).toHaveLength(4);
  });
});

describe("mergeAll", () => {
  it("merges multiple streams", async () => {
    const { HttpStreamPipeline: HSP } = await import("./http-stream.ts");

    const events = await HSP.mergeAll(
      api.getSSE("/events").filter((e) => e.event === "delta"),
      api.getSSE("/events").filter((e) => e.event === "delta"),
    ).collect();

    expect(events).toHaveLength(4);
  });
});

describe("through", () => {
  it("applies reusable pipe transformation", async () => {
    const uppercasePipe = (s: ReturnType<typeof api.getSSE>) =>
      s.filter((e) => e.event === "delta").map((e) => ({ ...e, data: e.data.toUpperCase() }));

    const events = await api.getSSE("/events").through(uppercasePipe).collect();

    expect(events).toHaveLength(2);
    expect(events[0].data).toBe("HELLO");
    expect(events[1].data).toBe("WORLD");
  });
});
