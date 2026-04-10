import { group, bench, run } from "mitata";
import { Effect, Fiber } from "effect";
import { z } from "zod";
import { DefaultHttpClient, HttpPipeline, createHttpPipeline, httpRequest } from "./index.ts";

// ---------------------------------------------------------------------------
// Test server
// ---------------------------------------------------------------------------

const UserSchema = z.object({ id: z.number(), name: z.string() });
const ItemSchema = z.object({ id: z.number(), value: z.string() });

const server = Bun.serve({
  port: 0,
  fetch(req) {
    const url = new URL(req.url);

    if (url.pathname === "/fast") {
      return Response.json({ id: 1, name: "Alice" });
    }

    if (url.pathname === "/delay") {
      const ms = parseInt(url.searchParams.get("ms") ?? "50");
      return new Promise((resolve) =>
        setTimeout(() => resolve(Response.json({ id: 1, name: "Delayed" })), ms),
      );
    }

    if (url.pathname === "/sse-burst") {
      const count = parseInt(url.searchParams.get("n") ?? "1000");
      const encoder = new TextEncoder();
      const stream = new ReadableStream({
        start(controller) {
          for (let i = 0; i < count; i++) {
            controller.enqueue(
              encoder.encode(`event: data\ndata: {"id":${i},"value":"item-${i}"}\n\n`),
            );
          }
          controller.close();
        },
      });
      return new Response(stream, {
        headers: { "Content-Type": "text/event-stream" },
      });
    }

    if (url.pathname === "/ndjson-burst") {
      const count = parseInt(url.searchParams.get("n") ?? "1000");
      const lines: string[] = [];
      for (let i = 0; i < count; i++) {
        lines.push(JSON.stringify({ id: i, value: `item-${i}` }));
      }
      return new Response(lines.join("\n") + "\n", {
        headers: { "Content-Type": "application/x-ndjson" },
      });
    }

    if (url.pathname === "/payload") {
      const size = parseInt(url.searchParams.get("size") ?? "100");
      return Response.json({ id: 1, name: "x".repeat(size) });
    }

    return new Response("not found", { status: 404 });
  },
});

const baseUrl = `http://localhost:${server.port}`;
const api = new DefaultHttpClient({ baseUrl });

// ---------------------------------------------------------------------------
// 1. Pipeline overhead vs raw fetch
// ---------------------------------------------------------------------------

group("pipeline overhead", () => {
  bench("raw fetch", async () => {
    const res = await fetch(`${baseUrl}/fast`);
    await res.json();
  });

  bench("HttpPipeline.get", async () => {
    await api.get("/fast", UserSchema).runPromise();
  });

  bench("HttpPipeline.getJson (no Zod)", async () => {
    await api.getJson("/fast").runPromise();
  });
});

// ---------------------------------------------------------------------------
// 2. Parallel execution
// ---------------------------------------------------------------------------

group("parallel execution", () => {
  bench("10x sequential (50ms delay)", async () => {
    for (let i = 0; i < 10; i++) {
      await api.get("/delay?ms=50", UserSchema).runPromise();
    }
  });

  bench("10x HttpPipeline.all (50ms delay)", async () => {
    await HttpPipeline.all(
      ...Array.from({ length: 10 }, () => api.get("/delay?ms=50", UserSchema)),
    ).runPromise();
  });

  bench("20x Effect.all concurrency=5", async () => {
    const effects = Array.from({ length: 20 }, () => api.get("/fast", UserSchema).toEffect());
    await Effect.runPromise(Effect.all(effects, { concurrency: 5 }));
  });
});

// ---------------------------------------------------------------------------
// 3. Streaming throughput
// ---------------------------------------------------------------------------

group("streaming throughput", () => {
  bench("SSE: 10k events", async () => {
    let _received = 0;
    await api.getSSE("/sse-burst?n=10000").forEach(() => {
      _received++;
    });
  });

  bench("NDJSON+Zod: 10k lines", async () => {
    await api.getNDJSON("/ndjson-burst?n=10000", ItemSchema).collect();
  });

  bench("SSE: 10k events with filter+map+tap chain", async () => {
    let _tapped = 0;
    await api
      .getSSE("/sse-burst?n=10000")
      .filter((e) => e.event === "data")
      .map((e) => e.data)
      .tap(() => {
        _tapped++;
      })
      .forEach(() => {});
  });
});

// ---------------------------------------------------------------------------
// 4. Retry
// ---------------------------------------------------------------------------

group("retry", () => {
  bench("retry (2 failures + success, 10ms base delay)", async () => {
    let attempts = 0;
    const flakyEffect = Effect.tryPromise({
      try: async () => {
        attempts++;
        if (attempts % 3 !== 0) throw new Error("fail");
        return { id: 1, name: "ok" };
      },
      catch: () => ({
        _tag: "HttpStatusError" as const,
        url: "test",
        status: 500,
        body: "error",
        message: "error",
        isRetryable: true,
        isClientError: false,
        isServerError: true,
      }),
    });

    const pipeline = createHttpPipeline(flakyEffect as any);
    await pipeline.retry({ maxRetries: 5, baseDelayMs: 10 }).runPromise();
  });
});

// ---------------------------------------------------------------------------
// 5. Resource cleanup
// ---------------------------------------------------------------------------

group("resource cleanup", () => {
  bench("interrupt 50 fibers (5s delay endpoints)", async () => {
    const fibers = Array.from({ length: 50 }, () =>
      Effect.runFork(
        httpRequest({
          url: `${baseUrl}/delay?ms=5000`,
          schema: UserSchema,
        }),
      ),
    );

    await new Promise((r) => setTimeout(r, 20));

    await Effect.runPromise(
      Effect.all(
        fibers.map((f) => Fiber.interrupt(f)),
        { concurrency: "unbounded" },
      ),
    );
  });

  bench("1000 sequential requests (leak check)", async () => {
    for (let i = 0; i < 1000; i++) {
      await api.get("/fast", UserSchema).runPromise();
    }
  });
});

// ---------------------------------------------------------------------------
// 6. Zod validation overhead
// ---------------------------------------------------------------------------

group("Zod validation", () => {
  bench("500x getJson (no validation)", async () => {
    for (let i = 0; i < 500; i++) {
      await api.getJson("/fast").runPromise();
    }
  });

  bench("500x get with Zod schema", async () => {
    for (let i = 0; i < 500; i++) {
      await api.get("/fast", UserSchema).runPromise();
    }
  });

  bench("200x small payload (100B)", async () => {
    for (let i = 0; i < 200; i++) {
      await api.get("/payload?size=100", UserSchema).runPromise();
    }
  });

  bench("200x large payload (10KB)", async () => {
    for (let i = 0; i < 200; i++) {
      await api.get("/payload?size=10000", UserSchema).runPromise();
    }
  });
});

// ---------------------------------------------------------------------------
// Run & cleanup
// ---------------------------------------------------------------------------

await run();
server.stop(true);
