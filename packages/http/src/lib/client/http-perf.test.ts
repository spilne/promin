/**
 * Performance tests for @ts-backend/http/client.
 *
 * These tests verify throughput, parallelism, overhead, and resource cleanup
 * under load. Not micro-benchmarks — they assert observable performance
 * properties that matter in production.
 */
import { it, expect, beforeAll, afterAll, describe } from "bun:test";
import { Effect, Fiber, Exit } from "effect";
import { z } from "zod";
import { DefaultHttpClient, HttpPipeline, createHttpPipeline, httpRequest } from "./index.ts";

// ---------------------------------------------------------------------------
// Test server
// ---------------------------------------------------------------------------

let server: ReturnType<typeof Bun.serve>;
let api: DefaultHttpClient;
let baseUrl: string;

const UserSchema = z.object({ id: z.number(), name: z.string() });
const ItemSchema = z.object({ id: z.number(), value: z.string() });

beforeAll(() => {
  let requestCount = 0;

  server = Bun.serve({
    port: 0,
    fetch(req) {
      requestCount++;
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

      if (url.pathname === "/request-count") {
        return Response.json({ count: requestCount });
      }

      if (url.pathname === "/reset-count") {
        requestCount = 0;
        return Response.json({ ok: true });
      }

      // SSE: emit N events as fast as possible
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

      // NDJSON: emit N lines
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

      // Echo with variable payload size
      if (url.pathname === "/payload") {
        const size = parseInt(url.searchParams.get("size") ?? "100");
        const data = { id: 1, name: "x".repeat(size) };
        return Response.json(data);
      }

      return new Response("not found", { status: 404 });
    },
  });

  baseUrl = `http://localhost:${server.port}`;
  api = new DefaultHttpClient({ baseUrl });
});

afterAll(() => {
  server.stop(true);
});

function resetCount() {
  return fetch(`${baseUrl}/reset-count`);
}

async function getCount(): Promise<number> {
  const res = await fetch(`${baseUrl}/request-count`);
  const data = (await res.json()) as { count: number };
  return data.count;
}

// ---------------------------------------------------------------------------
// 1. Pipeline overhead vs raw fetch
// ---------------------------------------------------------------------------

describe.skip("perf: pipeline overhead", () => {
  const ITERATIONS = 500;

  it(`raw fetch: ${ITERATIONS} sequential requests`, async () => {
    const start = performance.now();

    for (let i = 0; i < ITERATIONS; i++) {
      const res = await fetch(`${baseUrl}/fast`);
      const data = (await res.json()) as { id: number; name: string };
      if (data.id !== 1) throw new Error("unexpected");
    }

    const elapsed = performance.now() - start;
    const rps = (ITERATIONS / elapsed) * 1000;
    console.log(
      `    raw fetch: ${ITERATIONS} reqs in ${elapsed.toFixed(0)}ms (${rps.toFixed(0)} req/s)`,
    );
    expect(elapsed).toBeLessThan(10_000);
  });

  it(`HttpPipeline: ${ITERATIONS} sequential requests`, async () => {
    const start = performance.now();

    for (let i = 0; i < ITERATIONS; i++) {
      await api.get("/fast", UserSchema).runPromise();
    }

    const elapsed = performance.now() - start;
    const rps = (ITERATIONS / elapsed) * 1000;
    console.log(
      `    HttpPipeline: ${ITERATIONS} reqs in ${elapsed.toFixed(0)}ms (${rps.toFixed(0)} req/s)`,
    );
    expect(elapsed).toBeLessThan(15_000);
  });

  it("overhead per request is < 1ms", async () => {
    // Warm up
    for (let i = 0; i < 50; i++) {
      await api.get("/fast", UserSchema).runPromise();
    }

    // Measure raw fetch
    const rawStart = performance.now();
    for (let i = 0; i < 200; i++) {
      const res = await fetch(`${baseUrl}/fast`);
      await res.json();
    }
    const rawTime = performance.now() - rawStart;

    // Measure pipeline
    const pipeStart = performance.now();
    for (let i = 0; i < 200; i++) {
      await api.get("/fast", UserSchema).runPromise();
    }
    const pipeTime = performance.now() - pipeStart;

    const overheadPerReq = (pipeTime - rawTime) / 200;
    console.log(`    overhead per request: ${overheadPerReq.toFixed(3)}ms`);
    // Pipeline adds Effect wrapping, Zod parsing, acquireRelease — should be < 1ms
    expect(overheadPerReq).toBeLessThan(1);
  });
});

// ---------------------------------------------------------------------------
// 2. Parallel execution
// ---------------------------------------------------------------------------

describe.skip("perf: parallel execution", () => {
  it("HttpPipeline.all() is faster than sequential for slow endpoints", async () => {
    const COUNT = 10;
    const DELAY_MS = 50;

    // Sequential
    const seqStart = performance.now();
    for (let i = 0; i < COUNT; i++) {
      await api.get(`/delay?ms=${DELAY_MS}`, UserSchema).runPromise();
    }
    const seqTime = performance.now() - seqStart;

    // Parallel
    const parStart = performance.now();
    await HttpPipeline.all(
      ...Array.from({ length: COUNT }, () => api.get(`/delay?ms=${DELAY_MS}`, UserSchema)),
    ).runPromise();
    const parTime = performance.now() - parStart;

    const speedup = seqTime / parTime;
    console.log(
      `    sequential: ${seqTime.toFixed(0)}ms, parallel: ${parTime.toFixed(0)}ms, speedup: ${speedup.toFixed(1)}x`,
    );

    // Parallel should be significantly faster (at least 3x for 10 x 50ms requests)
    expect(speedup).toBeGreaterThan(3);
  });

  it("Effect.all with concurrency limit works correctly", async () => {
    await resetCount();

    const COUNT = 20;
    const effects = Array.from({ length: COUNT }, (_, i) =>
      api.get("/fast", UserSchema).toEffect(),
    );

    await Effect.runPromise(Effect.all(effects, { concurrency: 5 }));

    const count = await getCount();
    // All 20 should have been made (+ the reset-count and request-count calls)
    expect(count).toBeGreaterThanOrEqual(COUNT);
  });
});

// ---------------------------------------------------------------------------
// 3. Streaming throughput
// ---------------------------------------------------------------------------

describe.skip("perf: streaming throughput", () => {
  it("SSE: processes 10k events", async () => {
    const COUNT = 10_000;
    const start = performance.now();
    let received = 0;

    await api.getSSE(`/sse-burst?n=${COUNT}`).forEach(() => {
      received++;
    });

    const elapsed = performance.now() - start;
    const eps = (received / elapsed) * 1000;
    console.log(
      `    SSE: ${received} events in ${elapsed.toFixed(0)}ms (${eps.toFixed(0)} events/s)`,
    );

    expect(received).toBe(COUNT);
    expect(elapsed).toBeLessThan(10_000);
  });

  it("NDJSON: processes 10k lines with Zod validation", async () => {
    const COUNT = 10_000;
    const start = performance.now();

    const items = await api.getNDJSON(`/ndjson-burst?n=${COUNT}`, ItemSchema).collect();

    const elapsed = performance.now() - start;
    const lps = (items.length / elapsed) * 1000;
    console.log(
      `    NDJSON+Zod: ${items.length} items in ${elapsed.toFixed(0)}ms (${lps.toFixed(0)} items/s)`,
    );

    expect(items.length).toBe(COUNT);
    expect(elapsed).toBeLessThan(10_000);
  });

  it("SSE with filter+map chain doesn't degrade significantly", async () => {
    const COUNT = 10_000;

    // Baseline: just collect
    const baseStart = performance.now();
    await api.getSSE(`/sse-burst?n=${COUNT}`).forEach(() => {});
    const baseTime = performance.now() - baseStart;

    // With filter + map + tap chain
    const chainStart = performance.now();
    let tapped = 0;
    await api
      .getSSE(`/sse-burst?n=${COUNT}`)
      .filter((e) => e.event === "data")
      .map((e) => e.data)
      .tap(() => {
        tapped++;
      })
      .forEach(() => {});
    const chainTime = performance.now() - chainStart;

    const overhead = chainTime / baseTime;
    console.log(
      `    SSE baseline: ${baseTime.toFixed(0)}ms, with chain: ${chainTime.toFixed(0)}ms, ratio: ${overhead.toFixed(2)}x`,
    );

    expect(tapped).toBe(COUNT);
    // Chain operators should add < 3x overhead.
    // Ratio is noisy on CI (both times are small, ~80-100ms), so we use a generous bound.
    expect(overhead).toBeLessThan(3);
  });
});

// ---------------------------------------------------------------------------
// 4. Retry performance
// ---------------------------------------------------------------------------

describe.skip("perf: retry", () => {
  it("retry adds minimal latency between attempts", async () => {
    let attempts = 0;
    const flakyEffect = Effect.tryPromise({
      try: async () => {
        attempts++;
        if (attempts < 3) throw new Error("fail");
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

    const start = performance.now();
    const pipeline = createHttpPipeline(flakyEffect as any);
    await pipeline.retry({ maxRetries: 5, baseDelayMs: 10 }).runPromise();
    const elapsed = performance.now() - start;

    console.log(`    retry (2 failures + success): ${elapsed.toFixed(0)}ms, attempts: ${attempts}`);

    expect(attempts).toBe(3);
    // With 10ms base delay, 2 retries should take ~30ms (10 + 20), not seconds
    expect(elapsed).toBeLessThan(500);
  });
});

// ---------------------------------------------------------------------------
// 5. Resource cleanup under interruption
// ---------------------------------------------------------------------------

describe.skip("perf: resource cleanup", () => {
  it("interrupted fiber aborts fetch (no leaked connections)", async () => {
    await resetCount();

    // Start 50 requests to a slow endpoint, then interrupt them all
    const fibers = Array.from({ length: 50 }, () =>
      Effect.runFork(
        httpRequest({
          url: `${baseUrl}/delay?ms=5000`,
          schema: UserSchema,
        }),
      ),
    );

    // Wait a tick for requests to start
    await new Promise((r) => setTimeout(r, 20));

    // Interrupt all fibers
    await Effect.runPromise(
      Effect.all(
        fibers.map((f) => Fiber.interrupt(f)),
        { concurrency: "unbounded" },
      ),
    );

    // Verify all exits are interrupted
    const exits = await Effect.runPromise(
      Effect.all(
        fibers.map((f) => Fiber.await(f)),
        { concurrency: "unbounded" },
      ),
    );

    const interrupted = exits.filter(Exit.isInterrupted).length;
    console.log(`    ${interrupted}/50 fibers interrupted cleanly`);
    expect(interrupted).toBe(50);
  });

  it("scoped abort controller does not leak on normal requests", async () => {
    // Run many requests and verify no memory growth
    // (AbortController is GC'd after each request)
    const before = process.memoryUsage().heapUsed;

    for (let i = 0; i < 1000; i++) {
      await api.get("/fast", UserSchema).runPromise();
    }

    // Force GC if available
    if (typeof globalThis.gc === "function") globalThis.gc();

    const after = process.memoryUsage().heapUsed;
    const growthMB = (after - before) / 1024 / 1024;

    console.log(`    memory after 1000 requests: growth=${growthMB.toFixed(2)}MB`);

    // Should not grow more than 10MB for 1000 simple requests
    expect(growthMB).toBeLessThan(10);
  });
});

// ---------------------------------------------------------------------------
// 6. Zod validation overhead
// ---------------------------------------------------------------------------

describe.skip("perf: Zod validation", () => {
  it("Zod parsing overhead is negligible for small payloads", async () => {
    // Without validation (raw JSON)
    const rawStart = performance.now();
    for (let i = 0; i < 500; i++) {
      await api.getJson("/fast").runPromise();
    }
    const rawTime = performance.now() - rawStart;

    // With validation
    const zodStart = performance.now();
    for (let i = 0; i < 500; i++) {
      await api.get("/fast", UserSchema).runPromise();
    }
    const zodTime = performance.now() - zodStart;

    const overhead = ((zodTime - rawTime) / rawTime) * 100;
    console.log(
      `    raw JSON: ${rawTime.toFixed(0)}ms, with Zod: ${zodTime.toFixed(0)}ms, overhead: ${overhead.toFixed(1)}%`,
    );

    // Zod validation overhead should be negligible — network I/O dominates.
    // Allow up to 50% because CI environments have variable timing.
    expect(overhead).toBeLessThan(50);
  });

  it("Zod parsing scales with payload size", async () => {
    const SmallSchema = z.object({ id: z.number(), name: z.string() });
    const ITERATIONS = 200;

    // Small payload (~100 bytes)
    const smallStart = performance.now();
    for (let i = 0; i < ITERATIONS; i++) {
      await api.get("/payload?size=100", SmallSchema).runPromise();
    }
    const smallTime = performance.now() - smallStart;

    // Large payload (~10KB)
    const largeStart = performance.now();
    for (let i = 0; i < ITERATIONS; i++) {
      await api.get("/payload?size=10000", SmallSchema).runPromise();
    }
    const largeTime = performance.now() - largeStart;

    console.log(
      `    small payload: ${smallTime.toFixed(0)}ms, large payload: ${largeTime.toFixed(0)}ms`,
    );

    // Both should complete in reasonable time
    expect(smallTime).toBeLessThan(5_000);
    expect(largeTime).toBeLessThan(10_000);
  });
});

// ---------------------------------------------------------------------------
// 7. Polling performance
// ---------------------------------------------------------------------------

describe.skip("perf: polling", () => {
  it("pollUntil respects interval timing", async () => {
    let calls = 0;
    const INTERVAL_MS = 50;
    const EXPECTED_CALLS = 5;

    const start = performance.now();
    await api
      .get("/fast", UserSchema)
      .pollUntil({
        until: () => {
          calls++;
          return calls >= EXPECTED_CALLS;
        },
        intervalMs: INTERVAL_MS,
        maxAttempts: 20,
      })
      .runPromise();
    const elapsed = performance.now() - start;

    // Should take roughly (EXPECTED_CALLS - 1) * INTERVAL_MS
    // (no wait after the last successful call)
    const expectedMs = (EXPECTED_CALLS - 1) * INTERVAL_MS;
    console.log(`    ${calls} polls in ${elapsed.toFixed(0)}ms (expected ~${expectedMs}ms)`);

    expect(calls).toBe(EXPECTED_CALLS);
    // Allow 50% tolerance for timing jitter
    expect(elapsed).toBeGreaterThan(expectedMs * 0.5);
    expect(elapsed).toBeLessThan(expectedMs * 2);
  });
});
