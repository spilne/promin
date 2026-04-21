import { describe, it, expect } from "bun:test";
import { Effect } from "effect";
import { PipelineRateLimiter, RateLimitExceeded } from "../rate-limiter.ts";
import { Pipeline } from "../pipeline.ts";
import { rateLimiterTestSuite } from "../rate-limiter-test-suite.ts";

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

// ---------------------------------------------------------------------------
// Portable conformance suites — one per strategy (limit=2, windowMs=100)
// ---------------------------------------------------------------------------

rateLimiterTestSuite(
  () => PipelineRateLimiter.make({ limit: 2, windowMs: 100, strategy: "sliding-window" }),
  { strategy: "sliding-window" },
);

rateLimiterTestSuite(
  () => PipelineRateLimiter.make({ limit: 2, windowMs: 100, strategy: "fixed-window" }),
  { strategy: "fixed-window" },
);

rateLimiterTestSuite(
  () => PipelineRateLimiter.make({ limit: 2, windowMs: 100, strategy: "token-bucket" }),
  { strategy: "token-bucket" },
);

// ---------------------------------------------------------------------------
// Strategy-specific behavior
// ---------------------------------------------------------------------------

describe("PipelineRateLimiter — sliding-window specifics", () => {
  it("retryAfterMs reflects time until oldest entry expires", async () => {
    const rl = PipelineRateLimiter.make({ limit: 1, windowMs: 100, strategy: "sliding-window" });
    await rl.acquireAsync();
    try {
      await rl.acquireAsync();
      expect(true).toBe(false);
    } catch (e: any) {
      expect(e.retryAfterMs).toBeGreaterThan(0);
      expect(e.retryAfterMs).toBeLessThanOrEqual(100);
    }
  });
});

describe("PipelineRateLimiter — token-bucket specifics", () => {
  it("refills gradually over time", async () => {
    const rl = PipelineRateLimiter.make({ limit: 2, windowMs: 100, strategy: "token-bucket" });
    await rl.acquireAsync();
    await rl.acquireAsync();
    await sleep(60);
    await rl.acquireAsync();
  });
});

// ---------------------------------------------------------------------------
// Effect / Pipeline API
// ---------------------------------------------------------------------------

describe("PipelineRateLimiter — Effect API", () => {
  it("withLimit wraps effect correctly", async () => {
    const rl = PipelineRateLimiter.make({ limit: 1, windowMs: 100 });
    const result = await Effect.runPromise(rl.withLimit(Effect.succeed(42)));
    expect(result).toBe(42);
  });

  it("withLimit fails when exhausted", async () => {
    const rl = PipelineRateLimiter.make({ limit: 1, windowMs: 100 });
    await Effect.runPromise(rl.acquire);
    const exit = await Effect.runPromiseExit(rl.withLimit(Effect.succeed(42)));
    expect(exit._tag).toBe("Failure");
  });

  it("tryAcquire returns false when exhausted", async () => {
    const rl = PipelineRateLimiter.make({ limit: 1, windowMs: 100 });
    expect(await Effect.runPromise(rl.tryAcquire)).toBe(true);
    expect(await Effect.runPromise(rl.tryAcquire)).toBe(false);
  });

  it("withLimitPipeline wraps pipeline", async () => {
    const rl = PipelineRateLimiter.make({ limit: 1, windowMs: 100 });
    const p = Pipeline.from(Effect.succeed("hello"));
    const result = await rl.withLimitPipeline(p).runPromise();
    expect(result).toBe("hello");
  });

  it("default strategy is sliding-window", async () => {
    const rl = PipelineRateLimiter.make({ limit: 1, windowMs: 50 });
    await rl.acquireAsync();
    try {
      await rl.acquireAsync();
      expect(true).toBe(false);
    } catch (e: any) {
      expect(e._tag).toBe("RateLimitExceeded");
    }
    await sleep(60);
    await rl.acquireAsync();
  });
});
