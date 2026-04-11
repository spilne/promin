import { describe, it, expect } from "bun:test";
import { Effect } from "effect";
import { PipelineThrottle } from "./throttle.ts";
import { throttleTestSuite } from "./throttle-test-suite.ts";

// ---------------------------------------------------------------------------
// Portable conformance suite (Promise API) — permits=1, windowMs=100
// ---------------------------------------------------------------------------

throttleTestSuite(() => PipelineThrottle.make({ permits: 1, windowMs: 100 }));

// ---------------------------------------------------------------------------
// Effect API + extras
// ---------------------------------------------------------------------------

describe("PipelineThrottle — Effect API", () => {
  it("withPermit runs the effect after acquiring", async () => {
    const throttle = PipelineThrottle.make({ permits: 1, windowMs: 500 });
    const result = await Effect.runPromise(throttle.withPermit(Effect.succeed(42)));
    expect(result).toBe(42);
  });

  it("remaining reports correct count", async () => {
    const throttle = PipelineThrottle.make({ permits: 3, windowMs: 500 });
    const before = await Effect.runPromise(throttle.remaining);
    expect(before).toBe(3);

    await Effect.runPromise(throttle.acquire);
    await Effect.runPromise(throttle.acquire);
    const after = await Effect.runPromise(throttle.remaining);
    expect(after).toBe(1);
  });

  it("different instances are independent", async () => {
    const a = PipelineThrottle.make({ permits: 1, windowMs: 500 });
    const b = PipelineThrottle.make({ permits: 1, windowMs: 500 });

    await a.acquireAsync();
    const acquired = await b.tryAcquireAsync();
    expect(acquired).toBe(true);
  });

  it("multiple permits allows burst", async () => {
    const throttle = PipelineThrottle.make({ permits: 3, windowMs: 1000 });
    const start = Date.now();
    await throttle.acquireAsync();
    await throttle.acquireAsync();
    await throttle.acquireAsync();
    expect(Date.now() - start).toBeLessThan(50);
  });
});
