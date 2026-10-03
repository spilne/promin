import { describe, it, expect } from "bun:test";
import type { RateLimiter } from "./rate-limiter.ts";

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/**
 * Portable RateLimiter conformance suite.
 * Tests the Promise API which any backend must implement.
 *
 * @param factory — called per test group, should return a fresh instance.
 *   The `strategy` param lets the suite adapt expectations per strategy.
 */
export function rateLimiterTestSuite(
  factory: () => RateLimiter | Promise<RateLimiter>,
  options?: { strategy?: "sliding-window" | "fixed-window" | "token-bucket" },
) {
  let rl: RateLimiter;

  async function getRl(): Promise<RateLimiter> {
    rl = await factory();
    return rl;
  }

  const strategy = options?.strategy ?? "sliding-window";

  describe(`RateLimiter conformance (${strategy})`, () => {
    it("allows requests up to limit", async () => {
      const r = await getRl();
      await r.acquireAsync();
      await r.acquireAsync();
    });

    it("rejects when limit exhausted", async () => {
      const r = await getRl();
      await r.acquireAsync();
      await r.acquireAsync();
      try {
        await r.acquireAsync();
        expect(true).toBe(false);
      } catch (e: any) {
        expect(e._tag).toBe("RateLimitExceeded");
        expect(e.retryAfterMs).toBeGreaterThan(0);
      }
    });

    it("tryAcquire returns false when exhausted", async () => {
      const r = await getRl();
      await r.acquireAsync();
      await r.acquireAsync();
      expect(await r.tryAcquireAsync()).toBe(false);
    });

    it("allows again after window expires", async () => {
      const r = await getRl();
      await r.acquireAsync();
      await r.acquireAsync();
      await sleep(120);
      await r.acquireAsync();
    });

    it("withLimitAsync runs the function", async () => {
      const r = await getRl();
      const result = await r.withLimitAsync(async () => 42);
      expect(result).toBe(42);
    });

    it("withLimitAsync rejects when exhausted", async () => {
      const r = await getRl();
      await r.acquireAsync();
      await r.acquireAsync();
      try {
        await r.withLimitAsync(async () => "nope");
        expect(true).toBe(false);
      } catch (e: any) {
        expect(e._tag).toBe("RateLimitExceeded");
      }
    });

    it("remaining decreases after acquire", async () => {
      const r = await getRl();
      const before = await r.remainingAsync();
      expect(before).toBe(2);
      await r.acquireAsync();
      const after = await r.remainingAsync();
      expect(after).toBe(1);
    });

    it("different resources have independent limits", async () => {
      const r = await getRl();
      await r.acquireAsync("x");
      await r.acquireAsync("x");
      await r.acquireAsync("y");
      await r.acquireAsync("y");
      // Both exhausted independently
      expect(await r.tryAcquireAsync("x")).toBe(false);
      expect(await r.tryAcquireAsync("y")).toBe(false);
    });

    it("default resource is independent from named resource", async () => {
      const r = await getRl();
      await r.acquireAsync();
      await r.acquireAsync();
      await r.acquireAsync("named");
      await r.acquireAsync("named");
      expect(await r.tryAcquireAsync()).toBe(false);
      expect(await r.tryAcquireAsync("named")).toBe(false);
      // Remaining per resource
      expect(await r.remainingAsync()).toBe(0);
      expect(await r.remainingAsync("named")).toBe(0);
    });
  });
}
