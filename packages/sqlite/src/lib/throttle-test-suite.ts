import { describe, it, expect } from "bun:test";
import type { Throttle } from "./throttle.ts";

/**
 * Portable Throttle conformance suite.
 * Tests the Promise API which any backend must implement.
 *
 * @param factory — returns a Throttle with permits=1, windowMs=100 unless overridden
 */
export function throttleTestSuite(factory: () => Throttle | Promise<Throttle>) {
  let throttle: Throttle;

  async function getThrottle(): Promise<Throttle> {
    throttle = await factory();
    return throttle;
  }

  describe("Throttle conformance", () => {
    it("allows first acquire immediately", async () => {
      const t = await getThrottle();
      const start = Date.now();
      await t.acquireAsync();
      expect(Date.now() - start).toBeLessThan(50);
    });

    it("blocks when permits exhausted, resumes after window expires", async () => {
      const t = await getThrottle();
      await t.acquireAsync();

      const start = Date.now();
      await t.acquireAsync();
      expect(Date.now() - start).toBeGreaterThanOrEqual(80);
    });

    it("tryAcquire returns true then false when exhausted", async () => {
      const t = await getThrottle();
      expect(await t.tryAcquireAsync()).toBe(true);
      expect(await t.tryAcquireAsync()).toBe(false);
    });

    it("withPermitAsync runs the function after acquiring", async () => {
      const t = await getThrottle();
      const result = await t.withPermitAsync(async () => 42);
      expect(result).toBe(42);
    });

    it("withPermitAsync blocks when exhausted", async () => {
      const t = await getThrottle();
      await t.acquireAsync();

      const start = Date.now();
      const result = await t.withPermitAsync(async () => "ok");
      expect(Date.now() - start).toBeGreaterThanOrEqual(80);
      expect(result).toBe("ok");
    });

    it("permits replenish after window expires", async () => {
      const t = await getThrottle();
      await t.acquireAsync();
      expect(await t.tryAcquireAsync()).toBe(false);

      await new Promise((r) => setTimeout(r, 120));
      expect(await t.tryAcquireAsync()).toBe(true);
    });

    it("different resources have independent permits", async () => {
      const t = await getThrottle();
      expect(await t.tryAcquireAsync("a")).toBe(true);
      expect(await t.tryAcquireAsync("b")).toBe(true);
      // Both exhausted independently
      expect(await t.tryAcquireAsync("a")).toBe(false);
      expect(await t.tryAcquireAsync("b")).toBe(false);
    });

    it("default resource is independent from named resource", async () => {
      const t = await getThrottle();
      expect(await t.tryAcquireAsync()).toBe(true);
      expect(await t.tryAcquireAsync("named")).toBe(true);
      // Both exhausted independently
      expect(await t.tryAcquireAsync()).toBe(false);
      expect(await t.tryAcquireAsync("named")).toBe(false);
    });
  });
}
