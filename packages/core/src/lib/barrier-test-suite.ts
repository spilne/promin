import { describe, it, expect } from "bun:test";
import type { Barrier } from "./barrier.ts";

export function barrierTestSuite(factory: () => Barrier | Promise<Barrier>) {
  describe("Barrier conformance", () => {
    it("arrived starts at 0", async () => {
      const b = await factory();
      expect(await b.arrivedAsync()).toBe(0);
    });

    it("await blocks until all parties arrive", async () => {
      const b = await factory();
      let resolved = false;

      const p = b.awaitAsync().then(() => {
        resolved = true;
      });

      await new Promise((r) => setTimeout(r, 20));
      expect(resolved).toBe(false);

      // Arrive 2nd and 3rd party to unblock
      const p2 = b.awaitAsync();
      const p3 = b.awaitAsync();
      await Promise.all([p, p2, p3]);
      expect(resolved).toBe(true);
    });

    it("all parties unblock when last arrives", async () => {
      const b = await factory();
      const results: number[] = [];

      const p1 = b.awaitAsync().then(() => results.push(1));
      const p2 = b.awaitAsync().then(() => results.push(2));

      await new Promise((r) => setTimeout(r, 10));
      expect(await b.arrivedAsync()).toBe(2);
      expect(results).toEqual([]);

      const p3 = b.awaitAsync().then(() => results.push(3));
      await Promise.all([p1, p2, p3]);

      expect(results).toHaveLength(3);
    });

    it("arrived reflects count after some arrivals", async () => {
      const b = await factory();

      const p1 = b.awaitAsync();
      await new Promise((r) => setTimeout(r, 10));
      expect(await b.arrivedAsync()).toBe(1);

      const p2 = b.awaitAsync();
      await new Promise((r) => setTimeout(r, 10));
      expect(await b.arrivedAsync()).toBe(2);

      const p3 = b.awaitAsync();
      await Promise.all([p1, p2, p3]);
      expect(await b.arrivedAsync()).toBe(3);
    });
  });
}
