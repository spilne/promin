import { describe, it, expect } from "bun:test";
import type { DeferredValue } from "./deferred.ts";

export function deferredTestSuite(
  factory: () => DeferredValue<number> | Promise<DeferredValue<number>>,
) {
  let deferred: DeferredValue<number>;

  async function getDeferred(): Promise<DeferredValue<number>> {
    deferred = await factory();
    return deferred;
  }

  describe("DeferredValue conformance", () => {
    it("isDone is false initially", async () => {
      const d = await getDeferred();
      expect(await d.isDoneAsync()).toBe(false);
    });

    it("succeed completes the deferred, isDone becomes true", async () => {
      const d = await getDeferred();
      const ok = await d.succeedAsync(42);
      expect(ok).toBe(true);
      expect(await d.isDoneAsync()).toBe(true);
    });

    it("await resolves with the succeeded value", async () => {
      const d = await getDeferred();
      await d.succeedAsync(99);
      expect(await d.awaitAsync()).toBe(99);
    });

    it("succeed after first succeed is ignored", async () => {
      const d = await getDeferred();
      const first = await d.succeedAsync(1);
      const second = await d.succeedAsync(2);
      expect(first).toBe(true);
      expect(second).toBe(false);
      expect(await d.awaitAsync()).toBe(1);
    });

    it("fail completes the deferred, isDone becomes true", async () => {
      const d = await getDeferred();
      const ok = await d.failAsync(new Error("boom"));
      expect(ok).toBe(true);
      expect(await d.isDoneAsync()).toBe(true);
    });
  });
}
