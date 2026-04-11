import { describe, it, expect } from "bun:test";
import type { Latch } from "./latch.ts";

export function latchTestSuite(factory: () => Latch | Promise<Latch>) {
  let latch: Latch;

  async function getLatch(): Promise<Latch> {
    latch = await factory();
    return latch;
  }

  describe("Latch conformance", () => {
    it("remaining starts at configured count", async () => {
      const l = await getLatch();
      expect(await l.remainingAsync()).toBe(3);
    });

    it("countDown decrements remaining", async () => {
      const l = await getLatch();
      await l.countDownAsync();
      expect(await l.remainingAsync()).toBe(2);
    });

    it("await resolves when count reaches zero", async () => {
      const l = await getLatch();
      let resolved = false;
      void l.awaitAsync().then(() => {
        resolved = true;
      });

      await l.countDownAsync();
      await l.countDownAsync();
      expect(resolved).toBe(false);

      await l.countDownAsync();
      await new Promise((r) => setTimeout(r, 10));
      expect(resolved).toBe(true);
    });

    it("await resolves immediately if count is already 0", async () => {
      const l = await getLatch();
      await l.countDownAsync();
      await l.countDownAsync();
      await l.countDownAsync();
      await l.awaitAsync();
    });

    it("multiple countDown calls work", async () => {
      const l = await getLatch();
      await l.countDownAsync();
      expect(await l.remainingAsync()).toBe(2);
      await l.countDownAsync();
      expect(await l.remainingAsync()).toBe(1);
      await l.countDownAsync();
      expect(await l.remainingAsync()).toBe(0);
    });

    it("countDown past zero does not go negative", async () => {
      const l = await getLatch();
      await l.countDownAsync();
      await l.countDownAsync();
      await l.countDownAsync();
      await l.countDownAsync();
      expect(await l.remainingAsync()).toBe(0);
    });
  });
}
