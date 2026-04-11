import { describe, it, expect } from "bun:test";
import type { AsyncQueue } from "./queue.ts";

export function queueTestSuite(factory: () => AsyncQueue<number> | Promise<AsyncQueue<number>>) {
  let q: AsyncQueue<number>;

  async function getQ(): Promise<AsyncQueue<number>> {
    q = await factory();
    return q;
  }

  describe("AsyncQueue conformance", () => {
    it("offer and take round-trip items in order", async () => {
      const q = await getQ();
      await q.offerAsync(1);
      const item = await q.takeAsync();
      expect(item).toBe(1);
    });

    it("size reflects current count", async () => {
      const q = await getQ();
      expect(await q.sizeAsync()).toBe(0);
      await q.offerAsync(10);
      await q.offerAsync(20);
      expect(await q.sizeAsync()).toBe(2);
    });

    it("take blocks until item available", async () => {
      const q = await getQ();
      const taken = q.takeAsync();
      setTimeout(() => q.offerAsync(42), 50);
      expect(await taken).toBe(42);
    });

    it("multiple items maintain FIFO order", async () => {
      const q = await getQ();
      await q.offerAsync(1);
      await q.offerAsync(2);
      await q.offerAsync(3);
      expect(await q.takeAsync()).toBe(1);
      expect(await q.takeAsync()).toBe(2);
      expect(await q.takeAsync()).toBe(3);
    });

    it("size decreases after take", async () => {
      const q = await getQ();
      await q.offerAsync(1);
      await q.offerAsync(2);
      expect(await q.sizeAsync()).toBe(2);
      await q.takeAsync();
      expect(await q.sizeAsync()).toBe(1);
    });
  });
}
