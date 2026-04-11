import { describe, it, expect } from "bun:test";
import type { AtomicRef } from "./ref.ts";

export function refTestSuite(factory: () => AtomicRef<number> | Promise<AtomicRef<number>>) {
  let ref: AtomicRef<number>;

  async function getRef(): Promise<AtomicRef<number>> {
    ref = await factory();
    return ref;
  }

  describe("AtomicRef conformance", () => {
    it("get returns initial value", async () => {
      const r = await getRef();
      const value = await r.getAsync();
      expect(value).toBe(0);
    });

    it("set updates the value", async () => {
      const r = await getRef();
      await r.setAsync(42);
      expect(await r.getAsync()).toBe(42);
    });

    it("update applies function to current value", async () => {
      const r = await getRef();
      await r.setAsync(10);
      await r.updateAsync((v) => v + 5);
      expect(await r.getAsync()).toBe(15);
    });

    it("multiple updates are sequential", async () => {
      const r = await getRef();
      await r.setAsync(1);
      await r.updateAsync((v) => v * 3);
      await r.updateAsync((v) => v + 7);
      expect(await r.getAsync()).toBe(10);
    });
  });
}
