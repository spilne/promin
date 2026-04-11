import { describe, it, expect } from "bun:test";
import type { Signal } from "./signal.ts";

export function signalTestSuite(factory: () => Signal<number> | Promise<Signal<number>>) {
  let signal: Signal<number>;

  async function getSignal(): Promise<Signal<number>> {
    signal = await factory();
    return signal;
  }

  describe("Signal conformance", () => {
    it("get returns initial value", async () => {
      const s = await getSignal();
      const value = await s.getAsync();
      expect(value).toBe(0);
    });

    it("set updates the value", async () => {
      const s = await getSignal();
      await s.setAsync(42);
      expect(await s.getAsync()).toBe(42);
    });

    it("update applies function", async () => {
      const s = await getSignal();
      await s.setAsync(10);
      await s.updateAsync((v) => v + 5);
      expect(await s.getAsync()).toBe(15);
    });

    it("multiple set/update calls reflect final state", async () => {
      const s = await getSignal();
      await s.setAsync(5);
      await s.updateAsync((v) => v * 2);
      await s.setAsync(100);
      await s.updateAsync((v) => v - 1);
      expect(await s.getAsync()).toBe(99);
    });
  });
}
