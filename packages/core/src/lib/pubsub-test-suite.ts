import { describe, it, expect } from "bun:test";
import type { PubSubBroadcast } from "./pubsub.ts";

/**
 * Portable PubSubBroadcast conformance suite.
 * Tests the Promise API which any backend must implement.
 */
export function pubsubTestSuite(
  factory: () => PubSubBroadcast<number> | Promise<PubSubBroadcast<number>>,
) {
  let ps: PubSubBroadcast<number>;

  async function getPs(): Promise<PubSubBroadcast<number>> {
    ps = await factory();
    return ps;
  }

  describe("PubSubBroadcast conformance", () => {
    it("publishAsync returns true on active pubsub", async () => {
      const p = await getPs();
      const ok = await p.publishAsync(42);
      expect(ok).toBe(true);
    });

    it("shutdownAsync completes without error", async () => {
      const p = await getPs();
      await p.shutdownAsync();
    });
  });
}
