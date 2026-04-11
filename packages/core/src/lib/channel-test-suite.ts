import { describe, it, expect } from "bun:test";
import type { Channel } from "./channel.ts";

export function channelTestSuite(factory: () => Channel<number> | Promise<Channel<number>>) {
  let ch: Channel<number>;

  async function getCh(): Promise<Channel<number>> {
    ch = await factory();
    return ch;
  }

  describe("Channel conformance", () => {
    it("isClosed is false initially", async () => {
      const c = await getCh();
      expect(c.isClosed).toBe(false);
    });

    it("sendAsync succeeds on open channel", async () => {
      const c = await getCh();
      await expect(c.sendAsync(1)).resolves.toBeUndefined();
    });

    it("closeAsync sets isClosed to true", async () => {
      const c = await getCh();
      await c.closeAsync();
      expect(c.isClosed).toBe(true);
    });

    it("sendAsync after close rejects", async () => {
      const c = await getCh();
      await c.closeAsync();
      await expect(c.sendAsync(1)).rejects.toThrow();
    });
  });
}
