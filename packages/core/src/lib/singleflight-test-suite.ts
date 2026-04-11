import { describe, it, expect } from "bun:test";
import type { Singleflight } from "./singleflight.ts";

const delay = (ms: number) => new Promise((r) => setTimeout(r, ms));

/**
 * Portable Singleflight conformance suite.
 * Tests the Promise API (doAsync) which any backend must implement.
 */
export function singleflightTestSuite(factory: () => Singleflight | Promise<Singleflight>) {
  let sf: Singleflight;

  async function getSf(): Promise<Singleflight> {
    sf = await factory();
    return sf;
  }

  describe("Singleflight conformance", () => {
    it("single call executes normally", async () => {
      const s = await getSf();
      const result = await s.doAsync("k", async () => 42);
      expect(result).toBe(42);
    });

    it("two concurrent calls with same key — one execution", async () => {
      const s = await getSf();
      let executions = 0;

      const [a, b] = await Promise.all([
        s.doAsync("k", async () => {
          executions++;
          await delay(50);
          return "result";
        }),
        s.doAsync("k", async () => {
          executions++;
          await delay(50);
          return "result";
        }),
      ]);

      expect(executions).toBe(1);
      expect(a).toBe("result");
      expect(b).toBe("result");
    });

    it("three concurrent calls — one execution", async () => {
      const s = await getSf();
      let executions = 0;

      const run = () =>
        s.doAsync("k", async () => {
          executions++;
          await delay(50);
          return 99;
        });

      const results = await Promise.all([run(), run(), run()]);
      expect(executions).toBe(1);
      expect(results).toEqual([99, 99, 99]);
    });

    it("different keys execute independently", async () => {
      const s = await getSf();
      let executions = 0;

      const [a, b] = await Promise.all([
        s.doAsync("a", async () => {
          executions++;
          await delay(30);
          return 1;
        }),
        s.doAsync("b", async () => {
          executions++;
          await delay(30);
          return 2;
        }),
      ]);

      expect(executions).toBe(2);
      expect(a).toBe(1);
      expect(b).toBe(2);
    });

    it("key is cleared after completion — next call starts fresh", async () => {
      const s = await getSf();
      let executions = 0;

      const run = () =>
        s.doAsync("k", async () => {
          executions++;
          return "v";
        });

      await run();
      await run();
      expect(executions).toBe(2);
    });

    it("failure propagates to all joiners", async () => {
      const s = await getSf();
      let executions = 0;

      const run = () =>
        s.doAsync("k", async () => {
          executions++;
          await delay(50);
          throw new Error("boom");
        });

      const results = await Promise.allSettled([run(), run()]);
      expect(executions).toBe(1);
      for (const r of results) {
        expect(r.status).toBe("rejected");
        if (r.status === "rejected") {
          expect(r.reason.message).toBe("boom");
        }
      }
    });

    it("key is cleared after failure — next call starts fresh", async () => {
      const s = await getSf();
      let executions = 0;

      await s
        .doAsync("k", async () => {
          executions++;
          throw new Error("fail");
        })
        .catch(() => {});

      const result = await s.doAsync("k", async () => {
        executions++;
        return "ok";
      });

      expect(executions).toBe(2);
      expect(result).toBe("ok");
    });
  });
}
