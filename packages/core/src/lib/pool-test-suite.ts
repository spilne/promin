import { describe, it, expect } from "bun:test";
import type { ResourcePool } from "./pool.ts";

export function poolTestSuite(
  factory: () => ResourcePool<{ id: number }> | Promise<ResourcePool<{ id: number }>>,
) {
  let pool: ResourcePool<{ id: number }>;

  async function getPool(): Promise<ResourcePool<{ id: number }>> {
    pool = await factory();
    return pool;
  }

  describe("ResourcePool conformance", () => {
    it("useAsync provides a resource and returns the result", async () => {
      const p = await getPool();
      const result = await p.useAsync(async (r) => r.id);
      expect(typeof result).toBe("number");
    });

    it("size returns configured pool size", async () => {
      const p = await getPool();
      expect(p.size).toBeGreaterThan(0);
    });

    it("resource is released after use", async () => {
      const p = await getPool();
      const first = await p.useAsync(async (r) => r.id);
      const second = await p.useAsync(async (r) => r.id);
      expect(typeof first).toBe("number");
      expect(typeof second).toBe("number");
    });

    it("resource is released even on error", async () => {
      const p = await getPool();
      await expect(
        p.useAsync(async () => {
          throw new Error("boom");
        }),
      ).rejects.toThrow("boom");
      const result = await p.useAsync(async (r) => r.id);
      expect(typeof result).toBe("number");
    });
  });
}
