import { describe, expect, it } from "bun:test";
import type { NamespaceRegistry } from "./namespace-registry.ts";

export function namespaceRegistryTestSuite(
  factory: () => NamespaceRegistry | Promise<NamespaceRegistry>,
): void {
  async function make(): Promise<NamespaceRegistry> {
    return factory();
  }

  describe("NamespaceRegistry conformance", () => {
    it("starts empty", async () => {
      const registry = await make();
      expect(await registry.list()).toEqual([]);
    });

    it("creates and gets a namespace", async () => {
      const registry = await make();
      const created = await registry.create({
        id: "acme",
        displayName: "Acme",
        description: "Production tenant",
        capabilities: { ai: { enabled: true, maxTokensPerTurn: 8192 } },
        metadata: { owner: "ops" },
      });
      expect(created.id).toBe("acme");
      expect(created.status).toBe("active");

      const got = await registry.get("acme");
      expect(got?.displayName).toBe("Acme");
      expect(got?.capabilities.ai?.maxTokensPerTurn).toBe(8192);
      expect(got?.metadata.owner).toBe("ops");
    });

    it("filters active and archived namespaces", async () => {
      const registry = await make();
      await registry.create({ id: "active" });
      await registry.create({ id: "old" });
      await registry.archive("old");

      expect((await registry.list({ status: "active" })).map((ns) => ns.id)).toEqual(["active"]);
      expect((await registry.list({ status: "archived" })).map((ns) => ns.id)).toEqual(["old"]);
    });

    it("updates mutable fields", async () => {
      const registry = await make();
      await registry.create({ id: "acme" });
      const updated = await registry.update("acme", {
        displayName: "Acme Corp",
        description: "Updated",
        capabilities: { workflows: { maxConcurrentRuns: 5 } },
        metadata: { region: "ca" },
      });
      expect(updated.displayName).toBe("Acme Corp");
      expect(updated.description).toBe("Updated");
      expect(updated.capabilities.workflows?.maxConcurrentRuns).toBe(5);
      expect(updated.metadata.region).toBe("ca");
      expect(updated.updatedAt).toBeGreaterThanOrEqual(updated.createdAt);
    });

    it("rejects duplicate ids", async () => {
      const registry = await make();
      await registry.create({ id: "acme" });
      await expect(registry.create({ id: "acme" })).rejects.toThrow(/namespace already exists/);
    });
  });
}
