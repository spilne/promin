import { describe, expect, it } from "bun:test";
import { InMemoryNamespaceRegistry } from "../namespace-registry.ts";
import { namespaceRegistryTestSuite } from "../namespace-registry-test-suite.ts";

namespaceRegistryTestSuite(() => new InMemoryNamespaceRegistry());

describe("namespace registry validation", () => {
  it("rejects malformed capability policy values", async () => {
    const registry = new InMemoryNamespaceRegistry();
    await expect(
      registry.create({
        id: "acme",
        capabilities: { ai: { maxTokensPerTurn: "lots" } } as never,
      }),
    ).rejects.toThrow("invalid_namespace_capabilities");
  });

  it("rejects invalid status patches", async () => {
    const registry = new InMemoryNamespaceRegistry();
    await registry.create({ id: "acme" });
    await expect(registry.update("acme", { status: "deleted" as never })).rejects.toThrow(
      "invalid_namespace_status",
    );
  });
});
