import { describe, expect, it } from "bun:test";
import {
  InMemoryNamespaceRegistry,
  isValidNamespaceId,
  normalizeNamespaceCapabilities,
} from "../namespace-registry.ts";
import { namespaceRegistryTestSuite } from "../namespace-registry-test-suite.ts";

namespaceRegistryTestSuite({ factory: ({ clock }) => new InMemoryNamespaceRegistry({ clock }) });

describe("namespace validation", () => {
  it("isValidNamespaceId matches the registry's id rule", () => {
    expect(isValidNamespaceId("tenant-a")).toBe(true);
    expect(isValidNamespaceId("tenant a")).toBe(false);
  });

  it("drops undefined capability fields and copies arrays", () => {
    const models = ["m-1"];
    const out = normalizeNamespaceCapabilities({
      ai: { enabled: undefined, models },
      tools: undefined,
    });
    expect(out).toEqual({ ai: { models: ["m-1"] } });
    models.push("m-2");
    expect(out.ai?.models).toEqual(["m-1"]);
  });
});
