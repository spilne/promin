import { describe, expect, it } from "bun:test";
import { InMemoryRetrieverRegistry } from "../retriever-registry.ts";
import { retrieverRegistryTestSuite } from "../retriever-registry-test-suite.ts";

retrieverRegistryTestSuite(() => new InMemoryRetrieverRegistry());

describe("InMemoryRetrieverRegistry", () => {
  it("seeds rows from constructor inputs", () => {
    const registry = new InMemoryRetrieverRegistry([
      { id: "docs", retriever: { retrieve: async () => [] }, tags: ["public"] },
    ]);
    expect(registry.list().map((row) => row.id)).toEqual(["docs"]);
  });

  it("rejects an empty id", () => {
    const registry = new InMemoryRetrieverRegistry();
    expect(() => registry.register({ id: " ", retriever: { retrieve: async () => [] } })).toThrow(
      "id must not be empty",
    );
  });
});
