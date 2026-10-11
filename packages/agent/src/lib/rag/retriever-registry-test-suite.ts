import { describe, expect, it } from "bun:test";
import type { RegisterRetrieverInput, RetrieverRegistry } from "./retriever-registry.ts";
import type { Retriever } from "./types.ts";

const noopRetriever: Retriever = {
  retrieve: async () => [],
};

function input(params: {
  id: string;
  patch?: Partial<RegisterRetrieverInput>;
}): RegisterRetrieverInput {
  return { id: params.id, retriever: noopRetriever, ...params.patch };
}

/** Conformance suite every `RetrieverRegistry` implementation should pass. */
export function retrieverRegistryTestSuite(
  factory: () => RetrieverRegistry | Promise<RetrieverRegistry>,
): void {
  describe("RetrieverRegistry conformance", () => {
    it("registers and gets a retriever", async () => {
      const registry = await factory();
      const retriever: Retriever = { retrieve: async () => [] };

      const row = registry.register(
        input({
          id: "docs",
          patch: {
            retriever,
            description: "Product docs",
            tags: ["docs", "public"],
            metadata: { owner: "support" },
          },
        }),
      );

      expect(row.id).toBe("docs");
      expect(row.retriever).toBe(retriever);
      expect(row.description).toBe("Product docs");
      expect(row.tags).toEqual(["docs", "public"]);
      expect(row.metadata).toEqual({ owner: "support" });
      expect(registry.get("docs")).toEqual(row);
    });

    it("re-registering an id replaces the row", async () => {
      const registry = await factory();
      const first: Retriever = { retrieve: async () => [] };
      const second: Retriever = { retrieve: async () => [] };

      registry.register(input({ id: "docs", patch: { retriever: first, tags: ["old"] } }));
      registry.register(input({ id: "docs", patch: { retriever: second, tags: ["new"] } }));

      const row = registry.get("docs");
      expect(row?.retriever).toBe(second);
      expect(row?.tags).toEqual(["new"]);
      expect(registry.list()).toHaveLength(1);
    });

    it("returns null when missing", async () => {
      const registry = await factory();
      expect(registry.get("missing")).toBeNull();
    });

    it("lists rows by id and filters by tag", async () => {
      const registry = await factory();
      registry.register(input({ id: "z", patch: { tags: ["private"] } }));
      registry.register(input({ id: "a", patch: { tags: ["public"] } }));
      registry.register(input({ id: "m", patch: { tags: ["public", "docs"] } }));

      expect(registry.list().map((row) => row.id)).toEqual(["a", "m", "z"]);
      expect(registry.list({ tag: "public" }).map((row) => row.id)).toEqual(["a", "m"]);
      expect(registry.list({ tag: "missing" })).toEqual([]);
    });

    it("unregisters rows and is idempotent", async () => {
      const registry = await factory();
      registry.register(input({ id: "docs" }));

      registry.unregister("docs");
      registry.unregister("docs");

      expect(registry.get("docs")).toBeNull();
      expect(registry.list()).toEqual([]);
    });
  });
}
