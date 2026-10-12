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
      expect(row.namespaceId).toBeUndefined();
      expect(row.retriever).toBe(retriever);
      expect(row.description).toBe("Product docs");
      expect(row.tags).toEqual(["docs", "public"]);
      expect(row.metadata).toEqual({ owner: "support" });
      expect(registry.get({ id: "docs" })).toEqual(row);
    });

    it("re-registering an id replaces the row", async () => {
      const registry = await factory();
      const first: Retriever = { retrieve: async () => [] };
      const second: Retriever = { retrieve: async () => [] };

      registry.register(input({ id: "docs", patch: { retriever: first, tags: ["old"] } }));
      registry.register(input({ id: "docs", patch: { retriever: second, tags: ["new"] } }));

      const row = registry.get({ id: "docs" });
      expect(row?.retriever).toBe(second);
      expect(row?.tags).toEqual(["new"]);
      expect(registry.list()).toHaveLength(1);
    });

    it("returns null when missing", async () => {
      const registry = await factory();
      expect(registry.get({ id: "missing" })).toBeNull();
      expect(registry.get({ id: "missing", namespaceId: "acme" })).toBeNull();
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

      registry.unregister({ id: "docs" });
      registry.unregister({ id: "docs" });

      expect(registry.get({ id: "docs" })).toBeNull();
      expect(registry.list()).toEqual([]);
    });

    it("namespace rows are visible only from their namespace, global rows from all", async () => {
      const registry = await factory();
      const acme: Retriever = { retrieve: async () => [] };
      registry.register(input({ id: "shared" }));
      registry.register(input({ id: "handbook", patch: { namespaceId: "acme", retriever: acme } }));

      expect(registry.get({ id: "handbook", namespaceId: "acme" })?.retriever).toBe(acme);
      expect(registry.get({ id: "handbook", namespaceId: "beta" })).toBeNull();
      expect(registry.get({ id: "handbook" })).toBeNull();
      expect(registry.get({ id: "shared", namespaceId: "beta" })?.namespaceId).toBeUndefined();

      expect(registry.list({ namespaceId: "acme" }).map((row) => row.id)).toEqual([
        "handbook",
        "shared",
      ]);
      expect(registry.list({ namespaceId: "beta" }).map((row) => row.id)).toEqual(["shared"]);
      expect(registry.list().map((row) => row.id)).toEqual(["shared"]);
    });

    it("the same id in two namespaces are two rows; a namespace row shadows a global one", async () => {
      const registry = await factory();
      const global: Retriever = { retrieve: async () => [] };
      const acme: Retriever = { retrieve: async () => [] };
      const beta: Retriever = { retrieve: async () => [] };
      registry.register(input({ id: "docs", patch: { retriever: global } }));
      registry.register(input({ id: "docs", patch: { namespaceId: "acme", retriever: acme } }));
      registry.register(input({ id: "docs", patch: { namespaceId: "beta", retriever: beta } }));

      expect(registry.get({ id: "docs", namespaceId: "acme" })?.retriever).toBe(acme);
      expect(registry.get({ id: "docs", namespaceId: "beta" })?.retriever).toBe(beta);
      expect(registry.get({ id: "docs", namespaceId: "gamma" })?.retriever).toBe(global);
      expect(registry.list({ namespaceId: "acme" }).map((row) => row.retriever)).toEqual([acme]);

      registry.unregister({ id: "docs", namespaceId: "acme" });
      expect(registry.get({ id: "docs", namespaceId: "acme" })?.retriever).toBe(global);
      expect(registry.get({ id: "docs", namespaceId: "beta" })?.retriever).toBe(beta);
    });
  });
}
