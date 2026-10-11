// ---------------------------------------------------------------------------
// Portable `KnowledgeBaseStore` conformance suite. Every implementation
// (in-memory, SQLite, Postgres) must pass identically.
//
// Usage:
//   import { knowledgeBaseStoreTestSuite } from "@promin/agent/testing";
//   knowledgeBaseStoreTestSuite(() => new InMemoryKnowledgeBaseStore());
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import type {
  KnowledgeBaseRecord,
  KnowledgeBaseStore,
  KnowledgeSourceRecord,
} from "./knowledge-base-store.ts";

function base(overrides: Partial<KnowledgeBaseRecord> & { id: string }): KnowledgeBaseRecord {
  return { tags: [], metadata: {}, createdAt: 1_000, updatedAt: 1_000, ...overrides };
}

function source(overrides: Partial<KnowledgeSourceRecord> & { id: string }): KnowledgeSourceRecord {
  return {
    tags: [],
    metadata: {},
    text: `text of ${overrides.id}`,
    status: "ready",
    chunkCount: 1,
    createdAt: 1_000,
    updatedAt: 1_000,
    ...overrides,
  };
}

export function knowledgeBaseStoreTestSuite(
  factory: () => KnowledgeBaseStore | Promise<KnowledgeBaseStore>,
) {
  async function make(): Promise<KnowledgeBaseStore> {
    return factory();
  }

  describe("KnowledgeBaseStore conformance", () => {
    it("starts empty", async () => {
      const store = await make();
      expect(await store.loadAll()).toEqual([]);
    });

    it("round-trips every base and source field", async () => {
      const store = await make();
      const docs = base({
        id: "docs",
        description: "Product docs",
        tags: ["internal", "product"],
        metadata: { owner: "platform", nested: { level: 2 } },
        createdAt: 1_000,
        updatedAt: 2_000,
      });
      const runbook = source({
        id: "runbook",
        title: "Runbook",
        uri: "https://example.com/runbook",
        mimeType: "text/markdown",
        tags: ["ops"],
        metadata: { version: 1 },
        text: "Restart workers when heartbeat lag exceeds the threshold.",
        chunkCount: 2,
        createdAt: 1_500,
        updatedAt: 1_600,
      });
      const broken = source({
        id: "broken",
        status: "failed",
        error: "embedding provider unavailable",
        chunkCount: 0,
      });
      await store.saveBase(docs);
      await store.saveSource({ baseId: "docs", source: runbook });
      await store.saveSource({ baseId: "docs", source: broken });

      const all = await store.loadAll();
      expect(all).toHaveLength(1);
      expect(all[0]!.base).toEqual(docs);
      expect(all[0]!.sources).toEqual([broken, runbook]);
    });

    it("omits optional fields that were never set", async () => {
      const store = await make();
      await store.saveBase(base({ id: "bare" }));
      await store.saveSource({ baseId: "bare", source: source({ id: "s" }) });
      const [row] = await store.loadAll();
      expect(row!.base).not.toHaveProperty("description");
      expect(row!.sources[0]).not.toHaveProperty("title");
      expect(row!.sources[0]).not.toHaveProperty("uri");
      expect(row!.sources[0]).not.toHaveProperty("mimeType");
      expect(row!.sources[0]).not.toHaveProperty("error");
    });

    it("orders bases and sources by id", async () => {
      const store = await make();
      for (const id of ["zeta", "alpha", "mid"]) await store.saveBase(base({ id }));
      for (const id of ["c", "a", "b"]) {
        await store.saveSource({ baseId: "alpha", source: source({ id }) });
      }
      const all = await store.loadAll();
      expect(all.map((row) => row.base.id)).toEqual(["alpha", "mid", "zeta"]);
      expect(all[0]!.sources.map((s) => s.id)).toEqual(["a", "b", "c"]);
    });

    it("saveBase replaces the row and keeps its sources", async () => {
      const store = await make();
      await store.saveBase(base({ id: "docs", description: "v1" }));
      await store.saveSource({ baseId: "docs", source: source({ id: "s" }) });
      await store.saveBase(base({ id: "docs", tags: ["new"], updatedAt: 5_000 }));
      const [row] = await store.loadAll();
      expect(row!.base.description).toBeUndefined();
      expect(row!.base.tags).toEqual(["new"]);
      expect(row!.base.updatedAt).toBe(5_000);
      expect(row!.sources.map((s) => s.id)).toEqual(["s"]);
    });

    it("saveSource replaces a source with the same id", async () => {
      const store = await make();
      await store.saveBase(base({ id: "docs" }));
      await store.saveSource({ baseId: "docs", source: source({ id: "s", text: "old" }) });
      await store.saveSource({
        baseId: "docs",
        source: source({ id: "s", text: "new", status: "failed", error: "boom", chunkCount: 0 }),
      });
      const [row] = await store.loadAll();
      expect(row!.sources).toHaveLength(1);
      expect(row!.sources[0]).toMatchObject({ text: "new", status: "failed", error: "boom" });
    });

    it("keeps sources of different bases apart, even under the same source id", async () => {
      const store = await make();
      await store.saveBase(base({ id: "a" }));
      await store.saveBase(base({ id: "b" }));
      await store.saveSource({ baseId: "a", source: source({ id: "shared", text: "from a" }) });
      await store.saveSource({ baseId: "b", source: source({ id: "shared", text: "from b" }) });
      await store.deleteSource({ baseId: "a", sourceId: "shared" });
      const all = await store.loadAll();
      expect(all.find((row) => row.base.id === "a")!.sources).toEqual([]);
      expect(all.find((row) => row.base.id === "b")!.sources[0]!.text).toBe("from b");
    });

    it("deleteBase removes the base and its sources", async () => {
      const store = await make();
      await store.saveBase(base({ id: "docs" }));
      await store.saveSource({ baseId: "docs", source: source({ id: "s" }) });
      await store.deleteBase("docs");
      expect(await store.loadAll()).toEqual([]);

      // Re-creating the id starts with no sources.
      await store.saveBase(base({ id: "docs" }));
      expect((await store.loadAll())[0]!.sources).toEqual([]);
    });

    it("deleting what is absent is a no-op", async () => {
      const store = await make();
      await store.deleteBase("ghost");
      await store.deleteSource({ baseId: "ghost", sourceId: "s" });
      expect(await store.loadAll()).toEqual([]);
    });

    it("keeps large source text intact", async () => {
      const store = await make();
      const text = "paragraph with unicode — ✓ ünïcödé\n\n".repeat(20_000);
      await store.saveBase(base({ id: "big" }));
      await store.saveSource({ baseId: "big", source: source({ id: "s", text }) });
      expect((await store.loadAll())[0]!.sources[0]!.text).toBe(text);
    });
  });
}
