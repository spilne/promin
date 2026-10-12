// ---------------------------------------------------------------------------
// Portable `KnowledgeBaseStore` conformance suite. Every implementation
// (in-memory, SQLite, Postgres) must pass identically.
//
// Usage:
//   import { knowledgeBaseStoreTestSuite } from "@promin/agent/testing";
//   knowledgeBaseStoreTestSuite({ factory: () => new InMemoryKnowledgeBaseStore() });
//
// Claim staleness is measured on the store's own clock, so the suite only
// checks it with `staleAfterMs: 0` (any claim is stale) and a large value
// (no claim is stale); `waitPastStale` lets a store whose clock the suite
// cannot drive (a database server) make a 0 ms claim strictly older.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import type {
  KnowledgeBaseRecord,
  KnowledgeBaseStore,
  KnowledgeSourceRecord,
} from "./knowledge-base-store.ts";

const NS = "acme";
const LONG = 3_600_000;

function base(overrides: Partial<KnowledgeBaseRecord> & { id: string }): KnowledgeBaseRecord {
  return {
    namespaceId: NS,
    tags: [],
    metadata: {},
    createdAt: 1_000,
    updatedAt: 1_000,
    ...overrides,
  };
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

function summary(record: KnowledgeSourceRecord): Omit<KnowledgeSourceRecord, "text"> {
  const { text: _text, ...rest } = record;
  return rest;
}

export function knowledgeBaseStoreTestSuite(params: {
  factory: () => KnowledgeBaseStore | Promise<KnowledgeBaseStore>;
  /** Let a claim made now be older than 0 ms on the store's clock. Default: a 5 ms real wait. */
  waitPastStale?: () => Promise<void>;
}) {
  const make = async (): Promise<KnowledgeBaseStore> => params.factory();
  const waitPastStale =
    params.waitPastStale ?? (() => new Promise<void>((resolve) => setTimeout(resolve, 5)));

  describe("KnowledgeBaseStore conformance", () => {
    it("starts empty", async () => {
      const store = await make();
      expect(await store.listBases()).toEqual([]);
      expect(await store.getBase({ namespaceId: NS, id: "docs" })).toBeNull();
      expect(await store.getSource({ namespaceId: NS, baseId: "docs", sourceId: "s" })).toBeNull();
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
      await store.saveSource({ namespaceId: NS, baseId: "docs", source: runbook });
      await store.saveSource({ namespaceId: NS, baseId: "docs", source: broken });

      const all = await store.listBases();
      expect(all).toHaveLength(1);
      expect(all[0]!.base).toEqual(docs);
      expect(all[0]!.sources).toEqual([summary(broken), summary(runbook)]);
      expect(await store.getBase({ namespaceId: NS, id: "docs" })).toEqual(all[0]!);
      expect(
        await store.getSource({ namespaceId: NS, baseId: "docs", sourceId: "runbook" }),
      ).toEqual(runbook);
    });

    it("omits optional fields that were never set", async () => {
      const store = await make();
      await store.saveBase(base({ id: "bare" }));
      await store.saveSource({ namespaceId: NS, baseId: "bare", source: source({ id: "s" }) });
      const [row] = await store.listBases();
      expect(row!.base).not.toHaveProperty("description");
      expect(row!.sources[0]).not.toHaveProperty("title");
      expect(row!.sources[0]).not.toHaveProperty("uri");
      expect(row!.sources[0]).not.toHaveProperty("mimeType");
      expect(row!.sources[0]).not.toHaveProperty("error");
      expect(row!.sources[0]).not.toHaveProperty("text");
    });

    it("orders bases by namespace then id, and sources by id", async () => {
      const store = await make();
      for (const id of ["zeta", "alpha", "mid"]) await store.saveBase(base({ id }));
      await store.saveBase(base({ namespaceId: "beta", id: "zeta" }));
      for (const id of ["c", "a", "b"]) {
        await store.saveSource({ namespaceId: NS, baseId: "alpha", source: source({ id }) });
      }
      const all = await store.listBases();
      expect(all.map((row) => `${row.base.namespaceId}/${row.base.id}`)).toEqual([
        "acme/alpha",
        "acme/mid",
        "acme/zeta",
        "beta/zeta",
      ]);
      expect(all[0]!.sources.map((s) => s.id)).toEqual(["a", "b", "c"]);
    });

    it("keeps the same base id in different namespaces apart", async () => {
      const store = await make();
      await store.saveBase(base({ id: "docs", description: "acme docs" }));
      await store.saveBase(base({ namespaceId: "beta", id: "docs", description: "beta docs" }));
      await store.saveSource({
        namespaceId: NS,
        baseId: "docs",
        source: source({ id: "s", text: "acme text" }),
      });
      await store.saveSource({
        namespaceId: "beta",
        baseId: "docs",
        source: source({ id: "s", text: "beta text" }),
      });

      expect((await store.getBase({ namespaceId: "beta", id: "docs" }))!.base.description).toBe(
        "beta docs",
      );
      expect(
        (await store.getSource({ namespaceId: "beta", baseId: "docs", sourceId: "s" }))!.text,
      ).toBe("beta text");

      await store.deleteBase({ namespaceId: NS, id: "docs" });
      expect(await store.getBase({ namespaceId: NS, id: "docs" })).toBeNull();
      expect(
        (await store.getSource({ namespaceId: "beta", baseId: "docs", sourceId: "s" }))!.text,
      ).toBe("beta text");
    });

    it("saveBase replaces the row and keeps its sources", async () => {
      const store = await make();
      await store.saveBase(base({ id: "docs", description: "v1" }));
      await store.saveSource({ namespaceId: NS, baseId: "docs", source: source({ id: "s" }) });
      await store.saveBase(base({ id: "docs", tags: ["new"], updatedAt: 5_000 }));
      const [row] = await store.listBases();
      expect(row!.base.description).toBeUndefined();
      expect(row!.base.tags).toEqual(["new"]);
      expect(row!.base.updatedAt).toBe(5_000);
      expect(row!.sources.map((s) => s.id)).toEqual(["s"]);
    });

    it("saveSource replaces a source with the same id", async () => {
      const store = await make();
      await store.saveBase(base({ id: "docs" }));
      await store.saveSource({
        namespaceId: NS,
        baseId: "docs",
        source: source({ id: "s", text: "old" }),
      });
      await store.saveSource({
        namespaceId: NS,
        baseId: "docs",
        source: source({ id: "s", text: "new", status: "failed", error: "boom", chunkCount: 0 }),
      });
      const [row] = await store.listBases();
      expect(row!.sources).toHaveLength(1);
      expect(row!.sources[0]).toMatchObject({ status: "failed", error: "boom" });
      expect(
        (await store.getSource({ namespaceId: NS, baseId: "docs", sourceId: "s" }))!.text,
      ).toBe("new");
    });

    it("keeps sources of different bases apart, even under the same source id", async () => {
      const store = await make();
      await store.saveBase(base({ id: "a" }));
      await store.saveBase(base({ id: "b" }));
      await store.saveSource({
        namespaceId: NS,
        baseId: "a",
        source: source({ id: "shared", text: "from a" }),
      });
      await store.saveSource({
        namespaceId: NS,
        baseId: "b",
        source: source({ id: "shared", text: "from b" }),
      });
      await store.deleteSource({ namespaceId: NS, baseId: "a", sourceId: "shared" });
      const all = await store.listBases();
      expect(all.find((row) => row.base.id === "a")!.sources).toEqual([]);
      expect(
        (await store.getSource({ namespaceId: NS, baseId: "b", sourceId: "shared" }))!.text,
      ).toBe("from b");
    });

    it("deleteBase removes the base and its sources", async () => {
      const store = await make();
      await store.saveBase(base({ id: "docs" }));
      await store.saveSource({ namespaceId: NS, baseId: "docs", source: source({ id: "s" }) });
      await store.deleteBase({ namespaceId: NS, id: "docs" });
      expect(await store.listBases()).toEqual([]);

      // Re-creating the id starts with no sources.
      await store.saveBase(base({ id: "docs" }));
      expect((await store.listBases())[0]!.sources).toEqual([]);
    });

    it("deleting what is absent is a no-op", async () => {
      const store = await make();
      await store.deleteBase({ namespaceId: NS, id: "ghost" });
      await store.deleteSource({ namespaceId: NS, baseId: "ghost", sourceId: "s" });
      expect(await store.listBases()).toEqual([]);
    });

    it("keeps large source text intact", async () => {
      const store = await make();
      const text = "paragraph with unicode — ✓ ünïcödé\n\n".repeat(20_000);
      await store.saveBase(base({ id: "big" }));
      await store.saveSource({ namespaceId: NS, baseId: "big", source: source({ id: "s", text }) });
      expect((await store.getSource({ namespaceId: NS, baseId: "big", sourceId: "s" }))!.text).toBe(
        text,
      );
    });

    describe("ingestion queue", () => {
      async function seeded(): Promise<KnowledgeBaseStore> {
        const store = await make();
        await store.saveBase(base({ id: "docs" }));
        return store;
      }

      it("claims nothing when no source is pending", async () => {
        const store = await seeded();
        await store.saveSource({ namespaceId: NS, baseId: "docs", source: source({ id: "s" }) });
        expect(await store.claimSource({ workerId: "w1", staleAfterMs: LONG })).toBeNull();
      });

      it("claims the least recently written pending source, once", async () => {
        const store = await seeded();
        await store.saveSource({
          namespaceId: NS,
          baseId: "docs",
          source: source({ id: "late", status: "pending", updatedAt: 2_000 }),
        });
        await store.saveSource({
          namespaceId: NS,
          baseId: "docs",
          source: source({ id: "early", status: "pending", updatedAt: 1_000 }),
        });

        const first = await store.claimSource({ workerId: "w1", staleAfterMs: LONG });
        expect(first).toMatchObject({ namespaceId: NS, baseId: "docs" });
        expect(first!.source).toMatchObject({ id: "early", status: "ingesting" });
        expect(first!.source.text).toBe("text of early");

        const second = await store.claimSource({ workerId: "w2", staleAfterMs: LONG });
        expect(second!.source.id).toBe("late");
        expect(await store.claimSource({ workerId: "w3", staleAfterMs: LONG })).toBeNull();

        const [row] = await store.listBases();
        expect(row!.sources.map((s) => s.status)).toEqual(["ingesting", "ingesting"]);
      });

      it("concurrent claims never hand out the same source", async () => {
        const store = await seeded();
        for (const id of ["a", "b", "c"]) {
          await store.saveSource({
            namespaceId: NS,
            baseId: "docs",
            source: source({ id, status: "pending" }),
          });
        }
        const claims = await Promise.all(
          ["w1", "w2", "w3", "w4", "w5"].map((workerId) =>
            store.claimSource({ workerId, staleAfterMs: LONG }),
          ),
        );
        const ids = claims.flatMap((c) => (c ? [c.source.id] : []));
        expect(ids.sort()).toEqual(["a", "b", "c"]);
      });

      it("completeSource writes the outcome only while the claim holds", async () => {
        const store = await seeded();
        await store.saveSource({
          namespaceId: NS,
          baseId: "docs",
          source: source({ id: "s", status: "pending" }),
        });
        const claimed = await store.claimSource({ workerId: "w1", staleAfterMs: LONG });
        const ready = { ...claimed!.source, status: "ready" as const, chunkCount: 3 };

        expect(
          await store.completeSource({
            namespaceId: NS,
            baseId: "docs",
            workerId: "w2",
            source: ready,
          }),
        ).toBe(false);
        expect(
          await store.completeSource({
            namespaceId: NS,
            baseId: "docs",
            workerId: "w1",
            source: ready,
          }),
        ).toBe(true);
        expect(await store.getSource({ namespaceId: NS, baseId: "docs", sourceId: "s" })).toEqual(
          ready,
        );
        // The claim is gone with the outcome.
        expect(
          await store.completeSource({
            namespaceId: NS,
            baseId: "docs",
            workerId: "w1",
            source: ready,
          }),
        ).toBe(false);
      });

      it("a source rewritten during its ingestion drops the stale outcome", async () => {
        const store = await seeded();
        await store.saveSource({
          namespaceId: NS,
          baseId: "docs",
          source: source({ id: "s", status: "pending", text: "v1" }),
        });
        const claimed = await store.claimSource({ workerId: "w1", staleAfterMs: LONG });
        await store.saveSource({
          namespaceId: NS,
          baseId: "docs",
          source: source({ id: "s", status: "pending", text: "v2", updatedAt: 3_000 }),
        });
        expect(
          await store.completeSource({
            namespaceId: NS,
            baseId: "docs",
            workerId: "w1",
            source: { ...claimed!.source, status: "ready" },
          }),
        ).toBe(false);
        const again = await store.claimSource({ workerId: "w2", staleAfterMs: LONG });
        expect(again!.source.text).toBe("v2");
      });

      it("a deleted source cannot be completed", async () => {
        const store = await seeded();
        await store.saveSource({
          namespaceId: NS,
          baseId: "docs",
          source: source({ id: "s", status: "pending" }),
        });
        const claimed = await store.claimSource({ workerId: "w1", staleAfterMs: LONG });
        await store.deleteSource({ namespaceId: NS, baseId: "docs", sourceId: "s" });
        expect(
          await store.completeSource({
            namespaceId: NS,
            baseId: "docs",
            workerId: "w1",
            source: { ...claimed!.source, status: "ready" },
          }),
        ).toBe(false);
        expect(
          await store.getSource({ namespaceId: NS, baseId: "docs", sourceId: "s" }),
        ).toBeNull();
      });

      it("reclaims an ingesting source once its claim is stale", async () => {
        const store = await seeded();
        await store.saveSource({
          namespaceId: NS,
          baseId: "docs",
          source: source({ id: "s", status: "pending" }),
        });
        await store.claimSource({ workerId: "crashed", staleAfterMs: LONG });
        expect(await store.claimSource({ workerId: "w2", staleAfterMs: LONG })).toBeNull();

        await waitPastStale();
        const reclaimed = await store.claimSource({ workerId: "w2", staleAfterMs: 0 });
        expect(reclaimed!.source.id).toBe("s");
        // The crashed worker's late outcome no longer lands.
        expect(
          await store.completeSource({
            namespaceId: NS,
            baseId: "docs",
            workerId: "crashed",
            source: { ...reclaimed!.source, status: "ready" },
          }),
        ).toBe(false);
      });

      it("heartbeatClaim renews only the holder's live claim", async () => {
        const store = await seeded();
        await store.saveSource({
          namespaceId: NS,
          baseId: "docs",
          source: source({ id: "s", status: "pending" }),
        });
        await store.claimSource({ workerId: "w1", staleAfterMs: LONG });
        const key = { namespaceId: NS, baseId: "docs", sourceId: "s" };
        expect(await store.heartbeatClaim({ ...key, workerId: "w1" })).toBe(true);
        expect(await store.heartbeatClaim({ ...key, workerId: "w2" })).toBe(false);
        // A rewrite drops the claim.
        await store.saveSource({
          namespaceId: NS,
          baseId: "docs",
          source: source({ id: "s", status: "pending", updatedAt: 2_000 }),
        });
        expect(await store.heartbeatClaim({ ...key, workerId: "w1" })).toBe(false);
      });

      it("releaseClaims re-queues only the given worker's claims", async () => {
        const store = await seeded();
        for (const id of ["a", "b"]) {
          await store.saveSource({
            namespaceId: NS,
            baseId: "docs",
            source: source({ id, status: "pending" }),
          });
        }
        await store.claimSource({ workerId: "w1", staleAfterMs: LONG });
        await store.claimSource({ workerId: "w2", staleAfterMs: LONG });

        expect(await store.releaseClaims({ workerId: "w1" })).toBe(1);
        expect(await store.releaseClaims({ workerId: "w1" })).toBe(0);
        const statuses = (await store.listBases())[0]!.sources.map((s) => s.status).sort();
        expect(statuses).toEqual(["ingesting", "pending"]);
        expect(await store.claimSource({ workerId: "w3", staleAfterMs: LONG })).not.toBeNull();
      });
    });
  });
}
