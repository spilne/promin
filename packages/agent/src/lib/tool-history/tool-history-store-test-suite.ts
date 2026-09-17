// ---------------------------------------------------------------------------
// Portable `ToolHistoryStore` conformance suite. Every implementation
// must pass: in-memory, SQLite, Postgres.
//
// Usage:
//   import { toolHistoryStoreTestSuite } from "@promin/agent/testing";
//   toolHistoryStoreTestSuite(() => new InMemoryToolHistoryStore());
//
// Timestamps are not asserted exactly — a server-clock backend (Postgres)
// can't take an injected clock, so the suite checks relative invariants
// (firstSeenAt stable, lastSeenAt non-decreasing) only. Exact-time
// behaviour is covered by the in-memory store's own FakeClock tests.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import type { ToolHistoryStore, ToolObservation } from "./types.ts";

const obs = (over: Partial<ToolObservation> = {}): ToolObservation => ({
  name: "search",
  sourceKind: "in-process",
  sourceDetail: "",
  schemaHash: "hash-a",
  description: "Search the web",
  ...over,
});

export function toolHistoryStoreTestSuite(
  factory: () => ToolHistoryStore | Promise<ToolHistoryStore>,
) {
  const make = (): Promise<ToolHistoryStore> => Promise.resolve(factory());

  describe("ToolHistoryStore conformance", () => {
    it("records a new observation as one row with a coherent seen-window", async () => {
      const store = await make();
      await store.recordSnapshot([obs()]);

      const rows = await store.list();
      expect(rows).toHaveLength(1);
      const [r] = rows;
      expect(r).toMatchObject({
        name: "search",
        sourceKind: "in-process",
        sourceDetail: "",
        schemaHash: "hash-a",
        description: "Search the web",
      });
      expect(r?.firstSeenAt).toBeGreaterThan(0);
      expect(r?.lastSeenAt).toBeGreaterThanOrEqual(r?.firstSeenAt ?? 0);
    });

    it("re-recording the same tuple bumps lastSeenAt, keeps firstSeenAt, no dup row", async () => {
      const store = await make();
      await store.recordSnapshot([obs()]);
      const first = (await store.list())[0];

      await store.recordSnapshot([obs()]);
      const rows = await store.list();
      expect(rows).toHaveLength(1);
      expect(rows[0]?.firstSeenAt).toBe(first?.firstSeenAt ?? -1);
      expect(rows[0]?.lastSeenAt).toBeGreaterThanOrEqual(first?.lastSeenAt ?? 0);
    });

    it("a schema-hash change lands as a separate row", async () => {
      const store = await make();
      await store.recordSnapshot([obs({ schemaHash: "hash-a" })]);
      await store.recordSnapshot([obs({ schemaHash: "hash-b" })]);

      const hashes = (await store.list({ name: "search" })).map((r) => r.schemaHash).sort();
      expect(hashes).toEqual(["hash-a", "hash-b"]);
    });

    it("the same name from a different source is a separate row", async () => {
      const store = await make();
      await store.recordSnapshot([obs({ sourceKind: "in-process", sourceDetail: "" })]);
      await store.recordSnapshot([obs({ sourceKind: "file", sourceDetail: "/tools/search.ts" })]);
      expect(await store.list({ name: "search" })).toHaveLength(2);
    });

    it("refreshes description on re-record without splitting the row", async () => {
      const store = await make();
      await store.recordSnapshot([obs({ description: "old text" })]);
      await store.recordSnapshot([obs({ description: "new text" })]);
      const rows = await store.list();
      expect(rows).toHaveLength(1);
      expect(rows[0]?.description).toBe("new text");
    });

    it("list filters by name", async () => {
      const store = await make();
      await store.recordSnapshot([obs({ name: "search" }), obs({ name: "fetch" })]);
      expect((await store.list({ name: "fetch" })).map((r) => r.name)).toEqual(["fetch"]);
    });

    it("list filters by source kind", async () => {
      const store = await make();
      await store.recordSnapshot([
        obs({ name: "search", sourceKind: "in-process" }),
        obs({ name: "fetch", sourceKind: "mcp", sourceDetail: "web-srv" }),
      ]);
      expect((await store.list({ sourceKind: "mcp" })).map((r) => r.name)).toEqual(["fetch"]);
    });

    it("list filters by since (lastSeenAt >= since)", async () => {
      const store = await make();
      await store.recordSnapshot([obs()]);
      // since 0 sees everything; since the far future sees nothing.
      expect(await store.list({ since: 0 })).toHaveLength(1);
      expect(await store.list({ since: Number.MAX_SAFE_INTEGER })).toEqual([]);
    });

    it("an empty snapshot is a no-op", async () => {
      const store = await make();
      await store.recordSnapshot([]);
      expect(await store.list()).toEqual([]);
    });
  });
}
