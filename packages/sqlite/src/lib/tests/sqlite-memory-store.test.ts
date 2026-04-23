import { describe, it, expect } from "bun:test";
import { Database } from "bun:sqlite";
import { memoryStoreTestSuite } from "@promin/agent/testing";
import { SqliteMemoryStore } from "../sqlite-memory-store.ts";

function makeStore() {
  return SqliteMemoryStore.make({ db: new Database(":memory:") });
}

// ---- conformance suite ----

memoryStoreTestSuite(makeStore);

// ---- SQLite-specific tests ----

describe("SqliteMemoryStore", () => {
  it("persists across instances sharing the same db", async () => {
    const db = new Database(":memory:");
    const s1 = SqliteMemoryStore.make({ db });
    const id = await s1.save({ content: "persisted across instances" });

    const s2 = SqliteMemoryStore.make({ db });
    const entries = await s2.list();
    expect(entries.find((e) => e.id === id)).toBeDefined();
  });

  it("custom table name avoids conflicts", async () => {
    const db = new Database(":memory:");
    const a = SqliteMemoryStore.make({ db, table: "mem_a" });
    const b = SqliteMemoryStore.make({ db, table: "mem_b" });

    await a.save({ content: "only in a" });
    expect(await a.list()).toHaveLength(1);
    expect(await b.list()).toHaveLength(0);
  });

  // ---- namespace option ----

  it("store-level namespace isolates entries from unscoped stores", async () => {
    const db = new Database(":memory:");
    const global = SqliteMemoryStore.make({ db });
    const ns = SqliteMemoryStore.make({ db, namespace: "agent-1" });

    await global.save({ content: "global entry" });
    await ns.save({ content: "agent-1 entry" });

    expect(await global.list()).toHaveLength(1);
    expect((await global.list())[0]!.content).toBe("global entry");

    expect(await ns.list()).toHaveLength(1);
    expect((await ns.list())[0]!.content).toBe("agent-1 entry");
  });

  it("two stores with different namespaces share one table without interference", async () => {
    const db = new Database(":memory:");
    const a = SqliteMemoryStore.make({ db, namespace: "agent-a" });
    const b = SqliteMemoryStore.make({ db, namespace: "agent-b" });

    await a.save({ content: "memory A" });
    await b.save({ content: "memory B" });

    const aEntries = await a.list();
    const bEntries = await b.list();
    expect(aEntries).toHaveLength(1);
    expect(aEntries[0]!.content).toBe("memory A");
    expect(bEntries).toHaveLength(1);
    expect(bEntries[0]!.content).toBe("memory B");
  });

  it("explicit per-call scope overrides store-level namespace", async () => {
    const db = new Database(":memory:");
    const s = SqliteMemoryStore.make({ db, namespace: "agent-1" });

    await s.save({ content: "default ns entry" });
    await s.save({ content: "override ns entry" }, { namespaceId: "agent-2" });

    const ns1 = await s.list();
    expect(ns1).toHaveLength(1);
    expect(ns1[0]!.content).toBe("default ns entry");

    const ns2 = await s.list(undefined, { namespaceId: "agent-2" });
    expect(ns2).toHaveLength(1);
    expect(ns2[0]!.content).toBe("override ns entry");
  });

  it("store-level namespace applies to search", async () => {
    const db = new Database(":memory:");
    const a = SqliteMemoryStore.make({ db, namespace: "ns-a" });
    const b = SqliteMemoryStore.make({ db, namespace: "ns-b" });

    await a.save({ content: "capital of France is Paris" });
    await b.save({ content: "capital of Germany is Berlin" });

    const resultsA = await a.search("capital France");
    expect(resultsA).toHaveLength(1);
    expect(resultsA[0]!.content).toContain("France");

    const resultsB = await b.search("Paris France");
    expect(resultsB).toHaveLength(0);
  });

  it("sessionId still works within a store-level namespace", async () => {
    const db = new Database(":memory:");
    const s = SqliteMemoryStore.make({ db, namespace: "agent-1" });

    await s.save({ content: "session-1 memory" }, { sessionId: "s1" });
    await s.save({ content: "session-2 memory" }, { sessionId: "s2" });

    const s1 = await s.list(undefined, { sessionId: "s1" });
    expect(s1).toHaveLength(1);
    expect(s1[0]!.content).toBe("session-1 memory");
  });
});
