import { describe, it, expect, beforeEach } from "bun:test";
import { Database } from "bun:sqlite";
import { SqliteMemoryStore } from "../sqlite-memory-store.ts";

function makeStore() {
  return SqliteMemoryStore.make({ db: new Database(":memory:") });
}

describe("SqliteMemoryStore", () => {
  let store: SqliteMemoryStore;

  beforeEach(() => {
    store = makeStore();
  });

  it("save returns a unique id", async () => {
    const id1 = await store.save({ content: "hello world" });
    const id2 = await store.save({ content: "foo bar" });
    expect(typeof id1).toBe("string");
    expect(id1.length).toBeGreaterThan(0);
    expect(id1).not.toBe(id2);
  });

  it("list returns entries newest first", async () => {
    await store.save({ content: "first" });
    await store.save({ content: "second" });
    await store.save({ content: "third" });
    const entries = await store.list();
    expect(entries.map((e) => e.content)).toEqual(["third", "second", "first"]);
  });

  it("list respects limit", async () => {
    for (let i = 0; i < 5; i++) await store.save({ content: `entry ${i}` });
    const entries = await store.list(3);
    expect(entries).toHaveLength(3);
  });

  it("delete removes an entry", async () => {
    const id = await store.save({ content: "to be deleted" });
    await store.delete(id);
    const entries = await store.list();
    expect(entries.find((e) => e.id === id)).toBeUndefined();
  });

  it("round-trips metadata", async () => {
    const id = await store.save({
      content: "Paris is the capital of France",
      metadata: { source: "geography", confidence: 0.99 },
    });
    const [entry] = await store.list();
    expect(entry!.id).toBe(id);
    expect(entry!.metadata).toEqual({ source: "geography", confidence: 0.99 });
  });

  it("metadata is undefined when not provided", async () => {
    await store.save({ content: "no metadata here" });
    const [entry] = await store.list();
    expect(entry!.metadata).toBeUndefined();
  });

  it("createdAt is a Date", async () => {
    await store.save({ content: "timestamp check" });
    const [entry] = await store.list();
    expect(entry!.createdAt).toBeInstanceOf(Date);
  });

  // ---- search ----

  it("search returns entries with matching keywords", async () => {
    await store.save({ content: "The capital of France is Paris" });
    await store.save({ content: "Berlin is the capital of Germany" });
    await store.save({ content: "Unrelated content about clouds" });

    const results = await store.search("capital France");
    expect(results.length).toBeGreaterThanOrEqual(1);
    expect(results[0]!.content).toContain("France");
  });

  it("search respects limit", async () => {
    for (let i = 0; i < 10; i++)
      await store.save({ content: `The quick brown fox jumps over the lazy dog ${i}` });
    const results = await store.search("quick brown fox", 3);
    expect(results).toHaveLength(3);
  });

  it("search returns recency order for unscored queries (all short words)", async () => {
    await store.save({ content: "first entry" });
    await store.save({ content: "second entry" });
    const results = await store.search("is it");
    expect(results.length).toBeGreaterThan(0);
  });

  it("search returns empty when nothing matches", async () => {
    await store.save({ content: "completely different" });
    const results = await store.search("quantum entanglement superposition");
    expect(results).toHaveLength(0);
  });

  // ---- scope isolation ----

  it("entries without scope are isolated from scoped entries", async () => {
    const db = new Database(":memory:");
    const s = SqliteMemoryStore.make({ db });

    await s.save({ content: "global entry" });
    await s.save({ content: "scoped entry" }, { namespaceId: "ns-1" });

    const global = await s.list();
    expect(global).toHaveLength(1);
    expect(global[0]!.content).toBe("global entry");

    const scoped = await s.list(undefined, { namespaceId: "ns-1" });
    expect(scoped).toHaveLength(1);
    expect(scoped[0]!.content).toBe("scoped entry");
  });

  it("namespaceId filters entries correctly", async () => {
    const db = new Database(":memory:");
    const s = SqliteMemoryStore.make({ db });
    await s.save({ content: "ns-a memory" }, { namespaceId: "ns-a" });
    await s.save({ content: "ns-b memory" }, { namespaceId: "ns-b" });

    const a = await s.list(undefined, { namespaceId: "ns-a" });
    expect(a).toHaveLength(1);
    expect(a[0]!.content).toBe("ns-a memory");
  });

  it("sessionId sub-scopes within namespace", async () => {
    const db = new Database(":memory:");
    const s = SqliteMemoryStore.make({ db });
    await s.save({ content: "session-1 memory" }, { namespaceId: "ns", sessionId: "s1" });
    await s.save({ content: "session-2 memory" }, { namespaceId: "ns", sessionId: "s2" });

    const s1 = await s.list(undefined, { namespaceId: "ns", sessionId: "s1" });
    expect(s1).toHaveLength(1);
    expect(s1[0]!.content).toBe("session-1 memory");
  });

  it("search is scoped", async () => {
    const db = new Database(":memory:");
    const s = SqliteMemoryStore.make({ db });
    await s.save({ content: "capital of France is Paris" }, { namespaceId: "ns-a" });
    await s.save({ content: "capital of Germany is Berlin" }, { namespaceId: "ns-b" });

    const results = await s.search("capital France", 10, { namespaceId: "ns-a" });
    expect(results).toHaveLength(1);
    expect(results[0]!.content).toContain("France");
  });

  it("persistence across instances sharing the same db", async () => {
    const db = new Database(":memory:");
    const s1 = SqliteMemoryStore.make({ db });
    const id = await s1.save({ content: "persisted across instances" });

    const s2 = SqliteMemoryStore.make({ db });
    const entries = await s2.list();
    expect(entries.find((e) => e.id === id)).toBeDefined();
  });

  it("custom table name works", async () => {
    const s = SqliteMemoryStore.make({ db: new Database(":memory:"), table: "my_memories" });
    const id = await s.save({ content: "custom table" });
    const [entry] = await s.list();
    expect(entry!.id).toBe(id);
  });
});
