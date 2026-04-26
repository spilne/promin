import { describe, it, expect } from "bun:test";
import { InMemoryMemoryIndex } from "../memory-index.ts";

describe("MemoryIndex scoping", () => {
  it("unscoped save is only visible in unscoped search", async () => {
    const store = new InMemoryMemoryIndex();
    await store.save({ content: "global fact" });

    const global = await store.list();
    const scoped = await store.list(undefined, { namespaceId: "user1" });

    expect(global).toHaveLength(1);
    expect(scoped).toHaveLength(0);
  });

  it("scoped save is only visible when searching with matching scope", async () => {
    const store = new InMemoryMemoryIndex();
    await store.save({ content: "user1 fact" }, { namespaceId: "user1" });

    const forUser1 = await store.list(undefined, { namespaceId: "user1" });
    const forUser2 = await store.list(undefined, { namespaceId: "user2" });
    const global = await store.list();

    expect(forUser1).toHaveLength(1);
    expect(forUser2).toHaveLength(0);
    expect(global).toHaveLength(0);
  });

  it("two users cannot see each other's memories", async () => {
    const store = new InMemoryMemoryIndex();
    await store.save({ content: "alice memory" }, { namespaceId: "alice" });
    await store.save({ content: "bob memory" }, { namespaceId: "bob" });

    const alice = await store.list(undefined, { namespaceId: "alice" });
    const bob = await store.list(undefined, { namespaceId: "bob" });

    expect(alice).toHaveLength(1);
    expect(alice[0]!.content).toBe("alice memory");
    expect(bob).toHaveLength(1);
    expect(bob[0]!.content).toBe("bob memory");
  });

  it("sessionId scoping isolates sessions within a user", async () => {
    const store = new InMemoryMemoryIndex();
    await store.save({ content: "session1 memory" }, { namespaceId: "user1", sessionId: "s1" });
    await store.save({ content: "session2 memory" }, { namespaceId: "user1", sessionId: "s2" });

    const s1 = await store.list(undefined, { namespaceId: "user1", sessionId: "s1" });
    const s2 = await store.list(undefined, { namespaceId: "user1", sessionId: "s2" });
    const allUser1 = await store.list(undefined, { namespaceId: "user1" });

    expect(s1).toHaveLength(1);
    expect(s1[0]!.content).toBe("session1 memory");
    expect(s2).toHaveLength(1);
    expect(s2[0]!.content).toBe("session2 memory");
    // namespaceId-only query returns all threads for that user
    expect(allUser1).toHaveLength(2);
  });

  it("search respects scope", async () => {
    const store = new InMemoryMemoryIndex();
    await store.save({ content: "typescript rocks" }, { namespaceId: "user1" });
    await store.save({ content: "typescript rules" }, { namespaceId: "user2" });

    const user1Results = await store.search("typescript", 5, { namespaceId: "user1" });
    const user2Results = await store.search("typescript", 5, { namespaceId: "user2" });

    expect(user1Results).toHaveLength(1);
    expect(user1Results[0]!.content).toBe("typescript rocks");
    expect(user2Results).toHaveLength(1);
    expect(user2Results[0]!.content).toBe("typescript rules");
  });

  it("omitting scope behaves identically to before scoping", async () => {
    const store = new InMemoryMemoryIndex();
    await store.save({ content: "plain memory" });
    await store.save({ content: "scoped memory" }, { namespaceId: "user1" });

    const all = await store.list();
    // Only global (unscoped) entries returned
    expect(all).toHaveLength(1);
    expect(all[0]!.content).toBe("plain memory");
  });

  it("delete removes entry regardless of scope", async () => {
    const store = new InMemoryMemoryIndex();
    const id = await store.save({ content: "to delete" }, { namespaceId: "user1" });

    await store.delete(id);

    const results = await store.list(undefined, { namespaceId: "user1" });
    expect(results).toHaveLength(0);
  });
});
