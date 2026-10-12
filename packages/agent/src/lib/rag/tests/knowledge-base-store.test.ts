import { describe, expect, it } from "bun:test";
import { FakeWallClock } from "@promin/workflow";
import { InMemoryKnowledgeBaseStore } from "../knowledge-base-store.ts";
import { knowledgeBaseStoreTestSuite } from "../knowledge-base-store-test-suite.ts";

knowledgeBaseStoreTestSuite({ factory: () => new InMemoryKnowledgeBaseStore() });

describe("InMemoryKnowledgeBaseStore", () => {
  it("hands out copies, so callers cannot mutate stored rows", async () => {
    const store = new InMemoryKnowledgeBaseStore();
    const tags = ["a"];
    await store.saveBase({
      namespaceId: "acme",
      id: "docs",
      tags,
      metadata: {},
      createdAt: 1,
      updatedAt: 1,
    });
    tags.push("mutated");
    const [row] = await store.listBases();
    expect(row!.base.tags).toEqual(["a"]);
  });

  it("measures claim age on its clock", async () => {
    const clock = FakeWallClock.create(0);
    const store = new InMemoryKnowledgeBaseStore({ clock });
    await store.saveSource({
      namespaceId: "acme",
      baseId: "docs",
      source: {
        id: "s",
        tags: [],
        metadata: {},
        text: "t",
        status: "pending",
        chunkCount: 0,
        createdAt: 0,
        updatedAt: 0,
      },
    });
    await store.claimSource({ workerId: "w1", staleAfterMs: 1_000 });
    clock.advance(1_000);
    expect(await store.claimSource({ workerId: "w2", staleAfterMs: 1_000 })).toBeNull();
    clock.advance(1);
    expect((await store.claimSource({ workerId: "w2", staleAfterMs: 1_000 }))?.source.id).toBe("s");
  });

  it("a heartbeat restarts the claim age", async () => {
    const clock = FakeWallClock.create(0);
    const store = new InMemoryKnowledgeBaseStore({ clock });
    await store.saveSource({
      namespaceId: "acme",
      baseId: "docs",
      source: {
        id: "s",
        tags: [],
        metadata: {},
        text: "t",
        status: "pending",
        chunkCount: 0,
        createdAt: 0,
        updatedAt: 0,
      },
    });
    await store.claimSource({ workerId: "w1", staleAfterMs: 1_000 });
    clock.advance(900);
    expect(
      await store.heartbeatClaim({
        namespaceId: "acme",
        baseId: "docs",
        sourceId: "s",
        workerId: "w1",
      }),
    ).toBe(true);
    clock.advance(900);
    expect(await store.claimSource({ workerId: "w2", staleAfterMs: 1_000 })).toBeNull();
  });
});
