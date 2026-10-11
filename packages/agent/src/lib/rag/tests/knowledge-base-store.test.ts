import { describe, expect, it } from "bun:test";
import { InMemoryKnowledgeBaseStore } from "../knowledge-base-store.ts";
import { knowledgeBaseStoreTestSuite } from "../knowledge-base-store-test-suite.ts";

knowledgeBaseStoreTestSuite(() => new InMemoryKnowledgeBaseStore());

describe("InMemoryKnowledgeBaseStore", () => {
  it("hands out copies, so callers cannot mutate stored rows", async () => {
    const store = new InMemoryKnowledgeBaseStore();
    const tags = ["a"];
    await store.saveBase({ id: "docs", tags, metadata: {}, createdAt: 1, updatedAt: 1 });
    tags.push("mutated");
    const [row] = await store.loadAll();
    expect(row!.base.tags).toEqual(["a"]);
  });
});
