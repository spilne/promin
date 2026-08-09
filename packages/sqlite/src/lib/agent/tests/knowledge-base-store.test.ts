import { describe, expect, it } from "bun:test";
import { Database } from "bun:sqlite";
import { SqliteKnowledgeBaseStore } from "../knowledge-base-store.ts";

describe("SqliteKnowledgeBaseStore", () => {
  it("round-trips knowledge-base definitions and sources", async () => {
    const store = SqliteKnowledgeBaseStore.make({ db: new Database(":memory:") });
    await store.save({
      definition: {
        id: "docs",
        namespace: "acme",
        description: "Product docs",
        tags: ["internal"],
        metadata: { owner: "platform" },
        provider: "memory",
        status: "ready",
        sourceCount: 1,
        chunkCount: 2,
        createdAt: 1,
        updatedAt: 2,
      },
      sources: [
        {
          id: "runbook",
          title: "Runbook",
          tags: ["ops"],
          metadata: { version: 1 },
          text: "Restart workers.",
          status: "ready",
          chunkCount: 2,
          createdAt: 1,
          updatedAt: 2,
        },
      ],
    });

    const records = await store.load();
    expect(records[0]?.definition.namespace).toBe("acme");
    expect(records[0]?.sources[0]?.title).toBe("Runbook");
    expect(records[0]?.sources[0]?.metadata).toEqual({ version: 1 });

    await store.delete("acme", "docs");
    expect(await store.load()).toEqual([]);
  });
});
