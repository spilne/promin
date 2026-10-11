// `SqliteKnowledgeBaseStore` — runs the conformance suite plus SQLite-specific
// checks: rows persist across instances sharing one db, and the table prefix
// is validated before it reaches SQL.

import { describe, expect, it } from "bun:test";
import { Database } from "bun:sqlite";
import { knowledgeBaseStoreTestSuite } from "@promin/agent/testing";
import { SqliteKnowledgeBaseStore } from "../knowledge-base-store.ts";

knowledgeBaseStoreTestSuite(() => SqliteKnowledgeBaseStore.make({ db: new Database(":memory:") }));

describe("SqliteKnowledgeBaseStore", () => {
  it("rows persist across instances sharing one db", async () => {
    const db = new Database(":memory:");
    const writer = SqliteKnowledgeBaseStore.make({ db });
    await writer.saveBase({ id: "docs", tags: [], metadata: {}, createdAt: 1, updatedAt: 1 });
    await writer.saveSource({
      baseId: "docs",
      source: {
        id: "runbook",
        tags: [],
        metadata: {},
        text: "Restart workers.",
        status: "ready",
        chunkCount: 1,
        createdAt: 1,
        updatedAt: 1,
      },
    });

    const reader = SqliteKnowledgeBaseStore.make({ db });
    const [row] = await reader.loadAll();
    expect(row!.sources[0]!.text).toBe("Restart workers.");
  });

  it("keeps stores with different table prefixes apart", async () => {
    const db = new Database(":memory:");
    const a = SqliteKnowledgeBaseStore.make({ db, tablePrefix: "kb_a" });
    const b = SqliteKnowledgeBaseStore.make({ db, tablePrefix: "kb_b" });
    await a.saveBase({ id: "docs", tags: [], metadata: {}, createdAt: 1, updatedAt: 1 });
    expect(await b.loadAll()).toEqual([]);
  });

  it("rejects a table prefix that is not a plain identifier", () => {
    expect(() =>
      SqliteKnowledgeBaseStore.make({ db: new Database(":memory:"), tablePrefix: "x; DROP" }),
    ).toThrow("invalid tablePrefix");
  });
});
