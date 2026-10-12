// `SqliteKnowledgeBaseStore` — runs the conformance suite plus SQLite-specific
// checks: rows persist across instances sharing one db, claims age on the
// injected clock, and the table prefix is validated before it reaches SQL.

import { describe, expect, it } from "bun:test";
import { Database } from "bun:sqlite";
import { knowledgeBaseStoreTestSuite } from "@promin/agent/testing";
import { FakeWallClock } from "@promin/workflow";
import { SqliteKnowledgeBaseStore } from "../knowledge-base-store.ts";

knowledgeBaseStoreTestSuite({
  factory: () => SqliteKnowledgeBaseStore.make({ db: new Database(":memory:") }),
});

const pending = {
  id: "s",
  tags: [],
  metadata: {},
  text: "t",
  status: "pending" as const,
  chunkCount: 0,
  createdAt: 0,
  updatedAt: 0,
};

describe("SqliteKnowledgeBaseStore", () => {
  it("rows persist across instances sharing one db", async () => {
    const db = new Database(":memory:");
    const writer = SqliteKnowledgeBaseStore.make({ db });
    await writer.saveBase({
      namespaceId: "acme",
      id: "docs",
      tags: [],
      metadata: {},
      createdAt: 1,
      updatedAt: 1,
    });
    await writer.saveSource({
      namespaceId: "acme",
      baseId: "docs",
      source: { ...pending, id: "runbook", text: "Restart workers.", status: "ready" },
    });

    const reader = SqliteKnowledgeBaseStore.make({ db });
    expect(
      (await reader.getSource({ namespaceId: "acme", baseId: "docs", sourceId: "runbook" }))!.text,
    ).toBe("Restart workers.");
  });

  it("ages claims on its clock", async () => {
    const clock = FakeWallClock.create(10_000);
    const store = SqliteKnowledgeBaseStore.make({ db: new Database(":memory:"), clock });
    await store.saveSource({ namespaceId: "acme", baseId: "docs", source: pending });
    await store.claimSource({ workerId: "w1", staleAfterMs: 1_000 });
    clock.advance(1_000);
    expect(await store.claimSource({ workerId: "w2", staleAfterMs: 1_000 })).toBeNull();
    clock.advance(1);
    expect((await store.claimSource({ workerId: "w2", staleAfterMs: 1_000 }))?.source.id).toBe("s");
  });

  it("keeps stores with different table prefixes apart", async () => {
    const db = new Database(":memory:");
    const a = SqliteKnowledgeBaseStore.make({ db, tablePrefix: "kb_a" });
    const b = SqliteKnowledgeBaseStore.make({ db, tablePrefix: "kb_b" });
    await a.saveBase({
      namespaceId: "acme",
      id: "docs",
      tags: [],
      metadata: {},
      createdAt: 1,
      updatedAt: 1,
    });
    expect(await b.listBases()).toEqual([]);
  });

  it("rejects a table prefix that is not a plain identifier", () => {
    expect(() =>
      SqliteKnowledgeBaseStore.make({ db: new Database(":memory:"), tablePrefix: "x; DROP" }),
    ).toThrow("invalid tablePrefix");
  });
});
