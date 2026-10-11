// PostgresKnowledgeBaseStore — runs the conformance suite against a real
// Postgres container, plus a cross-instance persistence check.

import { afterEach, beforeAll, describe, expect, it } from "bun:test";
import { knowledgeBaseStoreTestSuite } from "@promin/agent/testing";
import { migrate } from "../../migrate.ts";
import { PostgresKnowledgeBaseStore } from "../knowledge-base-store.ts";
import { PostgresTestContainer } from "../../test-utils.ts";

const pg = new PostgresTestContainer();

beforeAll(async () => {
  await pg.start();
  await migrate(pg.db);
}, 60_000);

afterEach(async () => {
  await pg.sql`TRUNCATE knowledge_base, knowledge_base_source`;
});

knowledgeBaseStoreTestSuite(() => new PostgresKnowledgeBaseStore({ db: pg.db }));

describe("PostgresKnowledgeBaseStore — Postgres-specific", () => {
  it("two stores against the same db see each other's writes", async () => {
    const writer = new PostgresKnowledgeBaseStore({ db: pg.db });
    const reader = new PostgresKnowledgeBaseStore({ db: pg.db });
    await writer.saveBase({ id: "docs", tags: ["a"], metadata: {}, createdAt: 1, updatedAt: 2 });
    await writer.saveSource({
      baseId: "docs",
      source: {
        id: "runbook",
        tags: [],
        metadata: { v: 1 },
        text: "Restart workers.",
        status: "ready",
        chunkCount: 1,
        createdAt: 1,
        updatedAt: 1,
      },
    });
    const [row] = await reader.loadAll();
    expect(row!.base.tags).toEqual(["a"]);
    expect(row!.sources[0]!.metadata).toEqual({ v: 1 });
  });
});
