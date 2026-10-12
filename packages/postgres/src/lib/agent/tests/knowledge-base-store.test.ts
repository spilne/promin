// PostgresKnowledgeBaseStore — runs the conformance suite against a real
// Postgres container, plus cross-instance checks.

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

knowledgeBaseStoreTestSuite({ factory: () => new PostgresKnowledgeBaseStore({ db: pg.db }) });

describe("PostgresKnowledgeBaseStore — Postgres-specific", () => {
  it("two stores against the same db see each other's writes", async () => {
    const writer = new PostgresKnowledgeBaseStore({ db: pg.db });
    const reader = new PostgresKnowledgeBaseStore({ db: pg.db });
    await writer.saveBase({
      namespaceId: "acme",
      id: "docs",
      tags: ["a"],
      metadata: {},
      createdAt: 1,
      updatedAt: 2,
    });
    await writer.saveSource({
      namespaceId: "acme",
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
    const [row] = await reader.listBases();
    expect(row!.base.tags).toEqual(["a"]);
    expect(row!.sources[0]!.metadata).toEqual({ v: 1 });
  });

  it("stamps claims on the database clock", async () => {
    const store = new PostgresKnowledgeBaseStore({ db: pg.db });
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
        // An app timestamp far in the past must not make the claim look stale.
        createdAt: 1,
        updatedAt: 1,
      },
    });
    await store.claimSource({ workerId: "w1", staleAfterMs: 60_000 });
    const [row] = (await pg.sql`
      SELECT claimed_by, claimed_at,
             (extract(epoch from clock_timestamp()) * 1000)::bigint AS now_ms
      FROM knowledge_base_source
    `) as unknown as Array<{ claimed_by: string; claimed_at: string; now_ms: string }>;
    expect(row!.claimed_by).toBe("w1");
    expect(Number(row!.now_ms) - Number(row!.claimed_at)).toBeLessThan(60_000);
    expect(await store.claimSource({ workerId: "w2", staleAfterMs: 60_000 })).toBeNull();
  });
});
