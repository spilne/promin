// ---------------------------------------------------------------------------
// PgVectorRetriever — runs against a real Postgres with the pgvector
// extension (the stock `postgres:17-alpine` test image does not ship it).
// Embeddings come from a deterministic fake keyed on words in the text, so
// vector ordering is predictable and no network is involved.
// ---------------------------------------------------------------------------

import { afterAll, beforeAll, beforeEach, describe, expect, it } from "bun:test";
import type { EmbeddingProvider } from "@promin/agent";
import { FakeWallClock } from "@promin/workflow";
import { PgVectorRetriever } from "../pgvector-retriever.ts";
import { PostgresTestContainer } from "../../test-utils.ts";

const pg = new PostgresTestContainer({ image: "pgvector/pgvector:pg17" });

/** 3-d fake embedder: one axis per topic word. */
const topicEmbeddings: EmbeddingProvider = {
  embed: async (text) => {
    const t = text.toLowerCase();
    return [
      t.includes("return") ? 1 : 0.01,
      t.includes("security") ? 1 : 0.01,
      t.includes("deploy") ? 1 : 0.01,
    ];
  },
};

function makeRetriever(params: { tableName?: string; clock?: FakeWallClock } = {}) {
  return new PgVectorRetriever({
    db: pg.db,
    embeddings: topicEmbeddings,
    dimensions: 3,
    ...(params.tableName !== undefined && { tableName: params.tableName }),
    ...(params.clock !== undefined && { clock: params.clock }),
  });
}

beforeAll(async () => {
  await pg.start();
  await makeRetriever().ensureSchema({ createExtension: true });
}, 120_000);

afterAll(async () => {
  await pg.stop();
});

beforeEach(async () => {
  await pg.sql`TRUNCATE agent_knowledge_chunk`;
});

describe("PgVectorRetriever", () => {
  it("ingests documents and ranks chunks by cosine similarity", async () => {
    const clock = FakeWallClock.create(1_000);
    const retriever = makeRetriever({ clock });
    await retriever.addDocument({
      id: "returns",
      title: "Return Policy",
      uri: "https://example.test/returns",
      mimeType: "text/markdown",
      tags: ["policy"],
      metadata: { tenant: "acme" },
      text: "Return unopened items within 30 days.",
    });
    await retriever.addDocument({
      id: "security",
      title: "Security",
      tags: ["engineering"],
      text: "Security credentials must use scoped secrets.",
    });

    const results = await retriever.retrieve({ query: "how do returns work", topK: 2 });

    expect(results.map((r) => r.chunk.source.id)).toEqual(["returns", "security"]);
    expect(results[0]!.score).toBeGreaterThan(0.99);
    expect(results[0]!.score).toBeGreaterThan(results[1]!.score);
    expect(results[0]!.chunk).toEqual({
      id: "returns#0",
      text: "Return unopened items within 30 days.",
      index: 0,
      source: {
        id: "returns",
        title: "Return Policy",
        uri: "https://example.test/returns",
        mimeType: "text/markdown",
        tags: ["policy"],
        metadata: { tenant: "acme" },
      },
      metadata: {},
    });
    const [row] =
      await pg.sql`SELECT created_at, updated_at FROM agent_knowledge_chunk WHERE id = 'returns#0'`;
    expect(Number(row!.created_at)).toBe(1_000);
  });

  it("filters by tags and by merged source + chunk metadata", async () => {
    const retriever = makeRetriever();
    await retriever.addChunks([
      {
        id: "a",
        text: "Return policy for acme EU",
        index: 0,
        source: { id: "acme-policy", tags: ["policy", "eu"], metadata: { tenant: "acme" } },
        metadata: { section: "returns" },
      },
      {
        id: "b",
        text: "Return policy for globex",
        index: 0,
        source: { id: "globex-policy", tags: ["policy"], metadata: { tenant: "globex" } },
      },
    ]);

    const byTag = await retriever.retrieve({ query: "return", filter: { tags: ["policy", "eu"] } });
    const byMetadata = await retriever.retrieve({
      query: "return",
      filter: { metadata: { tenant: "acme", section: "returns" } },
    });
    const none = await retriever.retrieve({
      query: "return",
      filter: { metadata: { tenant: "initech" } },
    });

    expect(byTag.map((r) => r.chunk.id)).toEqual(["a"]);
    expect(byMetadata.map((r) => r.chunk.id)).toEqual(["a"]);
    expect(none).toEqual([]);
  });

  it("truncates returned chunk text and honours topK", async () => {
    const retriever = makeRetriever();
    await retriever.addDocument({ id: "r1", text: "Return window is 30 days." });
    await retriever.addDocument({ id: "r2", text: "Return shipping is free." });

    const results = await retriever.retrieve({
      query: "return",
      topK: 1,
      maxChunkCharacters: 6,
    });

    expect(results).toHaveLength(1);
    expect(results[0]!.chunk.text).toBe("Return");
  });

  it("upserts chunks by id and deletes a whole source", async () => {
    const retriever = makeRetriever();
    await retriever.addDocument({ id: "doc", text: "Deploy with the blue/green script." });
    await retriever.addDocument({ id: "doc", text: "Deploy with the canary script." });
    await retriever.addDocument({ id: "other", text: "Deploy checklist." });

    const before = await retriever.retrieve({ query: "deploy", topK: 10 });
    expect(before.map((r) => r.chunk.text).sort()).toEqual([
      "Deploy checklist.",
      "Deploy with the canary script.",
    ]);

    await retriever.deleteSource("doc");
    const after = await retriever.retrieve({ query: "deploy", topK: 10 });
    expect(after.map((r) => r.chunk.source.id)).toEqual(["other"]);
  });

  it("supports schema-qualified tables and ivfflat indexes", async () => {
    await pg.sql`CREATE SCHEMA IF NOT EXISTS kb`;
    const retriever = makeRetriever({ tableName: "kb.chunks" });
    await retriever.ensureSchema({ index: "ivfflat" });
    await retriever.ensureSchema({ index: "ivfflat" });
    await retriever.addDocument({ id: "sec", text: "Security review before launch." });

    const results = await retriever.retrieve({ query: "security" });

    expect(results.map((r) => r.chunk.source.id)).toEqual(["sec"]);
    const indexes = await pg.sql`SELECT indexname FROM pg_indexes WHERE schemaname = 'kb'`;
    expect(indexes.map((i) => i.indexname)).toContain("kb_chunks_embedding_ivfflat_idx");
  });

  it("returns nothing for a blank query without embedding it", async () => {
    let calls = 0;
    const retriever = new PgVectorRetriever({
      db: pg.db,
      dimensions: 3,
      embeddings: {
        embed: async () => {
          calls += 1;
          return [1, 0, 0];
        },
      },
    });
    expect(await retriever.retrieve({ query: "   " })).toEqual([]);
    expect(calls).toBe(0);
  });

  it("validates embedding dimensions and values", async () => {
    const wrongSize = new PgVectorRetriever({
      db: pg.db,
      dimensions: 3,
      embeddings: { embed: async () => [1, 0] },
    });
    const notFinite = new PgVectorRetriever({
      db: pg.db,
      dimensions: 3,
      embeddings: { embed: async () => [1, Number.NaN, 0] },
    });

    await expect(wrongSize.retrieve({ query: "anything" })).rejects.toThrow(
      "embedding dimension mismatch",
    );
    await expect(notFinite.addDocument({ id: "x", text: "x" })).rejects.toThrow("finite numbers");
  });

  it("rejects unsafe table names and bad dimensions", () => {
    const build = (tableName: string) => () => makeRetriever({ tableName });

    expect(build("agent_knowledge_chunk; drop table users")).toThrow("unsafe SQL identifier");
    expect(build('chunks"')).toThrow("unsafe SQL identifier");
    expect(build("a.b.c")).toThrow("unsafe SQL identifier");
    expect(
      () => new PgVectorRetriever({ db: pg.db, dimensions: 0, embeddings: topicEmbeddings }),
    ).toThrow("dimensions must be a positive integer");
  });
});
