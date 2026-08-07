import { describe, expect, it } from "bun:test";
import type { EmbeddingProvider } from "@promin/agent";
import type { DrizzleDb } from "../../drizzle-db.ts";
import { PgVectorRetriever } from "../pgvector-retriever.ts";

class FakeDb {
  readonly calls: unknown[] = [];
  private readonly results: unknown[][];

  constructor(results: unknown[][] = []) {
    this.results = results;
  }

  async execute(query: unknown): Promise<unknown[]> {
    this.calls.push(query);
    return this.results.shift() ?? [];
  }

  asDrizzle(): DrizzleDb {
    return this as unknown as DrizzleDb;
  }
}

describe("PgVectorRetriever", () => {
  it("ingests chunked documents with embeddings", async () => {
    const embedded: string[] = [];
    const embeddings: EmbeddingProvider = {
      embed: async (text) => {
        embedded.push(text);
        return [1, 0, 0];
      },
    };
    const db = new FakeDb();
    const retriever = new PgVectorRetriever({
      db: db.asDrizzle(),
      embeddings,
      dimensions: 3,
      now: () => 123,
    });

    const chunks = await retriever.addDocument({
      id: "handbook",
      title: "Handbook",
      tags: ["policy"],
      metadata: { tenant: "acme" },
      text: "Return policy.\n\nSecurity policy.",
    });

    expect(chunks).toHaveLength(1);
    expect(embedded).toEqual(["Return policy.\n\nSecurity policy."]);
    expect(db.calls).toHaveLength(1);
  });

  it("maps source-bearing vector search results", async () => {
    const embeddings: EmbeddingProvider = { embed: async () => [1, 0, 0] };
    const db = new FakeDb([
      [
        {
          id: "handbook#0",
          text: "Return policy.",
          source_id: "handbook",
          source_title: "Handbook",
          source_uri: "file:///handbook.md",
          source_mime_type: "text/markdown",
          source_tags: ["policy"],
          source_metadata: { tenant: "acme" },
          chunk_index: 0,
          parent_id: null,
          chunk_metadata: { section: "returns" },
          score: "0.91",
        },
      ],
    ]);
    const retriever = new PgVectorRetriever({
      db: db.asDrizzle(),
      embeddings,
      dimensions: 3,
    });

    const results = await retriever.retrieve({
      query: "return",
      topK: 3,
      filter: { tags: ["policy"], metadata: { tenant: "acme" } },
      maxChunkCharacters: 6,
    });

    expect(results).toEqual([
      {
        chunk: {
          id: "handbook#0",
          text: "Return",
          source: {
            id: "handbook",
            title: "Handbook",
            uri: "file:///handbook.md",
            mimeType: "text/markdown",
            tags: ["policy"],
            metadata: { tenant: "acme" },
          },
          index: 0,
          parentId: undefined,
          metadata: { section: "returns" },
        },
        score: 0.91,
      },
    ]);
    expect(db.calls).toHaveLength(1);
  });

  it("validates embedding dimensions", async () => {
    const embeddings: EmbeddingProvider = { embed: async () => [1, 0] };
    const retriever = new PgVectorRetriever({
      db: new FakeDb().asDrizzle(),
      embeddings,
      dimensions: 3,
    });

    await expect(retriever.retrieve({ query: "anything" })).rejects.toThrow(
      "embedding dimension mismatch",
    );
  });

  it("rejects unsafe table names", () => {
    const embeddings: EmbeddingProvider = { embed: async () => [1] };

    expect(
      () =>
        new PgVectorRetriever({
          db: new FakeDb().asDrizzle(),
          embeddings,
          dimensions: 1,
          tableName: "agent_knowledge_chunk; drop table users",
        }),
    ).toThrow("unsafe SQL identifier");
  });
});
