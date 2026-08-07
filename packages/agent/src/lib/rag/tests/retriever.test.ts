import { describe, expect, it } from "bun:test";
import { FlatTextChunker, InMemoryRetriever, createRetrieverTool } from "../index.ts";
import type { EmbeddingProvider } from "../../memory-index.ts";

describe("FlatTextChunker", () => {
  it("chunks text with overlap and source metadata", () => {
    const chunker = new FlatTextChunker({ maxSize: 30, overlap: 5 });
    const chunks = chunker.chunk({
      id: "doc-1",
      title: "Handbook",
      uri: "file:///handbook.md",
      tags: ["hr"],
      text: "Alpha policy.\n\nBeta policy.\n\nGamma policy.",
    });

    expect(chunks.length).toBeGreaterThan(1);
    expect(chunks[0]?.id).toBe("doc-1#0");
    expect(chunks[0]?.source.title).toBe("Handbook");
    expect(chunks[0]?.source.tags).toEqual(["hr"]);
  });
});

describe("InMemoryRetriever", () => {
  it("retrieves keyword matches with source metadata", async () => {
    const retriever = new InMemoryRetriever({
      documents: [
        {
          id: "returns",
          title: "Return Policy",
          uri: "https://example.test/returns",
          tags: ["policy"],
          metadata: { tenant: "acme" },
          text: "Customers may return unopened items within 30 days.",
        },
        {
          id: "security",
          title: "Security",
          tags: ["engineering"],
          text: "Production credentials must use scoped secrets.",
        },
      ],
    });

    const results = await retriever.retrieve({
      query: "return items",
      filter: { tags: ["policy"], metadata: { tenant: "acme" } },
    });

    expect(results).toHaveLength(1);
    expect(results[0]?.chunk.source.id).toBe("returns");
    expect(results[0]?.score).toBeGreaterThan(0);
  });

  it("uses embeddings when provided", async () => {
    const embeddings: EmbeddingProvider = {
      embed: async (text) => (text.includes("banana") ? [1, 0] : [0, 1]),
    };
    const retriever = new InMemoryRetriever({ embeddings });
    await retriever.addDocument({ id: "fruit", text: "banana bread recipe" });
    await retriever.addDocument({ id: "ops", text: "database migration checklist" });

    const results = await retriever.retrieve({ query: "banana", topK: 1 });

    expect(results[0]?.chunk.source.id).toBe("fruit");
    expect(results[0]?.score).toBe(1);
  });

  it("embeds constructor-loaded documents lazily", async () => {
    const embeddings: EmbeddingProvider = {
      embed: async (text) => (text.includes("banana") ? [1, 0] : [0, 1]),
    };
    const retriever = new InMemoryRetriever({
      embeddings,
      documents: [
        { id: "fruit", text: "banana bread recipe" },
        { id: "ops", text: "database migration checklist" },
      ],
    });

    const results = await retriever.retrieve({ query: "banana", topK: 1 });

    expect(results[0]?.chunk.source.id).toBe("fruit");
    expect(results[0]?.score).toBe(1);
  });
});

describe("createRetrieverTool", () => {
  it("wraps retrieval as a source-bearing agent tool", async () => {
    const retriever = new InMemoryRetriever({
      documents: [
        {
          id: "kb-1",
          title: "Runbook",
          uri: "https://example.test/runbook",
          text: "Restart workers when heartbeat lag exceeds the threshold.",
        },
      ],
    });
    const tool = createRetrieverTool({ retriever, includeScores: true });

    const output = await tool.execute({ query: "heartbeat lag" });
    const modelText = tool.toModelOutput?.(output);
    const metadata = tool.toResultMetadata?.(output);

    expect(tool.name).toBe("search_knowledge_base");
    expect(output.results[0]?.source?.title).toBe("Runbook");
    expect(modelText).toContain("source=Runbook");
    expect(modelText).toContain("score=");
    expect(metadata?.sources).toEqual([
      {
        id: "kb-1",
        title: "Runbook",
        uri: "https://example.test/runbook",
        tags: undefined,
        metadata: undefined,
      },
    ]);
  });
});
