import { describe, expect, it } from "bun:test";
import {
  FlatTextChunker,
  InMemoryRetriever,
  createRetrieverTool,
  retrieverToolName,
} from "../index.ts";
import type { EmbeddingProvider } from "../../embedding-provider.ts";
import type { RetrieveRequest, Retriever } from "../types.ts";

/** Deterministic fake embedder: "banana" texts point one way, the rest the other. */
const bananaEmbeddings: EmbeddingProvider = {
  embed: async (text) => (text.includes("banana") ? [1, 0] : [0, 1]),
};

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
    expect(chunks[1]?.text.startsWith("licy.")).toBe(true);
  });

  it("splits a paragraph longer than maxSize on a sliding window", () => {
    const chunker = new FlatTextChunker({ maxSize: 10, overlap: 2 });
    const chunks = chunker.chunk({ id: "long", text: "abcdefghijklmnopqrstuvwxyz" });

    expect(chunks.map((c) => c.text)).toEqual(["abcdefghij", "ijklmnopqr", "qrstuvwxyz"]);
    expect(chunks.map((c) => c.index)).toEqual([0, 1, 2]);
  });

  it("rejects an overlap that is not smaller than maxSize", () => {
    expect(() => new FlatTextChunker({ maxSize: 10, overlap: 10 })).toThrow(
      "overlap must be smaller than maxSize",
    );
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

  it("drops results the filter excludes and truncates chunk text", async () => {
    const retriever = new InMemoryRetriever({
      documents: [
        { id: "a", metadata: { tenant: "acme" }, text: "Return unopened items within 30 days." },
        { id: "b", metadata: { tenant: "globex" }, text: "Return items any time." },
      ],
    });

    const results = await retriever.retrieve({
      query: "return items",
      filter: { metadata: { tenant: "acme" } },
      maxChunkCharacters: 6,
    });

    expect(results.map((r) => r.chunk.text)).toEqual(["Return"]);
  });

  it("uses embeddings when provided", async () => {
    const retriever = new InMemoryRetriever({ embeddings: bananaEmbeddings });
    await retriever.addDocument({ id: "fruit", text: "banana bread recipe" });
    await retriever.addDocument({ id: "ops", text: "database migration checklist" });

    const results = await retriever.retrieve({ query: "banana", topK: 1 });

    expect(results[0]?.chunk.source.id).toBe("fruit");
    expect(results[0]?.score).toBe(1);
  });

  it("embeds constructor-loaded documents lazily", async () => {
    const retriever = new InMemoryRetriever({
      embeddings: bananaEmbeddings,
      documents: [
        { id: "fruit", text: "banana bread recipe" },
        { id: "ops", text: "database migration checklist" },
      ],
    });

    const results = await retriever.retrieve({ query: "banana", topK: 1 });

    expect(results[0]?.chunk.source.id).toBe("fruit");
    expect(results[0]?.score).toBe(1);
  });

  it("deletes every chunk of a source", async () => {
    const retriever = new InMemoryRetriever({
      documents: [
        { id: "returns", text: "Return unopened items within 30 days." },
        { id: "refunds", text: "Refund returns to the original card." },
      ],
    });

    await retriever.deleteSource("returns");
    const results = await retriever.retrieve({ query: "returns" });

    expect(results.map((r) => r.chunk.source.id)).toEqual(["refunds"]);
  });
});

describe("createRetrieverTool", () => {
  const runbook = () =>
    new InMemoryRetriever({
      documents: [
        {
          id: "kb-1",
          title: "Runbook",
          uri: "https://example.test/runbook",
          text: "Restart workers when heartbeat lag exceeds the threshold.",
        },
      ],
    });

  it("wraps retrieval as a source-bearing agent tool", async () => {
    const tool = createRetrieverTool({ retriever: runbook(), includeScores: true });

    const output = await tool.execute({ query: "heartbeat lag" });
    const modelText = tool.toModelOutput?.(output);
    const metadata = tool.toResultMetadata?.(output);

    expect(tool.name).toBe("search_knowledge_base");
    expect(output.results[0]?.source?.title).toBe("Runbook");
    expect(modelText).toContain("source=Runbook (https://example.test/runbook)");
    expect(modelText).toContain("score=");
    expect(metadata?.sources).toEqual([
      { id: "kb-1", title: "Runbook", uri: "https://example.test/runbook" },
    ]);
  });

  it("hides sources when includeSources is false", async () => {
    const tool = createRetrieverTool({ retriever: runbook(), includeSources: false });

    const output = await tool.execute({ query: "heartbeat lag" });

    expect(output.results[0]?.source).toBeUndefined();
    expect(tool.toModelOutput?.(output)).toContain("source=hidden");
    expect(tool.toResultMetadata?.(output)).toEqual({ sources: [] });
  });

  it("reports an empty search to the model", async () => {
    const tool = createRetrieverTool({ retriever: runbook() });
    const output = await tool.execute({ query: "unrelated payroll" });
    expect(tool.toModelOutput?.(output)).toBe("No relevant knowledge-base results found.");
  });

  it("merges the binding filter over the model's filter", async () => {
    const seen: RetrieveRequest[] = [];
    const spy: Retriever = {
      retrieve: async (request) => {
        seen.push(request);
        return [];
      },
    };
    const tool = createRetrieverTool({
      retriever: spy,
      topK: 3,
      filter: { tags: ["policy"], metadata: { tenant: "acme" } },
    });

    await tool.execute({
      query: "returns",
      filter: { tags: ["faq"], metadata: { tenant: "globex", lang: "en" } },
    });
    await tool.execute({ query: "returns", topK: 7 });

    expect(seen).toEqual([
      {
        query: "returns",
        topK: 3,
        filter: { tags: ["faq", "policy"], metadata: { tenant: "acme", lang: "en" } },
      },
      {
        query: "returns",
        topK: 7,
        filter: { tags: ["policy"], metadata: { tenant: "acme" } },
      },
    ]);
  });

  it("validates model input with its zod schema", () => {
    const tool = createRetrieverTool({ retriever: runbook() });
    expect(tool.parameters.safeParse({ query: "" }).success).toBe(false);
    expect(tool.parameters.safeParse({ query: "x", topK: 51 }).success).toBe(false);
    expect(
      tool.parameters.safeParse({ query: "x", filter: { metadata: { tenant: "acme" } } }).success,
    ).toBe(true);
  });
});

describe("retrieverToolName", () => {
  it("derives search tool names from binding ids", () => {
    expect(retrieverToolName("default")).toBe("search_knowledge_base");
    expect(retrieverToolName("docs")).toBe("search_docs");
    expect(retrieverToolName("org-handbook")).toBe("search_org_handbook");
    expect(retrieverToolName("--")).toBe("search_knowledge");
  });
});
