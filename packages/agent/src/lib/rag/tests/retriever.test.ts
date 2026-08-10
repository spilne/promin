import { describe, expect, it } from "bun:test";
import {
  FlatTextChunker,
  CombinedRetriever,
  FallbackRetriever,
  InMemoryRetriever,
  RerankingRetriever,
  RouterRetriever,
  createRetrieverTool,
} from "../index.ts";
import type { EmbeddingProvider } from "../../memory-index.ts";
import type { RetrieveRequest, RetrieveResult, Retriever } from "../types.ts";

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

describe("CombinedRetriever", () => {
  it("queries all sources, applies weights, dedupes chunks, and respects topK", async () => {
    const primary = new StaticRetriever([
      result("shared", "primary shared", 0.4),
      result("primary", "primary only", 0.7),
    ]);
    const secondary = new StaticRetriever([
      result("shared", "secondary shared", 0.9),
      result("secondary", "secondary only", 0.8),
    ]);
    const retriever = new CombinedRetriever({
      sources: [
        { retriever: primary, weight: 1 },
        { retriever: secondary, weight: 0.5 },
      ],
    });

    const results = await retriever.retrieve({ query: "shared", topK: 3 });

    expect(results.map((r) => r.chunk.id)).toEqual(["primary", "shared", "secondary"]);
    expect(results[1]?.chunk.text).toBe("secondary shared");
    expect(results[1]?.score).toBe(0.45);
  });
});

describe("FallbackRetriever", () => {
  it("uses the first source that satisfies the result gate", async () => {
    const weak = new StaticRetriever([result("weak", "weak", 0.2)]);
    const strong = new StaticRetriever([result("strong", "strong", 0.8)]);
    const unused = new CountingRetriever([result("unused", "unused", 1)]);
    const retriever = new FallbackRetriever({
      sources: [
        { retriever: weak, minScore: 0.5 },
        { retriever: strong, minScore: 0.5 },
        { retriever: unused, minScore: 0.5 },
      ],
    });

    const results = await retriever.retrieve({ query: "policy" });

    expect(results.map((r) => r.chunk.id)).toEqual(["strong"]);
    expect(unused.calls).toBe(0);
  });
});

describe("RerankingRetriever", () => {
  it("fetches extra candidates and applies the reranker", async () => {
    let observedTopK: number | undefined;
    const upstream: Retriever = {
      retrieve: async (request: RetrieveRequest) => {
        observedTopK = request.topK;
        return [result("a", "alpha", 0.2), result("b", "beta", 0.9), result("c", "gamma", 0.1)];
      },
    };
    const retriever = new RerankingRetriever({
      retriever: upstream,
      candidateMultiplier: 3,
      rerank: async (_request, results) =>
        [...results].sort((a, b) => a.chunk.id.localeCompare(b.chunk.id)),
    });

    const results = await retriever.retrieve({ query: "anything", topK: 1 });

    expect(observedTopK).toBe(3);
    expect(results.map((r) => r.chunk.id)).toEqual(["a"]);
  });
});

describe("RouterRetriever", () => {
  it("routes by request tags and merges duplicate chunks by best score", async () => {
    const policy = new StaticRetriever([
      result("shared", "policy shared", 0.3),
      result("policy", "policy only", 0.8),
    ]);
    const engineering = new StaticRetriever([
      result("shared", "engineering shared", 0.9),
      result("eng", "engineering only", 0.7),
    ]);
    const retriever = new RouterRetriever({
      routes: [
        { id: "policy", tags: ["policy"], retriever: policy },
        { id: "engineering", tags: ["engineering"], retriever: engineering },
      ],
    });

    const policyResults = await retriever.retrieve({
      query: "shared",
      filter: { tags: ["policy"] },
      topK: 5,
    });
    const allResults = await retriever.retrieve({ query: "shared", topK: 5 });

    expect(policyResults.map((r) => r.chunk.id)).toEqual(["policy", "shared"]);
    expect(allResults.map((r) => r.chunk.id)).toEqual(["shared", "policy", "eng"]);
    expect(allResults[0]?.score).toBe(0.9);
  });

  it("supports custom route selection", async () => {
    const primary = new StaticRetriever([result("primary", "primary", 0.5)]);
    const secondary = new StaticRetriever([result("secondary", "secondary", 0.6)]);
    const retriever = new RouterRetriever({
      routes: [
        { id: "primary", retriever: primary },
        { id: "secondary", retriever: secondary },
      ],
      selectRoutes: (_request, routes) => routes.filter((route) => route.id === "secondary"),
    });

    const results = await retriever.retrieve({ query: "anything" });

    expect(results.map((r) => r.chunk.id)).toEqual(["secondary"]);
  });
});

class StaticRetriever implements Retriever {
  constructor(private readonly results: ReadonlyArray<RetrieveResult>) {}

  async retrieve(): Promise<RetrieveResult[]> {
    return [...this.results];
  }
}

class CountingRetriever extends StaticRetriever {
  calls = 0;

  override async retrieve(): Promise<RetrieveResult[]> {
    this.calls += 1;
    return super.retrieve();
  }
}

function result(id: string, text: string, score: number): RetrieveResult {
  return {
    chunk: {
      id,
      text,
      index: 0,
      source: { id: `source-${id}` },
    },
    score,
  };
}
