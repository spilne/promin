import { describe, expect, it } from "bun:test";
import {
  CombinedRetriever,
  FallbackRetriever,
  FlatTextChunker,
  InMemoryRetriever,
  RerankingRetriever,
  RouterRetriever,
  createRetrieverTool,
  retrieverToolName,
} from "../index.ts";
import type { EmbeddingProvider } from "../../memory-index.ts";
import type { RetrieveRequest, RetrieveResult, Retriever } from "../types.ts";

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

describe("RerankingRetriever", () => {
  it("fetches extra candidates and applies the reranker", async () => {
    let observedTopK: number | undefined;
    const upstream: Retriever = {
      retrieve: async (request: RetrieveRequest) => {
        observedTopK = request.topK;
        return [
          result({ id: "a", text: "alpha", score: 0.2 }),
          result({ id: "b", text: "beta", score: 0.9 }),
          result({ id: "c", text: "gamma", score: 0.1 }),
        ];
      },
    };
    const retriever = new RerankingRetriever({
      retriever: upstream,
      candidateMultiplier: 3,
      rerank: async ({ results }) =>
        [...results].sort((a, b) => a.chunk.id.localeCompare(b.chunk.id)),
    });

    const results = await retriever.retrieve({ query: "anything", topK: 1 });

    expect(observedTopK).toBe(3);
    expect(results.map((r) => r.chunk.id)).toEqual(["a"]);
  });

  it("leaves topK unset upstream when the caller sets none", async () => {
    let observed: RetrieveRequest | undefined;
    const retriever = new RerankingRetriever({
      retriever: {
        retrieve: async (request) => {
          observed = request;
          return [result({ id: "a", text: "alpha", score: 0.2 })];
        },
      },
      rerank: ({ results }) => results,
    });

    expect(await retriever.retrieve({ query: "q" })).toHaveLength(1);
    expect(observed).toEqual({ query: "q" });
  });
});

describe("RouterRetriever", () => {
  it("routes by request tags and merges duplicate chunks by best score", async () => {
    const policy = new StaticRetriever([
      result({ id: "shared", text: "policy shared", score: 0.3 }),
      result({ id: "policy", text: "policy only", score: 0.8 }),
    ]);
    const engineering = new StaticRetriever([
      result({ id: "shared", text: "engineering shared", score: 0.9 }),
      result({ id: "eng", text: "engineering only", score: 0.7 }),
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

  it("prefers routes whose `when` predicate passes", async () => {
    const retriever = new RouterRetriever({
      routes: [
        {
          id: "code",
          retriever: new StaticRetriever([result({ id: "code", text: "code", score: 0.1 })]),
          when: (request) => request.query.includes("stack trace"),
        },
        {
          id: "docs",
          retriever: new StaticRetriever([result({ id: "docs", text: "docs", score: 0.9 })]),
        },
      ],
    });

    const results = await retriever.retrieve({ query: "explain this stack trace" });

    expect(results.map((r) => r.chunk.id)).toEqual(["code"]);
  });

  it("supports custom route selection", async () => {
    const primary = new StaticRetriever([result({ id: "primary", text: "primary", score: 0.5 })]);
    const secondary = new StaticRetriever([
      result({ id: "secondary", text: "secondary", score: 0.6 }),
    ]);
    const retriever = new RouterRetriever({
      routes: [
        { id: "primary", retriever: primary },
        { id: "secondary", retriever: secondary },
      ],
      selectRoutes: ({ routes }) => routes.filter((route) => route.id === "secondary"),
    });

    const results = await retriever.retrieve({ query: "anything" });

    expect(results.map((r) => r.chunk.id)).toEqual(["secondary"]);
  });

  it("requires at least one route", () => {
    expect(() => new RouterRetriever({ routes: [] })).toThrow("at least one route");
  });
});

describe("CombinedRetriever", () => {
  it("queries all sources, applies weights, dedupes chunks, and respects topK", async () => {
    const primary = new StaticRetriever([
      result({ id: "shared", text: "primary shared", score: 0.4 }),
      result({ id: "primary", text: "primary only", score: 0.7 }),
    ]);
    const secondary = new StaticRetriever([
      result({ id: "shared", text: "secondary shared", score: 0.9 }),
      result({ id: "secondary", text: "secondary only", score: 0.8 }),
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

  it("rejects a negative weight", () => {
    expect(
      () =>
        new CombinedRetriever({ sources: [{ retriever: new StaticRetriever([]), weight: -1 }] }),
    ).toThrow("non-negative");
  });
});

describe("FallbackRetriever", () => {
  it("uses the first source that satisfies the result gate", async () => {
    const weak = new StaticRetriever([result({ id: "weak", text: "weak", score: 0.2 })]);
    const strong = new StaticRetriever([result({ id: "strong", text: "strong", score: 0.8 })]);
    const unused = new CountingRetriever([result({ id: "unused", text: "unused", score: 1 })]);
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

  it("returns the last source's results when no source passes", async () => {
    const retriever = new FallbackRetriever({
      sources: [
        { retriever: new StaticRetriever([]) },
        {
          retriever: new StaticRetriever([result({ id: "a", text: "a", score: 0.1 })]),
          minResults: 2,
        },
      ],
    });

    const results = await retriever.retrieve({ query: "q" });

    expect(results.map((r) => r.chunk.id)).toEqual(["a"]);
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

function result(params: { id: string; text: string; score: number }): RetrieveResult {
  const { id, text, score } = params;
  return {
    chunk: { id, text, index: 0, source: { id: `source-${id}` } },
    score,
  };
}
