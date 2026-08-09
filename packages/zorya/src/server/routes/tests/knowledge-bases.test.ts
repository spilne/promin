import { describe, expect, it } from "bun:test";
import { createInMemoryRetriever, InMemoryRetrieverRegistry } from "@promin/agent";
import { listKnowledgeBases, searchKnowledgeBase } from "../knowledge-bases.ts";

function registry() {
  return new InMemoryRetrieverRegistry([
    {
      id: "handbook",
      description: "Engineering handbook",
      tags: ["internal"],
      metadata: { owner: "platform" },
      retriever: createInMemoryRetriever({
        documents: [
          {
            id: "doc-1",
            title: "Deployments",
            text: "Deployments require an approved change and a rollback plan.",
            tags: ["release"],
          },
        ],
      }),
    },
  ]);
}

describe("knowledge-base routes", () => {
  it("lists runtime knowledge bases without exposing retriever objects", async () => {
    const response = await listKnowledgeBases({ retrievers: registry() })();
    expect(response.status).toBe(200);
    const body = (await response.json()) as { knowledgeBases: Array<Record<string, unknown>> };
    expect(body.knowledgeBases).toEqual([
      {
        id: "handbook",
        description: "Engineering handbook",
        tags: ["internal"],
        metadata: { owner: "platform" },
      },
    ]);
    expect(body.knowledgeBases[0]).not.toHaveProperty("retriever");
  });

  it("searches a knowledge base and returns source-bearing chunks", async () => {
    const response = await searchKnowledgeBase({ retrievers: registry() })(
      new Request("http://test/api/knowledge-bases/handbook/search", {
        method: "POST",
        body: JSON.stringify({ query: "rollback plan", topK: 3 }),
      }),
      { id: "handbook" },
    );
    expect(response.status).toBe(200);
    const body = (await response.json()) as { results: Array<Record<string, unknown>> };
    expect(body.results[0]).toMatchObject({
      source: { id: "doc-1", title: "Deployments", tags: ["release"] },
      index: 0,
    });
  });
});
