import { describe, expect, it } from "bun:test";
import { createInMemoryRetriever, InMemoryRetrieverRegistry } from "@promin/agent";
import {
  createManagedKnowledgeBase,
  ingestKnowledgeBaseSource,
  listKnowledgeBaseChunks,
  listKnowledgeBaseSources,
  listManagedKnowledgeBases,
  listKnowledgeBases,
  searchKnowledgeBase,
  searchManagedKnowledgeBase,
} from "../knowledge-bases.ts";
import { InMemoryKnowledgeBaseStore, ZoryaKnowledgeBases } from "../../services/knowledge-bases.ts";
import { NamespaceService } from "../../services/namespaces.ts";

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

  it("supports managed CRUD, ingestion, source listing, chunks, and search", async () => {
    const namespaces = new NamespaceService();
    const knowledgeBases = new ZoryaKnowledgeBases();
    const deps = { knowledgeBases, namespaces };

    const created = await createManagedKnowledgeBase(deps)(
      jsonRequest("POST", { id: "docs", description: "Product docs" }),
    );
    expect(created.status).toBe(201);

    const ingested = await ingestKnowledgeBaseSource(deps)(
      jsonRequest("POST", {
        id: "runbook",
        title: "Runbook",
        tags: ["ops"],
        text: "Restart workers when heartbeat lag exceeds the threshold.",
      }),
      { id: "docs" },
    );
    expect(ingested.status).toBe(201);

    const sources = await listKnowledgeBaseSources(deps)(
      new Request("http://test/api/knowledge-bases/docs/sources"),
      { id: "docs" },
    );
    expect((await sources.json()).sources[0]?.id).toBe("runbook");

    const chunks = await listKnowledgeBaseChunks(deps)(
      new Request("http://test/api/knowledge-bases/docs/chunks"),
      { id: "docs" },
    );
    expect((await chunks.json()).chunks[0]?.source.title).toBe("Runbook");

    const search = await searchManagedKnowledgeBase(deps)(
      jsonRequest("POST", { query: "heartbeat lag" }),
      { id: "docs" },
    );
    expect((await search.json()).results[0]?.source.id).toBe("runbook");

    await deps.knowledgeBases.ingest("default", "docs", {
      id: "runbook",
      title: "Updated runbook",
      text: "Restart workers after a failed deployment.",
    });
    const updatedSearch = await deps.knowledgeBases.search("default", "docs", {
      query: "failed deployment",
    });
    expect(updatedSearch).toHaveLength(1);
    expect(updatedSearch[0]?.chunk.source.title).toBe("Updated runbook");
  });

  it("keeps managed bases isolated by namespace", async () => {
    const namespaces = new NamespaceService();
    await namespaces.ensure({ id: "acme" });
    await namespaces.ensureDefaultNamespace();
    const knowledgeBases = new ZoryaKnowledgeBases();
    const deps = { knowledgeBases, namespaces };

    await createManagedKnowledgeBase(deps)(jsonRequest("POST", { id: "public-docs" }));
    await createManagedKnowledgeBase(deps)(
      jsonRequest("POST", { id: "private-docs", namespace: "acme" }),
    );
    const listed = await listManagedKnowledgeBases(deps)(
      new Request("http://test/api/knowledge-bases?namespace=default"),
    );
    expect((await listed.json()).knowledgeBases.map((base: { id: string }) => base.id)).toEqual([
      "public-docs",
    ]);

    const listedAll = await listManagedKnowledgeBases(deps)(
      new Request("http://test/api/knowledge-bases"),
    );
    expect((await listedAll.json()).knowledgeBases.map((base: { id: string }) => base.id)).toEqual([
      "private-docs",
      "public-docs",
    ]);

    const missing = await searchManagedKnowledgeBase(deps)(
      jsonRequest("POST", { query: "secret", namespace: "default" }),
      { id: "private-docs" },
    );
    expect(missing.status).toBe(404);
  });

  it("rehydrates managed sources from the store", async () => {
    const store = new InMemoryKnowledgeBaseStore();
    const first = new ZoryaKnowledgeBases({ store });
    await first.create({ id: "docs", namespace: "default" });
    await first.ingest("default", "docs", { id: "one", text: "alpha content" });

    const second = new ZoryaKnowledgeBases({ store });
    expect((await second.listSources("default", "docs"))[0]?.id).toBe("one");
    expect((await second.search("default", "docs", { query: "alpha" }))[0]?.chunk.source.id).toBe(
      "one",
    );
  });
});

function jsonRequest(method: string, body: unknown): Request {
  return new Request("http://test/api/knowledge-bases", {
    method,
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
}
