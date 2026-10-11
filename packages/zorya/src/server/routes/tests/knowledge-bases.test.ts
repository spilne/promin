// ---------------------------------------------------------------------------
// Knowledge-base HTTP routes — end-to-end against a real ZoryaServer backed
// by ZoryaKnowledgeBases. Pins CRUD, ingestion (and how its failures
// surface), import through adapters, chunk inspection, search, request
// validation, and the 409 on deleting a base an agent recipe binds.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import {
  InMemoryAgentRegistry,
  InMemoryRetrieverRegistry,
  type EmbeddingProvider,
} from "@promin/agent";
import { InMemoryWorkflowStorage, createWorkflowRunner } from "@promin/workflow";
import { LocalWorkflows, ZoryaAgents, ZoryaKnowledgeBases } from "../../../index.ts";
import { ZoryaServer } from "../../server.ts";
import {
  KnowledgeSourceImportError,
  type KnowledgeSourceAdapter,
} from "../../services/knowledge/index.ts";

function bootServer(
  params: {
    embeddings?: EmbeddingProvider;
    sourceAdapters?: KnowledgeSourceAdapter[];
    withAgents?: boolean;
  } = {},
) {
  const storage = new InMemoryWorkflowStorage();
  const runner = createWorkflowRunner({ storage });
  const workflows = new LocalWorkflows({
    storage,
    runner,
    definitions: {},
    sleepScanIntervalMs: 0,
  });
  const retrievers = new InMemoryRetrieverRegistry();
  const knowledgeBases = new ZoryaKnowledgeBases({
    registry: retrievers,
    ...(params.embeddings !== undefined && { embeddings: params.embeddings }),
    ...(params.sourceAdapters !== undefined && { sourceAdapters: params.sourceAdapters }),
  });
  const agentRegistry = new InMemoryAgentRegistry();
  const agents = params.withAgents
    ? new ZoryaAgents({
        registry: agentRegistry,
        retrievers,
        resolve: async () => {
          throw new Error("not used");
        },
      })
    : undefined;
  const server = new ZoryaServer({
    insecure: true,
    workflows,
    knowledgeBases,
    ...(agents !== undefined && { agents }),
  });
  return { server, knowledgeBases, agentRegistry, retrievers };
}

function send(method: string, path: string, body?: unknown): Request {
  return new Request(`http://test${path}`, {
    method,
    ...(body !== undefined && {
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    }),
  });
}

async function call(server: ZoryaServer, req: Request): Promise<{ status: number; body: any }> {
  const res = await server.handle(req);
  const text = await res.text();
  return { status: res.status, body: text ? JSON.parse(text) : undefined };
}

describe("knowledge-base routes — CRUD", () => {
  it("creates, reads, lists, patches and deletes a base", async () => {
    const { server, retrievers } = bootServer();

    const created = await call(
      server,
      send("POST", "/api/knowledge-bases", { id: "docs", description: "Product docs" }),
    );
    expect(created.status).toBe(201);
    expect(created.body).toMatchObject({ id: "docs", status: "ready", sourceCount: 0 });
    expect(retrievers.get("docs")).not.toBeNull();

    expect((await call(server, send("GET", "/api/knowledge-bases/docs"))).body.id).toBe("docs");
    const listed = await call(server, send("GET", "/api/knowledge-bases"));
    expect(listed.body.knowledgeBases.map((b: { id: string }) => b.id)).toEqual(["docs"]);

    const patched = await call(
      server,
      send("PATCH", "/api/knowledge-bases/docs", { tags: ["public"], description: null }),
    );
    expect(patched.status).toBe(200);
    expect(patched.body.tags).toEqual(["public"]);
    expect(patched.body.description).toBeUndefined();

    expect((await call(server, send("POST", "/api/knowledge-bases", { id: "docs" }))).status).toBe(
      409,
    );

    expect((await call(server, send("DELETE", "/api/knowledge-bases/docs"))).status).toBe(204);
    const gone = await call(server, send("GET", "/api/knowledge-bases/docs"));
    expect(gone.status).toBe(404);
    expect(gone.body.error).toBe("knowledge_base_not_found");
    expect(retrievers.get("docs")).toBeNull();
  });

  it("validates bodies with the shared error envelope", async () => {
    const { server } = bootServer();
    const badId = await call(server, send("POST", "/api/knowledge-bases", { id: "_x" }));
    expect(badId.status).toBe(400);
    expect(badId.body.error).toBe("invalid_request");
    expect(badId.body.issues[0].path).toBe("id");

    const extra = await call(server, send("POST", "/api/knowledge-bases", { id: "x", nope: 1 }));
    expect(extra.status).toBe(400);

    const notJson = await server.handle(
      new Request("http://test/api/knowledge-bases", {
        method: "POST",
        headers: { "content-type": "text/plain" },
        body: JSON.stringify({ id: "x" }),
      }),
    );
    expect(notJson.status).toBe(415);

    await call(server, send("POST", "/api/knowledge-bases", { id: "docs" }));
    const emptyPatch = await call(server, send("PATCH", "/api/knowledge-bases/docs", {}));
    expect(emptyPatch.status).toBe(400);
    const emptyText = await call(
      server,
      send("POST", "/api/knowledge-bases/docs/sources", { id: "s", text: "" }),
    );
    expect(emptyText.status).toBe(400);
    const badSearch = await call(
      server,
      send("POST", "/api/knowledge-bases/docs/search", { query: "x", topK: 500 }),
    );
    expect(badSearch.status).toBe(400);
  });

  it("refuses to delete a base an agent binds, unless forced", async () => {
    const { server, agentRegistry } = bootServer({ withAgents: true });
    await call(server, send("POST", "/api/knowledge-bases", { id: "handbook" }));
    await agentRegistry.register({
      id: "helper",
      backend: {
        type: "local",
        model: { provider: "anthropic", id: "claude" },
        role: { inline: { systemPrompt: "Help." } },
        knowledge: [{ id: "handbook" }],
      },
    });

    const refused = await call(server, send("DELETE", "/api/knowledge-bases/handbook"));
    expect(refused.status).toBe(409);
    expect(refused.body).toMatchObject({ error: "knowledge_base_in_use", agents: ["helper"] });

    const forced = await call(server, send("DELETE", "/api/knowledge-bases/handbook?force=true"));
    expect(forced.status).toBe(204);
  });

  it("is not mounted without a knowledge-base service", async () => {
    const storage = new InMemoryWorkflowStorage();
    const workflows = new LocalWorkflows({
      storage,
      runner: createWorkflowRunner({ storage }),
      definitions: {},
      sleepScanIntervalMs: 0,
    });
    const server = new ZoryaServer({ insecure: true, workflows });
    const res = await server.handle(send("GET", "/api/knowledge-bases"));
    expect(res.status).not.toBe(200);
  });
});

describe("knowledge-base routes — sources, chunks and search", () => {
  it("ingests, lists, inspects, searches and removes sources", async () => {
    const { server } = bootServer();
    await call(server, send("POST", "/api/knowledge-bases", { id: "docs" }));

    const ingested = await call(
      server,
      send("POST", "/api/knowledge-bases/docs/sources", {
        id: "runbook",
        title: "Runbook",
        tags: ["ops"],
        text: "Restart workers when heartbeat lag exceeds the threshold.",
      }),
    );
    expect(ingested.status).toBe(201);
    expect(ingested.body).toMatchObject({ id: "runbook", status: "ready", chunkCount: 1 });
    expect(ingested.body).not.toHaveProperty("text");

    const sources = await call(server, send("GET", "/api/knowledge-bases/docs/sources"));
    expect(sources.body.sources.map((s: { id: string }) => s.id)).toEqual(["runbook"]);

    const chunks = await call(
      server,
      send("GET", "/api/knowledge-bases/docs/chunks?sourceId=runbook&limit=10"),
    );
    expect(chunks.body.total).toBe(1);
    expect(chunks.body.chunks[0].source.title).toBe("Runbook");

    const search = await call(
      server,
      send("POST", "/api/knowledge-bases/docs/search", { query: "heartbeat lag", topK: 3 }),
    );
    expect(search.status).toBe(200);
    expect(search.body.results[0]).toMatchObject({
      id: "runbook#0",
      index: 0,
      source: { id: "runbook", title: "Runbook", tags: ["ops"] },
    });

    const filtered = await call(
      server,
      send("POST", "/api/knowledge-bases/docs/search", { query: "heartbeat", tags: ["other"] }),
    );
    expect(filtered.body.results).toEqual([]);

    const removed = await call(server, send("DELETE", "/api/knowledge-bases/docs/sources/runbook"));
    expect(removed.status).toBe(204);
    const again = await call(server, send("DELETE", "/api/knowledge-bases/docs/sources/runbook"));
    expect(again.status).toBe(404);
    expect(again.body.error).toBe("source_not_found");
  });

  it("decodes source ids in the path", async () => {
    const { server } = bootServer();
    await call(server, send("POST", "/api/knowledge-bases", { id: "docs" }));
    const id = "https://docs.example.com/a b";
    await call(server, send("POST", "/api/knowledge-bases/docs/sources", { id, text: "alpha" }));
    const res = await call(
      server,
      send("DELETE", `/api/knowledge-bases/docs/sources/${encodeURIComponent(id)}`),
    );
    expect(res.status).toBe(204);
  });

  it("surfaces an indexing failure as 502 and lists the source as failed until re-indexed", async () => {
    let failing = true;
    const embeddings: EmbeddingProvider = {
      embed: async (text) => {
        if (failing) throw new Error("embedding quota exceeded");
        return [text.length, 1];
      },
    };
    const { server } = bootServer({ embeddings });
    await call(server, send("POST", "/api/knowledge-bases", { id: "docs" }));

    const failed = await call(
      server,
      send("POST", "/api/knowledge-bases/docs/sources", { id: "s", text: "alpha" }),
    );
    expect(failed.status).toBe(502);
    expect(failed.body.error).toBe("ingest_failed");
    expect(failed.body.message).toContain("embedding quota exceeded");

    const sources = await call(server, send("GET", "/api/knowledge-bases/docs/sources"));
    expect(sources.body.sources[0]).toMatchObject({
      id: "s",
      status: "failed",
      error: "embedding quota exceeded",
    });
    expect((await call(server, send("GET", "/api/knowledge-bases/docs"))).body.status).toBe(
      "degraded",
    );

    // Searching while embeddings are down is a 502, not a 500.
    const search = await call(
      server,
      send("POST", "/api/knowledge-bases/docs/search", { query: "alpha" }),
    );
    expect(search.status).toBe(502);
    expect(search.body.error).toBe("search_failed");

    failing = false;
    const reindexed = await call(
      server,
      send("POST", "/api/knowledge-bases/docs/sources/s/reindex"),
    );
    expect(reindexed.status).toBe(200);
    expect(reindexed.body.status).toBe("ready");
  });

  it("imports through adapters and reports adapter failures", async () => {
    const fixture: KnowledgeSourceAdapter = {
      kind: "fixture",
      load: async ({ config }) => {
        if (config === "down") {
          throw new KnowledgeSourceImportError({ reason: "unavailable", message: "upstream 503" });
        }
        return [{ id: String(config), text: "imported content" }];
      },
    };
    const { server } = bootServer({ sourceAdapters: [fixture] });
    await call(server, send("POST", "/api/knowledge-bases", { id: "docs" }));

    const kinds = await call(server, send("GET", "/api/knowledge-bases/_adapters"));
    expect(kinds.body).toEqual({ kinds: ["fixture"] });

    const imported = await call(
      server,
      send("POST", "/api/knowledge-bases/docs/sources/import", { kind: "fixture", config: "a" }),
    );
    expect(imported.status).toBe(201);
    expect(imported.body.sources.map((s: { id: string }) => s.id)).toEqual(["a"]);

    const down = await call(
      server,
      send("POST", "/api/knowledge-bases/docs/sources/import", {
        kind: "fixture",
        config: "down",
      }),
    );
    expect(down.status).toBe(502);
    expect(down.body).toMatchObject({ error: "source_unavailable", message: "upstream 503" });

    const unknown = await call(
      server,
      send("POST", "/api/knowledge-bases/docs/sources/import", { kind: "url", config: {} }),
    );
    expect(unknown.status).toBe(400);
    expect(unknown.body.error).toBe("unknown_source_kind");
  });

  it("answers 404 for sources of an unknown base", async () => {
    const { server } = bootServer();
    for (const req of [
      send("GET", "/api/knowledge-bases/ghost/sources"),
      send("POST", "/api/knowledge-bases/ghost/sources", { id: "s", text: "x" }),
      send("GET", "/api/knowledge-bases/ghost/chunks"),
      send("POST", "/api/knowledge-bases/ghost/search", { query: "x" }),
      send("DELETE", "/api/knowledge-bases/ghost"),
    ]) {
      const res = await call(server, req);
      expect(res.status).toBe(404);
      expect(res.body.error).toBe("knowledge_base_not_found");
    }
  });
});
