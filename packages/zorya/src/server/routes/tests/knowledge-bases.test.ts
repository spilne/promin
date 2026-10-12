// ---------------------------------------------------------------------------
// Knowledge-base HTTP routes — end-to-end against a real ZoryaServer backed
// by ZoryaKnowledgeBases. Pins namespace scoping (query, authorizer,
// namespace registry), CRUD, background ingestion (202, then the outcome in
// the source listing), import through adapters, chunk inspection, search,
// request validation, and the 409 on deleting a base an agent recipe binds.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import {
  InMemoryAgentRegistry,
  InMemoryRetrieverRegistry,
  type EmbeddingProvider,
} from "@promin/agent";
import {
  FakeWallClock,
  InMemoryNamespaceRegistry,
  InMemoryWorkflowStorage,
  createWorkflowRunner,
  type NamespaceRegistry,
} from "@promin/workflow";
import { LocalWorkflows, ZoryaAgents, ZoryaKnowledgeBases } from "../../../index.ts";
import { ZoryaServer } from "../../server.ts";
import {
  KnowledgeSourceImportError,
  type KnowledgeSourceAdapter,
} from "../../services/knowledge/index.ts";
import type { ScopeAuthorizer } from "../agent-gateway-kit.ts";

const KB = "/api/knowledge-bases";
const NS = "namespaceId=acme";

function bootServer(
  params: {
    embeddings?: EmbeddingProvider;
    sourceAdapters?: KnowledgeSourceAdapter[];
    withAgents?: boolean;
    authorizeScope?: ScopeAuthorizer;
    namespaces?: NamespaceRegistry;
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
    clock: FakeWallClock.create(0),
    ...(params.embeddings !== undefined && { embeddings: params.embeddings }),
    ...(params.sourceAdapters !== undefined && { sourceAdapters: params.sourceAdapters }),
  });
  const agentRegistry = new InMemoryAgentRegistry();
  const agents =
    params.withAgents || params.authorizeScope
      ? new ZoryaAgents({
          registry: agentRegistry,
          retrievers,
          resolve: async () => {
            throw new Error("not used");
          },
          ...(params.authorizeScope !== undefined && { authorizeScope: params.authorizeScope }),
        })
      : undefined;
  const server = new ZoryaServer({
    insecure: true,
    workflows,
    knowledgeBases,
    ...(agents !== undefined && { agents }),
    ...(params.namespaces !== undefined && { namespaces: params.namespaces }),
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

describe("knowledge-base routes — namespaces", () => {
  it("requires a namespace and keeps namespaces apart", async () => {
    const { server, retrievers } = bootServer();
    const missing = await call(server, send("GET", KB));
    expect(missing.status).toBe(400);
    expect(missing.body.issues[0].path).toBe("namespaceId");

    expect((await call(server, send("POST", `${KB}?${NS}`, { id: "docs" }))).status).toBe(201);
    expect(
      (await call(server, send("POST", `${KB}?namespaceId=beta`, { id: "docs" }))).status,
    ).toBe(201);
    expect(retrievers.get({ id: "docs", namespaceId: "acme" })).not.toBeNull();
    expect(retrievers.get({ id: "docs" })).toBeNull();

    expect((await call(server, send("DELETE", `${KB}/docs?${NS}`))).status).toBe(204);
    expect((await call(server, send("GET", `${KB}/docs?${NS}`))).status).toBe(404);
    expect((await call(server, send("GET", `${KB}/docs?namespaceId=beta`))).status).toBe(200);
    const gamma = await call(server, send("GET", `${KB}?namespaceId=gamma`));
    expect(gamma.body.knowledgeBases).toEqual([]);
  });

  it("asks the scope authorizer for the namespace (403 otherwise)", async () => {
    const asked: string[] = [];
    const { server } = bootServer({
      authorizeScope: ({ scope }) => {
        const namespaceId = "namespaceId" in scope ? scope.namespaceId : "";
        asked.push(namespaceId);
        return namespaceId === "acme";
      },
    });
    expect((await call(server, send("POST", `${KB}?${NS}`, { id: "docs" }))).status).toBe(201);
    for (const req of [
      send("GET", `${KB}?namespaceId=beta`),
      send("POST", `${KB}?namespaceId=beta`, { id: "docs" }),
      send("GET", `${KB}/docs?namespaceId=beta`),
      send("DELETE", `${KB}/docs?namespaceId=beta`),
      send("POST", `${KB}/docs/search?namespaceId=beta`, { query: "x" }),
    ]) {
      const res = await call(server, req);
      expect(res.status).toBe(403);
      expect(res.body.error).toBe("scope_forbidden");
    }
    expect(asked).toContain("beta");
  });

  it("with a namespace registry, writes need a registered, active namespace", async () => {
    const namespaces = new InMemoryNamespaceRegistry();
    const { server } = bootServer({ namespaces });
    const unknown = await call(server, send("POST", `${KB}?${NS}`, { id: "docs" }));
    expect(unknown.status).toBe(404);
    expect(unknown.body.error).toBe("namespace_not_found");

    await namespaces.create({ id: "acme" });
    expect((await call(server, send("POST", `${KB}?${NS}`, { id: "docs" }))).status).toBe(201);

    await namespaces.update({ id: "acme", patch: { status: "archived" } });
    const archived = await call(
      server,
      send("POST", `${KB}/docs/sources?${NS}`, { id: "s", text: "alpha" }),
    );
    expect(archived.status).toBe(409);
    expect(archived.body.error).toBe("namespace_archived");
    // Reads and deletes still work, so an archived namespace can be cleaned up.
    expect((await call(server, send("GET", `${KB}/docs?${NS}`))).status).toBe(200);
    expect((await call(server, send("DELETE", `${KB}/docs?${NS}`))).status).toBe(204);
  });

  it("the agent catalog lists global retrievers plus the requested namespace's bases", async () => {
    const { server, retrievers } = bootServer({ withAgents: true });
    retrievers.register({ id: "host", retriever: { retrieve: async () => [] } });
    await call(server, send("POST", `${KB}?${NS}`, { id: "docs" }));

    const scoped = await call(server, send("GET", `/api/agents/_catalog/retrievers?${NS}`));
    expect(scoped.body.retrievers).toEqual([
      { id: "docs", namespaceId: "acme", tags: [], metadata: {} },
      { id: "host", tags: [], metadata: {} },
    ]);
    const global = await call(server, send("GET", "/api/agents/_catalog/retrievers"));
    expect(global.body.retrievers.map((r: { id: string }) => r.id)).toEqual(["host"]);
  });
});

describe("knowledge-base routes — CRUD", () => {
  it("creates, reads, lists, patches and deletes a base", async () => {
    const { server, retrievers } = bootServer();

    const created = await call(
      server,
      send("POST", `${KB}?${NS}`, { id: "docs", description: "Product docs" }),
    );
    expect(created.status).toBe(201);
    expect(created.body).toMatchObject({
      namespaceId: "acme",
      id: "docs",
      status: "ready",
      sourceCount: 0,
    });
    expect(retrievers.get({ id: "docs", namespaceId: "acme" })).not.toBeNull();

    expect((await call(server, send("GET", `${KB}/docs?${NS}`))).body.id).toBe("docs");
    const listed = await call(server, send("GET", `${KB}?${NS}`));
    expect(listed.body.knowledgeBases.map((b: { id: string }) => b.id)).toEqual(["docs"]);

    const patched = await call(
      server,
      send("PATCH", `${KB}/docs?${NS}`, { tags: ["public"], description: null }),
    );
    expect(patched.status).toBe(200);
    expect(patched.body.tags).toEqual(["public"]);
    expect(patched.body.description).toBeUndefined();

    expect((await call(server, send("POST", `${KB}?${NS}`, { id: "docs" }))).status).toBe(409);

    expect((await call(server, send("DELETE", `${KB}/docs?${NS}`))).status).toBe(204);
    const gone = await call(server, send("GET", `${KB}/docs?${NS}`));
    expect(gone.status).toBe(404);
    expect(gone.body.error).toBe("knowledge_base_not_found");
    expect(retrievers.get({ id: "docs", namespaceId: "acme" })).toBeNull();
  });

  it("validates bodies with the shared error envelope", async () => {
    const { server } = bootServer();
    const badId = await call(server, send("POST", `${KB}?${NS}`, { id: "_x" }));
    expect(badId.status).toBe(400);
    expect(badId.body.error).toBe("invalid_request");
    expect(badId.body.issues[0].path).toBe("id");

    const extra = await call(server, send("POST", `${KB}?${NS}`, { id: "x", nope: 1 }));
    expect(extra.status).toBe(400);

    const notJson = await server.handle(
      new Request(`http://test${KB}?${NS}`, {
        method: "POST",
        headers: { "content-type": "text/plain" },
        body: JSON.stringify({ id: "x" }),
      }),
    );
    expect(notJson.status).toBe(415);

    await call(server, send("POST", `${KB}?${NS}`, { id: "docs" }));
    const emptyPatch = await call(server, send("PATCH", `${KB}/docs?${NS}`, {}));
    expect(emptyPatch.status).toBe(400);
    const emptyText = await call(
      server,
      send("POST", `${KB}/docs/sources?${NS}`, { id: "s", text: "" }),
    );
    expect(emptyText.status).toBe(400);
    const badSearch = await call(
      server,
      send("POST", `${KB}/docs/search?${NS}`, { query: "x", topK: 500 }),
    );
    expect(badSearch.status).toBe(400);
  });

  it("refuses to delete a base an agent binds, unless forced", async () => {
    const { server, agentRegistry } = bootServer({ withAgents: true });
    await call(server, send("POST", `${KB}?${NS}`, { id: "handbook" }));
    await agentRegistry.register({
      id: "helper",
      backend: {
        type: "local",
        model: { provider: "anthropic", id: "claude" },
        role: { inline: { systemPrompt: "Help.", tools: [] } },
        knowledge: [{ id: "handbook" }],
      },
    });

    const refused = await call(server, send("DELETE", `${KB}/handbook?${NS}`));
    expect(refused.status).toBe(409);
    expect(refused.body).toMatchObject({ error: "knowledge_base_in_use", agents: ["helper"] });

    const forced = await call(server, send("DELETE", `${KB}/handbook?${NS}&force=true`));
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
    const res = await server.handle(send("GET", `${KB}?${NS}`));
    expect(res.status).not.toBe(200);
  });
});

describe("knowledge-base routes — sources, chunks and search", () => {
  it("queues a source (202), then lists, inspects, searches and removes it", async () => {
    const { server, knowledgeBases } = bootServer();
    await call(server, send("POST", `${KB}?${NS}`, { id: "docs" }));

    const res = await server.handle(
      send("POST", `${KB}/docs/sources?${NS}`, {
        id: "runbook",
        title: "Runbook",
        tags: ["ops"],
        text: "Restart workers when heartbeat lag exceeds the threshold.",
      }),
    );
    expect(res.status).toBe(202);
    expect(res.headers.get("location")).toBe(`${KB}/docs/sources?namespaceId=acme`);
    const queued = (await res.json()) as Record<string, unknown>;
    expect(queued).toMatchObject({ id: "runbook", status: "pending" });
    expect(queued).not.toHaveProperty("text");

    await knowledgeBases.drain();
    const sources = await call(server, send("GET", `${KB}/docs/sources?${NS}`));
    expect(sources.body.sources).toMatchObject([{ id: "runbook", status: "ready", chunkCount: 1 }]);

    const chunks = await call(
      server,
      send("GET", `${KB}/docs/chunks?${NS}&sourceId=runbook&limit=10`),
    );
    expect(chunks.body.total).toBe(1);
    expect(chunks.body.chunks[0].source.title).toBe("Runbook");

    const search = await call(
      server,
      send("POST", `${KB}/docs/search?${NS}`, { query: "heartbeat lag", topK: 3 }),
    );
    expect(search.status).toBe(200);
    expect(search.body.results[0]).toMatchObject({
      id: expect.stringMatching(/^runbook@[0-9a-f]+#0$/),
      index: 0,
      source: { id: "runbook", title: "Runbook", tags: ["ops"] },
    });

    const filtered = await call(
      server,
      send("POST", `${KB}/docs/search?${NS}`, { query: "heartbeat", tags: ["other"] }),
    );
    expect(filtered.body.results).toEqual([]);

    const removed = await call(server, send("DELETE", `${KB}/docs/sources/runbook?${NS}`));
    expect(removed.status).toBe(204);
    const again = await call(server, send("DELETE", `${KB}/docs/sources/runbook?${NS}`));
    expect(again.status).toBe(404);
    expect(again.body.error).toBe("source_not_found");
  });

  it("decodes source ids in the path", async () => {
    const { server, knowledgeBases } = bootServer();
    await call(server, send("POST", `${KB}?${NS}`, { id: "docs" }));
    const id = "https://docs.example.com/a b";
    await call(server, send("POST", `${KB}/docs/sources?${NS}`, { id, text: "alpha" }));
    await knowledgeBases.drain();
    const res = await call(
      server,
      send("DELETE", `${KB}/docs/sources/${encodeURIComponent(id)}?${NS}`),
    );
    expect(res.status).toBe(204);
  });

  it("lists an indexing failure on the source with its reason until re-indexed", async () => {
    let failing = true;
    const embeddings: EmbeddingProvider = {
      embed: async (text) => {
        if (failing) throw new Error("embedding quota exceeded");
        return [text.length, 1];
      },
    };
    const { server, knowledgeBases } = bootServer({ embeddings });
    await call(server, send("POST", `${KB}?${NS}`, { id: "docs" }));

    const queued = await call(
      server,
      send("POST", `${KB}/docs/sources?${NS}`, { id: "s", text: "alpha" }),
    );
    expect(queued.status).toBe(202);
    await knowledgeBases.drain();

    const sources = await call(server, send("GET", `${KB}/docs/sources?${NS}`));
    expect(sources.body.sources[0]).toMatchObject({
      id: "s",
      status: "failed",
      error: "embedding quota exceeded",
    });
    expect((await call(server, send("GET", `${KB}/docs?${NS}`))).body.status).toBe("degraded");

    // Searching while embeddings are down is a 502, not a 500.
    const search = await call(server, send("POST", `${KB}/docs/search?${NS}`, { query: "alpha" }));
    expect(search.status).toBe(502);
    expect(search.body.error).toBe("search_failed");

    failing = false;
    const reindexed = await call(server, send("POST", `${KB}/docs/sources/s/reindex?${NS}`));
    expect(reindexed.status).toBe(202);
    expect(reindexed.body.status).toBe("pending");
    await knowledgeBases.drain();
    const after = await call(server, send("GET", `${KB}/docs/sources?${NS}`));
    expect(after.body.sources[0].status).toBe("ready");
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
    await call(server, send("POST", `${KB}?${NS}`, { id: "docs" }));

    const kinds = await call(server, send("GET", `${KB}/_adapters`));
    expect(kinds.body).toEqual({ kinds: ["fixture"] });

    const imported = await call(
      server,
      send("POST", `${KB}/docs/sources/import?${NS}`, { kind: "fixture", config: "a" }),
    );
    expect(imported.status).toBe(202);
    expect(imported.body.sources).toMatchObject([{ id: "a", status: "pending" }]);

    const down = await call(
      server,
      send("POST", `${KB}/docs/sources/import?${NS}`, { kind: "fixture", config: "down" }),
    );
    expect(down.status).toBe(502);
    expect(down.body).toMatchObject({ error: "source_unavailable", message: "upstream 503" });

    const unknown = await call(
      server,
      send("POST", `${KB}/docs/sources/import?${NS}`, { kind: "url", config: {} }),
    );
    expect(unknown.status).toBe(400);
    expect(unknown.body.error).toBe("unknown_source_kind");
  });

  it("answers 404 for sources of an unknown base", async () => {
    const { server } = bootServer();
    await call(server, send("POST", `${KB}?namespaceId=beta`, { id: "ghost" }));
    for (const req of [
      send("GET", `${KB}/ghost/sources?${NS}`),
      send("POST", `${KB}/ghost/sources?${NS}`, { id: "s", text: "x" }),
      send("GET", `${KB}/ghost/chunks?${NS}`),
      send("POST", `${KB}/ghost/search?${NS}`, { query: "x" }),
      send("DELETE", `${KB}/ghost?${NS}`),
    ]) {
      const res = await call(server, req);
      expect(res.status).toBe(404);
      expect(res.body.error).toBe("knowledge_base_not_found");
    }
  });
});
