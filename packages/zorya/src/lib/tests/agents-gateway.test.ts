// ---------------------------------------------------------------------------
// Agent gateway routes — end-to-end against a real ZoryaServer instance,
// exercising the full path from HTTP request → AgentRegistry → resolve →
// LocalAgent.bind() → invoke / stream / thread.
//
// Pins:
//   - GET /api/agents lists registered recipes
//   - GET /api/agents/:id returns the recipe (404 missing)
//   - POST /api/agents/:id/invoke binds tenant + runs one-shot
//   - POST /api/agents/:id/stream produces SSE deltas + a finish event
//   - POST /api/agents/:id/threads/:threadId persists messages
//   - GET  /api/agents/:id/threads/:threadId/messages reads them back
//   - 400 on missing body fields, 404 on missing agent
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import {
  AgentTurnGate,
  DefaultConsolidator,
  InMemoryAgentInstanceRegistry,
  InMemoryAgentRegistry,
  InMemoryLeaseStore,
  InMemoryMemoryStore,
  InMemorySecretsStorage,
  RateLimitedConsolidator,
  SecretScope,
  resolveLocalAgent,
} from "@promin/agent";
import type { AgentInstanceRegistry, LLMProvider, LLMResponse } from "@promin/agent";
import { InMemoryWorkflowStorage, createWorkflowRunner } from "@promin/workflow";
import { ZoryaServer } from "../../server/server.ts";
import { LocalWorkflows, ZoryaAgents } from "../../index.ts";

function mockLLM(responses: LLMResponse[]): LLMProvider {
  let i = 0;
  return {
    chat: async () => {
      const r = responses[i++];
      if (!r) throw new Error("Mock LLM exhausted");
      return r;
    },
  };
}

async function bootGateway(opts?: {
  responses?: LLMResponse[];
  withInstances?: boolean;
  withTurnGate?: boolean;
  /** Pre-acquire the lease for this thread before booting (simulates a turn already in flight). */
  preAcquireThread?: { namespaceId: string; threadId: string; ownerId: string };
  /** When true, mounts an InMemorySecretsStorage and exposes it to the gateway. */
  withSecrets?: boolean;
}) {
  const storage = new InMemoryWorkflowStorage();
  const runner = createWorkflowRunner({ storage });
  const memory = new InMemoryMemoryStore();
  const registry = new InMemoryAgentRegistry();
  const instanceRegistry: AgentInstanceRegistry | undefined = opts?.withInstances
    ? new InMemoryAgentInstanceRegistry()
    : undefined;
  const leaseStore =
    opts?.withTurnGate || opts?.preAcquireThread ? new InMemoryLeaseStore() : undefined;
  const turnGate = leaseStore ? new AgentTurnGate({ leaseStore }) : undefined;
  if (opts?.preAcquireThread && leaseStore) {
    await leaseStore.acquire({
      key: {
        namespaceId: opts.preAcquireThread.namespaceId,
        threadId: opts.preAcquireThread.threadId,
      },
      ownerId: opts.preAcquireThread.ownerId,
      ttlMs: 60_000,
    });
  }

  await registry.register({
    id: "support",
    backend: {
      type: "local",
      model: { provider: "anthropic", id: "claude-sonnet-4-6" },
      role: { inline: { systemPrompt: "Helpful", tools: [] } },
    },
    metadata: { capabilities: ["chat"], tags: ["beta"] },
  });

  const responses = opts?.responses ?? [{ content: "ok", finishReason: "stop" }];
  const workflows = new LocalWorkflows({
    storage,
    runner,
    definitions: {},
    sleepScanIntervalMs: 0,
  });
  const agents = new ZoryaAgents({
    registry,
    resolve: (recipe) =>
      resolveLocalAgent(recipe, {
        runner,
        memory,
        llm: () => mockLLM(responses),
        tools: {},
      }),
    memory,
    ...(instanceRegistry && { instances: instanceRegistry }),
    ...(turnGate && { turnGate, workerId: "worker-test" }),
  });
  const secrets = opts?.withSecrets ? new InMemorySecretsStorage() : undefined;
  const server = new ZoryaServer({
    workflows,
    agents,
    ...(secrets !== undefined && { secrets }),
  });

  return {
    server,
    storage,
    runner,
    memory,
    registry,
    instanceRegistry,
    leaseStore,
    turnGate,
    secrets,
  };
}

describe("agent gateway — discovery", () => {
  it("GET /api/agents lists registered recipes", async () => {
    const { server } = await bootGateway();
    const res = await server.handle(new Request("http://test/api/agents", { method: "GET" }));
    expect(res.status).toBe(200);
    const body = (await res.json()) as { agents: Array<{ id: string }> };
    expect(body.agents.map((a) => a.id)).toEqual(["support"]);
  });

  it("GET /api/agents/:id returns the recipe", async () => {
    const { server } = await bootGateway();
    const res = await server.handle(
      new Request("http://test/api/agents/support", { method: "GET" }),
    );
    expect(res.status).toBe(200);
    const body = (await res.json()) as { id: string };
    expect(body.id).toBe("support");
  });

  it("GET /api/agents/:id returns 404 when missing", async () => {
    const { server } = await bootGateway();
    const res = await server.handle(
      new Request("http://test/api/agents/missing", { method: "GET" }),
    );
    expect(res.status).toBe(404);
  });
});

describe("agent gateway — invoke", () => {
  it("POST /api/agents/:id/invoke runs one-shot and returns text", async () => {
    const { server } = await bootGateway({
      responses: [{ content: "answer from invoke", finishReason: "stop" }],
    });
    const res = await server.handle(
      new Request("http://test/api/agents/support/invoke", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          task: "what's up?",
          namespaceId: "acme",
          resourceId: "alice",
        }),
      }),
    );
    expect(res.status).toBe(200);
    const body = (await res.json()) as { text: string; finishReason: string };
    expect(body.text).toBe("answer from invoke");
    expect(body.finishReason).toBe("stop");
  });

  it("400 when namespaceId is missing", async () => {
    const { server } = await bootGateway();
    const res = await server.handle(
      new Request("http://test/api/agents/support/invoke", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ task: "hi" }),
      }),
    );
    expect(res.status).toBe(400);
  });

  it("404 when agent is unknown", async () => {
    const { server } = await bootGateway();
    const res = await server.handle(
      new Request("http://test/api/agents/missing/invoke", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ task: "hi", namespaceId: "acme" }),
      }),
    );
    expect(res.status).toBe(404);
  });
});

describe("agent gateway — stream (SSE)", () => {
  it("POST /api/agents/:id/stream emits a finish event", async () => {
    const { server } = await bootGateway({
      responses: [{ content: "streamed", finishReason: "stop" }],
    });
    const res = await server.handle(
      new Request("http://test/api/agents/support/stream", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ task: "hi", namespaceId: "acme", resourceId: "alice" }),
      }),
    );
    expect(res.status).toBe(200);
    expect(res.headers.get("content-type")).toBe("text/event-stream");
    const text = await res.text();
    expect(text).toContain("event: finish");
    expect(text).toContain('"text":"streamed"');
  });

  it("400 when scope identity is missing (resourceId | ownerId required)", async () => {
    const { server } = await bootGateway();
    const res = await server.handle(
      new Request("http://test/api/agents/support/invoke", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ task: "hi", namespaceId: "acme" }),
      }),
    );
    expect(res.status).toBe(400);
    const body = (await res.json()) as { error: string };
    expect(body.error).toBe("missing_scope_identity");
  });
});

describe("agent gateway — threads", () => {
  it("POST /api/agents/:id/threads/:threadId persists user + assistant messages", async () => {
    const { server, memory } = await bootGateway({
      responses: [{ content: "first reply", finishReason: "stop" }],
    });
    const res = await server.handle(
      new Request("http://test/api/agents/support/threads/alice-default", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          task: "hello",
          namespaceId: "acme",
          resourceId: "alice",
        }),
      }),
    );
    expect(res.status).toBe(200);
    const body = (await res.json()) as {
      threadId: string;
      isNew: boolean;
      text: string;
    };
    expect(body.threadId).toBe("alice-default");
    expect(body.isNew).toBe(true);
    expect(body.text).toBe("first reply");

    const messages = await memory.getMessages({
      namespaceId: "acme",
      threadId: "alice-default",
    });
    const roles = messages.map((m) => m.role);
    expect(roles).toContain("user");
    expect(roles).toContain("assistant");
  });

  it("GET /api/agents/:id/threads/:threadId/messages reads stored history", async () => {
    const { server } = await bootGateway({
      responses: [{ content: "first", finishReason: "stop" }],
    });
    // Send first to create the thread + persist messages.
    await server.handle(
      new Request("http://test/api/agents/support/threads/t-1", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ task: "hi", namespaceId: "acme", resourceId: "alice" }),
      }),
    );
    const res = await server.handle(
      new Request(
        "http://test/api/agents/support/threads/t-1/messages?namespaceId=acme&resourceId=alice",
        { method: "GET" },
      ),
    );
    expect(res.status).toBe(200);
    const body = (await res.json()) as { threadId: string; messages: Array<{ role: string }> };
    expect(body.threadId).toBe("t-1");
    expect(body.messages.length).toBeGreaterThan(0);
  });

  it("GET /messages returns 404 when thread is missing", async () => {
    const { server } = await bootGateway();
    const res = await server.handle(
      new Request(
        "http://test/api/agents/support/threads/never-existed/messages?namespaceId=acme",
        { method: "GET" },
      ),
    );
    expect(res.status).toBe(404);
  });

  it("GET /api/agents/:id/threads lists threads for a tenant", async () => {
    const { server } = await bootGateway({
      responses: [
        { content: "r1", finishReason: "stop" },
        { content: "r2", finishReason: "stop" },
      ],
    });
    // Seed two threads under (acme, alice).
    for (const tid of ["t-a", "t-b"]) {
      await server.handle(
        new Request(`http://test/api/agents/support/threads/${tid}`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ task: "hi", namespaceId: "acme", resourceId: "alice" }),
        }),
      );
    }
    const res = await server.handle(
      new Request("http://test/api/agents/support/threads?namespaceId=acme&resourceId=alice", {
        method: "GET",
      }),
    );
    expect(res.status).toBe(200);
    const body = (await res.json()) as { threads: Array<{ id: string; messageCount: number }> };
    const ids = body.threads.map((t) => t.id).sort();
    expect(ids).toEqual(["t-a", "t-b"]);
    for (const t of body.threads) expect(t.messageCount).toBeGreaterThan(0);
  });

  it("GET /api/agents/:id/threads requires namespaceId", async () => {
    const { server } = await bootGateway();
    const res = await server.handle(
      new Request("http://test/api/agents/support/threads", { method: "GET" }),
    );
    expect(res.status).toBe(400);
  });
});

describe("agent gateway — instance auto-resolve from ownerId", () => {
  it("invoke with ownerId resolves an instance and returns its id", async () => {
    const { server, instanceRegistry } = await bootGateway({
      responses: [{ content: "answer", finishReason: "stop" }],
      withInstances: true,
    });
    const res = await server.handle(
      new Request("http://test/api/agents/support/invoke", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          task: "hi",
          namespaceId: "acme",
          ownerId: "alice",
        }),
      }),
    );
    expect(res.status).toBe(200);
    const body = (await res.json()) as { instanceId?: string; text: string };
    expect(body.instanceId).toBe("acme::support::alice");

    // The registry has the row, scoped per (registeredAgentId, namespaceId, ownerId).
    const row = await instanceRegistry!.get("acme::support::alice");
    expect(row?.ownerId).toBe("alice");
    expect(row?.registeredAgentId).toBe("support");
    expect(row?.namespaceId).toBe("acme");
  });

  it("subsequent ownerId invokes reuse the same instance row", async () => {
    const { server, instanceRegistry } = await bootGateway({
      responses: [
        { content: "first", finishReason: "stop" },
        { content: "second", finishReason: "stop" },
      ],
      withInstances: true,
    });
    const post = (body: object) =>
      server.handle(
        new Request("http://test/api/agents/support/invoke", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify(body),
        }),
      );
    await post({ task: "hi", namespaceId: "acme", ownerId: "alice" });
    const before = (await instanceRegistry!.get("acme::support::alice"))!;
    await post({ task: "hi again", namespaceId: "acme", ownerId: "alice" });
    const after = (await instanceRegistry!.get("acme::support::alice"))!;
    expect(after.id).toBe(before.id);
    expect(after.createdAt).toBe(before.createdAt);
  });

  it("400 conflicting_identity when both ownerId and resourceId are sent", async () => {
    const { server } = await bootGateway({ withInstances: true });
    const res = await server.handle(
      new Request("http://test/api/agents/support/invoke", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          task: "hi",
          namespaceId: "acme",
          ownerId: "alice",
          resourceId: "alice",
        }),
      }),
    );
    expect(res.status).toBe(400);
    const body = (await res.json()) as { error: string };
    expect(body.error).toBe("conflicting_identity");
  });

  it("400 ownerId_unsupported when registry isn't configured", async () => {
    const { server } = await bootGateway(); // withInstances: false
    const res = await server.handle(
      new Request("http://test/api/agents/support/invoke", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          task: "hi",
          namespaceId: "acme",
          ownerId: "alice",
        }),
      }),
    );
    expect(res.status).toBe(400);
    const body = (await res.json()) as { error: string };
    expect(body.error).toBe("ownerId_unsupported");
  });

  it("raw resourceId path still works (no instance row touched)", async () => {
    const { server, instanceRegistry } = await bootGateway({
      responses: [{ content: "ok", finishReason: "stop" }],
      withInstances: true,
    });
    const res = await server.handle(
      new Request("http://test/api/agents/support/invoke", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          task: "hi",
          namespaceId: "acme",
          resourceId: "alice",
        }),
      }),
    );
    expect(res.status).toBe(200);
    const body = (await res.json()) as { instanceId?: string };
    expect(body.instanceId).toBeUndefined();
    expect(await instanceRegistry!.list()).toHaveLength(0);
  });

  it("thread send with ownerId scopes the thread to instance.id", async () => {
    const { server, memory } = await bootGateway({
      responses: [{ content: "first reply", finishReason: "stop" }],
      withInstances: true,
    });
    const res = await server.handle(
      new Request("http://test/api/agents/support/threads/main", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          task: "hi",
          namespaceId: "acme",
          ownerId: "alice",
        }),
      }),
    );
    expect(res.status).toBe(200);
    const body = (await res.json()) as { threadId: string; instanceId?: string };
    expect(body.instanceId).toBe("acme::support::alice");

    // The thread is keyed under the instance id, not "alice" raw.
    const thread = await memory.getThread({
      namespaceId: "acme",
      resourceId: "acme::support::alice",
      threadId: "main",
    });
    expect(thread).not.toBeNull();
  });
});

describe("agent gateway — recipe CRUD (gsze Phase 1)", () => {
  describe("POST /api/agents — create", () => {
    it("creates a recipe with id + backend, returns 201", async () => {
      const { server, registry } = await bootGateway();
      const res = await server.handle(
        new Request("http://test/api/agents", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            id: "new-bot",
            backend: {
              type: "local",
              model: { provider: "anthropic", id: "claude-sonnet-4-6" },
              role: { inline: { systemPrompt: "Helpful new bot", tools: [] } },
            },
            metadata: { capabilities: ["chat"], tags: ["alpha"] },
          }),
        }),
      );
      expect(res.status).toBe(201);
      const body = (await res.json()) as { id: string; metadata: { tags: string[] } };
      expect(body.id).toBe("new-bot");
      expect(body.metadata.tags).toContain("alpha");
      expect(await registry.get("new-bot")).not.toBeNull();
    });

    it("rejects ids starting with `_` (reserved for catalog routes)", async () => {
      const { server } = await bootGateway();
      const res = await server.handle(
        new Request("http://test/api/agents", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            id: "_internal",
            backend: {
              type: "local",
              model: { provider: "anthropic", id: "claude-sonnet-4-6" },
              role: { inline: { systemPrompt: null, tools: [] } },
            },
          }),
        }),
      );
      expect(res.status).toBe(400);
      const body = (await res.json()) as { error: string };
      expect(body.error).toBe("reserved_id_prefix");
    });

    it("rejects missing id / backend with 400", async () => {
      const { server } = await bootGateway();
      const noId = await server.handle(
        new Request("http://test/api/agents", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ backend: {} }),
        }),
      );
      expect(noId.status).toBe(400);
      const noBackend = await server.handle(
        new Request("http://test/api/agents", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ id: "x" }),
        }),
      );
      expect(noBackend.status).toBe(400);
    });
  });

  describe("PATCH /api/agents/:id — update", () => {
    it("partially replaces backend + metadata, preserves identity", async () => {
      const { server, registry } = await bootGateway();
      const res = await server.handle(
        new Request("http://test/api/agents/support", {
          method: "PATCH",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            metadata: { description: "now helpful", capabilities: ["chat", "search"], tags: [] },
          }),
        }),
      );
      expect(res.status).toBe(200);
      const updated = await registry.get("support");
      expect(updated?.metadata.description).toBe("now helpful");
      expect(updated?.metadata.capabilities).toContain("search");
    });

    it("404 when id is unknown", async () => {
      const { server } = await bootGateway();
      const res = await server.handle(
        new Request("http://test/api/agents/never", {
          method: "PATCH",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ metadata: { description: "x" } }),
        }),
      );
      expect(res.status).toBe(404);
    });
  });

  describe("DELETE /api/agents/:id — unregister", () => {
    it("removes all versions when ?version is omitted, returns 204", async () => {
      const { server, registry } = await bootGateway();
      const res = await server.handle(
        new Request("http://test/api/agents/support", { method: "DELETE" }),
      );
      expect(res.status).toBe(204);
      expect(await registry.get("support")).toBeNull();
    });

    it("removes a single version when ?version=X", async () => {
      const { server, registry } = await bootGateway();
      // Add a v2 first.
      await registry.register({
        id: "support",
        version: "v2",
        backend: {
          type: "local",
          model: { provider: "anthropic", id: "claude-sonnet-4-6" },
          role: { inline: { systemPrompt: null, tools: [] } },
        },
      });
      const res = await server.handle(
        new Request("http://test/api/agents/support?version=v2", { method: "DELETE" }),
      );
      expect(res.status).toBe(204);
      expect(await registry.get("support", "v2")).toBeNull();
      expect(await registry.get("support", "v1")).not.toBeNull();
    });
  });

  describe("GET /api/agents/:id/versions", () => {
    it("returns all versions of one id, oldest first", async () => {
      const { server, registry } = await bootGateway();
      await registry.register({
        id: "support",
        version: "v2",
        backend: {
          type: "local",
          model: { provider: "anthropic", id: "claude-sonnet-4-6" },
          role: { inline: { systemPrompt: "v2", tools: [] } },
        },
      });
      const res = await server.handle(
        new Request("http://test/api/agents/support/versions", { method: "GET" }),
      );
      expect(res.status).toBe(200);
      const body = (await res.json()) as { versions: Array<{ version: string }> };
      const versions = body.versions.map((v) => v.version);
      expect(versions).toContain("v1");
      expect(versions).toContain("v2");
    });

    it("404 for unknown id", async () => {
      const { server } = await bootGateway();
      const res = await server.handle(
        new Request("http://test/api/agents/never/versions", { method: "GET" }),
      );
      expect(res.status).toBe(404);
    });
  });

  describe("POST /api/agents/:id/clone", () => {
    it("clones an existing recipe under a new id, returns 201", async () => {
      const { server, registry } = await bootGateway();
      const res = await server.handle(
        new Request("http://test/api/agents/support/clone", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ targetId: "support-fork" }),
        }),
      );
      expect(res.status).toBe(201);
      const body = (await res.json()) as {
        recipe: { id: string };
        acceptedSecrets: string[];
      };
      expect(body.recipe.id).toBe("support-fork");
      expect(body.acceptedSecrets).toEqual([]);
      expect(await registry.get("support-fork")).not.toBeNull();
    });

    it("rejects missing targetId with 400", async () => {
      const { server } = await bootGateway();
      const res = await server.handle(
        new Request("http://test/api/agents/support/clone", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({}),
        }),
      );
      expect(res.status).toBe(400);
    });

    it("404 when source id is unknown", async () => {
      const { server } = await bootGateway();
      const res = await server.handle(
        new Request("http://test/api/agents/never/clone", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ targetId: "ok" }),
        }),
      );
      expect(res.status).toBe(404);
    });

    it("echoes back accepted secret names without storing values when no SecretsStorage configured", async () => {
      // Without `secrets` on the gateway deps, supplied values are
      // silently dropped. The acceptedSecrets echo lets callers see
      // their values were received but not persisted.
      const { server } = await bootGateway();
      const res = await server.handle(
        new Request("http://test/api/agents/support/clone", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            targetId: "support-fork",
            secrets: { anthropic_api_key: "sk-ant-test", openai_key: "sk-..." },
          }),
        }),
      );
      expect(res.status).toBe(201);
      const body = (await res.json()) as {
        recipe: { id: string };
        acceptedSecrets: string[];
      };
      expect(body.recipe.id).toBe("support-fork");
      expect(body.acceptedSecrets.sort()).toEqual(["anthropic_api_key", "openai_key"]);
    });

    it("clone-with-secrets persists supplied secrets at the requested scope", async () => {
      const { server, registry, secrets } = await bootGateway({ withSecrets: true });
      await registry.register({
        id: "anthropic-template",
        backend: {
          type: "local",
          model: {
            provider: "anthropic",
            id: "claude-sonnet-4-6",
            credentialRef: "anthropic_api_key",
          },
          role: { inline: { systemPrompt: "Cloneable", tools: [] } },
        },
        metadata: {
          description: null,
          capabilities: [],
          tags: ["template"],
        },
      });

      const res = await server.handle(
        new Request("http://test/api/agents/anthropic-template/clone", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            targetId: "my-bot",
            secrets: { anthropic_api_key: "sk-ant-tenant-key" },
            secretsScope: { kind: "namespace", namespaceId: "acme" },
          }),
        }),
      );
      expect(res.status).toBe(201);

      // Secret persisted at namespace scope.
      const stored = await secrets!.get({
        scope: SecretScope.namespace("acme"),
        key: "anthropic_api_key",
      });
      expect(stored).toBe("sk-ant-tenant-key");
    });

    it("reuses an already-stored required secret — no re-entry, no overwrite", async () => {
      const { server, registry, secrets } = await bootGateway({ withSecrets: true });
      await registry.register({
        id: "anthropic-template",
        backend: {
          type: "local",
          model: {
            provider: "anthropic",
            id: "claude-sonnet-4-6",
            credentialRef: "anthropic_api_key",
          },
          role: { inline: { systemPrompt: "Cloneable", tools: [] } },
        },
        metadata: {
          description: null,
          capabilities: [],
          tags: ["template"],
        },
      });
      // The tenant already holds the key at namespace scope (e.g. a prior clone).
      await secrets!.set({
        scope: SecretScope.namespace("acme"),
        key: "anthropic_api_key",
        value: "sk-ant-existing",
      });

      const res = await server.handle(
        new Request("http://test/api/agents/anthropic-template/clone", {
          method: "POST",
          headers: { "content-type": "application/json" },
          // No `secrets` body — relies on the already-stored key.
          body: JSON.stringify({
            targetId: "my-bot-2",
            secretsScope: { kind: "namespace", namespaceId: "acme" },
          }),
        }),
      );
      expect(res.status).toBe(201);
      // The stored value is untouched — the empty re-clone didn't overwrite it.
      expect(
        await secrets!.get({ scope: SecretScope.namespace("acme"), key: "anthropic_api_key" }),
      ).toBe("sk-ant-existing");
    });
  });

  describe("draft recipes (_draft)", () => {
    const draftBackend = {
      type: "local" as const,
      model: { provider: "anthropic", id: "claude-sonnet-4-6" },
      role: { inline: { systemPrompt: "draft prompt", tools: [] as string[] } },
    };

    async function createDraft(server: ZoryaServer): Promise<string> {
      const res = await server.handle(
        new Request("http://test/api/agents/_draft", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ backend: draftBackend, sourceId: "support" }),
        }),
      );
      expect(res.status).toBe(201);
      return ((await res.json()) as { recipe: { id: string } }).recipe.id;
    }

    it("registers a draft under a __draft__ id, hidden from the agent list", async () => {
      const { server } = await bootGateway();
      const id = await createDraft(server);
      expect(id.startsWith("__draft__")).toBe(true);

      const list = (await (await server.handle(new Request("http://test/api/agents"))).json()) as {
        agents: Array<{ id: string }>;
      };
      expect(list.agents.some((a) => a.id.startsWith("__draft__"))).toBe(false);
    });

    it("a draft is chattable through the standard invoke endpoint", async () => {
      const { server } = await bootGateway({
        responses: [{ content: "draft says hi", finishReason: "stop" }],
      });
      const id = await createDraft(server);
      const res = await server.handle(
        new Request(`http://test/api/agents/${encodeURIComponent(id)}/invoke`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ task: "hi", namespaceId: "acme", resourceId: "alice" }),
        }),
      );
      expect(res.status).toBe(200);
      expect(((await res.json()) as { text: string }).text).toBe("draft says hi");
    });

    it("DELETE /api/agents/_draft/:id removes the draft", async () => {
      const { server } = await bootGateway();
      const id = await createDraft(server);
      const del = await server.handle(
        new Request(`http://test/api/agents/_draft/${encodeURIComponent(id)}`, {
          method: "DELETE",
        }),
      );
      expect(del.status).toBe(204);
      const get = await server.handle(
        new Request(`http://test/api/agents/${encodeURIComponent(id)}`),
      );
      expect(get.status).toBe(404);
    });

    it("DELETE /api/agents/_draft/:id rejects a non-draft id", async () => {
      const { server } = await bootGateway();
      const del = await server.handle(
        new Request("http://test/api/agents/_draft/support", { method: "DELETE" }),
      );
      expect(del.status).toBe(400);
      expect(((await del.json()) as { error: string }).error).toBe("not_a_draft");
    });
  });
});

describe("agent gateway — disabled recipe gate", () => {
  it("invoke returns 410 when recipe.metadata.enabled === false", async () => {
    const { server, registry } = await bootGateway();
    await registry.register({
      id: "support",
      backend: {
        type: "local",
        model: { provider: "anthropic", id: "claude-sonnet-4-6" },
        role: { inline: { systemPrompt: "Helpful", tools: [] } },
      },
      metadata: { description: null, capabilities: [], tags: [], enabled: false },
    });
    const res = await server.handle(
      new Request("http://test/api/agents/support/invoke", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ task: "hi", namespaceId: "acme", resourceId: "alice" }),
      }),
    );
    expect(res.status).toBe(410);
    const body = (await res.json()) as { error: string };
    expect(body.error).toBe("agent_disabled");
  });

  it("thread-send returns 410 when recipe is disabled", async () => {
    const { server, registry } = await bootGateway();
    await registry.register({
      id: "support",
      backend: {
        type: "local",
        model: { provider: "anthropic", id: "claude-sonnet-4-6" },
        role: { inline: { systemPrompt: "Helpful", tools: [] } },
      },
      metadata: { description: null, capabilities: [], tags: [], enabled: false },
    });
    const res = await server.handle(
      new Request("http://test/api/agents/support/threads/t-1", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ task: "hi", namespaceId: "acme", resourceId: "alice" }),
      }),
    );
    expect(res.status).toBe(410);
  });

  it("read paths still work on disabled agents (browse + edit allowed)", async () => {
    const { server, registry } = await bootGateway();
    await registry.register({
      id: "support",
      backend: {
        type: "local",
        model: { provider: "anthropic", id: "claude-sonnet-4-6" },
        role: { inline: { systemPrompt: "Helpful", tools: [] } },
      },
      metadata: { description: null, capabilities: [], tags: [], enabled: false },
    });
    const get = await server.handle(
      new Request("http://test/api/agents/support", { method: "GET" }),
    );
    expect(get.status).toBe(200);
    const patch = await server.handle(
      new Request("http://test/api/agents/support", {
        method: "PATCH",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ metadata: { description: "edited while disabled" } }),
      }),
    );
    expect(patch.status).toBe(200);
  });

  it("re-enabling lets invocations through again", async () => {
    const { server, registry } = await bootGateway({
      responses: [{ content: "ok", finishReason: "stop" }],
    });
    await registry.register({
      id: "support",
      backend: {
        type: "local",
        model: { provider: "anthropic", id: "claude-sonnet-4-6" },
        role: { inline: { systemPrompt: "Helpful", tools: [] } },
      },
      metadata: { description: null, capabilities: [], tags: [], enabled: false },
    });
    const blocked = await server.handle(
      new Request("http://test/api/agents/support/invoke", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ task: "hi", namespaceId: "acme", resourceId: "alice" }),
      }),
    );
    expect(blocked.status).toBe(410);
    await server.handle(
      new Request("http://test/api/agents/support", {
        method: "PATCH",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ metadata: { enabled: true } }),
      }),
    );
    const ok = await server.handle(
      new Request("http://test/api/agents/support/invoke", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ task: "hi", namespaceId: "acme", resourceId: "alice" }),
      }),
    );
    expect(ok.status).toBe(200);
  });
});

describe("agent gateway — turn gate (per-thread coordination)", () => {
  it("succeeds when the lease is free (no contention)", async () => {
    const { server } = await bootGateway({
      responses: [{ content: "ok", finishReason: "stop" }],
      withTurnGate: true,
    });
    const res = await server.handle(
      new Request("http://test/api/agents/support/threads/t-1", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ task: "hi", namespaceId: "acme", resourceId: "alice" }),
      }),
    );
    expect(res.status).toBe(200);
  });

  it("returns 409 turn_in_progress when another worker holds the lease", async () => {
    const { server } = await bootGateway({
      responses: [{ content: "ok", finishReason: "stop" }],
      preAcquireThread: { namespaceId: "acme", threadId: "t-1", ownerId: "worker-other" },
    });
    const res = await server.handle(
      new Request("http://test/api/agents/support/threads/t-1", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ task: "hi", namespaceId: "acme", resourceId: "alice" }),
      }),
    );
    expect(res.status).toBe(409);
    const body = (await res.json()) as {
      error: string;
      currentOwner: string;
      expiresAt: number;
    };
    expect(body.error).toBe("turn_in_progress");
    expect(body.currentOwner).toBe("worker-other");
    expect(typeof body.expiresAt).toBe("number");
  });

  it("releases the lease after a successful turn so the next request succeeds", async () => {
    const { server } = await bootGateway({
      responses: [
        { content: "first", finishReason: "stop" },
        { content: "second", finishReason: "stop" },
      ],
      withTurnGate: true,
    });
    const a = await server.handle(
      new Request("http://test/api/agents/support/threads/t-1", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ task: "hi", namespaceId: "acme", resourceId: "alice" }),
      }),
    );
    expect(a.status).toBe(200);
    // Same thread, second call. Should succeed (lease released by previous turn).
    const b = await server.handle(
      new Request("http://test/api/agents/support/threads/t-1", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ task: "again", namespaceId: "acme", resourceId: "alice" }),
      }),
    );
    expect(b.status).toBe(200);
  });

  it("different threads do not contend with each other", async () => {
    const { server, leaseStore } = await bootGateway({
      responses: [
        { content: "r1", finishReason: "stop" },
        { content: "r2", finishReason: "stop" },
      ],
      withTurnGate: true,
    });
    // Pre-acquire t-1 — t-2 should still succeed.
    await leaseStore!.acquire({
      key: { namespaceId: "acme", threadId: "t-1" },
      ownerId: "worker-other",
      ttlMs: 60_000,
    });
    const res = await server.handle(
      new Request("http://test/api/agents/support/threads/t-2", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ task: "hi", namespaceId: "acme", resourceId: "alice" }),
      }),
    );
    expect(res.status).toBe(200);
  });

  it("stream route also returns 409 on contention", async () => {
    const { server } = await bootGateway({
      responses: [{ content: "ok", finishReason: "stop" }],
      preAcquireThread: { namespaceId: "acme", threadId: "t-1", ownerId: "worker-other" },
    });
    const res = await server.handle(
      new Request("http://test/api/agents/support/threads/t-1/stream", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ task: "hi", namespaceId: "acme", resourceId: "alice" }),
      }),
    );
    expect(res.status).toBe(409);
    const body = (await res.json()) as { error: string };
    expect(body.error).toBe("turn_in_progress");
  });

  it("without a turnGate configured, contention is not enforced (single-process default)", async () => {
    const { server } = await bootGateway({
      responses: [{ content: "ok", finishReason: "stop" }],
      // withTurnGate omitted — gate is undefined
    });
    const res = await server.handle(
      new Request("http://test/api/agents/support/threads/t-1", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ task: "hi", namespaceId: "acme", resourceId: "alice" }),
      }),
    );
    expect(res.status).toBe(200);
  });
});

describe("agent gateway — distill rate limit (429)", () => {
  it("POST /distill returns 429 with Retry-After when consolidator is rate-limited", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const memory = new InMemoryMemoryStore();
    const registry = new InMemoryAgentRegistry();

    await registry.register({
      id: "support",
      backend: {
        type: "local",
        model: { provider: "anthropic", id: "claude-sonnet-4-6" },
        role: { inline: { systemPrompt: "Helpful", tools: [] } },
      },
      metadata: { capabilities: ["chat"], tags: [] },
    });

    // Pre-seed enough distill episodes to hit the cap.
    const SCOPE = { namespaceId: "acme", resourceId: "alice" };
    await memory.appendResourceEpisode(SCOPE, {
      summary: "old distill",
      sourceThreadId: "earlier",
      salience: 0.5,
      facts: [],
      metadata: { kind: "distill" },
    });

    const llm = mockLLM([{ content: "ignored", finishReason: "stop" }]);
    const inner = new DefaultConsolidator({ store: memory, llm });
    const rateLimited = new RateLimitedConsolidator(inner, memory, {
      windowMs: 600_000,
      maxDistillsPerWindow: 1,
    });

    const workflows = new LocalWorkflows({
      storage,
      runner,
      definitions: {},
      sleepScanIntervalMs: 0,
    });
    const agents = new ZoryaAgents({
      registry,
      resolve: (recipe) =>
        resolveLocalAgent(recipe, {
          runner,
          memory,
          llm: () => llm,
          tools: {},
          consolidator: rateLimited,
        }),
      memory,
    });
    const server = new ZoryaServer({ workflows, agents });

    const res = await server.handle(
      new Request("http://test/api/agents/support/threads/t-now/distill", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ namespaceId: "acme", resourceId: "alice" }),
      }),
    );

    expect(res.status).toBe(429);
    const retryAfter = res.headers.get("retry-after");
    expect(retryAfter).not.toBeNull();
    expect(Number.parseInt(retryAfter ?? "0", 10)).toBeGreaterThan(0);
    const body = (await res.json()) as {
      error: string;
      max: number;
      seen: number;
      retryAfterSeconds: number;
    };
    expect(body.error).toBe("distill_rate_limited");
    expect(body.max).toBe(1);
    expect(body.seen).toBe(1);
    expect(body.retryAfterSeconds).toBeGreaterThan(0);
  });
});
