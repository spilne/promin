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
  InMemoryAgentInstanceRegistry,
  InMemoryAgentRegistry,
  InMemoryMemoryStore,
  resolveLocalAgent,
} from "@promin/agent";
import type { AgentInstanceRegistry, LLMProvider, LLMResponse } from "@promin/agent";
import { InMemoryWorkflowStorage, createWorkflowRunner } from "@promin/workflow";
import { ZoryaServer } from "../../server/server.ts";

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

async function bootGateway(opts?: { responses?: LLMResponse[]; withInstances?: boolean }) {
  const storage = new InMemoryWorkflowStorage();
  const runner = createWorkflowRunner({ storage });
  const memory = new InMemoryMemoryStore();
  const registry = new InMemoryAgentRegistry();
  const instanceRegistry: AgentInstanceRegistry | undefined = opts?.withInstances
    ? new InMemoryAgentInstanceRegistry()
    : undefined;

  await registry.register({
    id: "support",
    backend: {
      type: "local",
      model: { provider: "anthropic", id: "claude-sonnet-4-6" },
      systemPrompt: "Helpful",
      tools: [],
    },
    metadata: { capabilities: ["chat"], tags: ["beta"] },
  });

  const responses = opts?.responses ?? [{ content: "ok", finishReason: "stop" }];
  const server = new ZoryaServer({
    storage,
    agents: {
      registry,
      ...(instanceRegistry ? { instanceRegistry } : {}),
      resolve: (recipe) =>
        resolveLocalAgent(recipe, {
          runner,
          memory,
          llm: () => mockLLM(responses),
          tools: {},
        }),
    },
  });

  return { server, storage, runner, memory, registry, instanceRegistry };
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
        body: JSON.stringify({ task: "hi", namespaceId: "acme" }),
      }),
    );
    expect(res.status).toBe(200);
    expect(res.headers.get("content-type")).toBe("text/event-stream");
    const text = await res.text();
    expect(text).toContain("event: finish");
    expect(text).toContain('"text":"streamed"');
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
