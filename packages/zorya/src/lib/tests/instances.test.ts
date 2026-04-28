// ---------------------------------------------------------------------------
// Instance routes — verifies the HTTP surface stays in lockstep with the
// `AgentInstanceRegistry` contract.
//
// Pins:
//   - POST resolveOrCreate is idempotent
//   - distinct triples produce distinct ids
//   - GET list filters by namespaceId / ownerId
//   - GET single 404s when the instance belongs to a different agent
//   - PATCH updates displayName / metadata; 404 on unknown id
//   - DELETE cascades to memory (working memory cleared, threads gone)
//   - cross-agent list at /api/instances returns rows from any agent
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { InMemoryAgentInstanceRegistry, InMemoryMemoryStore } from "@promin/agent";
import { ZoryaServer } from "../../server/server.ts";
import { InMemoryWorkflowStorage } from "@promin/workflow";

function makeServer() {
  const registry = new InMemoryAgentInstanceRegistry();
  const memory = new InMemoryMemoryStore();
  const server = new ZoryaServer({
    storage: new InMemoryWorkflowStorage(),
    instances: { registry, memory },
  });
  return { server, registry, memory };
}

describe("POST /api/agents/:id/instances — resolveOrCreate", () => {
  it("creates an instance on first call", async () => {
    const { server } = makeServer();
    const res = await server.handle(
      new Request("http://test/api/agents/writer/instances", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ namespaceId: "acme", ownerId: "alice" }),
      }),
    );
    expect(res.status).toBe(200);
    const body = (await res.json()) as { instance: { id: string; ownerId: string } };
    expect(body.instance.id).toBe("acme::writer::alice");
    expect(body.instance.ownerId).toBe("alice");
  });

  it("is idempotent for the same triple", async () => {
    const { server } = makeServer();
    const post = (b: object) =>
      server.handle(
        new Request("http://test/api/agents/writer/instances", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify(b),
        }),
      );
    const a = await (await post({ namespaceId: "acme", ownerId: "alice" })).json();
    const b = await (await post({ namespaceId: "acme", ownerId: "alice" })).json();
    expect((a as { instance: { id: string } }).instance.id).toBe(
      (b as { instance: { id: string } }).instance.id,
    );
  });

  it("400s on missing namespaceId or ownerId", async () => {
    const { server } = makeServer();
    const a = await server.handle(
      new Request("http://test/api/agents/writer/instances", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ ownerId: "alice" }),
      }),
    );
    expect(a.status).toBe(400);
    const b = await server.handle(
      new Request("http://test/api/agents/writer/instances", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ namespaceId: "acme" }),
      }),
    );
    expect(b.status).toBe(400);
  });
});

describe("GET /api/agents/:id/instances — list", () => {
  it("returns instances filtered to this agent", async () => {
    const { server, registry } = makeServer();
    await registry.resolveOrCreate({
      registeredAgentId: "writer",
      namespaceId: "acme",
      ownerId: "alice",
    });
    await registry.resolveOrCreate({
      registeredAgentId: "writer",
      namespaceId: "acme",
      ownerId: "bob",
    });
    await registry.resolveOrCreate({
      registeredAgentId: "reviewer",
      namespaceId: "acme",
      ownerId: "alice",
    });

    const res = await server.handle(new Request("http://test/api/agents/writer/instances"));
    const body = (await res.json()) as { instances: Array<{ ownerId: string }> };
    expect(body.instances.map((i) => i.ownerId).sort()).toEqual(["alice", "bob"]);
  });

  it("supports ?ownerId filter", async () => {
    const { server, registry } = makeServer();
    await registry.resolveOrCreate({
      registeredAgentId: "writer",
      namespaceId: "acme",
      ownerId: "alice",
    });
    await registry.resolveOrCreate({
      registeredAgentId: "writer",
      namespaceId: "acme",
      ownerId: "bob",
    });
    const res = await server.handle(
      new Request("http://test/api/agents/writer/instances?ownerId=alice"),
    );
    const body = (await res.json()) as { instances: Array<{ ownerId: string }> };
    expect(body.instances).toHaveLength(1);
    expect(body.instances[0]!.ownerId).toBe("alice");
  });
});

describe("GET /api/agents/:id/instances/:instanceId — single", () => {
  it("returns the instance when ids match", async () => {
    const { server, registry } = makeServer();
    const id = (
      await registry.resolveOrCreate({
        registeredAgentId: "writer",
        namespaceId: "acme",
        ownerId: "alice",
      })
    ).id;
    const res = await server.handle(
      new Request(`http://test/api/agents/writer/instances/${encodeURIComponent(id)}`),
    );
    expect(res.status).toBe(200);
  });

  it("404s when the path agent doesn't match the instance's agent", async () => {
    const { server, registry } = makeServer();
    const id = (
      await registry.resolveOrCreate({
        registeredAgentId: "writer",
        namespaceId: "acme",
        ownerId: "alice",
      })
    ).id;
    const res = await server.handle(
      new Request(`http://test/api/agents/reviewer/instances/${encodeURIComponent(id)}`),
    );
    expect(res.status).toBe(404);
  });
});

describe("PATCH /api/agents/:id/instances/:instanceId — update", () => {
  it("updates displayName + metadata", async () => {
    const { server, registry } = makeServer();
    const id = (
      await registry.resolveOrCreate({
        registeredAgentId: "writer",
        namespaceId: "acme",
        ownerId: "alice",
      })
    ).id;
    const res = await server.handle(
      new Request(`http://test/api/agents/writer/instances/${encodeURIComponent(id)}`, {
        method: "PATCH",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ displayName: "Alice's writer", metadata: { color: "blue" } }),
      }),
    );
    expect(res.status).toBe(200);
    const body = (await res.json()) as {
      instance: { displayName: string | null; metadata: Record<string, unknown> };
    };
    expect(body.instance.displayName).toBe("Alice's writer");
    expect(body.instance.metadata).toEqual({ color: "blue" });
  });

  it("404s on unknown id", async () => {
    const { server } = makeServer();
    const res = await server.handle(
      new Request("http://test/api/agents/writer/instances/acme::writer::ghost", {
        method: "PATCH",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ displayName: "x" }),
      }),
    );
    expect(res.status).toBe(404);
  });

  it("400s on empty patch", async () => {
    const { server, registry } = makeServer();
    const id = (
      await registry.resolveOrCreate({
        registeredAgentId: "writer",
        namespaceId: "acme",
        ownerId: "alice",
      })
    ).id;
    const res = await server.handle(
      new Request(`http://test/api/agents/writer/instances/${encodeURIComponent(id)}`, {
        method: "PATCH",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({}),
      }),
    );
    expect(res.status).toBe(400);
  });
});

describe("DELETE /api/agents/:id/instances/:instanceId — cascading wipe", () => {
  it("removes the row and clears resource memory", async () => {
    const { server, registry, memory } = makeServer();
    const instance = await registry.resolveOrCreate({
      registeredAgentId: "writer",
      namespaceId: "acme",
      ownerId: "alice",
    });
    // Stand up some state under resourceId = instance.id.
    await memory.upsertResource(
      { namespaceId: "acme", resourceId: instance.id },
      { workingMemory: "draft" },
    );
    await memory.createThread({
      namespaceId: "acme",
      resourceId: instance.id,
      threadId: "t1",
    });
    await memory.appendMessages({ namespaceId: "acme", resourceId: instance.id, threadId: "t1" }, [
      { role: "user", content: "hi" },
    ]);

    const res = await server.handle(
      new Request(`http://test/api/agents/writer/instances/${encodeURIComponent(instance.id)}`, {
        method: "DELETE",
      }),
    );
    expect(res.status).toBe(200);
    const body = (await res.json()) as { threadsDeleted: number };
    expect(body.threadsDeleted).toBe(1);

    expect(await registry.get(instance.id)).toBeNull();
    expect(await memory.listThreads({ namespaceId: "acme", resourceId: instance.id })).toHaveLength(
      0,
    );
    const row = await memory.getResource({ namespaceId: "acme", resourceId: instance.id });
    expect(row?.workingMemory).toBeNull();
  });
});

describe("GET /api/instances — cross-agent list", () => {
  it("returns instances for any agent in the namespace", async () => {
    const { server, registry } = makeServer();
    await registry.resolveOrCreate({
      registeredAgentId: "writer",
      namespaceId: "acme",
      ownerId: "alice",
    });
    await registry.resolveOrCreate({
      registeredAgentId: "reviewer",
      namespaceId: "acme",
      ownerId: "alice",
    });
    await registry.resolveOrCreate({
      registeredAgentId: "writer",
      namespaceId: "globex",
      ownerId: "alice",
    });

    const res = await server.handle(
      new Request("http://test/api/instances?namespaceId=acme&ownerId=alice"),
    );
    expect(res.status).toBe(200);
    const body = (await res.json()) as { instances: Array<{ registeredAgentId: string }> };
    expect(body.instances.map((i) => i.registeredAgentId).sort()).toEqual(["reviewer", "writer"]);
  });
});
