// ---------------------------------------------------------------------------
// Recipe correctness through `ZoryaServer.handle`: metadata merge on PATCH,
// create-only POST / clone / extract-role, active-version edits, backend
// and secrets validation, If-Match, file-managed refusal, role / skill
// edits and referential integrity, instance ownership, memory validation,
// thread housekeeping, and 500 bodies that never carry internals.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { SecretScope, type RegisteredAgent } from "@promin/agent";
import { bootAgents, jsonOf, localBackend, req } from "./agent-gateway-harness.ts";

describe("agent recipes — PATCH", () => {
  it("merges metadata: a description edit keeps a disabled agent disabled and keeps its tags", async () => {
    const { server, registry } = await bootAgents();
    await registry.register({
      id: "support",
      backend: localBackend,
      metadata: { description: "d", capabilities: ["chat"], tags: ["beta"], enabled: false },
    });
    const res = await server.handle(
      req("PATCH", "/api/agents/support", { metadata: { description: "new" } }),
    );
    expect(res.status).toBe(200);
    const after = await registry.get("support");
    expect(after?.metadata).toEqual({
      description: "new",
      capabilities: ["chat"],
      tags: ["beta"],
      enabled: false,
    });
  });

  it("editing an older version is refused, so it never becomes the live one", async () => {
    const { server, registry } = await bootAgents();
    await registry.register({ id: "support", version: "v2", backend: localBackend });
    const res = await server.handle(
      req("PATCH", "/api/agents/support", { version: "v1", metadata: { tags: ["x"] } }),
    );
    expect(res.status).toBe(409);
    expect((await jsonOf<{ error: string }>(res)).error).toBe("version_not_active");
    expect((await registry.get("support"))?.version).toBe("v2");
  });

  it("validates the backend and metadata", async () => {
    const { server } = await bootAgents();
    for (const body of [
      { backend: { type: "local" } },
      { backend: { type: "remote", endpoint: "not a url", remoteAgentId: "x" } },
      { backend: { type: "martian" } },
      { metadata: { enabled: "yes" } },
      { metadata: { tags: "a,b" } },
    ]) {
      const res = await server.handle(req("PATCH", "/api/agents/support", body));
      expect([JSON.stringify(body), res.status]).toEqual([JSON.stringify(body), 400]);
    }
  });

  it("If-Match makes the edit conditional on the version read", async () => {
    const { server, clock } = await bootAgents();
    const got = await server.handle(req("GET", "/api/agents/support"));
    clock.advance(1);
    const etag = got.headers.get("etag")!;
    expect(etag).toBeTruthy();
    const stale = await server.handle(
      req("PATCH", "/api/agents/support", { metadata: { tags: ["a"] } }, { "if-match": '"nope"' }),
    );
    expect(stale.status).toBe(412);
    const ok = await server.handle(
      req("PATCH", "/api/agents/support", { metadata: { tags: ["a"] } }, { "if-match": etag }),
    );
    expect(ok.status).toBe(200);
    expect(ok.headers.get("etag")).not.toBe(etag);
  });
});

describe("agent recipes — create-only", () => {
  it("POST /api/agents with an existing (id, version) is 409 and leaves it untouched", async () => {
    const { server, registry } = await bootAgents();
    const before = await registry.get("support");
    const res = await server.handle(
      req("POST", "/api/agents", { id: "support", backend: localBackend }),
    );
    expect(res.status).toBe(409);
    expect(await registry.get("support")).toEqual(before);
    // A new version is a publish, and becomes the active one.
    const pub = await server.handle(
      req("POST", "/api/agents", { id: "support", version: "v2", backend: localBackend }),
    );
    expect(pub.status).toBe(201);
    expect((await registry.get("support"))?.version).toBe("v2");
  });

  it("POST /api/agents rejects an invalid backend", async () => {
    const { server, registry } = await bootAgents();
    const res = await server.handle(
      req("POST", "/api/agents", { id: "bad", backend: { type: "local" } }),
    );
    expect(res.status).toBe(400);
    expect(await registry.get("bad")).toBeNull();
  });

  it("clone onto an existing id is 409", async () => {
    const { server, registry } = await bootAgents();
    const before = await registry.get("support");
    const res = await server.handle(
      req("POST", "/api/agents/other/clone", { targetId: "support" }),
    );
    expect(res.status).toBe(409);
    expect(await registry.get("support")).toEqual(before);
  });

  it("clone validates secrets with the /api/secrets rules", async () => {
    const { server, secrets, registry } = await bootAgents({ secrets: true });
    for (const secretsBody of ["abc", { "bad key!": "v" }, { ok: 123 }, ["x"]]) {
      const res = await server.handle(
        req("POST", "/api/agents/support/clone", { targetId: "c1", secrets: secretsBody }),
      );
      expect([JSON.stringify(secretsBody), res.status]).toEqual([JSON.stringify(secretsBody), 400]);
    }
    expect(await registry.get("c1")).toBeNull();
    expect(await secrets!.list({ scope: SecretScope.global() })).toEqual([]);
    const ok = await server.handle(
      req("POST", "/api/agents/support/clone", { targetId: "c1", secrets: { API_KEY: "v" } }),
    );
    expect(ok.status).toBe(201);
    expect(await secrets!.list({ scope: SecretScope.global() })).toEqual(["API_KEY"]);
  });

  it("extract-role is create-only on the role id", async () => {
    const { server, roles, registry } = await bootAgents();
    await roles.register({ id: "taken", definition: { systemPrompt: "x", tools: [] } });
    const res = await server.handle(
      req("POST", "/api/agents/support/extract-role", { roleId: "taken" }),
    );
    expect(res.status).toBe(409);
    expect((await roles.get("taken"))?.definition.systemPrompt).toBe("x");
    const recipe = (await registry.get("support")) as RegisteredAgent;
    expect(recipe.backend.type === "local" && "inline" in recipe.backend.role).toBe(true);
  });
});

describe("agent recipes — file-managed and disabled agents", () => {
  it("read and housekeeping routes keep working on a disabled agent; invoking is 410", async () => {
    const { server, registry } = await bootAgents();
    const send = await server.handle(
      req("POST", "/api/agents/support/threads/t1", {
        task: "hi",
        namespaceId: "ns",
        resourceId: "alice",
      }),
    );
    expect(send.status).toBe(200);
    await registry.register({ id: "support", backend: localBackend, metadata: { enabled: false } });
    const scope = "namespaceId=ns&resourceId=alice";
    for (const path of [
      `/api/agents/support/threads?${scope}`,
      `/api/agents/support/threads/t1/messages?${scope}`,
      `/api/agents/support/threads/t1/trace?${scope}`,
    ]) {
      expect([path, (await server.handle(req("GET", path))).status]).toEqual([path, 200]);
    }
    const rename = await server.handle(
      req("PATCH", "/api/agents/support/threads/t1", {
        namespaceId: "ns",
        resourceId: "alice",
        title: "Hello",
      }),
    );
    expect(rename.status).toBe(200);
    const renamed = await jsonOf<{ title: string; metadata: Record<string, unknown> }>(rename);
    expect(renamed.title).toBe("Hello");
    expect(renamed.metadata.agentId).toBe("support");
    const invoke = await server.handle(
      req("POST", "/api/agents/support/invoke", { task: "x", namespaceId: "ns", resourceId: "a" }),
    );
    expect(invoke.status).toBe(410);
  });
});

describe("thread housekeeping inputs", () => {
  async function withThread() {
    const h = await bootAgents();
    await h.server.handle(
      req("POST", "/api/agents/support/threads/t1", {
        task: "hi",
        namespaceId: "ns",
        resourceId: "alice",
      }),
    );
    return h;
  }

  it("archive validates archivedAt", async () => {
    const { server } = await withThread();
    const bad = await server.handle(
      req("POST", "/api/agents/support/threads/t1/archive", {
        namespaceId: "ns",
        resourceId: "alice",
        archivedAt: "yesterday",
      }),
    );
    expect(bad.status).toBe(400);
  });

  it("compact validates keepRecent and answers 409 when there is nothing to compact", async () => {
    const { server } = await withThread();
    const neg = await server.handle(
      req("POST", "/api/agents/support/threads/t1/compact", {
        namespaceId: "ns",
        resourceId: "alice",
        keepRecent: -1,
      }),
    );
    expect(neg.status).toBe(400);
    const empty = await server.handle(
      req("POST", "/api/agents/support/threads/t1/compact", {
        namespaceId: "ns",
        resourceId: "alice",
        keepRecent: 50,
      }),
    );
    expect(empty.status).toBe(409);
    expect((await jsonOf<{ error: string }>(empty)).error).toBe("nothing_to_compact");
  });

  it("approve for an unknown approval is 404 and creates no thread", async () => {
    const { server, memory } = await withThread();
    const res = await server.handle(
      req("POST", "/api/agents/support/threads/t1/approve", {
        namespaceId: "ns",
        resourceId: "alice",
        toolCallId: "nope",
        approved: true,
      }),
    );
    expect(res.status).toBe(404);
    expect((await jsonOf<{ error: string }>(res)).error).toBe("approval_not_found");
    const unknownThread = await server.handle(
      req("POST", "/api/agents/support/threads/ghost/approve", {
        namespaceId: "ns",
        resourceId: "alice",
        toolCallId: "nope",
        approved: true,
      }),
    );
    expect(unknownThread.status).toBe(404);
    expect(await memory.getThread({ namespaceId: "ns", threadId: "ghost" })).toBeNull();
  });
});

describe("500 bodies", () => {
  it("an unexpected agent failure answers a code and request id, never the message", async () => {
    const { server } = await bootAgents({
      wrapAgent: () => ({
        withScope() {
          return this;
        },
        invoke: async () => {
          throw new Error("postgres://admin:hunter2@db leaked");
        },
      }),
    });
    const res = await server.handle(
      req("POST", "/api/agents/support/invoke", { task: "x", namespaceId: "ns", resourceId: "a" }),
    );
    expect(res.status).toBe(500);
    const text = await res.text();
    expect(text).not.toContain("hunter2");
    const body = JSON.parse(text) as { error: string; requestId: string };
    expect(body.error).toBe("invoke_failed");
    expect(body.requestId).toMatch(/[0-9a-f-]{36}/);
  });
});

describe("roles and skills", () => {
  it("role POST is create-only and PATCH merges metadata", async () => {
    const { server, roles } = await bootAgents();
    const def = { systemPrompt: "x", tools: [] };
    expect(
      (await server.handle(req("POST", "/api/roles", { id: "r", definition: def }))).status,
    ).toBe(201);
    expect(
      (await server.handle(req("POST", "/api/roles", { id: "r", definition: def }))).status,
    ).toBe(409);
    await server.handle(
      req("PATCH", "/api/roles/r", { metadata: { description: "desc", tags: ["t"] } }),
    );
    await server.handle(req("PATCH", "/api/roles/r", { metadata: { description: "new" } }));
    expect((await roles.get("r"))?.metadata.tags).toEqual(["t"]);
  });

  it("deleting a role an agent binds is 409 unless forced", async () => {
    const { server, registry } = await bootAgents();
    await server.handle(
      req("POST", "/api/roles", { id: "r", definition: { systemPrompt: "x", tools: [] } }),
    );
    await registry.register({
      id: "bound",
      backend: { ...localBackend, role: { ref: { id: "r" } } },
    });
    const res = await server.handle(req("DELETE", "/api/roles/r"));
    expect(res.status).toBe(409);
    expect((await jsonOf<{ agents: string[] }>(res)).agents).toEqual(["bound"]);
    expect((await server.handle(req("DELETE", "/api/roles/r?force=true"))).status).toBe(204);
    expect((await server.handle(req("DELETE", "/api/roles/r"))).status).toBe(404);
  });
});

describe("agent instances", () => {
  it("PATCH / DELETE through another agent's path are 404 and change nothing", async () => {
    const { server, instances } = await bootAgents();
    const inst = await instances.resolveOrCreate({
      registeredAgentId: "support",
      namespaceId: "ns",
      ownerId: "u",
    });
    const path = `/api/agents/other/instances/${encodeURIComponent(inst.id)}`;
    expect((await server.handle(req("PATCH", path, { displayName: "x" }))).status).toBe(404);
    expect((await server.handle(req("DELETE", path))).status).toBe(404);
    expect((await instances.get(inst.id))?.displayName).toBeNull();
    const own = `/api/agents/support/instances/${encodeURIComponent(inst.id)}`;
    expect((await server.handle(req("PATCH", own, { displayName: "ok" }))).status).toBe(200);
  });

  it("creating an instance of an unknown agent is 404", async () => {
    const { server } = await bootAgents();
    const res = await server.handle(
      req("POST", "/api/agents/ghost/instances", { namespaceId: "ns", ownerId: "u" }),
    );
    expect(res.status).toBe(404);
  });
});

describe("memory routes", () => {
  it("validates namespace patches and 404s an unknown fact", async () => {
    const { server, memory } = await bootAgents();
    const bad = await server.handle(req("PATCH", "/api/memory/namespace/ns", { staticRules: 123 }));
    expect(bad.status).toBe(400);
    expect(await memory.getNamespace("ns")).toBeNull();
    const missing = await server.handle(req("DELETE", "/api/memory/namespace/ns/facts/nope"));
    expect(missing.status).toBe(404);
  });

  it("resolves the persona of an agent bound to a role by ref", async () => {
    const { server, roles, registry } = await bootAgents();
    await roles.register({
      id: "r",
      definition: { systemPrompt: "I am the ref persona", tools: [] },
    });
    await registry.register({
      id: "reffed",
      backend: { ...localBackend, role: { ref: { id: "r" } } },
    });
    const res = await server.handle(
      req("GET", "/api/memory/inspect?namespaceId=ns&agentId=reffed"),
    );
    expect((await jsonOf<{ persona: string }>(res)).persona).toBe("I am the ref persona");
  });
});
