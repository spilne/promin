// ---------------------------------------------------------------------------
// Thread ownership through `ZoryaServer.handle`: a thread belongs to the
// (namespace, resource, agent) that created it. Every thread route
// requires a scope identity, answers a foreign thread exactly like a
// missing one, and never lists another resource's or agent's threads.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { resolveLocalAgent, type LLMProvider } from "@promin/agent";
import { createWorkflowRunner } from "@promin/workflow";
import { bootAgents, jsonOf, req } from "./agent-gateway-harness.ts";

async function aliceThread() {
  const h = await bootAgents({ reply: () => "secret reply for alice" });
  const res = await h.server.handle(
    req("POST", "/api/agents/support/threads/t1", {
      task: "my secret",
      namespaceId: "ns",
      resourceId: "alice",
    }),
  );
  expect(res.status).toBe(200);
  return h;
}

describe("thread isolation — reads", () => {
  it("another resource cannot read the thread's messages, trace or memory", async () => {
    const { server } = await aliceThread();
    for (const path of [
      "/api/agents/support/threads/t1/messages?namespaceId=ns&resourceId=bob",
      "/api/agents/support/threads/t1/trace?namespaceId=ns&resourceId=bob",
    ]) {
      const res = await server.handle(req("GET", path));
      expect([path, res.status]).toEqual([path, 404]);
      expect(await res.text()).not.toContain("secret");
    }
    const mem = await server.handle(
      req("GET", "/api/memory/inspect?namespaceId=ns&resourceId=bob&threadId=t1"),
    );
    expect(mem.status).toBe(200);
    const body = await jsonOf<{ thread: { row: unknown; messages: unknown[] } }>(mem);
    expect(body.thread.row).toBeNull();
    expect(body.thread.messages).toEqual([]);
  });

  it("thread routes without a scope identity are rejected", async () => {
    const { server } = await aliceThread();
    const noRes = await server.handle(
      req("GET", "/api/agents/support/threads/t1/messages?namespaceId=ns"),
    );
    expect(noRes.status).toBe(400);
    expect((await jsonOf<{ error: string }>(noRes)).error).toBe("missing_scope_identity");
    const list = await server.handle(req("GET", "/api/agents/support/threads?namespaceId=ns"));
    expect(list.status).toBe(400);
  });

  it("the owner reads its own thread", async () => {
    const { server } = await aliceThread();
    const res = await server.handle(
      req("GET", "/api/agents/support/threads/t1/messages?namespaceId=ns&resourceId=alice"),
    );
    expect(res.status).toBe(200);
    const body = await jsonOf<{ messages: Array<{ content: string }> }>(res);
    expect(body.messages.map((m) => m.content)).toContain("my secret");
    const mem = await server.handle(
      req("GET", "/api/memory/inspect?namespaceId=ns&resourceId=alice&threadId=t1&agentId=support"),
    );
    const snap = await jsonOf<{ thread: { messages: unknown[] } }>(mem);
    expect(snap.thread.messages.length).toBeGreaterThan(0);
  });

  it("another agent cannot list or open the thread", async () => {
    const { server } = await aliceThread();
    const list = await server.handle(
      req("GET", "/api/agents/other/threads?namespaceId=ns&resourceId=alice"),
    );
    expect(list.status).toBe(200);
    expect((await jsonOf<{ threads: unknown[] }>(list)).threads).toEqual([]);
    const msgs = await server.handle(
      req("GET", "/api/agents/other/threads/t1/messages?namespaceId=ns&resourceId=alice"),
    );
    expect(msgs.status).toBe(404);
    const own = await server.handle(
      req("GET", "/api/agents/support/threads?namespaceId=ns&resourceId=alice"),
    );
    expect(
      (await jsonOf<{ threads: Array<{ id: string }> }>(own)).threads.map((t) => t.id),
    ).toEqual(["t1"]);
  });

  it("listing never returns another resource's threads", async () => {
    const { server } = await aliceThread();
    const res = await server.handle(
      req("GET", "/api/agents/support/threads?namespaceId=ns&resourceId=bob"),
    );
    expect((await jsonOf<{ threads: unknown[] }>(res)).threads).toEqual([]);
  });
});

describe("thread isolation — writes", () => {
  it("another resource cannot append to the thread (and nothing is written)", async () => {
    const { server, memory } = await aliceThread();
    const before = (await memory.getMessages({ namespaceId: "ns", threadId: "t1" })).length;
    const res = await server.handle(
      req("POST", "/api/agents/support/threads/t1", {
        task: "bob writes",
        namespaceId: "ns",
        resourceId: "bob",
      }),
    );
    expect(res.status).toBe(404);
    const stream = await server.handle(
      req("POST", "/api/agents/support/threads/t1/stream", {
        task: "bob streams",
        namespaceId: "ns",
        resourceId: "bob",
      }),
    );
    expect(stream.status).toBe(404);
    expect((await memory.getMessages({ namespaceId: "ns", threadId: "t1" })).length).toBe(before);
  });

  it("another agent cannot append to the thread", async () => {
    const { server } = await aliceThread();
    const res = await server.handle(
      req("POST", "/api/agents/other/threads/t1", {
        task: "hijack",
        namespaceId: "ns",
        resourceId: "alice",
      }),
    );
    expect(res.status).toBe(404);
  });

  it("rename, archive, compact, distill and approve on a foreign thread answer 404", async () => {
    const { server, memory } = await aliceThread();
    const bob = { namespaceId: "ns", resourceId: "bob" };
    const calls: Array<[string, string, unknown]> = [
      ["PATCH", "/api/agents/support/threads/t1", { ...bob, title: "pwned" }],
      ["POST", "/api/agents/support/threads/t1/archive", bob],
      ["POST", "/api/agents/support/threads/t1/compact", bob],
      ["POST", "/api/agents/support/threads/t1/distill", bob],
      [
        "POST",
        "/api/agents/support/threads/t1/approve",
        { ...bob, toolCallId: "c", approved: true },
      ],
    ];
    for (const [method, path, body] of calls) {
      const res = await server.handle(req(method, path, body));
      expect([path, res.status]).toEqual([path, 404]);
    }
    const row = await memory.getThread({ namespaceId: "ns", threadId: "t1" });
    expect(row?.title).toBeNull();
    expect(row?.archivedAt).toBeNull();
  });

  it("ownerId scopes threads to the owner's instance", async () => {
    const { server } = await bootAgents();
    const send = (ownerId: string) =>
      server.handle(
        req("POST", "/api/agents/support/threads/shared", {
          task: "hi",
          namespaceId: "ns",
          ownerId,
        }),
      );
    expect((await send("u-1")).status).toBe(200);
    expect((await send("u-2")).status).toBe(404);
    const read = await server.handle(
      req("GET", "/api/agents/support/threads/shared/messages?namespaceId=ns&ownerId=u-1"),
    );
    expect(read.status).toBe(200);
  });
});

describe("scope authorization hook", () => {
  it("authorizeScope binds the requested resource to the caller (403 otherwise)", async () => {
    const { server } = await bootAgents({
      authorizeScope: ({ req: r, scope }) => r.headers.get("x-user") === scope.resourceId,
    });
    const asAlice = { "x-user": "alice" };
    const ok = await server.handle(
      req(
        "POST",
        "/api/agents/support/threads/t1",
        { task: "hi", namespaceId: "ns", resourceId: "alice" },
        asAlice,
      ),
    );
    expect(ok.status).toBe(200);
    const spoof = await server.handle(
      req(
        "GET",
        "/api/agents/support/threads/t1/messages?namespaceId=ns&resourceId=bob",
        undefined,
        asAlice,
      ),
    );
    expect(spoof.status).toBe(403);
    expect((await jsonOf<{ error: string }>(spoof)).error).toBe("scope_forbidden");
    const mem = await server.handle(
      req("GET", "/api/memory/inspect?namespaceId=ns&resourceId=bob", undefined, asAlice),
    );
    expect(mem.status).toBe(403);
  });
});

describe("thread isolation — threads created outside the gateway", () => {
  it("a thread a LocalAgent created programmatically is listed and opened by its agent only", async () => {
    const h = await bootAgents();
    const recipe = await h.registry.get("support");
    const llm: LLMProvider = { chat: async () => ({ content: "ok", finishReason: "stop" }) };
    const agent = resolveLocalAgent(recipe!, {
      runner: createWorkflowRunner({ storage: h.storage }),
      memory: h.memory,
      llm: () => llm,
      tools: {},
    }).withScope({ namespaceId: "ns", resourceId: "alice" });
    const thread = await agent.thread("prog-1");
    await thread.send({ task: "made outside the gateway" });

    const own = await h.server.handle(
      req("GET", "/api/agents/support/threads?namespaceId=ns&resourceId=alice"),
    );
    expect(
      (await jsonOf<{ threads: Array<{ id: string }> }>(own)).threads.map((t) => t.id),
    ).toEqual(["prog-1"]);
    const msgs = await h.server.handle(
      req("GET", "/api/agents/support/threads/prog-1/messages?namespaceId=ns&resourceId=alice"),
    );
    expect(msgs.status).toBe(200);
    const body = await jsonOf<{ messages: Array<{ content: string }> }>(msgs);
    expect(body.messages.map((m) => m.content)).toContain("made outside the gateway");

    const foreign = await h.server.handle(
      req("GET", "/api/agents/other/threads/prog-1/messages?namespaceId=ns&resourceId=alice"),
    );
    expect(foreign.status).toBe(404);
  });
});
