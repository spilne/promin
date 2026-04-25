// ---------------------------------------------------------------------------
// Memory inspector route — verifies the per-scope snapshot + resolveContext
// summary stay consistent with the underlying MemoryStore.
//
// Pins:
//   - 400 when namespaceId missing
//   - namespace-only inspection returns ns row + facts; resource/thread null
//   - resource scope appears when resourceId given
//   - thread scope appears when threadId given, including resolved system
//     prompt and the message tail
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { InMemoryMemoryStore } from "@promin/agent";
import { ZoryaServer } from "../../server/server.ts";
import { InMemoryWorkflowStorage } from "@promin/workflow";

function makeServer(memory: InMemoryMemoryStore) {
  return new ZoryaServer({
    storage: new InMemoryWorkflowStorage(),
    memoryInspector: { memory },
  });
}

describe("memory inspector — /api/memory/inspect", () => {
  it("400 without namespaceId", async () => {
    const server = makeServer(new InMemoryMemoryStore());
    const res = await server.handle(
      new Request("http://test/api/memory/inspect", { method: "GET" }),
    );
    expect(res.status).toBe(400);
  });

  it("returns namespace snapshot when only namespaceId given", async () => {
    const memory = new InMemoryMemoryStore();
    await memory.upsertNamespace("acme", { staticRules: "be polite" });
    await memory.appendNamespaceFact("acme", "company name is Acme");

    const server = makeServer(memory);
    const res = await server.handle(
      new Request("http://test/api/memory/inspect?namespaceId=acme", { method: "GET" }),
    );
    expect(res.status).toBe(200);
    const body = (await res.json()) as {
      namespaceId: string;
      namespace: { row: { staticRules: string | null }; facts: Array<{ text: string }> };
      resource: unknown;
      thread: unknown;
      resolved: unknown;
    };
    expect(body.namespaceId).toBe("acme");
    expect(body.namespace.row?.staticRules).toBe("be polite");
    expect(body.namespace.facts.map((f) => f.text)).toEqual(["company name is Acme"]);
    expect(body.resource).toBeNull();
    expect(body.thread).toBeNull();
    expect(body.resolved).toBeNull();
  });

  it("includes resource + thread snapshots and the resolved system prompt", async () => {
    const memory = new InMemoryMemoryStore();
    await memory.upsertNamespace("acme", { staticRules: "be polite" });
    await memory.upsertResource(
      { namespaceId: "acme", resourceId: "alice" },
      { workingMemory: "alice prefers terse replies" },
    );
    await memory.appendResourceFact({ namespaceId: "acme", resourceId: "alice" }, "is in EU");
    await memory.createThread({
      namespaceId: "acme",
      resourceId: "alice",
      threadId: "t1",
    });
    await memory.appendMessages({ namespaceId: "acme", resourceId: "alice", threadId: "t1" }, [
      { role: "user", content: "hi" },
      { role: "assistant", content: "hello there" },
    ]);

    const server = makeServer(memory);
    const res = await server.handle(
      new Request("http://test/api/memory/inspect?namespaceId=acme&resourceId=alice&threadId=t1", {
        method: "GET",
      }),
    );
    expect(res.status).toBe(200);
    const body = (await res.json()) as {
      resource: { row: { workingMemory: string | null }; facts: Array<{ text: string }> };
      thread: {
        row: { threadId: string };
        messages: Array<{ role: string; content: string | null }>;
      };
      resolved: { systemPrompt: string; messageCount: number };
    };
    expect(body.resource.row?.workingMemory).toContain("terse");
    expect(body.resource.facts.map((f) => f.text)).toEqual(["is in EU"]);
    expect(body.thread.row?.threadId).toBe("t1");
    expect(body.thread.messages.map((m) => m.role)).toEqual(["user", "assistant"]);
    expect(body.resolved.systemPrompt).toContain("be polite");
    expect(body.resolved.systemPrompt).toContain("terse");
    expect(body.resolved.messageCount).toBe(2);
  });
});

describe("namespace mutations — operator-only writes", () => {
  it("PATCH /api/memory/namespace/:id upserts staticRules + workingMemory", async () => {
    const memory = new InMemoryMemoryStore();
    const server = makeServer(memory);
    const res = await server.handle(
      new Request("http://test/api/memory/namespace/acme", {
        method: "PATCH",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          staticRules: "be polite",
          workingMemory: "shipping v0.5 this week",
        }),
      }),
    );
    expect(res.status).toBe(200);
    const row = await memory.getNamespace("acme");
    expect(row?.staticRules).toBe("be polite");
    expect(row?.workingMemory).toBe("shipping v0.5 this week");
  });

  it("PATCH preserves omitted fields — sending only staticRules does not clear workingMemory", async () => {
    const memory = new InMemoryMemoryStore();
    await memory.upsertNamespace("acme", { workingMemory: "in flight" });
    const server = makeServer(memory);
    const res = await server.handle(
      new Request("http://test/api/memory/namespace/acme", {
        method: "PATCH",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ staticRules: "new rule" }),
      }),
    );
    expect(res.status).toBe(200);
    const row = await memory.getNamespace("acme");
    expect(row?.staticRules).toBe("new rule");
    expect(row?.workingMemory).toBe("in flight");
  });

  it("PATCH 400s on empty body", async () => {
    const server = makeServer(new InMemoryMemoryStore());
    const res = await server.handle(
      new Request("http://test/api/memory/namespace/acme", {
        method: "PATCH",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({}),
      }),
    );
    expect(res.status).toBe(400);
  });

  it("POST /facts appends and the next inspect lists it", async () => {
    const memory = new InMemoryMemoryStore();
    const server = makeServer(memory);
    const res = await server.handle(
      new Request("http://test/api/memory/namespace/acme/facts", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ text: "company name is Acme" }),
      }),
    );
    expect(res.status).toBe(201);
    const facts = await memory.listNamespaceFacts("acme");
    expect(facts.map((f) => f.text)).toEqual(["company name is Acme"]);
  });

  it("POST /facts 400s on missing text", async () => {
    const server = makeServer(new InMemoryMemoryStore());
    const res = await server.handle(
      new Request("http://test/api/memory/namespace/acme/facts", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ text: "   " }),
      }),
    );
    expect(res.status).toBe(400);
  });

  it("DELETE /facts/:factId removes the fact", async () => {
    const memory = new InMemoryMemoryStore();
    const fact = await memory.appendNamespaceFact("acme", "transient");
    const server = makeServer(memory);
    const res = await server.handle(
      new Request(`http://test/api/memory/namespace/acme/facts/${encodeURIComponent(fact.id)}`, {
        method: "DELETE",
      }),
    );
    expect(res.status).toBe(200);
    expect(await memory.listNamespaceFacts("acme")).toEqual([]);
  });
});
