import { describe, expect, it } from "bun:test";
import { InMemoryAgentRegistry, InMemoryMemoryStore, resolveLocalAgent } from "@promin/agent";
import { InMemoryWorkflowStorage, createWorkflowRunner } from "@promin/workflow";
import { LocalWorkflows } from "../../workflows/local-workflows.ts";
import { RetainedSubagents, type RetainedParent } from "../retained-subagents.ts";
import { ZoryaAgents } from "../zorya-agents.ts";
import { ZoryaServer } from "../../../server.ts";

const parent: RetainedParent = {
  namespaceId: "acme",
  resourceId: "alice",
  agentId: "parent",
  threadId: "conversation",
};

interface HarnessOptions {
  readonly retained?: Partial<ConstructorParameters<typeof RetainedSubagents>[0]>;
  readonly wrapStorage?: (storage: InMemoryWorkflowStorage) => InMemoryWorkflowStorage;
  readonly chat?: (content: string) => string;
  readonly turnGate?: false;
}

/** Overrides one storage method, delegating everything else to the real store. */
function overrideStorage<K extends keyof InMemoryWorkflowStorage>(
  storage: InMemoryWorkflowStorage,
  method: K,
  replacement: (original: InMemoryWorkflowStorage[K]) => InMemoryWorkflowStorage[K],
): InMemoryWorkflowStorage {
  return new Proxy(storage, {
    get(target, prop) {
      const value = Reflect.get(target, prop, target);
      if (prop === method) return replacement((value as Function).bind(target) as never);
      return typeof value === "function" ? value.bind(target) : value;
    },
  });
}

async function harness(options: HarnessOptions = {}) {
  const baseStorage = new InMemoryWorkflowStorage();
  const storage = options.wrapStorage ? options.wrapStorage(baseStorage) : baseStorage;
  const memory = new InMemoryMemoryStore();
  const registry = new InMemoryAgentRegistry();
  const backend = {
    type: "local" as const,
    model: { provider: "test", id: "test" },
    role: { inline: { systemPrompt: "help", tools: [] as string[] } },
  };
  await registry.register({ id: "parent", backend });
  await registry.register({ id: "worker", backend });
  let runner = createWorkflowRunner({ storage });
  const make = () => {
    runner = createWorkflowRunner({ storage });
    const workflows = new LocalWorkflows({
      storage,
      runner,
      definitions: {},
      signalScanIntervalMs: 10,
      sleepScanIntervalMs: 10,
      orphanRecovery: false,
    });
    const agents = new ZoryaAgents({
      registry,
      memory,
      ...(options.turnGate === false && { turnGate: false as const }),
      resolve: (recipe) =>
        resolveLocalAgent(recipe, {
          runner,
          memory,
          tools: {},
          llm: () => ({
            chat: async (request) => ({
              content: options.chat
                ? options.chat(String(request.messages.findLast((m) => m.role === "user")?.content))
                : `reply:${request.messages.findLast((m) => m.role === "user")?.content ?? ""}`,
              finishReason: "stop" as const,
            }),
          }),
        }),
    });
    const retained = new RetainedSubagents({
      agents,
      workflows,
      memory,
      canDelegate: (from, to) => from === "parent" && to === "worker",
      ...options.retained,
    });
    return { workflows, agents, retained };
  };
  const current = make();
  const recipe = await registry.get({ id: "parent" });
  const agent = (await current.agents.resolve(recipe!, parent)).withScope(parent);
  await agent.thread(parent.threadId);
  return { storage, memory, make, ...current };
}

async function waitFor(predicate: () => Promise<boolean>) {
  for (let i = 0; i < 200; i++) {
    if (await predicate()) return;
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  throw new Error("condition not reached");
}

describe("retained subagents", () => {
  it("admits a child, preserves its inbox across a service restart, and isolates ownership", async () => {
    const h = await harness();
    await h.workflows.start();
    const child = await h.retained.spawnAgent({
      parent,
      agentId: "worker",
      task: "initial",
      childId: "child-1",
    });
    expect(child.status).toBe("admitted");
    await waitFor(
      async () =>
        (await h.retained.readAgentMessages(parent, child.childId)).transcript.length >= 2,
    );
    expect(await h.retained.listChildAgents(parent)).toHaveLength(1);
    await h.workflows.stop();
    const restarted = h.make();
    await restarted.retained.reconcileMailboxes();
    await restarted.workflows.start();
    await restarted.retained.messageAgent({
      parent,
      childId: child.childId,
      task: "follow up",
      mode: "follow-up",
      messageId: "m1",
    });
    await waitFor(
      async () =>
        (await restarted.retained.readAgentMessages(parent, child.childId)).transcript.length >= 4,
    );
    const messages = await restarted.retained.readAgentMessages(parent, child.childId);
    expect(messages.transcript.map((m) => m.content)).toContain("follow up");
    expect(
      (
        await restarted.retained.messageAgent({
          parent,
          childId: child.childId,
          task: "follow up",
          mode: "follow-up",
          messageId: "m1",
        })
      ).seq,
    ).toBe(1);
    await expect(
      restarted.retained.messageAgent({
        parent,
        childId: child.childId,
        task: "different",
        mode: "follow-up",
        messageId: "m1",
      }),
    ).rejects.toThrow("id conflict");
    await expect(
      restarted.retained.readAgentMessages({ ...parent, resourceId: "bob" }, child.childId),
    ).rejects.toThrow();
    await restarted.retained.stopAgent(parent, child.childId);
    await restarted.workflows.stop();
  });

  it("enforces capability and depth policy", async () => {
    const h = await harness();
    await expect(
      h.retained.spawnAgent({ parent, agentId: "parent", task: "denied" }),
    ).rejects.toThrow("capability");
  });

  it("steer supersedes queued work at the next turn boundary", async () => {
    const h = await harness();
    await h.workflows.start();
    const child = await h.retained.spawnAgent({
      parent,
      agentId: "worker",
      task: "initial",
      childId: "steered",
    });
    await waitFor(
      async () =>
        (await h.retained.readAgentMessages(parent, child.childId)).transcript.length >= 2,
    );
    await h.workflows.stop();
    await h.retained.messageAgent({
      parent,
      childId: child.childId,
      task: "old plan",
      mode: "queued",
      messageId: "q1",
    });
    await h.retained.messageAgent({
      parent,
      childId: child.childId,
      task: "new plan",
      mode: "steer",
      messageId: "s2",
    });
    await h.workflows.start();
    await waitFor(
      async () =>
        (await h.retained.readAgentMessages(parent, child.childId)).transcript.length >= 4,
    );
    const transcript = (await h.retained.readAgentMessages(parent, child.childId)).transcript.map(
      (m) => m.content,
    );
    expect(transcript).toContain("new plan");
    expect(transcript).not.toContain("old plan");
    await h.workflows.stop();
  });

  it("exposes scoped HTTP admission and mailbox routes", async () => {
    const h = await harness();
    const server = new ZoryaServer({
      insecure: true,
      workflows: h.workflows,
      agents: h.agents,
      retainedSubagents: h.retained,
    });
    const send = (method: string, path: string, body?: unknown) =>
      server.handle(
        new Request(`http://test${path}`, {
          method,
          headers: { "content-type": "application/json" },
          ...(body !== undefined && { body: JSON.stringify(body) }),
        }),
      );
    const created = await send("POST", "/api/agents/parent/threads/conversation/children", {
      namespaceId: "acme",
      resourceId: "alice",
      agentId: "worker",
      task: "work",
    });
    expect(created.status).toBe(202);
    const { childId } = (await created.json()) as { childId: string };
    expect(
      (
        await send(
          "GET",
          "/api/agents/parent/threads/conversation/children?namespaceId=acme&resourceId=alice",
        )
      ).status,
    ).toBe(200);
    expect(
      (
        await send(
          "GET",
          `/api/agents/parent/threads/conversation/children/${childId}/messages?namespaceId=acme&resourceId=bob`,
        )
      ).status,
    ).toBe(404);
  });

  it("runs concurrent children and cleans up one without changing the other", async () => {
    const h = await harness();
    await h.workflows.start();
    const [a, b] = await Promise.all(
      ["a", "b"].map((childId) =>
        h.retained.spawnAgent({ parent, agentId: "worker", task: childId, childId }),
      ),
    );
    await waitFor(
      async () =>
        (await h.retained.readAgentMessages(parent, a.childId)).transcript.length >= 2 &&
        (await h.retained.readAgentMessages(parent, b.childId)).transcript.length >= 2,
    );
    await h.retained.deleteChildAgent(parent, a.childId);
    await expect(h.retained.readAgentMessages(parent, a.childId)).rejects.toThrow("not found");
    expect((await h.retained.readAgentMessages(parent, b.childId)).transcript).toHaveLength(2);
    await h.workflows.stop();
  });

  it("survives storage returning reordered jsonb keys on spawn", async () => {
    const reorder = (value: unknown): unknown =>
      value && typeof value === "object" && !Array.isArray(value)
        ? Object.fromEntries(
            Object.entries(value as Record<string, unknown>)
              .reverse()
              .map(([k, v]) => [k, reorder(v)]),
          )
        : value;
    const h = await harness({
      wrapStorage: (storage) =>
        overrideStorage(storage, "loadWorkflow", (original) => async (id: string) => {
          const state = await original(id);
          return state && state.input ? { ...state, input: reorder(state.input) } : state;
        }) as InMemoryWorkflowStorage,
    });
    await h.workflows.start();
    const child = await h.retained.spawnAgent({
      parent,
      agentId: "worker",
      task: "initial",
      childId: "reordered",
    });
    expect(child.childId).toBe("reordered");
    await h.workflows.stop();
  });

  it("processes a message whose wake signal was lost and the ones after it", async () => {
    let failNext = false;
    const h = await harness({
      retained: { pollIntervalMs: 50 },
      wrapStorage: (storage) =>
        overrideStorage(
          storage,
          "deliverSignal",
          (original) =>
            (async (p: never) => {
              if (failNext) {
                failNext = false;
                throw new Error("signal write failed");
              }
              return original(p);
            }) as never,
        ),
    });
    await h.workflows.start();
    const child = await h.retained.spawnAgent({
      parent,
      agentId: "worker",
      task: "initial",
      childId: "lost",
    });
    await waitFor(
      async () =>
        (await h.retained.readAgentMessages(parent, child.childId)).transcript.length >= 2,
    );
    failNext = true;
    await h.retained.messageAgent({
      parent,
      childId: child.childId,
      task: "first",
      messageId: "m1",
    });
    await h.retained.messageAgent({
      parent,
      childId: child.childId,
      task: "second",
      messageId: "m2",
    });
    await waitFor(async () => {
      const t = (await h.retained.readAgentMessages(parent, child.childId)).transcript;
      return t.some((m) => m.content === "second") && t.some((m) => m.content === "first");
    });
    await h.workflows.stop();
  });

  it("completes an idle child after the idle TTL", async () => {
    const h = await harness({ retained: { pollIntervalMs: 30, idleTtlMs: 90 } });
    await h.workflows.start();
    const child = await h.retained.spawnAgent({
      parent,
      agentId: "worker",
      task: "initial",
      childId: "idle",
    });
    await waitFor(
      async () => (await h.retained.listChildAgents(parent))[0]?.status === "completed",
    );
    expect(child.childId).toBe("idle");
    await expect(
      h.retained.messageAgent({ parent, childId: "idle", task: "late" }),
    ).rejects.toThrow("stopped");
    await h.workflows.stop();
  });

  it("keeps the child alive after a failed turn and records the failure", async () => {
    const h = await harness({
      retained: { pollIntervalMs: 50 },
      chat: (content) => {
        if (content === "boom") throw new Error("model exploded");
        return `reply:${content}`;
      },
    });
    await h.workflows.start();
    const child = await h.retained.spawnAgent({
      parent,
      agentId: "worker",
      task: "initial",
      childId: "sturdy",
    });
    await waitFor(
      async () =>
        (await h.retained.readAgentMessages(parent, child.childId)).transcript.length >= 2,
    );
    await h.retained.messageAgent({ parent, childId: child.childId, task: "boom", messageId: "b" });
    await waitFor(async () =>
      (await h.retained.readAgentMessages(parent, child.childId)).outcomes.some((o) =>
        String(o.content).startsWith("failed"),
      ),
    );
    await h.retained.messageAgent({
      parent,
      childId: child.childId,
      task: "after",
      messageId: "a",
    });
    await waitFor(async () =>
      (await h.retained.readAgentMessages(parent, child.childId)).transcript.some(
        (m) => m.content === "after",
      ),
    );
    expect((await h.retained.listChildAgents(parent))[0]?.status).not.toBe("failed");
    await h.workflows.stop();
  });

  it("passes the caller scope to canDelegate and authorizes the child agent on the route", async () => {
    const seen: unknown[] = [];
    const h = await harness({
      retained: {
        canDelegate: (_from, _to, options) => {
          seen.push(options.scope);
          return true;
        },
      },
    });
    await h.retained.spawnAgent({ parent, agentId: "worker", task: "x" });
    expect(seen).toEqual([{ namespaceId: "acme", resourceId: "alice" }]);
    const denied = new ZoryaServer({
      insecure: true,
      workflows: h.workflows,
      agents: new ZoryaAgents({
        registry: h.agents.registry,
        memory: h.memory,
        resolve: h.agents.resolve,
        authorizeScope: ({ scope }) => !("agentId" in scope && scope.agentId === "worker"),
      }),
      retainedSubagents: h.retained,
    });
    const response = await denied.handle(
      new Request("http://test/api/agents/parent/threads/conversation/children", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          namespaceId: "acme",
          resourceId: "alice",
          agentId: "worker",
          task: "w",
        }),
      }),
    );
    expect(response.status).toBe(403);
  });

  it("enforces the child limit under concurrent spawns without a turn gate", async () => {
    const h = await harness({ retained: { maxChildren: 2 }, turnGate: false });
    const results = await Promise.allSettled(
      ["a", "b", "c", "d"].map((childId) =>
        h.retained.spawnAgent({ parent, agentId: "worker", task: childId, childId }),
      ),
    );
    expect(results.filter((r) => r.status === "fulfilled")).toHaveLength(2);
  });

  it("does not list a deleted child and records superseded messages", async () => {
    const h = await harness();
    await h.workflows.start();
    const child = await h.retained.spawnAgent({
      parent,
      agentId: "worker",
      task: "initial",
      childId: "gone",
    });
    await waitFor(
      async () =>
        (await h.retained.readAgentMessages(parent, child.childId)).transcript.length >= 2,
    );
    await h.workflows.stop();
    await h.retained.messageAgent({ parent, childId: child.childId, task: "old", messageId: "q" });
    await h.retained.messageAgent({
      parent,
      childId: child.childId,
      task: "new",
      mode: "steer",
      messageId: "s",
    });
    await h.workflows.start();
    await waitFor(async () =>
      (await h.retained.readAgentMessages(parent, child.childId)).outcomes.some((o) =>
        String(o.content).startsWith("superseded"),
      ),
    );
    await h.retained.deleteChildAgent(parent, child.childId);
    expect(await h.retained.listChildAgents(parent)).toHaveLength(0);
    await h.workflows.stop();
  });

  it("reconcile does not re-signal consumed messages", async () => {
    let delivered = 0;
    const h = await harness({
      wrapStorage: (storage) =>
        overrideStorage(
          storage,
          "deliverSignal",
          (original) =>
            (async (p: never) => {
              delivered++;
              return original(p);
            }) as never,
        ),
    });
    await h.workflows.start();
    const child = await h.retained.spawnAgent({
      parent,
      agentId: "worker",
      task: "initial",
      childId: "rec",
    });
    await h.retained.messageAgent({ parent, childId: child.childId, task: "one", messageId: "o" });
    await waitFor(
      async () => (await h.retained.readAgentMessages(parent, child.childId)).outcomes.length >= 2,
    );
    const before = delivered;
    await h.retained.reconcileMailboxes();
    expect(delivered).toBe(before);
    await h.workflows.stop();
  });
});
