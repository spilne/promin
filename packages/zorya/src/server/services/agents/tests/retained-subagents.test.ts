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

async function harness() {
  const storage = new InMemoryWorkflowStorage();
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
      sleepScanIntervalMs: 0,
      signalScanIntervalMs: 10,
      orphanRecovery: false,
    });
    const agents = new ZoryaAgents({
      registry,
      memory,
      resolve: (recipe) =>
        resolveLocalAgent(recipe, {
          runner,
          memory,
          tools: {},
          llm: () => ({
            chat: async (request) => ({
              content: `reply:${request.messages.findLast((m) => m.role === "user")?.content ?? ""}`,
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
    expect((await h.retained.readAgentMessages(parent, a.childId)).transcript).toHaveLength(0);
    expect((await h.retained.readAgentMessages(parent, b.childId)).transcript).toHaveLength(2);
    await h.workflows.stop();
  });
});
