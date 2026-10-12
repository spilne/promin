// ---------------------------------------------------------------------------
// Thread owner stamp — every thread a `LocalAgent` creates carries
// `metadata[THREAD_AGENT_METADATA_KEY]` = the agent's id, however it was
// created: `thread()` (gateway or programmatic), a lazy re-create on a
// handle whose row went away, or a scheduled dispatch into a new thread.
// `setMetadata` keeps the stamp.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { InMemoryWorkflowStorage, createWorkflowRunner } from "@promin/workflow";
import { LocalAgent } from "../local-agent.ts";
import { THREAD_AGENT_METADATA_KEY } from "../types.ts";
import { InMemoryMemoryStore } from "../../memory/in-memory-memory-store.ts";
import { InMemoryAgentRegistry } from "../../versioned-registry/in-memory-registries.ts";
import { resolveLocalAgent } from "../../registry/resolve-local-agent.ts";
import type { RegisteredAgent } from "../../registry/types.ts";
import { dispatchAgentSchedule } from "../../tools/dispatch-agent-schedule.ts";
import type { LLMProvider } from "../../llm-provider.ts";

const LLM: LLMProvider = { chat: async () => ({ content: "ok", finishReason: "stop" }) };

function setup() {
  const storage = new InMemoryWorkflowStorage();
  const runner = createWorkflowRunner({ storage });
  const memory = new InMemoryMemoryStore();
  const registry = new InMemoryAgentRegistry();
  const resolve = (recipe: RegisteredAgent) =>
    resolveLocalAgent(recipe, { runner, memory, llm: () => LLM, tools: {} });
  return { runner, memory, registry, resolve };
}

const BACKEND = {
  type: "local" as const,
  model: { provider: "anthropic", id: "claude-sonnet-4-6" },
  role: { inline: { systemPrompt: null, tools: [] } },
};

async function ownerOf(params: {
  memory: InMemoryMemoryStore;
  threadId: string;
  resourceId?: string;
}): Promise<unknown> {
  const row = await params.memory.getThread({
    namespaceId: "acme",
    threadId: params.threadId,
    ...(params.resourceId !== undefined && { resourceId: params.resourceId }),
  });
  return row?.metadata[THREAD_AGENT_METADATA_KEY];
}

describe("LocalAgent — thread owner stamp", () => {
  it("stamps a programmatically created thread with the recipe id", async () => {
    const { memory, registry, resolve } = setup();
    const recipe = await registry.register({ id: "writer", backend: BACKEND });
    const agent = (await resolve(recipe)).withScope({ namespaceId: "acme", resourceId: "alice" });

    const thread = await agent.thread("t1", { metadata: { topic: "launch" } });
    expect(thread.isNew).toBe(true);
    const row = await memory.getThread({
      namespaceId: "acme",
      resourceId: "alice",
      threadId: "t1",
    });
    expect(row?.metadata).toEqual({ topic: "launch", [THREAD_AGENT_METADATA_KEY]: "writer" });
  });

  it("keeps its own stamp over one the caller passes", async () => {
    const { memory, registry, resolve } = setup();
    const recipe = await registry.register({ id: "writer", backend: BACKEND });
    const agent = (await resolve(recipe)).withScope({ namespaceId: "acme", resourceId: "alice" });
    await agent.thread("t1", { metadata: { [THREAD_AGENT_METADATA_KEY]: "someone-else" } });
    expect(await ownerOf({ memory, threadId: "t1", resourceId: "alice" })).toBe("writer");
  });

  it("stamps `agentId` when set, else the loop name", async () => {
    const { runner, memory } = setup();
    const named = new LocalAgent({
      agent: { name: "loop-name", llm: LLM },
      runner,
      memory,
      namespaceId: "acme",
      agentId: "recipe-id",
    });
    await named.thread("a");
    expect(await ownerOf({ memory, threadId: "a" })).toBe("recipe-id");

    const unnamed = new LocalAgent({ agent: { name: "loop-name", llm: LLM }, runner, memory });
    await unnamed.thread("b", { namespaceId: "acme" });
    expect(await ownerOf({ memory, threadId: "b" })).toBe("loop-name");
  });

  it("re-creates a vanished row stamped, and setMetadata keeps the stamp", async () => {
    const { memory, registry, resolve } = setup();
    const recipe = await registry.register({ id: "writer", backend: BACKEND });
    const agent = (await resolve(recipe)).withScope({ namespaceId: "acme", resourceId: "alice" });
    const thread = await agent.thread("t1");

    await memory.deleteThread({ namespaceId: "acme", resourceId: "alice", threadId: "t1" });
    await thread.setTitle("Renamed");
    expect(await ownerOf({ memory, threadId: "t1", resourceId: "alice" })).toBe("writer");

    await thread.setMetadata({ topic: "x", [THREAD_AGENT_METADATA_KEY]: "intruder" });
    expect(await thread.metadata()).toEqual({ topic: "x", [THREAD_AGENT_METADATA_KEY]: "writer" });

    await memory.deleteThread({ namespaceId: "acme", resourceId: "alice", threadId: "t1" });
    const out = await thread.send({ task: "hello" });
    expect(await out.text).toBe("ok");
    expect(await ownerOf({ memory, threadId: "t1", resourceId: "alice" })).toBe("writer");
  });

  it("stamps the thread a scheduled dispatch creates", async () => {
    const { memory, registry, resolve } = setup();
    await registry.register({ id: "writer", backend: BACKEND });
    const result = await dispatchAgentSchedule(
      {
        scheduleId: "s-1",
        scheduledAt: new Date("2026-04-28T14:00:00Z"),
        firedAt: new Date("2026-04-28T14:00:01Z"),
        incarnation: "inc1",
        tickNumber: 0,
      },
      {
        id: "s-1",
        cron: "0 * * * *",
        metadata: {
          agentTrigger: true,
          agentId: "writer",
          task: "Daily digest",
          namespaceId: "acme",
          resourceId: "alice",
          threadId: "digest",
          createdByAgent: "writer",
        },
      },
      { registry, resolve },
    );
    expect(result.ok).toBe(true);
    const listed = await memory.listThreads({
      namespaceId: "acme",
      metadataFilter: { [THREAD_AGENT_METADATA_KEY]: "writer" },
    });
    expect(listed.map((t) => t.threadId)).toEqual(["digest"]);
  });
});
