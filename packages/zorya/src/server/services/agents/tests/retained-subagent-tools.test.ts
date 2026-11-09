import { describe, expect, it } from "bun:test";
import type { LLMProvider, ToolExecuteContext } from "@promin/agent";
import { retainedSubagentTools } from "../retained-subagent-tools.ts";
import { harness, parent, waitFor } from "./retained-harness.ts";

const scopeOf = (p: typeof parent): ToolExecuteContext => ({
  scope: {
    namespaceId: p.namespaceId,
    resourceId: p.resourceId,
    threadId: p.threadId,
    agentId: p.agentId,
  },
  toolCallId: "call-1",
});

describe("retained subagent tools", () => {
  it("spawn, message, list, read and stop act on the calling thread's children only", async () => {
    const h = await harness();
    await h.workflows.start();
    const tools = retainedSubagentTools(h.retained);
    const ctx = scopeOf(parent);

    const handle = await tools.spawn_subagent!.execute({ agentId: "worker", task: "first" }, ctx);
    await waitFor(async () => {
      const read = await tools.read_subagent!.execute({ childId: handle.childId }, ctx);
      return read.outcomes.length === 1;
    });
    await tools.message_subagent!.execute(
      { childId: handle.childId, task: "second", mode: "follow-up" },
      { ...ctx, toolCallId: "call-2" },
    );
    await waitFor(async () => {
      const read = await tools.read_subagent!.execute({ childId: handle.childId }, ctx);
      return read.outcomes.length === 2;
    });
    const read = await tools.read_subagent!.execute(
      { childId: handle.childId, lastMessages: 1 },
      ctx,
    );
    expect(read.transcript).toHaveLength(1);
    expect(read.transcript[0].content).toBe("reply:second");
    const listed = await tools.list_subagents!.execute({}, ctx);
    expect(listed.children.map((c: { childId: string }) => c.childId)).toEqual([handle.childId]);

    // Another thread of the same agent cannot see or reach the child.
    const other = scopeOf({ ...parent, threadId: "other" });
    const parentRecipe = (await h.agents.registry.get({ id: "parent" }))!;
    await (await h.agents.resolve(parentRecipe, parent)).withScope(parent).thread("other");
    expect((await tools.list_subagents!.execute({}, other)).children).toEqual([]);
    await expect(
      tools.message_subagent!.execute({ childId: handle.childId, task: "hijack" }, other),
    ).rejects.toMatchObject({ code: "not_found" });

    await tools.stop_subagent!.execute({ childId: handle.childId }, ctx);
    await expect(
      tools.message_subagent!.execute({ childId: handle.childId, task: "late" }, ctx),
    ).rejects.toMatchObject({ code: "conflict" });
    await h.workflows.stop();
  });

  it("applies canDelegate, the child limit and a complete-scope requirement", async () => {
    const h = await harness({ retained: { maxChildren: 1 } });
    const tools = retainedSubagentTools(h.retained);
    const ctx = scopeOf(parent);
    await expect(
      tools.spawn_subagent!.execute({ agentId: "parent", task: "not allowed" }, ctx),
    ).rejects.toMatchObject({ code: "forbidden" });
    await tools.spawn_subagent!.execute({ agentId: "worker", task: "one" }, ctx);
    await expect(
      tools.spawn_subagent!.execute({ agentId: "worker", task: "two" }, ctx),
    ).rejects.toMatchObject({ code: "conflict" });
    await expect(
      tools.spawn_subagent!.execute({ agentId: "worker", task: "x" }, { toolCallId: "c" }),
    ).rejects.toMatchObject({ code: "invalid" });
  });

  it("lets a model spawn a subagent through its own tool loop", async () => {
    // The orchestrator calls spawn_subagent once, then answers.
    let called = false;
    const orchestratorLlm: LLMProvider = {
      chat: async () => {
        if (called) return { content: "spawned", finishReason: "stop" as const };
        called = true;
        return {
          content: null,
          finishReason: "tool_calls" as const,
          toolCalls: [
            {
              id: "tc-1",
              name: "spawn_subagent",
              input: { agentId: "worker", task: "from model" },
            },
          ],
        };
      },
    };
    const h = await harness({
      orchestratorLlm,
      retained: { canDelegate: (from, to) => from === "orchestrator" && to === "worker" },
    });
    await h.workflows.start();
    const orchestrator = { ...parent, agentId: "orchestrator", threadId: "orch-thread" };
    const recipe = (await h.agents.registry.get({ id: "orchestrator" }))!;
    const agent = (await h.agents.resolve(recipe, orchestrator)).withScope(orchestrator);
    const thread = await agent.thread(orchestrator.threadId);
    await (
      await thread.send({ task: "delegate this" })
    ).text;

    const [child] = await h.retained.listChildAgents(orchestrator);
    expect(child?.agentId).toBe("worker");
    await waitFor(async () => {
      const { transcript } = await h.retained.readAgentMessages(orchestrator, child!.childId);
      return transcript.some((m) => m.content === "reply:from model");
    });
    await h.workflows.stop();
  });
});
