// ---------------------------------------------------------------------------
// Scheduler tool integration — proves the full loop:
//   1. An agent calls `scheduler.create(...)` via the scope flowing through
//      `ctx.scope` (no LocalAgent special-casing for the scheduler tool).
//   2. A schedule row lands in storage with the right metadata.
//   3. `dispatchAgentSchedule` re-invokes the registered agent with
//      `source: { kind: "scheduled", ... }` and the original task.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import {
  InMemorySchedulerStorage,
  InMemoryWorkflowStorage,
  createWorkflowRunner,
} from "@promin/workflow";
import { LocalAgent } from "../../agent/local-agent.ts";
import { InMemoryAgentRegistry } from "../../registry/in-memory-agent-registry.ts";
import { createDurableSchedulerTool } from "../durable-scheduler-tool.ts";
import { inProcessSchedulerClient } from "../scheduler-client.ts";
import { dispatchAgentSchedule, isAgentSchedule } from "../dispatch-agent-schedule.ts";
import type { AgentInput } from "../../agent/types.ts";
import type { LLMResponse } from "../../llm-provider.ts";

function mockLLM(responses: LLMResponse[]) {
  let i = 0;
  return {
    chat: async () => {
      const resp = responses[i++];
      if (!resp) throw new Error("Mock LLM exhausted");
      return resp;
    },
  };
}

describe("scheduler tool — end-to-end via LocalAgent + dispatchAgentSchedule", () => {
  it("agent creates a schedule via ctx.scope, dispatch re-invokes the agent with source=scheduled", async () => {
    const schedulerStorage = new InMemorySchedulerStorage();
    const workflowStorage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage: workflowStorage });

    const schedulerTool = createDurableSchedulerTool({
      getClient: (scope) => inProcessSchedulerClient({ storage: schedulerStorage, scope }),
    });

    // First run: assistant calls scheduler.create + we don't expect a
    // follow-up since the tool result is enough to wrap the turn.
    const llmResponses: LLMResponse[] = [
      {
        content: null,
        finishReason: "tool_calls",
        toolCalls: [
          {
            id: "tc-1",
            name: "scheduler",
            input: {
              command: "create",
              task: "Check Twitter for AI posts",
              intervalMs: 3_600_000,
            },
          },
        ],
      },
      { content: "Scheduled.", finishReason: "stop" },
    ];

    const agent = new LocalAgent({
      namespaceId: "acme",
      resourceId: "alice",
      agentId: "writer",
      runner,
      agent: {
        name: "writer",
        llm: mockLLM(llmResponses),
        tools: { scheduler: schedulerTool },
      },
    });

    const thread = await agent.thread("t1");
    const out = await thread.send({ task: "Set up a Twitter check every hour" });
    expect(await out.text).toBe("Scheduled.");

    // The tool wrote a row — verify scope routing landed correctly.
    const stored = await schedulerStorage.listSchedules({ namespace: "acme", limit: 10 });
    expect(stored).toHaveLength(1);
    const sched = stored[0]!;
    expect(sched.metadata).toMatchObject({
      agentTrigger: true,
      agentId: "writer",
      task: "Check Twitter for AI posts",
      namespaceId: "acme",
      resourceId: "alice",
      threadId: "t1",
      createdByAgent: "writer",
    });

    // Now fire the schedule: dispatchAgentSchedule should re-invoke
    // the agent with source.kind === "scheduled" and the task wired
    // through. We use a fresh registry + a capturing fake agent so
    // the assertions don't drag the real LLM machinery in.
    const registry = new InMemoryAgentRegistry();
    await registry.register({
      id: "writer",
      backend: { type: "local", model: { provider: "x", id: "y" }, systemPrompt: null, tools: [] },
    });
    const captures: Array<{ task: string; thread?: string; source: AgentInput["source"] }> = [];
    const fakeAgent = makeCapturingAgent(captures);

    const result = await dispatchAgentSchedule(
      {
        scheduleId: sched.id,
        scheduledAt: new Date("2026-04-28T15:00:00Z"),
        firedAt: new Date("2026-04-28T15:00:01Z"),
        tickNumber: 0,
      },
      sched,
      { registry, resolve: () => fakeAgent },
    );
    expect(result.ok).toBe(true);
    expect(captures).toHaveLength(1);
    expect(captures[0]).toMatchObject({
      task: "Check Twitter for AI posts",
      thread: "t1",
      source: {
        kind: "scheduled",
        firedAt: new Date("2026-04-28T15:00:00Z"),
        scheduleId: sched.id,
      },
    });
  });

  it("isAgentSchedule + the fire-callback shape match the schedule the tool writes", async () => {
    // Sanity check on the contract — what the tool writes is what
    // isAgentSchedule recognises (so the loop's fire override picks
    // up the row without re-running the agent).
    const storage = new InMemorySchedulerStorage();
    const tool = createDurableSchedulerTool({
      getClient: (scope) => inProcessSchedulerClient({ storage, scope }),
    });
    await tool.execute(
      { command: "create", task: "ping", cron: "0 * * * *" },
      {
        scope: {
          namespaceId: "acme",
          resourceId: "alice",
          threadId: "t-x",
          agentId: "writer",
        },
      },
    );
    const [sched] = await storage.listSchedules({ namespace: "acme", limit: 5 });
    expect(sched).toBeDefined();
    expect(isAgentSchedule(sched!)).toBe(true);
  });
});

function makeCapturingAgent(
  captures: Array<{ task: string; thread?: string; source: AgentInput["source"] }>,
) {
  const noopOutput = {
    text: Promise.resolve("ok"),
    finishReason: Promise.resolve("stop"),
    usage: Promise.resolve({ inputTokens: 0, outputTokens: 0 }),
    messages: Promise.resolve([]),
    events: { async *[Symbol.asyncIterator]() {} },
    fullStream: { async *[Symbol.asyncIterator]() {} },
    textStream: { async *[Symbol.asyncIterator]() {} },
    cancel: async () => {},
  } as unknown as Awaited<ReturnType<LocalAgent["invoke"]>>;
  // biome-ignore lint/suspicious/noExplicitAny: minimal stub
  const agent: any = {
    withScope: () => agent,
    invoke: async (input: AgentInput) => {
      captures.push({ task: input.task, source: input.source });
      return noopOutput;
    },
    thread: async (id: string) => ({
      id,
      isNew: false,
      send: async (input: AgentInput) => {
        captures.push({ task: input.task, thread: id, source: input.source });
        return noopOutput;
      },
      stream: () => noopOutput,
    }),
  };
  return agent;
}
