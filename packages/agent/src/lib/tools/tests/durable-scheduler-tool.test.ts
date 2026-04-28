// ---------------------------------------------------------------------------
// createDurableSchedulerTool + dispatchAgentSchedule — verifies:
//   - create writes a schedule with the right metadata shape
//   - create rejects when no trigger / multiple triggers
//   - create enforces the per-thread cap
//   - list filters to this thread's agent-created schedules
//   - cancel deletes; rejects cross-thread cancel attempts
//   - dispatch helper resolves the recipe + invokes the agent
//     with a [Scheduled trigger: ...] prefixed task
//   - dispatch skips non-agent schedules + missing recipes cleanly
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { InMemorySchedulerStorage } from "@promin/workflow";
import { InMemoryAgentRegistry } from "../../registry/in-memory-agent-registry.ts";
import { createDurableSchedulerTool } from "../durable-scheduler-tool.ts";
import { dispatchAgentSchedule, isAgentSchedule } from "../dispatch-agent-schedule.ts";
import { inProcessSchedulerClient } from "../scheduler-client.ts";
import type { Agent, AgentInput, AgentRunOutput } from "../../agent/types.ts";

const SCOPE = {
  namespaceId: "acme",
  resourceId: "alice",
  threadId: "t1",
  agentId: "writer",
} as const;

function clientFor(storage: InMemorySchedulerStorage) {
  return inProcessSchedulerClient({ storage, scope: SCOPE });
}

describe("createDurableSchedulerTool — create", () => {
  it("writes a schedule with agent-trigger metadata + scope routing", async () => {
    const storage = new InMemorySchedulerStorage();
    const tool = createDurableSchedulerTool({ client: clientFor(storage) });

    const result = await tool.execute({
      command: "create",
      task: "Check Twitter for AI posts and summarize",
      cron: "0 * * * *",
      name: "hourly twitter",
    });

    expect(result.ok).toBe(true);
    if (!result.ok || !("id" in result)) throw new Error("expected create ok");

    const stored = await storage.loadSchedule(result.id);
    expect(stored).not.toBeNull();
    expect(stored?.cron).toBe("0 * * * *");
    expect(stored?.name).toBe("hourly twitter");
    expect(stored?.namespace).toBe("acme");
    expect(stored?.metadata).toMatchObject({
      agentTrigger: true,
      agentId: "writer",
      task: "Check Twitter for AI posts and summarize",
      namespaceId: "acme",
      resourceId: "alice",
      threadId: "t1",
      createdByAgent: "writer",
    });
  });

  it("agentId override targets a peer instead of self", async () => {
    const storage = new InMemorySchedulerStorage();
    const tool = createDurableSchedulerTool({ client: clientFor(storage) });

    const result = await tool.execute({
      command: "create",
      task: "Draft a status update",
      agentId: "summarizer",
      intervalMs: 60_000,
    });
    expect(result.ok).toBe(true);
    if (!result.ok || !("id" in result)) throw new Error("expected create ok");

    const stored = await storage.loadSchedule(result.id);
    expect(stored?.metadata?.agentId).toBe("summarizer");
    expect(stored?.metadata?.createdByAgent).toBe("writer"); // who scheduled it
  });

  it("rejects when no trigger is set", async () => {
    const tool = createDurableSchedulerTool({
      client: clientFor(new InMemorySchedulerStorage()),
    });
    const result = await tool.execute({ command: "create", task: "x" });
    expect(result.ok).toBe(false);
    if ("error" in result) expect(result.error).toMatch(/exactly one/);
  });

  it("rejects when multiple triggers are set", async () => {
    const tool = createDurableSchedulerTool({
      client: clientFor(new InMemorySchedulerStorage()),
    });
    const result = await tool.execute({
      command: "create",
      task: "x",
      cron: "* * * * *",
      intervalMs: 60_000,
    });
    expect(result.ok).toBe(false);
    if ("error" in result) expect(result.error).toMatch(/exactly one/);
  });

  it("enforces the per-thread cap", async () => {
    const storage = new InMemorySchedulerStorage();
    const tool = createDurableSchedulerTool({
      client: clientFor(storage),
      maxPerThread: 2,
    });
    await tool.execute({ command: "create", task: "a", cron: "* * * * *" });
    await tool.execute({ command: "create", task: "b", cron: "* * * * *" });
    const result = await tool.execute({ command: "create", task: "c", cron: "* * * * *" });
    expect(result.ok).toBe(false);
    if ("error" in result) expect(result.error).toMatch(/cap 2/);
  });
});

describe("createDurableSchedulerTool — list / cancel", () => {
  it("list returns this thread's agent-created schedules only", async () => {
    const storage = new InMemorySchedulerStorage();

    // Other thread's agent schedule
    await storage.upsertSchedule({
      id: "other-thread",
      namespace: "acme",
      cron: "* * * * *",
      metadata: {
        agentTrigger: true,
        agentId: "x",
        task: "x",
        namespaceId: "acme",
        threadId: "t2",
      },
    });
    // Operator-created schedule (no agentTrigger flag)
    await storage.upsertSchedule({
      id: "operator-schedule",
      namespace: "acme",
      cron: "* * * * *",
      metadata: { workflowName: "x" },
    });
    // This thread's
    const tool = createDurableSchedulerTool({ client: clientFor(storage) });
    const created = await tool.execute({ command: "create", task: "ours", cron: "* * * * *" });
    if (!created.ok || !("id" in created)) throw new Error("expected create ok");

    const list = await tool.execute({ command: "list" });
    expect(list.ok).toBe(true);
    if (!("schedules" in list)) throw new Error("expected list result");
    expect(list.schedules.map((s) => s.id)).toEqual([created.id]);
    expect(list.schedules[0]!.task).toBe("ours");
  });

  it("cancel deletes the row", async () => {
    const storage = new InMemorySchedulerStorage();
    const tool = createDurableSchedulerTool({ client: clientFor(storage) });
    const created = await tool.execute({ command: "create", task: "x", cron: "* * * * *" });
    if (!created.ok || !("id" in created)) throw new Error("expected create ok");

    const cancelled = await tool.execute({ command: "cancel", id: created.id });
    expect(cancelled.ok).toBe(true);
    expect(await storage.loadSchedule(created.id)).toBeNull();
  });

  it("cancel rejects schedules from other threads", async () => {
    const storage = new InMemorySchedulerStorage();
    await storage.upsertSchedule({
      id: "their-schedule",
      namespace: "acme",
      cron: "* * * * *",
      metadata: {
        agentTrigger: true,
        agentId: "x",
        task: "x",
        namespaceId: "acme",
        threadId: "different-thread",
      },
    });

    const tool = createDurableSchedulerTool({ client: clientFor(storage) });
    const result = await tool.execute({ command: "cancel", id: "their-schedule" });
    expect(result.ok).toBe(false);
    if ("error" in result) expect(result.error).toMatch(/doesn't belong/);
    expect(await storage.loadSchedule("their-schedule")).not.toBeNull();
  });

  it("cancel returns error for unknown id", async () => {
    const tool = createDurableSchedulerTool({
      client: clientFor(new InMemorySchedulerStorage()),
    });
    const result = await tool.execute({ command: "cancel", id: "ghost" });
    expect(result.ok).toBe(false);
    if ("error" in result) expect(result.error).toMatch(/no schedule/);
  });
});

describe("dispatchAgentSchedule", () => {
  type Capture = { task: string; thread?: string; source?: AgentInput["source"] };
  function fakeAgent(captures: Capture[]): Agent {
    const noopOutput: AgentRunOutput = {
      text: Promise.resolve("ok"),
      finishReason: Promise.resolve("stop"),
      usage: Promise.resolve({ inputTokens: 0, outputTokens: 0 }),
      messages: Promise.resolve([]),
      events: { async *[Symbol.asyncIterator]() {} },
      textStream: { async *[Symbol.asyncIterator]() {} },
      cancel: async () => {},
    };
    const agent: Agent = {
      // biome-ignore lint/suspicious/noExplicitAny: test stub
      withScope: () => agent as any,
      invoke: async (input: AgentInput) => {
        captures.push({ task: input.task, source: input.source });
        return noopOutput;
      },
      stream: () => noopOutput,
      thread: async (id: string) => ({
        id,
        isNew: false,
        send: async (input: AgentInput) => {
          captures.push({ task: input.task, thread: id, source: input.source });
          return noopOutput;
        },
        stream: () => noopOutput,
        delete: async () => {},
        messages: async () => [],
        workingMemory: async () => null,
        setWorkingMemory: async () => {},
        setMetadata: async () => {},
        compact: async () => null,
        distill: async () => null,
      }),
      listThreads: async () => [],
    } as unknown as Agent;
    return agent;
  }

  it("invokes the recipe with the bare task + scheduled source on the named thread", async () => {
    const captures: Capture[] = [];
    const registry = new InMemoryAgentRegistry();
    await registry.register({
      id: "writer",
      backend: {
        type: "local",
        model: { provider: "test", id: "x" },
        systemPrompt: null,
        tools: [],
      },
    });
    const tick = {
      scheduleId: "s-1",
      scheduledAt: new Date("2026-04-28T14:00:00Z"),
      firedAt: new Date("2026-04-28T14:00:01Z"),
      tickNumber: 0,
    };
    const schedule = {
      id: "s-1",
      cron: "0 * * * *",
      metadata: {
        agentTrigger: true,
        agentId: "writer",
        task: "Check Twitter",
        namespaceId: "acme",
        resourceId: "alice",
        threadId: "t1",
        createdByAgent: "writer",
      },
    };
    const result = await dispatchAgentSchedule(tick, schedule, {
      registry,
      resolve: () => fakeAgent(captures),
    });
    expect(result.ok).toBe(true);
    expect(captures).toHaveLength(1);
    expect(captures[0]?.thread).toBe("t1");
    expect(captures[0]?.task).toBe("Check Twitter");
    expect(captures[0]?.source).toEqual({
      kind: "scheduled",
      firedAt: tick.scheduledAt,
      scheduleId: "s-1",
    });
  });

  it("falls back to invoke (no thread) when threadId isn't set", async () => {
    const captures: Capture[] = [];
    const registry = new InMemoryAgentRegistry();
    await registry.register({
      id: "writer",
      backend: { type: "local", model: { provider: "x", id: "y" }, systemPrompt: null, tools: [] },
    });
    const result = await dispatchAgentSchedule(
      {
        scheduleId: "s-1",
        scheduledAt: new Date(0),
        firedAt: new Date(0),
        tickNumber: 0,
      },
      {
        id: "s-1",
        cron: "* * * * *",
        metadata: {
          agentTrigger: true,
          agentId: "writer",
          task: "scan logs",
          namespaceId: "acme",
          createdByAgent: "writer",
        },
      },
      { registry, resolve: () => fakeAgent(captures) },
    );
    expect(result.ok).toBe(true);
    expect(captures[0]?.thread).toBeUndefined();
  });

  it("skips non-agent schedules", async () => {
    const result = await dispatchAgentSchedule(
      { scheduleId: "x", scheduledAt: new Date(0), firedAt: new Date(0), tickNumber: 0 },
      { id: "x", cron: "* * * * *", metadata: { workflowName: "send-email" } },
      {
        registry: new InMemoryAgentRegistry(),
        resolve: () => {
          throw new Error("should not be called");
        },
      },
    );
    expect(result.ok).toBe(false);
    expect(result.skipped).toBe("not_agent_schedule");
  });

  it("returns recipe_not_found when the agentId is unknown", async () => {
    const result = await dispatchAgentSchedule(
      { scheduleId: "x", scheduledAt: new Date(0), firedAt: new Date(0), tickNumber: 0 },
      {
        id: "x",
        cron: "* * * * *",
        metadata: {
          agentTrigger: true,
          agentId: "ghost",
          task: "x",
          namespaceId: "acme",
          createdByAgent: "writer",
        },
      },
      {
        registry: new InMemoryAgentRegistry(),
        resolve: () => {
          throw new Error("should not be called");
        },
      },
    );
    expect(result.ok).toBe(false);
    expect(result.skipped).toBe("recipe_not_found");
  });
});

describe("isAgentSchedule", () => {
  it("returns true for valid metadata", () => {
    expect(
      isAgentSchedule({
        id: "x",
        cron: "* * * * *",
        metadata: {
          agentTrigger: true,
          agentId: "writer",
          task: "x",
          namespaceId: "acme",
        },
      }),
    ).toBe(true);
  });

  it("returns false when agentTrigger is not true", () => {
    expect(
      isAgentSchedule({
        id: "x",
        cron: "* * * * *",
        metadata: { workflowName: "send-email" },
      }),
    ).toBe(false);
  });

  it("returns false when required metadata is missing", () => {
    expect(
      isAgentSchedule({
        id: "x",
        cron: "* * * * *",
        metadata: { agentTrigger: true, agentId: "writer" }, // missing task + namespaceId
      }),
    ).toBe(false);
  });
});
