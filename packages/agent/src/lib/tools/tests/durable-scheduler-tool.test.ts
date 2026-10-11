// ---------------------------------------------------------------------------
// createDurableSchedulerTools + dispatchAgentSchedule — verifies:
//   - schedulerCreate writes a schedule with the right metadata shape
//   - schedulerCreate rejects when no trigger / multiple triggers
//   - schedulerCreate enforces the per-thread cap
//   - schedulerList filters to this thread's agent-created schedules
//   - schedulerCancel deletes; rejects cross-thread cancel attempts
//   - dispatch helper resolves the recipe + invokes the agent
//     with a [Scheduled trigger: ...] prefixed task
//   - dispatch skips non-agent schedules + missing recipes cleanly
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { FakeWallClock } from "@promin/workflow";
import { InMemorySchedulerStorage } from "@promin/workflow/scheduler";
import { InMemoryAgentRegistry } from "../../registry/in-memory-agent-registry.ts";
import {
  createDurableSchedulerTools,
  type DurableSchedulerToolDeps,
} from "../durable-scheduler-tool.ts";
import { dispatchAgentSchedule, isAgentSchedule } from "../dispatch-agent-schedule.ts";
import { httpSchedulerClient, inProcessSchedulerClient } from "../scheduler-client.ts";
import type { Agent, AgentInput, AgentRunOutput } from "../../agent/types.ts";

const SCOPE = {
  namespaceId: "acme",
  resourceId: "alice",
  threadId: "t1",
  agentId: "writer",
} as const;

/** Mimic the agent runtime: build the tools with a per-call client factory. */
function toolsWith(
  storage: InMemorySchedulerStorage,
  opts: Omit<DurableSchedulerToolDeps, "getClient"> = {},
) {
  return createDurableSchedulerTools({
    getClient: (scope) => inProcessSchedulerClient({ storage, scope }),
    ...opts,
  });
}

/** Default ctx populated by the agent runtime — what the tool reads at execute. */
const CTX = { scope: SCOPE };

describe("createDurableSchedulerTools — schedulerCreate", () => {
  it("writes a schedule with agent-trigger metadata + scope routing", async () => {
    const storage = new InMemorySchedulerStorage();
    const tools = toolsWith(storage);

    const result = await tools.schedulerCreate.execute(
      {
        task: "Check Twitter for AI posts and summarize",
        cron: "0 * * * *",
        name: "hourly twitter",
      },
      CTX,
    );

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

  it("agentId override targets a peer when the host authorizes it", async () => {
    const storage = new InMemorySchedulerStorage();
    const asked: Array<{ agentId: string; targetAgentId: string }> = [];
    const tools = toolsWith(storage, {
      authorizeDelegation: ({ scope, targetAgentId }) => {
        asked.push({ agentId: scope.agentId, targetAgentId });
        return targetAgentId === "summarizer";
      },
    });

    const result = await tools.schedulerCreate.execute(
      {
        task: "Draft a status update",
        agentId: "summarizer",
        intervalMs: 60_000,
      },
      CTX,
    );
    expect(result.ok).toBe(true);
    if (!result.ok || !("id" in result)) throw new Error("expected create ok");

    const stored = await storage.loadSchedule(result.id);
    expect(stored?.metadata?.agentId).toBe("summarizer");
    expect(stored?.metadata?.createdByAgent).toBe("writer"); // who scheduled it
    expect(asked).toEqual([{ agentId: "writer", targetAgentId: "summarizer" }]);
  });

  it("refuses to delegate to another agent without an authorization hook", async () => {
    const storage = new InMemorySchedulerStorage();
    const result = await toolsWith(storage).schedulerCreate.execute(
      { task: "Wire money", agentId: "payments-admin", intervalMs: 3_600_000 },
      CTX,
    );
    expect(result).toMatchObject({ ok: false });
    if (!result.ok) expect(result.error).toContain("payments-admin");
    expect(await storage.countSchedules()).toBe(0);
  });

  it("requires approval on create by default, not on list / cancel", () => {
    const tools = toolsWith(new InMemorySchedulerStorage());
    expect(tools.schedulerCreate.requireApproval).toBe(true);
    expect(tools.schedulerList.requireApproval).toBeFalsy();
    expect(tools.schedulerCancel.requireApproval).toBeFalsy();
    const unattended = toolsWith(new InMemorySchedulerStorage(), { requireApproval: false });
    expect(unattended.schedulerCreate.requireApproval).toBe(false);
  });

  it("refuses cadences faster than the minimum interval", async () => {
    const storage = new InMemorySchedulerStorage();
    const tools = toolsWith(storage, { minIntervalMs: 60_000 });
    for (const trigger of [
      { intervalMs: 1_000 },
      { cron: "*/5 * * * * *" },
      { rrule: "RRULE:FREQ=SECONDLY;INTERVAL=10" },
    ]) {
      const result = await tools.schedulerCreate.execute({ task: "spam", ...trigger }, CTX);
      expect([trigger, result.ok]).toEqual([trigger, false]);
      if (!result.ok) expect(result.error).toContain("minimum is 60 s");
    }
    const ok = await tools.schedulerCreate.execute({ task: "fine", cron: "* * * * *" }, CTX);
    expect(ok.ok).toBe(true);
    expect(await storage.countSchedules()).toBe(1);
  });

  it("validates startAt / endAt / timezone / trigger syntax", async () => {
    const clock = FakeWallClock.create("2026-05-01T00:00:00Z");
    const storage = new InMemorySchedulerStorage();
    const tools = toolsWith(storage, { clock });
    const cases: Array<[Record<string, unknown>, string]> = [
      [{ startAt: "next tuesday" }, "ISO 8601"],
      [{ startAt: "2026-05-02" }, "ISO 8601"],
      [{ startAt: "2026-04-30T00:00:00Z" }, "in the future"],
      [{ endAt: "2026-04-30T00:00:00Z" }, "in the future"],
      [{ startAt: "2026-05-03T00:00:00Z", endAt: "2026-05-02T00:00:00Z" }, "after startAt"],
      [{ timezone: "Mars/Olympus" }, "timezone"],
      [{ cron: "not a cron", intervalMs: undefined }, "cron"],
    ];
    for (const [extra, message] of cases) {
      const result = await tools.schedulerCreate.execute(
        { task: "x", intervalMs: 3_600_000, ...extra } as never,
        CTX,
      );
      expect([extra, result.ok]).toEqual([extra, false]);
      if (!result.ok)
        expect([extra, result.error]).toEqual([extra, expect.stringContaining(message)]);
    }
    expect(await storage.countSchedules()).toBe(0);

    const ok = await tools.schedulerCreate.execute(
      { task: "x", intervalMs: 3_600_000, startAt: "2026-05-02T09:00:00+02:00" },
      CTX,
    );
    expect(ok.ok).toBe(true);
    if (ok.ok) {
      expect((await storage.loadSchedule(ok.id))?.startAt).toEqual(
        new Date("2026-05-02T07:00:00Z"),
      );
    }
  });

  it("rejects when no trigger is set", async () => {
    const tools = toolsWith(new InMemorySchedulerStorage());
    const result = await tools.schedulerCreate.execute({ task: "x" }, CTX);
    expect(result.ok).toBe(false);
    if ("error" in result) expect(result.error).toMatch(/exactly one/);
  });

  it("rejects when multiple triggers are set", async () => {
    const tools = toolsWith(new InMemorySchedulerStorage());
    const result = await tools.schedulerCreate.execute(
      {
        task: "x",
        cron: "* * * * *",
        intervalMs: 60_000,
      },
      CTX,
    );
    expect(result.ok).toBe(false);
    if ("error" in result) expect(result.error).toMatch(/exactly one/);
  });

  it("enforces the per-thread cap", async () => {
    const storage = new InMemorySchedulerStorage();
    const tools = toolsWith(storage, { maxPerThread: 2 });
    await tools.schedulerCreate.execute({ task: "a", cron: "* * * * *" }, CTX);
    await tools.schedulerCreate.execute({ task: "b", cron: "* * * * *" }, CTX);
    const result = await tools.schedulerCreate.execute({ task: "c", cron: "* * * * *" }, CTX);
    expect(result.ok).toBe(false);
    if ("error" in result) expect(result.error).toMatch(/cap 2/);
  });

  it("holds the cap under concurrent creates", async () => {
    const storage = new InMemorySchedulerStorage();
    const tools = toolsWith(storage, { maxPerThread: 3 });
    const results = await Promise.all(
      Array.from({ length: 12 }, (_, i) =>
        tools.schedulerCreate.execute({ task: `t${i}`, cron: "* * * * *" }, CTX),
      ),
    );
    expect(results.filter((r) => r.ok)).toHaveLength(3);
    expect(await storage.countSchedules()).toBe(3);
  });

  it("frees a slot on cancel and keeps threads' caps separate", async () => {
    const storage = new InMemorySchedulerStorage();
    const tools = toolsWith(storage, { maxPerThread: 1 });
    const first = await tools.schedulerCreate.execute({ task: "a", cron: "* * * * *" }, CTX);
    if (!first.ok) throw new Error("expected create ok");
    const other = await tools.schedulerCreate.execute(
      { task: "b", cron: "* * * * *" },
      { scope: { ...SCOPE, threadId: "t2" } },
    );
    expect(other.ok).toBe(true);
    expect((await tools.schedulerCreate.execute({ task: "c", cron: "* * * * *" }, CTX)).ok).toBe(
      false,
    );
    await tools.schedulerCancel.execute({ id: first.id }, CTX);
    expect((await tools.schedulerCreate.execute({ task: "d", cron: "* * * * *" }, CTX)).ok).toBe(
      true,
    );
  });

  it("seeds nextRun so the schedule is immediately visible to findDue", async () => {
    // Regression: agent-created schedules used to land with next_run NULL,
    // which made them invisible to SchedulerLoop.findDue and only fired
    // after a full server restart re-seeded them. Pin the contract that
    // create() puts the row into due-tracking right away.
    const storage = new InMemorySchedulerStorage();
    const tools = toolsWith(storage);
    const result = await tools.schedulerCreate.execute({ task: "hi", cron: "0 * * * *" }, CTX);
    expect(result.ok).toBe(true);
    const due = await storage.findDue({
      now: new Date(),
      limit: 10,
      namespace: SCOPE.namespaceId,
    });
    expect(due).toContain("id" in result ? result.id : "");
  });
});

describe("createDurableSchedulerTools — schedulerList / schedulerCancel", () => {
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
    const tools = toolsWith(storage);
    const created = await tools.schedulerCreate.execute({ task: "ours", cron: "* * * * *" }, CTX);
    if (!created.ok || !("id" in created)) throw new Error("expected create ok");

    const list = await tools.schedulerList.execute({}, CTX);
    expect(list.ok).toBe(true);
    if (!("schedules" in list)) throw new Error("expected list result");
    expect(list.schedules.map((s) => s.id)).toEqual([created.id]);
    expect(list.schedules[0]!.task).toBe("ours");
  });

  it("cancel deletes the row", async () => {
    const storage = new InMemorySchedulerStorage();
    const tools = toolsWith(storage);
    const created = await tools.schedulerCreate.execute({ task: "x", cron: "* * * * *" }, CTX);
    if (!created.ok || !("id" in created)) throw new Error("expected create ok");

    const cancelled = await tools.schedulerCancel.execute({ id: created.id }, CTX);
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

    const tools = toolsWith(storage);
    const result = await tools.schedulerCancel.execute({ id: "their-schedule" }, CTX);
    expect(result.ok).toBe(false);
    if ("error" in result) expect(result.error).toMatch(/doesn't belong/);
    expect(await storage.loadSchedule("their-schedule")).not.toBeNull();
  });

  it("cancel returns error for unknown id", async () => {
    const tools = toolsWith(new InMemorySchedulerStorage());
    const result = await tools.schedulerCancel.execute({ id: "ghost" }, CTX);
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
        role: { inline: { systemPrompt: null, tools: [] } },
      },
    });
    const tick = {
      scheduleId: "s-1",
      scheduledAt: new Date("2026-04-28T14:00:00Z"),
      firedAt: new Date("2026-04-28T14:00:01Z"),
      incarnation: "inc1",
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
      backend: {
        type: "local",
        model: { provider: "x", id: "y" },
        role: { inline: { systemPrompt: null, tools: [] } },
      },
    });
    const result = await dispatchAgentSchedule(
      {
        scheduleId: "s-1",
        scheduledAt: new Date(0),
        firedAt: new Date(0),
        incarnation: "inc1",
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
      {
        scheduleId: "x",
        scheduledAt: new Date(0),
        firedAt: new Date(0),
        incarnation: "inc1",
        tickNumber: 0,
      },
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
      {
        scheduleId: "x",
        scheduledAt: new Date(0),
        firedAt: new Date(0),
        incarnation: "inc1",
        tickNumber: 0,
      },
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

describe("httpSchedulerClient.create", () => {
  it("reports a taken id (409) as not created instead of throwing", async () => {
    const client = httpSchedulerClient({
      baseUrl: "http://zorya.test",
      scope: SCOPE,
      fetch: (async () =>
        Response.json({ error: "schedule_exists" }, { status: 409 })) as unknown as typeof fetch,
    });
    expect(await client.create({ id: "slot-1", intervalMs: 60_000, metadata: {} })).toEqual({
      created: false,
    });
  });
});
