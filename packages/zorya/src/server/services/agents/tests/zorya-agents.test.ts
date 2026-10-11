// ---------------------------------------------------------------------------
// ZoryaAgents tests — schedule dispatch wiring + lifecycle.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { InMemoryAgentRegistry, InMemoryMemoryStore, type Agent } from "@promin/agent";
import type { DurableScheduleConfig, ScheduleTick } from "@promin/workflow/scheduler";
import { ZoryaAgents } from "../index.ts";

function makeAgent(): Agent & { calls: string[] } {
  const calls: string[] = [];
  const agent: Agent & { calls: string[] } = {
    calls,
    withScope: () => agent,
    invoke: async (input: { task: string }) => {
      calls.push(input.task);
      return { text: Promise.resolve("ok") };
    },
  } as unknown as Agent & { calls: string[] };
  return agent;
}

describe("ZoryaAgents.dispatchSchedule", () => {
  it("delegates to dispatchAgentSchedule with registry + resolve", async () => {
    const registry = new InMemoryAgentRegistry();
    await registry.register({
      id: "echo-bot",
      backend: { type: "local", llm: { provider: "echo" } },
    } as unknown as Parameters<typeof registry.register>[0]);

    const agent = makeAgent();
    const agents = new ZoryaAgents({
      registry,
      resolve: () => agent,
      memory: new InMemoryMemoryStore(),
    });

    const schedule: DurableScheduleConfig = {
      id: "agent-sched",
      name: "agent-sched",
      intervalMs: 60_000,
      enabled: true,
      metadata: {
        agentTrigger: true,
        agentId: "echo-bot",
        task: "say hi",
        namespaceId: "tenant-a",
        createdByAgent: "echo-bot",
      },
    };

    const tick: ScheduleTick = {
      scheduleId: "agent-sched",
      scheduleName: "agent-sched",
      scheduledAt: new Date(),
      firedAt: new Date(),
      incarnation: "inc1",
      tickNumber: 0,
      metadata: schedule.metadata,
    };

    await agents.dispatchSchedule(tick, schedule);

    expect(agent.calls).toEqual(["say hi"]);
  });
});

describe("ZoryaAgents.dispatchSchedule — starts the turn, doesn't await it", () => {
  const schedule: DurableScheduleConfig = {
    id: "digest",
    name: "digest",
    intervalMs: 60_000,
    enabled: true,
    metadata: {
      agentTrigger: true,
      agentId: "slow-bot",
      task: "summarise",
      namespaceId: "tenant-a",
      createdByAgent: "slow-bot",
    },
  };
  const tick: ScheduleTick = {
    scheduleId: "digest",
    scheduleName: "digest",
    scheduledAt: new Date(0),
    firedAt: new Date(0),
    incarnation: "inc1",
    tickNumber: 3,
    metadata: schedule.metadata,
  };

  it("resolves while the model is still answering, under the tick's run id", async () => {
    const registry = new InMemoryAgentRegistry();
    await registry.register({
      id: "slow-bot",
      backend: { type: "local", llm: { provider: "echo" } },
    } as unknown as Parameters<typeof registry.register>[0]);
    const runIds: Array<string | undefined> = [];
    let answer!: (text: string) => void;
    const agent = {
      withScope: () => agent,
      invoke: (_input: unknown, opts?: { runId?: string }) => {
        runIds.push(opts?.runId);
        return new Promise((r) => (answer = (text) => r({ text: Promise.resolve(text) })));
      },
    } as unknown as Agent;
    const agents = new ZoryaAgents({ registry, resolve: () => agent });

    const started = await agents.dispatchSchedule(tick, schedule);
    expect(runIds).toEqual(["digest.inc1.3"]);

    answer("done");
    // `AgentScheduleStart.done` declares only `ok`/`error`; the turn's text
    // rides along at runtime.
    expect<unknown>(await started.done).toEqual({ ok: true, text: "done" });
  });

  it("rejects when the recipe is gone, so the tick is delivered again", async () => {
    const agents = new ZoryaAgents({
      registry: new InMemoryAgentRegistry(),
      resolve: () => makeAgent(),
    });
    await expect(agents.dispatchSchedule(tick, schedule)).rejects.toThrow("slow-bot");
  });
});

describe("ZoryaAgents lifecycle", () => {
  it("start/stop with no scan config is a no-op", async () => {
    const registry = new InMemoryAgentRegistry();
    const agents = new ZoryaAgents({
      registry,
      resolve: () => makeAgent(),
    });
    await agents.start();
    await agents.stop();
  });

  it("start is idempotent with scan disabled", async () => {
    const registry = new InMemoryAgentRegistry();
    const agents = new ZoryaAgents({
      registry,
      resolve: () => makeAgent(),
    });
    await agents.start();
    await agents.start(); // no-op (no scan config)
    await agents.stop();
    await agents.stop();
  });
});
