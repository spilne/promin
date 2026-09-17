// ---------------------------------------------------------------------------
// ZoryaAgents tests — schedule dispatch wiring + lifecycle.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import {
  InMemoryAgentRegistry,
  InMemoryMemoryStore,
  type Agent,
  type DurableScheduleConfig,
  type RegisteredAgent,
} from "@promin/agent";
import type { ScheduleTick } from "@promin/workflow";
import { ZoryaAgents } from "../index.ts";

function makeAgent(): Agent & { calls: string[] } {
  const calls: string[] = [];
  const agent: Agent & { calls: string[] } = {
    calls,
    withScope: () => agent,
    invoke: (input: { task: string }) => {
      calls.push(input.task);
      return { text: Promise.resolve("ok") } as ReturnType<Agent["invoke"]>;
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
      tickNumber: 0,
      metadata: schedule.metadata,
    };

    await agents.dispatchSchedule(tick, schedule);

    expect(agent.calls).toEqual(["say hi"]);
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
