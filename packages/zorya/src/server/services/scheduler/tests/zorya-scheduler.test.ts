// ---------------------------------------------------------------------------
// ZoryaScheduler tests — dispatch routing (workflows vs agents vs override).
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { FakeWallClock, InMemoryWorkflowStorage } from "@promin/workflow";
import {
  InMemorySchedulerStorage,
  type DurableScheduleConfig,
  type ScheduleTick,
} from "@promin/workflow/scheduler";
import { QueuedWorkflows } from "../../workflows/index.ts";
import { ZoryaScheduler, type AgentScheduleDispatcher } from "../index.ts";
import type { SchedulerLoopErrorEvent } from "../../scheduler-loop.ts";

async function seedSchedule(
  storage: InMemorySchedulerStorage,
  cfg: Partial<DurableScheduleConfig> & { id: string },
): Promise<void> {
  await storage.upsertSchedule({
    id: cfg.id,
    name: cfg.name ?? cfg.id,
    intervalMs: 60_000,
    enabled: true,
    metadata: cfg.metadata ?? {},
    ...cfg,
  } as DurableScheduleConfig);
  await storage.setNextRun(cfg.id, new Date(Date.now() - 1000));
}

describe("ZoryaScheduler.fireOnce", () => {
  it("dispatches a workflow schedule via workflows.trigger", async () => {
    const wfStorage = new InMemoryWorkflowStorage();
    const schedStorage = new InMemorySchedulerStorage();
    const workflows = new QueuedWorkflows({ storage: wfStorage, acceptAny: true });
    const scheduler = new ZoryaScheduler({ storage: schedStorage, workflows });

    await seedSchedule(schedStorage, {
      id: "wf-sched",
      metadata: { workflowName: "my-wf", input: { x: 1 } },
    });

    await scheduler.fireOnce("wf-sched");

    // The trigger flow should have created a workflow row
    const runs = await wfStorage.listWorkflows({ limit: 10 });
    expect(runs.length).toBe(1);
    expect(runs[0]?.workflowName).toBe("my-wf");
    expect(runs[0]?.runSource).toBe("schedule");
    expect(runs[0]?.runSourceId).toBe("wf-sched");
  });

  it("runs in the schedule's namespace, not metadata.namespace", async () => {
    const wfStorage = new InMemoryWorkflowStorage();
    const schedStorage = new InMemorySchedulerStorage();
    const workflows = new QueuedWorkflows({ storage: wfStorage, acceptAny: true });
    const scheduler = new ZoryaScheduler({ storage: schedStorage, workflows, namespaces: "all" });

    await seedSchedule(schedStorage, {
      id: "wf-tenant",
      namespace: "tenant-a",
      metadata: { workflowName: "my-wf", namespace: "other-tenant" },
    });
    await seedSchedule(schedStorage, {
      id: "wf-global",
      metadata: { workflowName: "my-wf", namespace: "tenant-b" },
    });

    await scheduler.fireOnce("wf-tenant");
    await scheduler.fireOnce("wf-global");

    const runs = await wfStorage.listWorkflows({ limit: 10 });
    const bySchedule = new Map(runs.map((r) => [r.runSourceId, r.namespace]));
    expect(bySchedule.get("wf-tenant")).toBe("tenant-a");
    // A schedule in the global namespace still honours metadata.namespace.
    expect(bySchedule.get("wf-global")).toBe("tenant-b");
  });

  it("routes agent schedules through AgentScheduleDispatcher", async () => {
    const wfStorage = new InMemoryWorkflowStorage();
    const schedStorage = new InMemorySchedulerStorage();
    const workflows = new QueuedWorkflows({ storage: wfStorage, acceptAny: true });

    const dispatched: Array<{ tick: ScheduleTick; schedule: DurableScheduleConfig }> = [];
    const agents: AgentScheduleDispatcher = {
      dispatchSchedule: async (tick, schedule) => {
        dispatched.push({ tick, schedule });
        return { done: Promise.resolve({ ok: true }) };
      },
    };

    const scheduler = new ZoryaScheduler({ storage: schedStorage, workflows, agents });

    // Agent-targeted schedule per @promin/agent isAgentSchedule
    await seedSchedule(schedStorage, {
      id: "agent-sched",
      metadata: {
        agentTrigger: true,
        agentId: "echo-bot",
        task: "say hello",
        namespaceId: "tenant-a",
        createdByAgent: "echo-bot",
      },
    });

    await scheduler.fireOnce("agent-sched");

    expect(dispatched.length).toBe(1);
    expect(dispatched[0]?.tick.scheduleId).toBe("agent-sched");
    // Workflow trigger should NOT have fired
    const runs = await wfStorage.listWorkflows({ limit: 10 });
    expect(runs.length).toBe(0);
  });

  it("user fire override wins over both routes", async () => {
    const wfStorage = new InMemoryWorkflowStorage();
    const schedStorage = new InMemorySchedulerStorage();
    const workflows = new QueuedWorkflows({ storage: wfStorage, acceptAny: true });

    const overrideFires: string[] = [];
    const scheduler = new ZoryaScheduler({
      storage: schedStorage,
      workflows,
      fire: async (tick) => {
        overrideFires.push(tick.scheduleId);
        return { handled: true };
      },
    });

    await seedSchedule(schedStorage, {
      id: "any",
      metadata: { workflowName: "my-wf", input: {} },
    });

    await scheduler.fireOnce("any");

    expect(overrideFires).toEqual(["any"]);
    const runs = await wfStorage.listWorkflows({ limit: 10 });
    expect(runs.length).toBe(0);
  });

  it("user fire returning { handled: false } falls through to workflows", async () => {
    const wfStorage = new InMemoryWorkflowStorage();
    const schedStorage = new InMemorySchedulerStorage();
    const workflows = new QueuedWorkflows({ storage: wfStorage, acceptAny: true });

    const scheduler = new ZoryaScheduler({
      storage: schedStorage,
      workflows,
      fire: async () => ({ handled: false }),
    });

    await seedSchedule(schedStorage, {
      id: "fall",
      metadata: { workflowName: "wf", input: {} },
    });

    await scheduler.fireOnce("fall");

    const runs = await wfStorage.listWorkflows({ limit: 10 });
    expect(runs.length).toBe(1);
  });
});

describe("ZoryaScheduler lifecycle", () => {
  it("start/stop are clean", async () => {
    const wfStorage = new InMemoryWorkflowStorage();
    const schedStorage = new InMemorySchedulerStorage();
    const workflows = new QueuedWorkflows({ storage: wfStorage, acceptAny: true });
    const scheduler = new ZoryaScheduler({
      storage: schedStorage,
      workflows,
      pollIntervalMs: 50,
    });

    await scheduler.start();
    await scheduler.stop();
  });
});

describe("ZoryaScheduler — agent ticks start turns, they don't wait for them", () => {
  const T0 = Date.parse("2026-01-01T00:00:00Z");
  const agentMetadata = {
    agentTrigger: true,
    agentId: "digest-bot",
    task: "write the digest",
    namespaceId: "tenant-a",
    createdByAgent: "digest-bot",
  };

  function setup(params: { turn: Promise<{ ok: boolean; error?: string }> }) {
    const clock = FakeWallClock.create(T0);
    const schedStorage = new InMemorySchedulerStorage({ clock });
    const wfStorage = new InMemoryWorkflowStorage();
    const workflows = new QueuedWorkflows({ storage: wfStorage, acceptAny: true });
    const started: string[] = [];
    const errors: SchedulerLoopErrorEvent[] = [];
    const agents: AgentScheduleDispatcher = {
      dispatchSchedule: async (tick) => {
        started.push(`${tick.scheduleId}.${tick.tickNumber}`);
        return { done: params.turn };
      },
    };
    const scheduler = new ZoryaScheduler({
      storage: schedStorage,
      workflows,
      agents,
      clock,
      onError: (e) => void errors.push(e),
    });
    return { clock, schedStorage, wfStorage, scheduler, started, errors };
  }

  it("a poll returns while the agent turn runs, and the other schedules keep firing", async () => {
    const turn = new Promise<{ ok: boolean }>(() => {}); // an LLM turn that never ends here
    const { clock, schedStorage, wfStorage, scheduler, started } = setup({ turn });
    await schedStorage.upsertSchedule({
      id: "digest",
      intervalMs: 3_600_000,
      metadata: agentMetadata,
    });
    await schedStorage.upsertSchedule({
      id: "heartbeat",
      intervalMs: 1_000,
      metadata: { workflowName: "hb" },
    });

    const first = await scheduler.tickOnce();
    expect(first.map((t) => t.scheduleId).sort()).toEqual(["digest", "heartbeat"]);
    for (let s = 1; s <= 5; s++) {
      clock.advance(1_000);
      expect((await scheduler.tickOnce()).map((t) => `${t.scheduleId}.${t.tickNumber}`)).toEqual([
        `heartbeat.${s}`,
      ]);
    }

    expect(started).toEqual(["digest.0"]);
    expect((await schedStorage.loadScheduleState("digest"))?.tickCount).toBe(1);
    expect(await wfStorage.listWorkflows({ name: "hb", limit: 10 })).toHaveLength(6);
  });

  it("a tick redelivered while its turn still runs doesn't start a second turn", async () => {
    let finish!: (r: { ok: boolean }) => void;
    const turn = new Promise<{ ok: boolean }>((r) => (finish = r));
    const { schedStorage, scheduler, started, errors } = setup({ turn });
    await schedStorage.upsertSchedule({
      id: "digest",
      intervalMs: 3_600_000,
      metadata: agentMetadata,
    });
    const commit = schedStorage.commitPoll.bind(schedStorage);
    let failCommit = true;
    schedStorage.commitPoll = async (p) => {
      if (failCommit) {
        failCommit = false;
        throw new Error("commit blip");
      }
      return commit(p);
    };

    await scheduler.tickOnce();
    await scheduler.tickOnce(); // the same tick, delivered again

    expect(started).toEqual(["digest.0"]);
    expect(errors.map((e) => e.phase)).toEqual(["commit"]);
    expect((await schedStorage.loadScheduleState("digest"))?.tickCount).toBe(1);
    finish({ ok: true });
  });

  it("a failed turn is reported after the fact", async () => {
    const { schedStorage, scheduler, errors } = setup({
      turn: Promise.resolve({ ok: false, error: "model unavailable" }),
    });
    await schedStorage.upsertSchedule({
      id: "digest",
      intervalMs: 3_600_000,
      metadata: agentMetadata,
    });

    await scheduler.tickOnce();
    for (let i = 0; i < 2_000 && errors.length === 0; i++) {
      await new Promise((r) => setTimeout(r, 0));
    }

    expect(errors.map((e) => [e.phase, e.scheduleId, e.tickNumber])).toEqual([
      ["turn", "digest", 0],
    ]);
    expect((errors[0]!.error as Error).message).toBe("model unavailable");
  });

  it("an agent tick that could not start stays due", async () => {
    const clock = FakeWallClock.create(T0);
    const schedStorage = new InMemorySchedulerStorage({ clock });
    const errors: SchedulerLoopErrorEvent[] = [];
    const scheduler = new ZoryaScheduler({
      storage: schedStorage,
      workflows: new QueuedWorkflows({ storage: new InMemoryWorkflowStorage(), acceptAny: true }),
      agents: {
        dispatchSchedule: async () => {
          throw new Error('recipe "digest-bot" not found');
        },
      },
      clock,
      onError: (e) => void errors.push(e),
    });
    await schedStorage.upsertSchedule({
      id: "digest",
      intervalMs: 3_600_000,
      metadata: agentMetadata,
    });

    expect(await scheduler.tickOnce()).toEqual([]);
    expect(errors.map((e) => [e.phase, e.scheduleId])).toEqual([["dispatch", "digest"]]);
    expect((await schedStorage.loadScheduleState("digest"))?.tickCount).toBe(0);
  });
});
