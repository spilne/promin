// ---------------------------------------------------------------------------
// ZoryaScheduler tests — dispatch routing (workflows vs agents vs override).
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import {
  InMemorySchedulerStorage,
  InMemoryWorkflowStorage,
  type DurableScheduleConfig,
  type ScheduleTick,
} from "@promin/workflow";
import { QueuedWorkflows } from "../../workflows/index.ts";
import { ZoryaScheduler, type AgentScheduleDispatcher } from "../index.ts";

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

  it("uses schedule namespace and sampleInput when metadata omits input", async () => {
    const wfStorage = new InMemoryWorkflowStorage();
    const schedStorage = new InMemorySchedulerStorage();
    const workflows = new QueuedWorkflows({ storage: wfStorage, acceptAny: true });
    const scheduler = new ZoryaScheduler({
      storage: schedStorage,
      workflows,
      sampleInput: (name) => ({ seededFor: name }),
    });

    await seedSchedule(schedStorage, {
      id: "wf-default-input",
      namespace: "tenant-a",
      metadata: { workflowName: "my-wf", namespace: "legacy-tenant" },
    });

    await scheduler.fireOnce("wf-default-input");

    const runs = await wfStorage.listWorkflows({ limit: 10 });
    expect(runs.length).toBe(1);
    expect(runs[0]?.workflowName).toBe("my-wf");
    expect(runs[0]?.input).toEqual({ seededFor: "my-wf" });
    expect(runs[0]?.namespace).toBe("tenant-a");
  });

  it("routes agent schedules through AgentScheduleDispatcher", async () => {
    const wfStorage = new InMemoryWorkflowStorage();
    const schedStorage = new InMemorySchedulerStorage();
    const workflows = new QueuedWorkflows({ storage: wfStorage, acceptAny: true });

    const dispatched: Array<{ tick: ScheduleTick; schedule: DurableScheduleConfig }> = [];
    const agents: AgentScheduleDispatcher = {
      dispatchSchedule: async (tick, schedule) => {
        dispatched.push({ tick, schedule });
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
