// ---------------------------------------------------------------------------
// Step groups end to end: a coordinator and a worker pool over one step
// queue. A step in a rate-limit / concurrency group that the group holds back
// is deferred — not failed, no retry attempt used — and is visible in
// `getStatus().waitingOn` and the tracer until the group has room.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { succeed } from "@spilne/perfect-core";
import { workflow } from "../../durable/durable-pipeline.ts";
import { InMemoryWorkflowStorage } from "../../durable/in-memory-storage.ts";
import type {
  WorkflowTraceStepThrottleEnd,
  WorkflowTraceStepThrottled,
} from "../../durable/workflow-tracer.ts";
import { FakeWallClock } from "../../shared/wall-clock.ts";
import { createDistributedWorkflowRunner } from "../coordinator.ts";
import { InMemoryStepQueue } from "../in-memory-step-queue.ts";
import { MapStepRegistry } from "../step-registry.ts";
import { createWorker } from "../worker.ts";

const POLL_MS = 50;

async function flush(): Promise<void> {
  for (let i = 0; i < 20; i++) await new Promise<void>((r) => setImmediate(r));
}

async function driveUntil(params: {
  clock: FakeWallClock;
  done: () => boolean | Promise<boolean>;
  maxMs?: number;
}): Promise<void> {
  const { clock, done, maxMs = 120_000 } = params;
  for (let elapsed = 0; elapsed <= maxMs; elapsed += POLL_MS) {
    await flush();
    if (await done()) return;
    clock.advance(POLL_MS);
  }
  expect(await done()).toBe(true);
}

/** Settle `runs` by driving the fake clock. */
async function settle(params: { clock: FakeWallClock; runs: Promise<unknown>[] }): Promise<void> {
  let pending = params.runs.length;
  for (const r of params.runs)
    void r.then(
      () => pending--,
      () => pending--,
    );
  await driveUntil({ clock: params.clock, done: () => pending === 0 });
}

function setup(params: { windowedStarts?: boolean }) {
  const clock = FakeWallClock.create(0);
  const storage = new InMemoryWorkflowStorage({ clock });
  const queue = new InMemoryStepQueue({ clock });
  const thrown: WorkflowTraceStepThrottled[] = [];
  const ended: WorkflowTraceStepThrottleEnd[] = [];
  const coordinator = createDistributedWorkflowRunner({
    storage,
    stepQueue: queue,
    pollIntervalMs: POLL_MS,
    stepPollIntervalMs: POLL_MS,
    clock,
    tracer: {
      stepThrottled: (e) => void thrown.push(e),
      stepThrottleEnd: (e) => void ended.push(e),
    },
  });
  const group = params.windowedStarts
    ? { name: "openai", maxStarts: 1, windowMs: 10_000 }
    : { name: "openai", maxConcurrent: 1 };
  const wf = workflow<{ tenant: string }>({ name: "call-llm" })
    .step("call", () => succeed("ok"), {
      queue: {
        group: { ...group, fairnessKey: (ctx) => (ctx.input as { tenant: string }).tenant },
      },
    })
    .build();

  let release: () => void = () => {};
  const gate = new Promise<void>((r) => (release = r));
  const started: string[] = [];
  const registry = new MapStepRegistry();
  registry.register({
    stepName: "call",
    handler: async (ctx) => {
      started.push(ctx.workflowId);
      if (!params.windowedStarts) await gate;
      return "ok";
    },
  });
  const worker = createWorker({
    stepQueue: queue,
    registry,
    pollIntervalMs: POLL_MS,
    clock,
    workerId: "w-1",
    concurrency: 4,
  });
  void worker.start();
  return { clock, storage, queue, coordinator, wf, release, started, thrown, ended, worker };
}

describe("step groups: a throttled step is deferred, visible and then runs", () => {
  it("concurrency: the second run's step waits (waitingOn, tracer) without failing or using an attempt", async () => {
    const t = setup({});
    const a = t.coordinator.run({ workflow: t.wf, workflowId: "run-a", input: { tenant: "a" } });
    const b = t.coordinator.run({ workflow: t.wf, workflowId: "run-b", input: { tenant: "b" } });
    await driveUntil({
      clock: t.clock,
      done: () => t.started.length === 1 && t.thrown.length === 1,
    });

    const heldId = t.started[0] === "run-a" ? "run-b" : "run-a";
    const status = await t.coordinator.getStatus({ workflowId: heldId });
    expect(["pending", "running"].includes(status?.state ?? "")).toBe(true);
    expect(status?.waitingOn).toHaveLength(1);
    expect(status?.waitingOn[0]).toMatchObject({
      kind: "step-queue",
      stepName: "call",
      groupName: "openai",
      reason: "concurrency",
      attempt: 1,
    });
    expect(status?.waitingOnSummary ?? "").toContain("group openai");
    // The run that holds the slot is not waiting on the queue: its step is
    // running, on the worker that claimed it (once it has run for a moment).
    t.clock.advance(1_000);
    await flush();
    expect((await t.coordinator.getStatus({ workflowId: t.started[0]! }))?.waitingOn).toEqual([
      expect.objectContaining({ kind: "activity", stepName: "call", holder: "w-1" }),
    ]);
    expect(t.thrown[0]).toMatchObject({
      workflowId: heldId,
      workflowName: "call-llm",
      stepName: "call",
      groupName: "openai",
      reason: "concurrency",
      attempt: 1,
    });
    expect(t.ended).toHaveLength(0);

    t.release();
    await settle({ clock: t.clock, runs: [a, b] });
    expect(t.started.sort()).toEqual(["run-a", "run-b"]);
    expect(t.ended).toHaveLength(1);
    expect(t.ended[0]).toMatchObject({ workflowId: heldId, groupName: "openai" });
    expect(t.ended[0]!.waitedMs).toBeGreaterThanOrEqual(0);

    // Deferred, not retried: one attempt, and the task was claimed once.
    for (const id of ["run-a", "run-b"]) {
      const state = (await t.storage.loadWorkflow(id))!;
      expect(state.status).toBe("completed");
      expect(state.steps["call"]!.attempt).toBe(1);
    }
    expect(t.queue.getAllTasks().map((x) => x.deliveries)).toEqual([1, 1]);
    // Nothing is waiting any more.
    expect((await t.coordinator.getStatus({ workflowId: heldId }))?.waitingOn).toEqual([]);
    await t.worker.stop();
  });

  it("rate window: the held step reports when the window frees a slot, then starts after it", async () => {
    const t = setup({ windowedStarts: true });
    const a = t.coordinator.run({ workflow: t.wf, workflowId: "run-a", input: { tenant: "a" } });
    const b = t.coordinator.run({ workflow: t.wf, workflowId: "run-b", input: { tenant: "b" } });
    await driveUntil({
      clock: t.clock,
      done: () => t.started.length === 1 && t.thrown.length === 1,
    });
    const startedAt = t.clock.currentTimeMs();

    const heldId = t.started[0] === "run-a" ? "run-b" : "run-a";
    const wait = (await t.coordinator.getStatus({ workflowId: heldId }))?.waitingOn[0];
    expect(wait).toMatchObject({ kind: "step-queue", groupName: "openai", reason: "rate" });
    // The window frees the slot 10s after the first start (a few polls ago).
    expect(wait!.until!.getTime()).toBeGreaterThan(startedAt);
    expect(wait!.until!.getTime()).toBeLessThanOrEqual(startedAt + 10_000);
    expect(t.thrown[0]!.reason).toBe("rate");

    await settle({ clock: t.clock, runs: [a, b] });
    expect(t.clock.currentTimeMs()).toBeGreaterThanOrEqual(10_000);
    expect(t.started.sort()).toEqual(["run-a", "run-b"]);
    await t.worker.stop();
  });
});
