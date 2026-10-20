// ---------------------------------------------------------------------------
// StepQueueExecutor wait bounds: a step that gets no outcome within
// `stepWaitTimeoutMs` fails with `StepWaitTimeoutError` (a running task
// loses its claim), and a wait whose run was deleted or ended stops with
// `StepWaitAbandonedError`. All timing runs on FakeWallClock.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { succeed } from "@spilne/perfect-core";
import { DistributedWorkflowRunner } from "../coordinator.ts";
import { InMemoryStepQueue } from "../in-memory-step-queue.ts";
import {
  StepQueueExecutor,
  StepWaitAbandonedError,
  StepWaitTimeoutError,
} from "../step-queue-executor.ts";
import { InMemoryWorkflowStorage } from "../../durable/in-memory-storage.ts";
import { workflow } from "../../durable/durable-pipeline.ts";
import type { StepExecutionResult } from "../../durable/workflow-runner.ts";
import { FakeWallClock } from "../../shared/wall-clock.ts";

const POLL_MS = 100;

/** Yield to the event loop until `predicate` holds (bounded). */
async function waitFor(predicate: () => boolean | Promise<boolean>): Promise<void> {
  for (let i = 0; i < 2_000 && !(await predicate()); i++) {
    await new Promise<void>((r) => setImmediate(r));
  }
  expect(await predicate()).toBe(true);
}

/**
 * Advance the clock one poll at a time, each time after the executor has
 * scheduled its next wait, until `done()` holds or `maxMs` has passed.
 */
async function advancePolls(params: {
  clock: FakeWallClock;
  done: () => boolean;
  maxMs: number;
}): Promise<void> {
  const { clock, done, maxMs } = params;
  for (let elapsed = 0; elapsed < maxMs && !done(); elapsed += POLL_MS) {
    await waitFor(() => done() || clock.pendingCount() > 0);
    if (done()) return;
    clock.advance(POLL_MS);
  }
  await waitFor(() => done() || clock.pendingCount() > 0);
}

async function setup(params: { stepWaitTimeoutMs?: number } = {}) {
  const clock = FakeWallClock.create(0);
  const queue = new InMemoryStepQueue({ clock });
  const storage = new InMemoryWorkflowStorage({ clock });
  await storage.createWorkflow({ workflowId: "wf", workflowName: "x", input: {} });
  const executor = new StepQueueExecutor({
    stepQueue: queue,
    storage,
    clock,
    pollIntervalMs: POLL_MS,
    staleTimeoutMs: 60 * 60_000,
    ...params,
  });
  const box: { result?: StepExecutionResult } = {};
  void executor
    .executeStep({ workflowId: "wf", stepName: "s", input: {}, prevResults: {}, attempt: 1 })
    .then((r) => (box.result = r));
  await waitFor(() => queue.getAllTasks().length === 1);
  return { clock, queue, storage, box };
}

describe("StepQueueExecutor — wait deadline", () => {
  it("fails a step nobody picks up once stepWaitTimeoutMs has passed, not before", async () => {
    const { clock, queue, box } = await setup({ stepWaitTimeoutMs: 1_000 });

    await advancePolls({ clock, done: () => box.result !== undefined, maxMs: 900 });
    expect(box.result).toBeUndefined();

    await advancePolls({ clock, done: () => box.result !== undefined, maxMs: 500 });
    const result = box.result;
    expect(result?.ok).toBe(false);
    if (result?.ok !== false || result.kind !== "failed") throw new Error("expected a failure");
    expect(result.errorTag).toBe("StepWaitTimeoutError");
    expect(result.cause).toBeInstanceOf(StepWaitTimeoutError);
    expect(result.cause).toMatchObject({
      workflowId: "wf",
      stepName: "s",
      taskId: queue.getAllTasks()[0]!.id,
      timeoutMs: 1_000,
    });
    expect(result.error).toContain("got no outcome within 1000ms");
    expect(clock.currentTimeMs()).toBe(1_000);
    // A pending task can't be cancelled; it stays queued.
    expect(queue.getAllTasks()[0]!.status).toBe("pending");
  });

  it("takes the claim away from a worker still running the step at the deadline", async () => {
    const { clock, queue, box } = await setup({ stepWaitTimeoutMs: 500 });
    const [task] = await queue.claim({ workerId: "w-slow", limit: 1 });

    await advancePolls({ clock, done: () => box.result !== undefined, maxMs: 1_000 });
    expect(box.result).toMatchObject({
      ok: false,
      kind: "failed",
      errorTag: "StepWaitTimeoutError",
    });
    const record = await queue.get(task!.id);
    expect(record?.status).toBe("failed");
    expect(record?.error).toContain("got no outcome");
    // The slow worker's late commit is rejected.
    expect(await queue.heartbeat({ taskId: task!.id, claimToken: task!.claimToken })).toBe(false);
    expect(
      await queue.complete({
        taskId: task!.id,
        claimToken: task!.claimToken,
        result: 1,
        durationMs: 1,
      }),
    ).toBe(false);
  });

  it("an outcome written at the deadline still wins", async () => {
    const { clock, storage, box } = await setup({ stepWaitTimeoutMs: 300 });
    await advancePolls({ clock, done: () => box.result !== undefined, maxMs: 200 });
    await storage.saveStepResult({
      workflowId: "wf",
      stepName: "s",
      result: 7,
      durationMs: 1,
      startedAt: clock.now(),
    });
    await advancePolls({ clock, done: () => box.result !== undefined, maxMs: 500 });
    expect(box.result).toEqual({ ok: true, result: 7, storageAlreadyCheckpointed: true });
  });

  it("waits indefinitely with stepWaitTimeoutMs: Infinity", async () => {
    const { clock, box } = await setup({ stepWaitTimeoutMs: Infinity });
    clock.advance(30 * 24 * 60 * 60_000);
    await advancePolls({ clock, done: () => box.result !== undefined, maxMs: 2_000 });
    expect(box.result).toBeUndefined();
  });

  it("rejects a non-positive stepWaitTimeoutMs", () => {
    const clock = FakeWallClock.create(0);
    const make = (ms: number) =>
      new StepQueueExecutor({
        stepQueue: new InMemoryStepQueue({ clock }),
        storage: new InMemoryWorkflowStorage({ clock }),
        stepWaitTimeoutMs: ms,
      });
    expect(() => make(0)).toThrow("stepWaitTimeoutMs must be positive");
    expect(() => make(Number.NaN)).toThrow("stepWaitTimeoutMs must be positive");
  });
});

describe("StepQueueExecutor — abandoned waits", () => {
  it("stops waiting when the run is deleted", async () => {
    const { clock, storage, box } = await setup();
    storage.loadWorkflow = async () => null;

    await advancePolls({ clock, done: () => box.result !== undefined, maxMs: 1_000 });
    const result = box.result;
    if (result?.ok !== false || result.kind !== "failed") throw new Error("expected a failure");
    expect(result.errorTag).toBe("StepWaitAbandonedError");
    expect(result.cause).toBeInstanceOf(StepWaitAbandonedError);
    expect(result.cause).toMatchObject({ reason: "deleted", workflowId: "wf", stepName: "s" });
    expect(result.error).toContain('workflow "wf" was deleted');
    expect(clock.pendingCount()).toBe(0);
  });

  it("stops waiting when the run reaches a terminal status (cancelled)", async () => {
    const { clock, storage, box } = await setup();
    await advancePolls({ clock, done: () => box.result !== undefined, maxMs: 300 });
    expect(box.result).toBeUndefined();
    await storage.cancelWorkflow("wf");

    await advancePolls({ clock, done: () => box.result !== undefined, maxMs: 1_000 });
    expect(box.result).toMatchObject({
      ok: false,
      kind: "failed",
      errorTag: "StepWaitAbandonedError",
      cause: { reason: "terminal", status: "failed" },
    });
  });
});

describe("DistributedWorkflowRunner — stepWaitTimeoutMs", () => {
  it("a step no worker hosts fails the run once the wait deadline passes", async () => {
    const clock = FakeWallClock.create(0);
    const storage = new InMemoryWorkflowStorage({ clock });
    const queue = new InMemoryStepQueue({ clock });
    const runner = new DistributedWorkflowRunner({
      storage,
      stepQueue: queue,
      stepPollIntervalMs: POLL_MS,
      stepWaitTimeoutMs: 2_000,
      clock,
      onError: () => {},
    });
    const wf = workflow<number>({ name: "orphan-step" })
      .step("s", ({ input }) => succeed(input))
      .build();

    await runner.submit({ workflow: wf, workflowId: "o", input: 1 });
    await waitFor(() => queue.getAllTasks().length === 1);
    let failed = false;
    for (let i = 0; i < 100 && !failed; i++) {
      clock.advance(POLL_MS);
      await waitFor(async () => {
        failed = (await storage.loadWorkflow("o"))?.status === "failed";
        return failed || clock.pendingCount() > 0;
      });
    }

    const state = await storage.loadWorkflow("o");
    expect(state?.status).toBe("failed");
    expect(state?.steps["s"]?.status).toBe("failed");
    expect(state?.steps["s"]?.error).toContain("got no outcome within 2000ms");
    expect(clock.currentTimeMs()).toBeLessThan(3_000);
  });
});
