// ---------------------------------------------------------------------------
// StepQueue call sites: the worker routes through the claim (workerId +
// registry step names + versions) and releases what its taskFilter rejects;
// the executor forwards the runner's attempt and gives up on a dead-lettered
// task; the runner's dead-worker sweep reclaims by the worker's own id.
// All timing runs on FakeWallClock.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { DefaultWorker } from "../worker.ts";
import { DistributedWorkflowRunner } from "../coordinator.ts";
import { InMemoryStepQueue } from "../in-memory-step-queue.ts";
import { InMemoryWorkerRegistry } from "../worker-registry.ts";
import { MapStepRegistry } from "../step-registry.ts";
import { deadLetterError } from "../step-queue.ts";
import { StepQueueExecutor } from "../step-queue-executor.ts";
import { InMemoryWorkflowStorage } from "../../durable/in-memory-storage.ts";
import type { StepExecutionResult } from "../../durable/workflow-runner.ts";
import { FakeWallClock } from "../../shared/wall-clock.ts";

/** Yield to the event loop until `predicate` holds (bounded). */
async function waitFor(predicate: () => boolean | Promise<boolean>): Promise<void> {
  for (let i = 0; i < 2_000 && !(await predicate()); i++) {
    await new Promise<void>((r) => setImmediate(r));
  }
  expect(await predicate()).toBe(true);
}

describe("DefaultWorker — routing inside the claim", () => {
  it("runs the task behind a head task for a step it doesn't host", async () => {
    const clock = FakeWallClock.create(0);
    const queue = new InMemoryStepQueue({ clock });
    const foreign = await queue.enqueue({
      workflowId: "a",
      stepName: "foreign",
      input: {},
      prevResults: {},
      priority: 9,
    });
    const mine = await queue.enqueue({
      workflowId: "b",
      stepName: "mine",
      input: {},
      prevResults: {},
      priority: 5,
    });
    const storage = new InMemoryWorkflowStorage({ clock });
    await storage.createWorkflow({ workflowId: "b", workflowName: "x", input: {} });
    const registry = new MapStepRegistry();
    registry.register("mine", async () => "ran");
    const worker = new DefaultWorker({
      storage,
      stepQueue: queue,
      registry,
      clock,
      workerId: "w-mine",
    });

    void worker.start();
    await waitFor(async () => (await queue.get(mine))?.status === "completed");
    await worker.stop();

    expect((await queue.get(mine))?.claimedBy).toBe("w-mine");
    const head = await queue.get(foreign);
    expect(head?.status).toBe("pending");
    expect(head?.deliveries).toBe(0);
  });

  it("claims only supported versions", async () => {
    const clock = FakeWallClock.create(0);
    const queue = new InMemoryStepQueue({ clock });
    const v3 = await queue.enqueue({
      workflowId: "a",
      stepName: "s",
      input: {},
      prevResults: {},
      priority: 9,
      version: "3",
    });
    const v1 = await queue.enqueue({
      workflowId: "b",
      stepName: "s",
      input: {},
      prevResults: {},
      version: "1",
    });
    const storage = new InMemoryWorkflowStorage({ clock });
    await storage.createWorkflow({ workflowId: "b", workflowName: "x", input: {} });
    const registry = new MapStepRegistry();
    registry.register("s", async () => "ran");
    const worker = new DefaultWorker({
      storage,
      stepQueue: queue,
      registry,
      clock,
      supportedVersions: ["1"],
    });

    void worker.start();
    await waitFor(async () => (await queue.get(v1))?.status === "completed");
    await worker.stop();
    expect((await queue.get(v3))?.status).toBe("pending");
  });

  it("releases tasks its taskFilter rejects, without counting a delivery", async () => {
    const clock = FakeWallClock.create(0);
    const queue = new InMemoryStepQueue({ clock });
    const id = await queue.enqueue({
      workflowId: "a",
      stepName: "s",
      input: {},
      prevResults: {},
      metadata: { tenant: "other" },
    });
    const registry = new MapStepRegistry();
    let ran = false;
    registry.register("s", async () => {
      ran = true;
    });
    let filtered = 0;
    const worker = new DefaultWorker({
      storage: new InMemoryWorkflowStorage({ clock }),
      stepQueue: queue,
      registry,
      clock,
      taskFilter: (task) => {
        filtered++;
        return task.metadata?.tenant === "mine";
      },
    });

    void worker.start();
    await waitFor(() => filtered >= 1);
    await worker.stop();

    expect(ran).toBe(false);
    const record = await queue.get(id);
    expect(record?.status).toBe("pending");
    expect(record?.deliveries).toBe(0);
  });
});

describe("StepQueueExecutor", () => {
  it("forwards the runner's attempt to the queued task", async () => {
    const clock = FakeWallClock.create(0);
    const queue = new InMemoryStepQueue({ clock });
    const storage = new InMemoryWorkflowStorage({ clock });
    await storage.createWorkflow({ workflowId: "wf", workflowName: "x", input: {} });
    const executor = new StepQueueExecutor({ stepQueue: queue, storage, clock });

    void executor.executeStep({
      workflowId: "wf",
      stepName: "s",
      input: {},
      prevResults: {},
      attempt: 3,
    });
    await waitFor(() => queue.getAllTasks().length === 1);
    expect(queue.getAllTasks()[0]!.attempt).toBe(3);
  });

  it("fails the step when its task is dead-lettered", async () => {
    const clock = FakeWallClock.create(0);
    const queue = new InMemoryStepQueue({ clock, maxDeliveries: 1 });
    const storage = new InMemoryWorkflowStorage({ clock });
    await storage.createWorkflow({ workflowId: "wf", workflowName: "x", input: {} });
    const executor = new StepQueueExecutor({
      stepQueue: queue,
      storage,
      clock,
      pollIntervalMs: 100,
      staleTimeoutMs: 1_000,
    });

    let result: StepExecutionResult | undefined;
    void executor
      .executeStep({ workflowId: "wf", stepName: "s", input: {}, prevResults: {}, attempt: 1 })
      .then((r) => (result = r));
    await waitFor(() => queue.getAllTasks().length === 1);
    // A worker claims the task and crashes every time it runs it.
    await queue.claim({ workerId: "w-crashy", limit: 1 });

    for (let i = 0; i < 30 && result === undefined; i++) {
      clock.advance(100);
      await new Promise<void>((r) => setImmediate(r));
      await new Promise<void>((r) => setImmediate(r));
    }

    expect(result).toEqual({ ok: false, error: deadLetterError(1) });
  });
});

describe("DistributedWorkflowRunner — dead-worker sweep", () => {
  it("reclaims tasks claimed under the dead worker's own id", async () => {
    const clock = FakeWallClock.create(0);
    const queue = new InMemoryStepQueue({ clock });
    const workerRegistry = new InMemoryWorkerRegistry({ clock });
    await workerRegistry.register({ workerId: "w-dead", capabilities: [], concurrency: 1 });
    const id = await queue.enqueue({ workflowId: "wf", stepName: "s", input: {}, prevResults: {} });
    const [dead] = await queue.claim({ workerId: "w-dead", limit: 1 });
    const kept = await queue.enqueue({
      workflowId: "wf-2",
      stepName: "s",
      input: {},
      prevResults: {},
    });
    const [alive] = await queue.claim({ workerId: "w-alive", limit: 1 });

    const runner = new DistributedWorkflowRunner({
      storage: new InMemoryWorkflowStorage({ clock }),
      stepQueue: queue,
      workerRegistry,
      workerTimeoutMs: 10_000,
      pollIntervalMs: 1_000,
      clock,
    });
    // Both task leases are fresh, so the stale sweep requeues nothing; only
    // w-dead's registry heartbeat has lapsed.
    clock.advance(11_000);
    await queue.heartbeat({ taskId: dead!.id, claimToken: dead!.claimToken });
    await queue.heartbeat({ taskId: alive!.id, claimToken: alive!.claimToken });

    const loop = runner.startLoop();
    await waitFor(async () => (await queue.get(id))?.status === "pending");
    await runner.stopLoop();
    await loop;

    expect((await queue.get(kept))?.status).toBe("running");
  });
});
