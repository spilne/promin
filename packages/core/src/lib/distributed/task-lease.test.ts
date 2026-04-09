import { describe, it, expect } from "bun:test";
import { InMemoryStepQueue } from "./in-memory-step-queue.ts";

describe("task leasing race condition", () => {
  it("orphaned task is requeued after stale timeout", async () => {
    const queue = new InMemoryStepQueue({ workerId: "crashed-worker" });

    // Enqueue and claim a task
    await queue.enqueue({
      workflowId: "wf-1",
      stepName: "step-1",
      queue: "default",
      input: {},
      prevResults: {},
    });
    const claimed = await queue.claim({ queues: ["default"], limit: 1 });
    expect(claimed).toHaveLength(1);

    // Worker crashes — never calls complete() or fail().
    // Task sits in 'running' state with no way to recover via timeout.

    // Wait for the task to become stale
    await new Promise((r) => setTimeout(r, 50));

    // requeueStuck with a stale timeout should requeue orphaned tasks
    // regardless of which worker claimed them — any task in 'running'
    // state for longer than staleTimeoutMs gets requeued.
    const requeued = await queue.requeueStuck({ staleTimeoutMs: 30 });
    expect(requeued).toBe(1);

    // Task should be claimable again
    const metrics = await queue.metrics();
    expect(metrics["default"]!.pending).toBe(1);
    expect(metrics["default"]!.running).toBe(0);
  });

  it("running task within timeout is not requeued", async () => {
    const queue = new InMemoryStepQueue({ workerId: "active-worker" });

    await queue.enqueue({
      workflowId: "wf-1",
      stepName: "step-1",
      queue: "default",
      input: {},
      prevResults: {},
    });
    await queue.claim({ queues: ["default"], limit: 1 });

    // Task was just claimed — should NOT be requeued (still within timeout)
    const requeued = await queue.requeueStuck({ staleTimeoutMs: 60_000 });
    expect(requeued).toBe(0);

    const metrics = await queue.metrics();
    expect(metrics["default"]!.running).toBe(1);
  });
});
