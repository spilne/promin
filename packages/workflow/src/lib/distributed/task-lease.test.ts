import { describe, it, expect } from "bun:test";
import { InMemoryStepQueue } from "./in-memory-step-queue.ts";

describe("task leasing race condition", () => {
  it("orphaned task is requeued after stale timeout", async () => {
    const queue = new InMemoryStepQueue({ workerId: "crashed-worker" });

    await queue.enqueue({
      workflowId: "wf-1",
      stepName: "step-1",
      input: {},
      prevResults: {},
    });
    const claimed = await queue.claim({ limit: 1 });
    expect(claimed).toHaveLength(1);

    // Worker crashes — never calls complete() or fail().
    await new Promise((r) => setTimeout(r, 50));

    const requeued = await queue.requeueStuck({ staleTimeoutMs: 30 });
    expect(requeued).toBe(1);

    const metrics = await queue.metrics({ since: new Date(Date.now() - 60_000) });
    expect(metrics.pending).toBe(1);
    expect(metrics.running).toBe(0);
  });

  it("running task within timeout is not requeued", async () => {
    const queue = new InMemoryStepQueue({ workerId: "active-worker" });

    await queue.enqueue({
      workflowId: "wf-1",
      stepName: "step-1",
      input: {},
      prevResults: {},
    });
    await queue.claim({ limit: 1 });

    const requeued = await queue.requeueStuck({ staleTimeoutMs: 60_000 });
    expect(requeued).toBe(0);

    const metrics = await queue.metrics({ since: new Date(Date.now() - 60_000) });
    expect(metrics.running).toBe(1);
  });
});
