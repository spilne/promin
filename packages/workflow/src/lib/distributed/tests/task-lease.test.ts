import { describe, it, expect } from "bun:test";
import { InMemoryStepQueue } from "../in-memory-step-queue.ts";
import { FakeWallClock } from "../../shared/wall-clock.ts";

describe("task leasing race condition", () => {
  it("orphaned task is requeued after stale timeout", async () => {
    const clock = FakeWallClock.create("2026-01-01T00:00:00Z");
    const queue = new InMemoryStepQueue({ clock });

    await queue.enqueue({
      workflowId: "wf-1",
      stepName: "step-1",
      input: {},
    });
    const claimed = await queue.claim({ workerId: "crashed-worker", limit: 1 });
    expect(claimed).toHaveLength(1);

    // Worker crashes — never calls complete() or fail(). Exactly at the
    // stale timeout the claim still counts as live; past it, it is requeued.
    clock.advance(30);
    expect((await queue.requeueStuck({ mode: "stale", olderThanMs: 30 })).requeued).toBe(0);
    clock.advance(1);
    const { requeued } = await queue.requeueStuck({ mode: "stale", olderThanMs: 30 });
    expect(requeued).toBe(1);

    const metrics = await queue.metrics({ since: new Date(clock.currentTimeMs() - 60_000) });
    expect(metrics.pending).toBe(1);
    expect(metrics.running).toBe(0);
  });

  it("running task within timeout is not requeued", async () => {
    const queue = new InMemoryStepQueue();

    await queue.enqueue({
      workflowId: "wf-1",
      stepName: "step-1",
      input: {},
    });
    await queue.claim({ workerId: "active-worker", limit: 1 });

    const { requeued } = await queue.requeueStuck({ mode: "stale", olderThanMs: 60_000 });
    expect(requeued).toBe(0);

    const metrics = await queue.metrics({ since: new Date(Date.now() - 60_000) });
    expect(metrics.running).toBe(1);
  });
});
