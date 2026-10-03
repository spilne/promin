import { describe, it, expect } from "bun:test";
import { InMemoryStepQueue } from "../in-memory-step-queue.ts";
import { FakeWallClock } from "../../shared/wall-clock.ts";

describe("InMemoryStepQueue — leases across a reclaim", () => {
  it("a re-claimed task isn't requeued by the next sweep because of the old claim's heartbeat", async () => {
    const clock = FakeWallClock.create(0);
    const q = new InMemoryStepQueue({ clock });
    await q.enqueue({ workflowId: "wf", stepName: "s", input: {}, prevResults: {} });

    const [a] = await q.claim({ workerId: "w", limit: 1 });
    clock.advance(1_000);
    expect(await q.heartbeat({ taskId: a!.id, claimToken: a!.claimToken })).toBe(true);

    // Worker A dies; its heartbeat goes stale and the sweep requeues.
    clock.advance(60_000);
    expect((await q.requeueStuck({ mode: "stale", olderThanMs: 30_000 })).requeued).toBe(1);

    // Worker B claims, and a sweep 1 ms later must leave B's lease alone.
    const [b] = await q.claim({ workerId: "w", limit: 1 });
    expect(b?.id).toBe(a!.id);
    clock.advance(1);
    expect((await q.requeueStuck({ mode: "stale", olderThanMs: 30_000 })).requeued).toBe(0);
    expect(await q.claim({ workerId: "w", limit: 1 })).toEqual([]);

    // B's lease still expires on its own schedule.
    clock.advance(30_000);
    expect((await q.requeueStuck({ mode: "stale", olderThanMs: 30_000 })).requeued).toBe(1);
  });
});
