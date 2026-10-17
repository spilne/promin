import { describe, it, expect } from "bun:test";
import { InMemoryWorkflowStartQueue } from "../workflow-start-queue.ts";
import { workflowStartQueueTestSuite } from "../workflow-start-queue-test-suite.ts";
import { FakeWallClock } from "../../shared/wall-clock.ts";

workflowStartQueueTestSuite(
  ({ clock, reclaimAfterMs }) => new InMemoryWorkflowStartQueue({ clock, reclaimAfterMs }),
);

describe("InMemoryWorkflowStartQueue — stamps and reclaim on an injected clock", () => {
  const specs = [{ name: "wf", versions: [] }];

  it("stamps enqueuedAt / claimedAt from the clock", async () => {
    const clock = FakeWallClock.create("2026-01-01T00:00:00Z");
    const queue = new InMemoryWorkflowStartQueue({ clock });

    await queue.enqueue({ workflowId: "w-1", workflowName: "wf", input: {} });
    clock.advance(250);
    const [claimed] = await queue.claim({ workflowSpecs: specs, workerId: "a", limit: 1 });

    const t0 = Date.parse("2026-01-01T00:00:00Z");
    expect(claimed?.enqueuedAt).toBe(t0);
    expect(claimed?.claimedAt).toBe(t0 + 250);
  });

  it("requeues a stale claim only once the clock passes reclaimAfterMs", async () => {
    const clock = FakeWallClock.create("2026-01-01T00:00:00Z");
    const queue = new InMemoryWorkflowStartQueue({ reclaimAfterMs: 1_000, clock });

    await queue.enqueue({ workflowId: "w-2", workflowName: "wf", input: {} });
    const first = await queue.claim({ workflowSpecs: specs, workerId: "a", limit: 1 });
    expect(first).toHaveLength(1);

    // At the cutoff boundary the claim is not yet stale (strict `<`).
    clock.advance(1_000);
    expect(await queue.claim({ workflowSpecs: specs, workerId: "b", limit: 1 })).toHaveLength(0);

    clock.advance(1);
    const reclaimed = await queue.claim({ workflowSpecs: specs, workerId: "b", limit: 1 });
    expect(reclaimed).toHaveLength(1);
    expect(reclaimed[0]?.workflowId).toBe("w-2");
    expect(reclaimed[0]?.claimedBy).toBe("b");
  });
});
