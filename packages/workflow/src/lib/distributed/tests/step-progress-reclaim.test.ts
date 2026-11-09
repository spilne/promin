// ---------------------------------------------------------------------------
// waitingOn `activity` follows the claim: a task a stale sweep put back and
// another worker claimed shows the new worker, not the one that lost it.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { succeed } from "@spilne/perfect-core";
import { workflow } from "../../durable/durable-pipeline.ts";
import { InMemoryWorkflowStorage } from "../../durable/in-memory-storage.ts";
import { FakeWallClock } from "../../shared/wall-clock.ts";
import { createDistributedWorkflowRunner } from "../coordinator.ts";
import { InMemoryStepQueue } from "../in-memory-step-queue.ts";

const POLL_MS = 50;

async function flush(): Promise<void> {
  for (let i = 0; i < 20; i++) await new Promise<void>((r) => setImmediate(r));
}

/** Advance the fake clock in poll-sized steps so the executor polls and the progress delay elapses. */
async function drive(clock: FakeWallClock, ms: number): Promise<void> {
  for (let elapsed = 0; elapsed < ms; elapsed += POLL_MS) {
    await flush();
    clock.advance(POLL_MS);
  }
  await flush();
}

describe("step progress follows the queue claim", () => {
  it("a requeued task claimed by another worker reports the new holder", async () => {
    const clock = FakeWallClock.create(0);
    const storage = new InMemoryWorkflowStorage({ clock });
    const queue = new InMemoryStepQueue({ clock });
    const coordinator = createDistributedWorkflowRunner({
      storage,
      stepQueue: queue,
      pollIntervalMs: POLL_MS,
      stepPollIntervalMs: POLL_MS,
      clock,
    });
    const wf = workflow<unknown>({ name: "reclaim" })
      .step("work", () => succeed("ok"))
      .build();
    const run = coordinator.run({ workflow: wf, workflowId: "rc-1", input: {} });
    await drive(clock, 200);

    const holder = async () =>
      (await coordinator.getStatus({ workflowId: "rc-1" }))?.waitingOn.map(
        (w) => [w.kind, w.holder] as const,
      );

    const [a] = await queue.claim({ workerId: "A", limit: 1 });
    expect(a).toBeDefined();
    await drive(clock, 1_500);
    expect(await holder()).toEqual([["activity", "A"]]);

    // A's claim goes stale; B claims the task again.
    await queue.requeueStuck({ mode: "worker", workerId: "A" });
    const [b] = await queue.claim({ workerId: "B", limit: 1 });
    expect(b?.id).toBe(a!.id);
    await drive(clock, 500);
    expect(await holder()).toEqual([["activity", "B"]]);

    await queue.complete({
      taskId: b!.id,
      claimToken: b!.claimToken,
      result: "ok",
      durationMs: 1,
    });
    await drive(clock, 500);
    await run;
  });
});
