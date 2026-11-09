// ---------------------------------------------------------------------------
// Heartbeat details through a worker: `ctx.heartbeat(details)` rides the next
// lease heartbeat, and the worker that picks the task up after a crash or a
// lost lease starts from `ctx.lastHeartbeatDetails`.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { DefaultWorker } from "../worker.ts";
import { InMemoryStepQueue } from "../in-memory-step-queue.ts";
import { MapStepRegistry, type WorkerStepContext } from "../step-registry.ts";
import { FakeWallClock } from "../../shared/wall-clock.ts";

async function waitFor(predicate: () => boolean | Promise<boolean>): Promise<void> {
  for (let i = 0; i < 2_000; i++) {
    if (await predicate()) return;
    await new Promise<void>((r) => setImmediate(r));
  }
  expect(await predicate()).toBe(true);
}

describe("worker heartbeat details", () => {
  it("a redelivered task resumes from the details the lost worker last sent", async () => {
    const clock = FakeWallClock.create(0);
    const queue = new InMemoryStepQueue({ clock });
    const taskId = await queue.enqueue({ workflowId: "wf", stepName: "import", input: {} });

    // Worker A processes items one by one, then stalls on item 3.
    let gate!: (v: unknown) => void;
    const stall = new Promise<unknown>((r) => (gate = r));
    const startedAt: unknown[] = [];
    const registryA = new MapStepRegistry();
    registryA.register({
      stepName: "import",
      handler: async (ctx: WorkerStepContext) => {
        startedAt.push(ctx.lastHeartbeatDetails);
        ctx.heartbeat({ done: 1 });
        ctx.heartbeat({ done: 2 }); // only the latest is sent
        return stall;
      },
    });
    const a = new DefaultWorker({
      workerId: "a",
      stepQueue: queue,
      registry: registryA,
      heartbeatIntervalMs: 100,
      clock,
    });
    void a.start();
    await waitFor(() => startedAt.length === 1);
    // Nothing is sent before the lease heartbeat is due.
    expect((await queue.get(taskId))?.lastHeartbeatDetails).toBeUndefined();
    clock.advance(100);
    await waitFor(async () => (await queue.get(taskId))?.lastHeartbeatDetails !== undefined);
    expect((await queue.get(taskId))?.lastHeartbeatDetails).toEqual({ done: 2 });

    // A is declared dead; B takes the task over and resumes from the details.
    await queue.requeueStuck({ mode: "worker", workerId: "a" });
    const registryB = new MapStepRegistry();
    const resumedFrom: unknown[] = [];
    registryB.register({
      stepName: "import",
      handler: async (ctx: WorkerStepContext) => {
        resumedFrom.push(ctx.lastHeartbeatDetails);
        ctx.heartbeat({ done: 3 });
        expect(ctx.lastHeartbeatDetails).toEqual({ done: 3 });
        return "finished";
      },
    });
    const b = new DefaultWorker({
      workerId: "b",
      stepQueue: queue,
      registry: registryB,
      pollIntervalMs: 10,
      clock,
    });
    void b.start();
    await waitFor(() => resumedFrom.length === 1);
    expect(resumedFrom).toEqual([{ done: 2 }]);
    await waitFor(async () => (await queue.get(taskId))?.status === "completed");

    gate("late");
    clock.advance(1_000);
    await a.stop();
    await b.stop();
  });
});
