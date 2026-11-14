// ---------------------------------------------------------------------------
// Wakeups — a wake only shortens a wait. With poll intervals of a minute and
// the fake clock never advanced, a run still completes because enqueue and
// settle wake the worker and the coordinator; without wakeups (or with a
// channel that drops every wake) the same run waits for the poll interval.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { succeed } from "@spilne/perfect-core";
import { workflow, InMemoryWorkflowStorage } from "../../../index.ts";
import { MapStepRegistry } from "../step-registry.ts";
import { InMemoryStepQueue } from "../in-memory-step-queue.ts";
import { createDistributedWorkflowRunner } from "../coordinator.ts";
import { createWorker } from "../worker.ts";
import { createSignalScanner } from "../signal-scanner.ts";
import { InMemoryWakeups, type Wakeups, type WakeTopic } from "../../shared/wakeups.ts";
import { FakeWallClock } from "../../shared/wall-clock.ts";

const MINUTE = 60_000;

async function flush(): Promise<void> {
  for (let i = 0; i < 40; i++) await new Promise<void>((r) => setImmediate(r));
}

const wf = workflow<{ n: number }>({ name: "wakeups" })
  .step("double", ({ input }) => succeed(input.n * 2))
  .step("add-ten", ({ prev }) => succeed(prev + 10))
  .build();

/** Run `wf` on a fleet whose loops all poll once a minute; returns the result if it finished without the clock moving. */
async function runWithoutTime(params: {
  /** The wake channel the queue and loops share, or what a loop sees as its channel. */
  queueWakeups?: Wakeups;
  loopWakeups?: Wakeups;
}): Promise<{
  result: unknown;
  finishAfterPoll: () => Promise<unknown>;
  stop: () => Promise<void>;
}> {
  const clock = FakeWallClock.create("2026-01-01T00:00:00Z");
  const storage = new InMemoryWorkflowStorage({ clock });
  const queue = new InMemoryStepQueue({
    clock,
    ...(params.queueWakeups && { wakeups: params.queueWakeups }),
  });
  const registry = new MapStepRegistry();
  registry.register({
    stepName: "double",
    handler: (ctx) => succeed((ctx.input as { n: number }).n * 2),
  });
  registry.register({ stepName: "add-ten", handler: (ctx) => succeed((ctx.prev as number) + 10) });
  const loop = params.loopWakeups ? { wakeups: params.loopWakeups } : {};
  const coordinator = createDistributedWorkflowRunner({
    storage,
    stepQueue: queue,
    pollIntervalMs: MINUTE,
    stepPollIntervalMs: MINUTE,
    clock,
    ...loop,
  });
  const worker = createWorker({
    stepQueue: queue,
    registry,
    capabilities: [],
    pollIntervalMs: MINUTE,
    clock,
    ...loop,
  });
  const box: { result?: unknown } = {};
  void coordinator.startLoop();
  void worker.start();
  void coordinator
    .run({ workflow: wf, workflowId: "w-1", input: { n: 5 } })
    .then((r) => (box.result = r));
  await flush();
  return {
    result: box.result,
    finishAfterPoll: async () => {
      for (let i = 0; i < 20 && box.result === undefined; i++) {
        await flush();
        clock.advance(MINUTE);
      }
      return box.result;
    },
    stop: async () => {
      await coordinator.stopLoop();
      await worker.stop();
    },
  };
}

describe("wakeups", () => {
  it("enqueue and settle wake the worker and the coordinator, so a run needs no poll interval", async () => {
    const wakeups = new InMemoryWakeups();
    const run = await runWithoutTime({ queueWakeups: wakeups, loopWakeups: wakeups });
    expect(run.result).toBe(20);
    await run.stop();
  });

  it("polling stays the correctness path: wakes that never arrive only cost the interval", async () => {
    const dropped: WakeTopic[] = [];
    const lossy: Wakeups = {
      notify: ({ topic }) => void dropped.push(topic),
      onWake: () => () => {},
    };
    const run = await runWithoutTime({ queueWakeups: lossy, loopWakeups: lossy });
    expect(run.result).toBeUndefined();
    expect(await run.finishAfterPoll()).toBe(20);
    expect(dropped.length).toBeGreaterThan(0);
    await run.stop();

    const none = await runWithoutTime({});
    expect(none.result).toBeUndefined();
    expect(await none.finishAfterPoll()).toBe(20);
    await none.stop();
  });

  it("a throwing notify never fails the queue operation", async () => {
    const queue = new InMemoryStepQueue({
      wakeups: {
        notify: () => {
          throw new Error("boom");
        },
        onWake: () => () => {},
      },
    });
    const id = await queue.enqueue({ workflowId: "x", stepName: "s", input: {} });
    const [task] = await queue.claim({ workerId: "w", limit: 1 });
    expect(task?.id).toBe(id);
    expect(
      await queue.complete({ taskId: id, claimToken: task!.claimToken!, result: 1, durationMs: 1 }),
    ).toBe(true);
  });

  it("a signals wake makes the signal scanner scan before its interval", async () => {
    const clock = FakeWallClock.create("2026-01-01T00:00:00Z");
    const storage = new InMemoryWorkflowStorage({ clock });
    const wakeups = new InMemoryWakeups();
    const waiter = workflow<string>({ name: "waiter", version: "1" })
      .waitForSignal<{ ok: boolean }, "decision">("decision", { signalName: "go" })
      .step("after", ({ prev }) => succeed(prev.ok))
      .build();
    const { createWorkflowRunner } = await import("../../durable/workflow-runner.ts");
    await createWorkflowRunner({ storage, clock }).runSafe({
      workflow: waiter,
      workflowId: "s-1",
      input: "x",
    });

    const woken: string[] = [];
    const scanner = createSignalScanner({
      storage,
      clock,
      scanIntervalMs: MINUTE,
      wakeups,
      wake: async (run) => void woken.push(run.workflowId),
    });
    void scanner.start();
    await flush();
    await storage.deliverSignal({ workflowId: "s-1", signalName: "go", payload: { ok: true } });
    await flush();
    expect(woken).toEqual([]);
    wakeups.notify({ topic: "signals", key: "s-1" });
    await flush();
    expect(woken).toContain("s-1");
    await scanner.stop();
  });

  it("an unsubscribed handler hears nothing more", async () => {
    const wakeups = new InMemoryWakeups();
    let woken = 0;
    const off = wakeups.onWake({ topic: "results", handler: () => woken++ });
    wakeups.notify({ topic: "results", key: "7" });
    off();
    wakeups.notify({ topic: "results", key: "7" });
    expect(woken).toBe(1);
  });
});
