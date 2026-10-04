// ---------------------------------------------------------------------------
// Worker lease loss and graceful stop: a heartbeat that finds the claim gone
// aborts `ctx.signal` and drops the outcome; `stop({ timeoutMs })` gives
// unfinished tasks back to the queue and aborts their handlers; retry waits
// end when the signal aborts.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { DefaultWorker, TaskLeaseLostError, WorkerStoppingError } from "../worker.ts";
import { InMemoryStepQueue } from "../in-memory-step-queue.ts";
import { MapStepRegistry } from "../step-registry.ts";
import { InMemoryWorkerRegistry } from "../worker-registry.ts";
import { retryAsync } from "../../shared/retry-policy.ts";
import { InMemoryWorkflowStorage } from "../../durable/in-memory-storage.ts";
import { FakeWallClock } from "../../shared/wall-clock.ts";

async function waitFor(predicate: () => boolean | Promise<boolean>): Promise<void> {
  for (let i = 0; i < 2_000; i++) {
    if (await predicate()) return;
    await new Promise<void>((r) => setImmediate(r));
  }
  expect(await predicate()).toBe(true);
}

/** A handler that runs until released, recording the signal it was given. */
function blockingHandler() {
  const seen: { signal?: AbortSignal } = {};
  let release!: (value: unknown) => void;
  const gate = new Promise<unknown>((r) => (release = r));
  return {
    seen,
    release: (value: unknown) => release(value),
    handler: (ctx: { signal: AbortSignal }) => {
      seen.signal = ctx.signal;
      return gate;
    },
  };
}

async function setup() {
  const clock = FakeWallClock.create(0);
  const storage = new InMemoryWorkflowStorage({ clock });
  const queue = new InMemoryStepQueue({ clock });
  await storage.createWorkflow({ workflowId: "wf", workflowName: "w", input: {} });
  const taskId = await queue.enqueue({
    workflowId: "wf",
    stepName: "slow",
    input: {},
    prevResults: {},
  });
  return { clock, storage, queue, taskId };
}

describe("worker lease loss", () => {
  it("a lost claim aborts ctx.signal, fires onLeaseLost and writes nothing", async () => {
    const { clock, storage, queue, taskId } = await setup();
    const slow = blockingHandler();
    const registry = new MapStepRegistry();
    registry.register({ stepName: "slow", handler: slow.handler });
    const lost: string[] = [];
    const worker = new DefaultWorker({
      workerId: "a",
      storage,
      stepQueue: queue,
      registry,
      pollIntervalMs: 1_000,
      heartbeatIntervalMs: 100,
      clock,
      hooks: { onLeaseLost: (task) => void lost.push(task.id) },
    });
    const running = worker.start();
    await waitFor(() => slow.seen.signal !== undefined);

    // The worker stalls; the coordinator declares it dead and requeues.
    await queue.requeueStuck({ mode: "worker", workerId: "a" });
    expect(slow.seen.signal!.aborted).toBe(false);
    clock.advance(100);
    await waitFor(() => slow.seen.signal!.aborted);
    expect(slow.seen.signal!.reason).toBeInstanceOf(TaskLeaseLostError);
    expect(lost).toEqual([taskId]);

    // The handler finishes anyway; its result is discarded.
    slow.release("late result");
    await waitFor(async () => (await queue.get(taskId))?.status === "pending");
    const stop = worker.stop();
    clock.advance(1_000);
    await stop;
    await running;
    expect((await storage.loadWorkflow("wf"))?.steps["slow"]).toBeUndefined();
    expect((await queue.get(taskId))?.status).toBe("pending");
  });

  it("a failing heartbeat is reported but doesn't abort the task", async () => {
    const { clock, storage, queue, taskId } = await setup();
    const slow = blockingHandler();
    const registry = new MapStepRegistry();
    registry.register({ stepName: "slow", handler: slow.handler });
    const phases: string[] = [];
    const heartbeat = queue.heartbeat.bind(queue);
    let failHeartbeats = true;
    queue.heartbeat = async (p) => {
      if (failHeartbeats) throw new Error("db blip");
      return heartbeat(p);
    };
    const worker = new DefaultWorker({
      storage,
      stepQueue: queue,
      registry,
      heartbeatIntervalMs: 100,
      clock,
      onError: (e) => void phases.push(e.phase),
    });
    void worker.start();
    await waitFor(() => slow.seen.signal !== undefined);
    clock.advance(100);
    await waitFor(() => phases.includes("heartbeat"));
    expect(slow.seen.signal!.aborted).toBe(false);

    failHeartbeats = false;
    slow.release("ok");
    await waitFor(async () => (await queue.get(taskId))?.status === "completed");
    expect((await storage.loadWorkflow("wf"))?.steps["slow"]?.result).toBe("ok");
    const stop = worker.stop();
    clock.advance(1_000);
    await stop;
  });
});

describe("worker stop({ timeoutMs })", () => {
  it("gives unfinished tasks back to the queue, aborts their handlers and deregisters", async () => {
    const { clock, storage, queue, taskId } = await setup();
    const slow = blockingHandler();
    const registry = new MapStepRegistry();
    registry.register({ stepName: "slow", handler: slow.handler });
    const workers = new InMemoryWorkerRegistry({ clock });
    const worker = new DefaultWorker({
      workerId: "a",
      storage,
      stepQueue: queue,
      registry,
      workerRegistry: workers,
      heartbeatIntervalMs: 100_000,
      pollIntervalMs: 1_000,
      clock,
    });
    void worker.start();
    await waitFor(() => slow.seen.signal !== undefined);
    expect((await queue.get(taskId))?.status).toBe("running");

    const timeouts: number[] = [];
    const setTimeoutOn = clock.setTimeout.bind(clock);
    clock.setTimeout = (fn, ms) => (timeouts.push(ms), setTimeoutOn(fn, ms));
    let stopped = false;
    const stopping = worker.stop({ timeoutMs: 5_000 }).then(() => (stopped = true));
    await waitFor(() => timeouts.includes(5_000));
    clock.advance(4_999);
    await new Promise<void>((r) => setImmediate(r));
    expect(stopped).toBe(false);

    clock.advance(1);
    await stopping;
    expect(slow.seen.signal!.aborted).toBe(true);
    expect(slow.seen.signal!.reason).toBeInstanceOf(WorkerStoppingError);
    // Released, not left for the stale timeout: claimable now, no extra delivery.
    const task = await queue.get(taskId);
    expect(task?.status).toBe("pending");
    expect((await workers.list()).map((w) => w.status)).toEqual(["retired"]);

    // A late finish of the aborted handler writes nothing.
    slow.release("too late");
    await new Promise<void>((r) => setImmediate(r));
    expect((await storage.loadWorkflow("wf"))?.steps["slow"]).toBeUndefined();
  });

  it("without a timeout, stop waits for the task to finish", async () => {
    const { clock, storage, queue, taskId } = await setup();
    const slow = blockingHandler();
    const registry = new MapStepRegistry();
    registry.register({ stepName: "slow", handler: slow.handler });
    const worker = new DefaultWorker({ storage, stepQueue: queue, registry, clock });
    void worker.start();
    await waitFor(() => slow.seen.signal !== undefined);
    let stopped = false;
    const stopping = worker.stop().then(() => (stopped = true));
    clock.advance(600_000);
    await new Promise<void>((r) => setImmediate(r));
    expect(stopped).toBe(false);
    slow.release("done");
    await stopping;
    expect((await queue.get(taskId))?.status).toBe("completed");
  });
});

describe("retryAsync", () => {
  it("backs off on the clock and stops retrying when the signal aborts", async () => {
    const clock = FakeWallClock.create(0);
    const controller = new AbortController();
    let calls = 0;
    const result = retryAsync({
      policy: { maxRetries: 5, baseDelayMs: 100 },
      clock,
      signal: controller.signal,
      run: async () => {
        calls++;
        throw new Error(`fail ${calls}`);
      },
    }).catch((e: unknown) => e);
    await waitFor(() => calls === 1 && clock.pendingCount() === 1);
    clock.advance(100);
    await waitFor(() => calls === 2 && clock.pendingCount() === 1);
    controller.abort(new Error("stopped"));
    expect(((await result) as Error).message).toBe("stopped");
    expect(clock.pendingCount()).toBe(0);
    expect(calls).toBe(2);
  });
});
