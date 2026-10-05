// ---------------------------------------------------------------------------
// DefaultWorker robustness: the claim loop survives queue errors, nothing a
// task does escapes as an unhandled rejection, the outcome reaches the queue
// only through the claim-fenced `complete` / `fail` (the worker writes no
// storage), and a full batch re-claims at once. All timing runs on
// FakeWallClock.
// ---------------------------------------------------------------------------

import { describe, it, expect, afterEach, beforeEach } from "bun:test";
import { fail, TaggedError } from "@spilne/perfect-core";
import { DefaultWorker, type WorkerErrorEvent } from "../worker.ts";
import { InMemoryStepQueue } from "../in-memory-step-queue.ts";
import { MapStepRegistry, type StepRegistry } from "../step-registry.ts";
import type { StepQueue, StepTask } from "../step-queue.ts";
import { StepQueueExecutor } from "../step-queue-executor.ts";
import { InMemoryWorkflowStorage } from "../../durable/in-memory-storage.ts";
import { FakeWallClock } from "../../shared/wall-clock.ts";

/** Yield to the event loop until `predicate` holds (bounded). */
async function waitFor(predicate: () => boolean | Promise<boolean>): Promise<void> {
  for (let i = 0; i < 2_000 && !(await predicate()); i++) {
    await new Promise<void>((r) => setImmediate(r));
  }
  expect(await predicate()).toBe(true);
}

/** Delegating StepQueue whose methods can be overridden per test. */
function wrapQueue(inner: InMemoryStepQueue, overrides: Partial<StepQueue>): StepQueue {
  return {
    enqueue: (p) => inner.enqueue(p),
    claim: (p) => inner.claim(p),
    release: (p) => inner.release(p),
    get: (id) => inner.get(id),
    purge: (p) => inner.purge(p),
    complete: (p) => inner.complete(p),
    fail: (p) => inner.fail(p),
    heartbeat: (p) => inner.heartbeat(p),
    requeueStuck: (p) => inner.requeueStuck(p),
    consume: (p) => inner.consume(p),
    consumeSettled: (p) => inner.consumeSettled(p),
    metrics: (p) => inner.metrics(p),
    ...overrides,
  };
}

class CardDeclined extends TaggedError("CardDeclined")<{ readonly message: string }>() {}

let unhandled: unknown[] = [];
const onUnhandled = (reason: unknown) => {
  unhandled.push(reason);
};

beforeEach(() => {
  unhandled = [];
  process.on("unhandledRejection", onUnhandled);
});

afterEach(() => {
  process.off("unhandledRejection", onUnhandled);
});

describe("DefaultWorker — claim loop resilience", () => {
  it("a claim() failure is reported and the worker keeps polling", async () => {
    const clock = FakeWallClock.create(0);
    const inner = new InMemoryStepQueue({ clock });
    let claims = 0;
    const queue = wrapQueue(inner, {
      claim: async (p) => {
        claims++;
        if (claims === 2) throw new Error("db blip");
        return inner.claim(p);
      },
    });
    const registry = new MapStepRegistry();
    registry.register({ stepName: "s", handler: async () => 42 });
    const errors: WorkerErrorEvent[] = [];
    const worker = new DefaultWorker({
      stepQueue: queue,
      registry,
      pollIntervalMs: 100,
      clock,
      onError: (e) => errors.push(e),
    });

    const running = worker.start();
    await waitFor(() => claims === 1 && clock.pendingCount() === 1);
    clock.advance(100);
    await waitFor(() => claims === 2 && clock.pendingCount() === 1);
    expect(errors).toHaveLength(1);
    expect(errors[0]!.phase).toBe("claim");
    expect((errors[0]!.error as Error).message).toBe("db blip");

    // Still polling: work enqueued after the failure gets done.
    await inner.enqueue({ workflowId: "wf", stepName: "s", input: {} });
    clock.advance(100);
    await waitFor(() => claims >= 3);
    await waitFor(() => inner.getAllTasks()[0]!.status === "completed");
    expect(inner.getAllTasks()[0]!.result).toBe(42);

    await worker.stop();
    await running;
    expect(clock.pendingCount()).toBe(0);
    expect(unhandled).toEqual([]);
  });

  it("stop() waits for an in-flight claim and the tasks it returns", async () => {
    const clock = FakeWallClock.create(0);
    const inner = new InMemoryStepQueue({ clock });
    await inner.enqueue({ workflowId: "wf", stepName: "s", input: {} });
    let releaseClaim!: () => void;
    const claimGate = new Promise<void>((r) => (releaseClaim = r));
    let claimStarted = false;
    const queue = wrapQueue(inner, {
      claim: async (p) => {
        claimStarted = true;
        await claimGate;
        return inner.claim(p);
      },
    });
    const registry = new MapStepRegistry();
    registry.register({ stepName: "s", handler: async () => "done" });
    const worker = new DefaultWorker({ stepQueue: queue, registry, clock });

    void worker.start();
    await waitFor(() => claimStarted);
    let stopped = false;
    const stopping = worker.stop().then(() => (stopped = true));
    await new Promise<void>((r) => setImmediate(r));
    expect(stopped).toBe(false);

    releaseClaim();
    await stopping;
    // The claimed task ran to completion before stop() resolved.
    expect(inner.getAllTasks()[0]!.status).toBe("completed");
    expect(inner.getAllTasks()[0]!.result).toBe("done");
  });
});

describe("DefaultWorker — no unhandled rejections", () => {
  it("a failing handler settles its task failed, with the error's tag", async () => {
    const clock = FakeWallClock.create(0);
    const queue = new InMemoryStepQueue({ clock });
    await queue.enqueue({ workflowId: "wf", stepName: "typed", input: {} });
    await queue.enqueue({ workflowId: "wf", stepName: "thrown", input: {} });
    const registry = new MapStepRegistry();
    registry.register({
      stepName: "typed",
      handler: () => fail(new CardDeclined({ message: "card declined" })),
    });
    registry.register({
      stepName: "thrown",
      handler: async () => {
        throw new Error("boom");
      },
    });
    const worker = new DefaultWorker({ stepQueue: queue, registry, concurrency: 2, clock });

    void worker.start();
    await waitFor(() => queue.getAllTasks().every((t) => t.status === "failed"));
    const [typed, thrown] = queue.getAllTasks();
    expect(typed).toMatchObject({ error: "card declined", errorTag: "CardDeclined" });
    expect(thrown!.error).toBe("boom");
    expect(thrown!.errorTag).toBeUndefined();

    await worker.stop();
    expect(unhandled).toEqual([]);
  });

  it("a rejection escaping a task is caught and reported through onError", async () => {
    const clock = FakeWallClock.create(0);
    const queue = new InMemoryStepQueue({ clock });
    await queue.enqueue({ workflowId: "wf", stepName: "s", input: {} });
    const registry: StepRegistry = {
      register: () => {},
      has: () => true,
      list: () => ["s"],
      resolve: () => {
        throw new Error("registry exploded");
      },
    };
    const errors: WorkerErrorEvent[] = [];
    const worker = new DefaultWorker({
      stepQueue: queue,
      registry,
      clock,
      onError: (e) => errors.push(e),
    });

    void worker.start();
    await waitFor(() => errors.length === 1);
    expect(errors[0]!.phase).toBe("task");
    expect(errors[0]!.task?.stepName).toBe("s");
    expect((errors[0]!.error as Error).message).toBe("registry exploded");

    // The worker is still alive and stops cleanly.
    await worker.stop();
    expect(unhandled).toEqual([]);
  });

  it("throwing hooks are reported; the step outcome stands", async () => {
    const clock = FakeWallClock.create(0);
    const queue = new InMemoryStepQueue({ clock });
    await queue.enqueue({ workflowId: "wf", stepName: "ok", input: {} });
    await queue.enqueue({ workflowId: "wf", stepName: "bad", input: {} });
    const registry = new MapStepRegistry();
    registry.register({ stepName: "ok", handler: async () => 1 });
    registry.register({
      stepName: "bad",
      handler: async () => {
        throw new Error("step failed");
      },
    });
    const errors: WorkerErrorEvent[] = [];
    const worker = new DefaultWorker({
      stepQueue: queue,
      registry,
      concurrency: 2,
      clock,
      hooks: {
        afterStep: () => {
          throw new Error("afterStep threw");
        },
        onError: () => {
          throw new Error("onError hook threw");
        },
      },
      onError: (e) => errors.push(e),
    });

    void worker.start();
    await waitFor(() => errors.length === 2);
    expect(errors.map((e) => e.phase)).toEqual(["hook", "hook"]);
    expect(queue.getAllTasks().map((t) => t.status)).toEqual(["completed", "failed"]);

    await worker.stop();
    expect(unhandled).toEqual([]);
  });
});

describe("DefaultWorker — the queue is the only thing it writes", () => {
  it("a failed complete() leaves the task claimed; it is redelivered and the step settles", async () => {
    const clock = FakeWallClock.create(0);
    const inner = new InMemoryStepQueue({ clock });
    const storage = new InMemoryWorkflowStorage({ clock });
    await storage.createWorkflow({ workflowId: "wf", workflowName: "x", input: {} });
    let completeFailures = 1;
    const queue = wrapQueue(inner, {
      complete: async (p) => {
        if (completeFailures > 0) {
          completeFailures--;
          throw new Error("queue blip");
        }
        return inner.complete(p);
      },
    });
    const registry = new MapStepRegistry();
    let runs = 0;
    registry.register({
      stepName: "s",
      handler: async () => {
        runs++;
        return 42;
      },
    });
    const errors: WorkerErrorEvent[] = [];
    const worker = new DefaultWorker({
      stepQueue: queue,
      registry,
      pollIntervalMs: 100,
      heartbeatIntervalMs: 1_000,
      clock,
      workerId: "w-1",
      onError: (e) => errors.push(e),
    });
    const executor = new StepQueueExecutor({
      stepQueue: inner,
      storage,
      pollIntervalMs: 100,
      staleTimeoutMs: 5_000,
      clock,
    });

    let result: unknown;
    const executing = executor
      .executeStep({ workflowId: "wf", stepName: "s", input: {}, prevResults: {}, attempt: 1 })
      .then((r) => (result = r));
    void worker.start();

    await waitFor(() => errors.length === 1);
    expect(errors[0]!.phase).toBe("commit");
    expect((errors[0]!.error as Error).message).toBe("queue blip");
    expect(inner.getAllTasks()[0]!.status).toBe("running");
    // The worker wrote nothing to storage.
    expect((await storage.loadWorkflow("wf"))?.steps["s"]).toBeUndefined();

    // The lease goes stale; the executor's sweep requeues the task and the
    // worker runs it again — this time the outcome settles.
    for (let i = 0; i < 300 && result === undefined; i++) {
      clock.advance(100);
      await new Promise<void>((r) => setImmediate(r));
    }
    await executing;
    expect(result).toEqual({
      ok: true,
      result: 42,
      attempt: 1,
      failedAttempts: [],
      executorId: "w-1",
    });
    expect(runs).toBe(2);
    expect(inner.getAllTasks()[0]!.status).toBe("completed");

    await worker.stop();
    expect(unhandled).toEqual([]);
  });

  it("a lost claim skips the commit entirely", async () => {
    const clock = FakeWallClock.create(0);
    const queue = new InMemoryStepQueue({ clock });
    await queue.enqueue({ workflowId: "wf", stepName: "s", input: {} });
    const registry = new MapStepRegistry();
    let release!: () => void;
    const gate = new Promise<void>((r) => (release = r));
    let started = false;
    registry.register({
      stepName: "s",
      handler: async () => {
        started = true;
        await gate;
        return "stale";
      },
    });
    const worker = new DefaultWorker({
      stepQueue: queue,
      registry,
      clock,
      workerId: "worker-a",
    });

    void worker.start();
    await waitFor(() => started);
    // A dead-worker sweep reclaims the task while the handler still runs.
    expect(await queue.requeueStuck({ mode: "worker", workerId: "worker-a" })).toEqual({
      requeued: 1,
      deadLettered: 0,
    });
    release();
    await worker.stop();

    const [task] = queue.getAllTasks();
    expect(task!.status).toBe("pending");
    expect(task!.result).toBeUndefined();
  });
});

describe("DefaultWorker — throughput", () => {
  it("a full batch re-claims as slots free up, without waiting out the poll interval", async () => {
    const clock = FakeWallClock.create(0);
    const queue = new InMemoryStepQueue({ clock });
    const N = 20;
    for (let i = 0; i < N; i++) {
      await queue.enqueue({ workflowId: `wf-${i}`, stepName: "s", input: {} });
    }
    const registry = new MapStepRegistry();
    registry.register({ stepName: "s", handler: async () => 1 });
    let claims = 0;
    const counting = wrapQueue(queue, {
      claim: (p) => {
        claims++;
        return queue.claim(p);
      },
    });
    const worker = new DefaultWorker({
      stepQueue: counting,
      registry,
      concurrency: 3,
      pollIntervalMs: 60_000,
      clock,
    });

    void worker.start();
    await waitFor(() => queue.getAllTasks().every((t: StepTask) => t.status === "completed"));
    // No clock time passed: every re-claim came from a freed slot.
    expect(clock.currentTimeMs()).toBe(0);
    expect(claims).toBeGreaterThanOrEqual(Math.ceil(N / 3));

    // Once the queue drains, the worker goes back to its poll interval.
    await waitFor(() => clock.pendingCount() === 1);
    const settled = claims;
    for (let i = 0; i < 5; i++) await new Promise<void>((r) => setImmediate(r));
    expect(claims).toBe(settled);

    await worker.stop();
  });
});
