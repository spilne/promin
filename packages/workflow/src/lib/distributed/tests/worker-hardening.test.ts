// ---------------------------------------------------------------------------
// DefaultWorker robustness: the claim loop survives queue errors, nothing a
// task does escapes as an unhandled rejection, outcomes are written to
// storage before the queue settles, and a full batch re-claims at once.
// All timing runs on FakeWallClock.
// ---------------------------------------------------------------------------

import { describe, it, expect, afterEach, beforeEach } from "bun:test";
import { DefaultWorker, type WorkerErrorEvent } from "../worker.ts";
import { InMemoryStepQueue } from "../in-memory-step-queue.ts";
import { MapStepRegistry, type StepRegistry } from "../step-registry.ts";
import type { StepQueue, StepTask } from "../step-queue.ts";
import { StepQueueExecutor } from "../step-queue-executor.ts";
import { InMemoryWorkflowStorage } from "../../durable/in-memory-storage.ts";
import type { WorkflowStorage } from "../../durable/workflow-storage.ts";
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
    metrics: (p) => inner.metrics(p),
    ...overrides,
  };
}

/** Storage whose `saveStepResult` fails the first `failures` times. */
function flakyStepResultStorage(inner: InMemoryWorkflowStorage, failures: number): WorkflowStorage {
  let left = failures;
  return new Proxy(inner, {
    get(target, prop, receiver) {
      if (prop === "saveStepResult") {
        return async (...args: Parameters<WorkflowStorage["saveStepResult"]>) => {
          if (left > 0) {
            left--;
            throw new Error("storage blip");
          }
          return target.saveStepResult(...args);
        };
      }
      const value = Reflect.get(target, prop, receiver);
      return typeof value === "function" ? value.bind(target) : value;
    },
  });
}

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
    const storage = new InMemoryWorkflowStorage({ clock });
    await storage.createWorkflow({ workflowId: "wf", workflowName: "x", input: {} });
    const registry = new MapStepRegistry();
    registry.register("s", async () => 42);
    const errors: WorkerErrorEvent[] = [];
    const worker = new DefaultWorker({
      storage,
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
    await inner.enqueue({ workflowId: "wf", stepName: "s", input: {}, prevResults: {} });
    clock.advance(100);
    await waitFor(() => claims >= 3);
    await waitFor(
      async () => (await storage.loadWorkflow("wf"))?.steps["s"]?.status === "completed",
    );
    expect(inner.getAllTasks()[0]!.status).toBe("completed");

    await worker.stop();
    await running;
    expect(clock.pendingCount()).toBe(0);
    expect(unhandled).toEqual([]);
  });

  it("stop() waits for an in-flight claim and the tasks it returns", async () => {
    const clock = FakeWallClock.create(0);
    const inner = new InMemoryStepQueue({ clock });
    await inner.enqueue({ workflowId: "wf", stepName: "s", input: {}, prevResults: {} });
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
    const storage = new InMemoryWorkflowStorage({ clock });
    await storage.createWorkflow({ workflowId: "wf", workflowName: "x", input: {} });
    const registry = new MapStepRegistry();
    registry.register("s", async () => "done");
    const worker = new DefaultWorker({ storage, stepQueue: queue, registry, clock });

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
    expect((await storage.loadWorkflow("wf"))?.steps["s"]?.status).toBe("completed");
  });
});

describe("DefaultWorker — no unhandled rejections", () => {
  it("a fallback that throws fails the step instead of crashing the process", async () => {
    const clock = FakeWallClock.create(0);
    const queue = new InMemoryStepQueue({ clock });
    await queue.enqueue({ workflowId: "wf", stepName: "s", input: {}, prevResults: {} });
    const storage = new InMemoryWorkflowStorage({ clock });
    await storage.createWorkflow({ workflowId: "wf", workflowName: "x", input: {} });
    const registry = new MapStepRegistry();
    registry.register(
      "s",
      async () => {
        throw new Error("boom");
      },
      {
        onFailure: {
          fallback: () => {
            throw new Error("fallback threw");
          },
        },
      },
    );
    const worker = new DefaultWorker({ storage, stepQueue: queue, registry, clock });

    void worker.start();
    await waitFor(() => queue.getAllTasks()[0]!.status === "failed");
    const step = (await storage.loadWorkflow("wf"))?.steps["s"];
    expect(step?.status).toBe("failed");
    expect(step?.error).toContain("fallback threw");
    expect(step?.error).toContain("boom");

    await worker.stop();
    expect(unhandled).toEqual([]);
  });

  it("a rejection escaping a task is caught and reported through onError", async () => {
    const clock = FakeWallClock.create(0);
    const queue = new InMemoryStepQueue({ clock });
    await queue.enqueue({ workflowId: "wf", stepName: "s", input: {}, prevResults: {} });
    const storage = new InMemoryWorkflowStorage({ clock });
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
      storage,
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
    await queue.enqueue({ workflowId: "wf", stepName: "ok", input: {}, prevResults: {} });
    await queue.enqueue({ workflowId: "wf", stepName: "bad", input: {}, prevResults: {} });
    const storage = new InMemoryWorkflowStorage({ clock });
    await storage.createWorkflow({ workflowId: "wf", workflowName: "x", input: {} });
    const registry = new MapStepRegistry();
    registry.register("ok", async () => 1);
    registry.register("bad", async () => {
      throw new Error("step failed");
    });
    const errors: WorkerErrorEvent[] = [];
    const worker = new DefaultWorker({
      storage,
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
    const steps = (await storage.loadWorkflow("wf"))?.steps;
    expect(steps?.["ok"]?.status).toBe("completed");
    expect(steps?.["bad"]?.status).toBe("failed");
    expect(queue.getAllTasks().map((t) => t.status)).toEqual(["completed", "failed"]);

    await worker.stop();
    expect(unhandled).toEqual([]);
  });
});

describe("DefaultWorker — storage first, then queue", () => {
  it("a failed storage write leaves the task claimed; it is redelivered and the workflow step lands", async () => {
    const clock = FakeWallClock.create(0);
    const queue = new InMemoryStepQueue({ clock });
    const base = new InMemoryWorkflowStorage({ clock });
    await base.createWorkflow({ workflowId: "wf", workflowName: "x", input: {} });
    const storage = flakyStepResultStorage(base, 1);
    const registry = new MapStepRegistry();
    let runs = 0;
    registry.register("s", async () => {
      runs++;
      return 42;
    });
    const errors: WorkerErrorEvent[] = [];
    const worker = new DefaultWorker({
      storage,
      stepQueue: queue,
      registry,
      pollIntervalMs: 100,
      heartbeatIntervalMs: 1_000,
      clock,
      onError: (e) => errors.push(e),
    });
    const executor = new StepQueueExecutor({
      stepQueue: queue,
      storage: base,
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
    expect((errors[0]!.error as Error).message).toBe("storage blip");
    // The queue must not claim success storage never recorded.
    expect(queue.getAllTasks()[0]!.status).toBe("running");
    expect((await base.loadWorkflow("wf"))?.steps["s"]).toBeUndefined();

    // The lease goes stale; the executor's sweep requeues the task and the
    // worker runs it again — this time the write lands.
    for (let i = 0; i < 300 && result === undefined; i++) {
      clock.advance(100);
      await new Promise<void>((r) => setImmediate(r));
    }
    await executing;
    expect(result).toEqual({ ok: true, result: 42, storageAlreadyCheckpointed: true });
    expect(runs).toBe(2);
    expect(queue.getAllTasks()[0]!.status).toBe("completed");

    await worker.stop();
    expect(unhandled).toEqual([]);
  });

  it("a lost claim skips the commit entirely", async () => {
    const clock = FakeWallClock.create(0);
    const queue = new InMemoryStepQueue({ clock });
    await queue.enqueue({ workflowId: "wf", stepName: "s", input: {}, prevResults: {} });
    const storage = new InMemoryWorkflowStorage({ clock });
    await storage.createWorkflow({ workflowId: "wf", workflowName: "x", input: {} });
    const registry = new MapStepRegistry();
    let release!: () => void;
    const gate = new Promise<void>((r) => (release = r));
    let started = false;
    registry.register("s", async () => {
      started = true;
      await gate;
      return "stale";
    });
    const worker = new DefaultWorker({
      storage,
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

    expect(queue.getAllTasks()[0]!.status).toBe("pending");
    expect((await storage.loadWorkflow("wf"))?.steps["s"]).toBeUndefined();
  });

  it("skip and fallback outcomes write attempt rows like success and failure do", async () => {
    const clock = FakeWallClock.create(0);
    const queue = new InMemoryStepQueue({ clock });
    await queue.enqueue({ workflowId: "wf", stepName: "skipped", input: {}, prevResults: {} });
    await queue.enqueue({ workflowId: "wf", stepName: "fellback", input: {}, prevResults: {} });
    const storage = new InMemoryWorkflowStorage({ clock });
    await storage.createWorkflow({ workflowId: "wf", workflowName: "x", input: {} });
    const registry = new MapStepRegistry();
    const boom = async () => {
      throw new Error("boom");
    };
    registry.register("skipped", boom, { onFailure: "skip" });
    registry.register("fellback", boom, { onFailure: { fallback: () => "plan b" } });
    const worker = new DefaultWorker({
      storage,
      stepQueue: queue,
      registry,
      concurrency: 2,
      clock,
    });

    void worker.start();
    await waitFor(() => queue.getAllTasks().every((t) => t.status === "completed"));
    await worker.stop();

    const attempts = await storage.loadStepAttempts("wf");
    expect(attempts.map((a) => [a.stepName, a.status, a.result]).sort()).toEqual([
      ["fellback", "completed", "plan b"],
      ["skipped", "completed", undefined],
    ]);
    expect((await storage.loadWorkflow("wf"))?.steps["fellback"]?.result).toBe("plan b");
  });
});

describe("DefaultWorker — throughput", () => {
  it("a full batch re-claims as slots free up, without waiting out the poll interval", async () => {
    const clock = FakeWallClock.create(0);
    const queue = new InMemoryStepQueue({ clock });
    const storage = new InMemoryWorkflowStorage({ clock });
    const N = 20;
    for (let i = 0; i < N; i++) {
      await storage.createWorkflow({ workflowId: `wf-${i}`, workflowName: "x", input: {} });
      await queue.enqueue({ workflowId: `wf-${i}`, stepName: "s", input: {}, prevResults: {} });
    }
    const registry = new MapStepRegistry();
    registry.register("s", async () => 1);
    let claims = 0;
    const counting = wrapQueue(queue, {
      claim: (p) => {
        claims++;
        return queue.claim(p);
      },
    });
    const worker = new DefaultWorker({
      storage,
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
