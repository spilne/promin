// ---------------------------------------------------------------------------
// DistributedWorkflowRunner: sleep / signal steps run on the coordinator,
// recovery adopts only orphaned runs (on taking leadership, then at a coarse
// cadence), result waits never hang, the sweep is fenced by the leader lease,
// and step waits share their storage reads.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { succeed } from "@spilne/perfect-core";
import { DistributedWorkflowRunner, type DistributedRunnerErrorEvent } from "../coordinator.ts";
import { InMemoryStepQueue } from "../in-memory-step-queue.ts";
import { MapStepRegistry } from "../step-registry.ts";
import { createWorker } from "../worker.ts";
import { createSleepScanner } from "../sleep-scanner.ts";
import { createSignalScanner } from "../signal-scanner.ts";
import { coordinatorLeaderKey } from "../leader-election.ts";
import type { StepQueue } from "../step-queue.ts";
import { InMemoryWorkflowStorage } from "../../durable/in-memory-storage.ts";
import type { WorkflowStorage } from "../../durable/workflow-storage.ts";
import { InMemoryWorkflowVersionRegistry } from "../../durable/workflow-version-registry.ts";
import { workflow } from "../../durable/durable-pipeline.ts";
import { FakeWallClock } from "../../shared/wall-clock.ts";
import {
  InMemoryLeaderLeases,
  LeaseLeaderElection,
  isStaleLeaseError,
} from "../../scheduler/leader-lease.ts";

/** Yield to the event loop until `predicate` holds (bounded). */
async function waitFor(predicate: () => boolean | Promise<boolean>): Promise<void> {
  for (let i = 0; i < 2_000; i++) {
    if (await predicate()) return;
    await new Promise<void>((r) => setImmediate(r));
  }
  expect(await predicate()).toBe(true);
}

/** Settle outstanding microtasks / immediates a few times over. */
async function flush(): Promise<void> {
  for (let i = 0; i < 20; i++) await new Promise<void>((r) => setImmediate(r));
}

/** Delegate every StepQueue call to `inner`, overriding some. */
function wrapQueue(inner: StepQueue, overrides: Partial<StepQueue>): StepQueue {
  return {
    enqueue: (p) => inner.enqueue(p),
    claim: (p) => inner.claim(p),
    release: (p) => inner.release(p),
    get: (id) => inner.get(id),
    purge: (p) => inner.purge(p),
    complete: (p) => inner.complete(p),
    fail: (p) => inner.fail(p),
    heartbeat: (p) => inner.heartbeat(p),
    metrics: (p) => inner.metrics(p),
    requeueStuck: (p) => inner.requeueStuck(p),
    ...overrides,
  };
}

/** `inner` without `listOrphanedRuns`, so recovery takes the listing fallback. */
function withoutOrphanQuery(inner: InMemoryWorkflowStorage): WorkflowStorage {
  return new Proxy(inner, {
    get(target, prop, receiver) {
      if (prop === "listOrphanedRuns") return undefined;
      const value = Reflect.get(target, prop, receiver);
      return typeof value === "function" ? value.bind(target) : value;
    },
    has(target, prop) {
      return prop === "listOrphanedRuns" ? false : Reflect.has(target, prop);
    },
  });
}

const sleepy = workflow<{ n: number }>({ name: "sleepy" })
  .step("before", ({ input }) => succeed(input.n))
  .sleep("nap", 60_000)
  .step("after", ({ prev }) => succeed((prev as number) + 1))
  .build();

const waiting = workflow<{ n: number }>({ name: "waiting" })
  .step("before", ({ input }) => succeed(input.n))
  .waitForSignal<number>("wait", { signalName: "go" })
  .step("after", ({ prev }) => succeed((prev as number) * 10))
  .build();

function workerFor(params: {
  storage: InMemoryWorkflowStorage;
  queue: StepQueue;
  clock: FakeWallClock;
}) {
  const registry = new MapStepRegistry();
  registry.register("before", (ctx) => succeed((ctx.input as { n: number }).n));
  registry.register("after", (ctx) => succeed(ctx.deps["wait"]));
  return createWorker({
    storage: params.storage,
    stepQueue: params.queue,
    registry,
    pollIntervalMs: 10,
    clock: params.clock,
  });
}

describe("sleep and signal steps under the distributed runner", () => {
  it("a sleep step suspends the run on the coordinator — no queue task — and the sleep scanner resumes it", async () => {
    const clock = FakeWallClock.create(0);
    const storage = new InMemoryWorkflowStorage({ clock });
    const queue = new InMemoryStepQueue({ clock });
    const runner = new DistributedWorkflowRunner({
      storage,
      stepQueue: queue,
      stepPollIntervalMs: 10,
      clock,
    });
    const registry = new MapStepRegistry();
    registry.register("before", (ctx) => succeed((ctx.input as { n: number }).n));
    registry.register("after", (ctx) => succeed((ctx.deps["nap"] as number) + 1));
    const worker = createWorker({ storage, stepQueue: queue, registry, pollIntervalMs: 10, clock });
    void worker.start();

    await runner.submit({ workflow: sleepy, workflowId: "s", input: { n: 1 } });
    await waitFor(async () => {
      clock.advance(10);
      return (await storage.loadWorkflow("s"))?.status === "suspended";
    });
    const state = await storage.loadWorkflow("s");
    expect(state?.steps["nap"]?.status).toBe("sleeping");
    // Only the ordinary step ever went to the queue.
    expect(queue.getAllTasks().map((t) => t.stepName)).toEqual(["before"]);

    const scanner = createSleepScanner({
      storage,
      runner,
      scanIntervalMs: 1_000,
      resolveWorkflow: () => sleepy as never,
      clock,
    });
    void scanner.start();
    clock.advance(60_000);
    await waitFor(async () => {
      clock.advance(10);
      return (await storage.loadWorkflow("s"))?.status === "completed";
    });
    expect((await storage.loadWorkflow("s"))?.result).toBe(2);
    expect(queue.getAllTasks().map((t) => t.stepName)).toEqual(["before", "after"]);

    await scanner.stop();
    await worker.stop();
  });

  it("a signal wait suspends on the coordinator and the signal scanner resumes it with the payload", async () => {
    const clock = FakeWallClock.create(0);
    const storage = new InMemoryWorkflowStorage({ clock });
    const queue = new InMemoryStepQueue({ clock });
    const runner = new DistributedWorkflowRunner({
      storage,
      stepQueue: queue,
      stepPollIntervalMs: 10,
      clock,
    });
    const worker = workerFor({ storage, queue, clock });
    void worker.start();

    await runner.submit({ workflow: waiting, workflowId: "w", input: { n: 1 } });
    await waitFor(async () => {
      clock.advance(10);
      return (await storage.loadWorkflow("w"))?.status === "suspended";
    });
    expect((await storage.loadWorkflow("w"))?.steps["wait"]?.status).toBe("waiting_for_signal");
    expect(queue.getAllTasks().map((t) => t.stepName)).toEqual(["before"]);

    await storage.deliverSignal({ workflowId: "w", signalName: "go", payload: 7 });
    const scanner = createSignalScanner({
      storage,
      runner,
      scanIntervalMs: 1_000,
      resolveWorkflow: () => waiting as never,
      clock,
    });
    void scanner.start();
    await waitFor(async () => {
      clock.advance(10);
      return (await storage.loadWorkflow("w"))?.status === "completed";
    });
    // The worker's "after" passes the signal payload through.
    expect((await storage.loadWorkflow("w"))?.result).toBe(7);

    await scanner.stop();
    await worker.stop();
  });
});

describe("leadership-triggered, orphan-only recovery", () => {
  it("an idle leader doesn't replay suspended runs on every tick", async () => {
    const clock = FakeWallClock.create(0);
    const storage = new InMemoryWorkflowStorage({ clock });
    const queue = new InMemoryStepQueue({ clock });
    const runner = new DistributedWorkflowRunner({
      storage,
      stepQueue: queue,
      pollIntervalMs: 50,
      stepPollIntervalMs: 10,
      clock,
    });
    const worker = workerFor({ storage, queue, clock });
    void worker.start();
    for (let i = 0; i < 50; i++) {
      await runner.submit({ workflow: sleepy, workflowId: `s-${i}`, input: { n: i } });
    }
    await waitFor(async () => {
      clock.advance(10);
      const states = await storage.listWorkflows({ status: "suspended", limit: 100 });
      return states.length === 50;
    });
    await worker.stop();

    let loads = 0;
    let creates = 0;
    let locks = 0;
    const load = storage.loadWorkflow.bind(storage);
    storage.loadWorkflow = (id) => (loads++, load(id));
    const create = storage.createWorkflow.bind(storage);
    storage.createWorkflow = (p) => (creates++, create(p));
    const lock = storage.tryLock.bind(storage);
    storage.tryLock = ({ workflowId: id, lockDurationMs: ms }) => (
      locks++, lock({ workflowId: id, lockDurationMs: ms })
    );

    const loop = runner.startLoop();
    for (let i = 0; i < 20; i++) {
      await flush();
      clock.advance(50);
    }
    await runner.stopLoop();
    await loop;
    // 20 idle sweeps with 50 suspended runs: nothing was re-submitted or locked.
    expect({ loads, creates, locks }).toEqual({ loads: 0, creates: 0, locks: 0 });
  });

  it("adopts orphaned pending runs on taking leadership; leaves locked and fresh ones alone", async () => {
    const clock = FakeWallClock.create(0);
    const storage = new InMemoryWorkflowStorage({ clock });
    const queue = new InMemoryStepQueue({ clock });
    // A previous coordinator created three runs and died.
    const metadata = { _dag: sleepy.dag };
    for (const workflowId of ["orphan", "locked", "fresh"]) {
      await storage.createWorkflow({
        workflowId,
        workflowName: "sleepy",
        input: { n: 1 },
        metadata,
      });
      if (workflowId === "locked") clock.advance(60_000);
    }
    // "locked" is still held by a live instance; "fresh" is younger than the grace.
    await storage.tryLock({ workflowId: "locked", lockDurationMs: 600_000 });

    let orphanScans = 0;
    const list = storage.listOrphanedRuns.bind(storage);
    storage.listOrphanedRuns = (p) => (orphanScans++, list(p));

    const errors: DistributedRunnerErrorEvent[] = [];
    const runner = new DistributedWorkflowRunner({
      storage,
      stepQueue: queue,
      pollIntervalMs: 1_000,
      stepPollIntervalMs: 10,
      orphanGraceMs: 30_000,
      recoveryIntervalMs: 10_000,
      clock,
      onError: (e) => errors.push(e),
    });
    const loop = runner.startLoop();
    await waitFor(() => queue.getAllTasks().length > 0);
    // The orphan (adopted through a stub of its stored DAG) dispatched its
    // first step; the others weren't touched.
    expect(queue.getAllTasks().map((t) => [t.workflowId, t.stepName])).toEqual([
      ["orphan", "before"],
    ]);
    expect(orphanScans).toBe(1);

    // Recovery doesn't run on every sweep, only every recoveryIntervalMs.
    for (let i = 0; i < 9; i++) {
      clock.advance(1_000);
      await flush();
    }
    expect(orphanScans).toBe(1);
    clock.advance(1_000);
    await waitFor(() => orphanScans === 2);

    await runner.stopLoop();
    await loop;
    expect(errors).toEqual([]);
  });
});

describe("recovery without listOrphanedRuns", () => {
  /** A one-step saga; its rollback records the run id in `undone`. */
  function sagaFor(undone: string[]) {
    return workflow<{ n: number }>({ name: "saga", version: "1" })
      .step("charge", ({ input }) => succeed(input.n), {
        compensate: async ({ workflowId }) => {
          undone.push(workflowId);
        },
      })
      .build();
  }

  /** A run a dead coordinator left `compensating` after `charge` completed. */
  async function leaveCompensating(storage: InMemoryWorkflowStorage, workflowId: string) {
    await storage.createWorkflow({
      workflowId,
      workflowName: "saga",
      input: { n: 1 },
      version: "1",
    });
    await storage.saveStepResult({
      workflowId,
      stepName: "charge",
      result: 1,
      durationMs: 1,
      startedAt: new Date(0),
    });
    expect(await storage.beginCompensation({ workflowId, error: "declined" })).toBe(true);
  }

  it("adopts a compensating run and finishes its rollback", async () => {
    const clock = FakeWallClock.create(0);
    const inner = new InMemoryWorkflowStorage({ clock });
    const undone: string[] = [];
    await leaveCompensating(inner, "comp");
    clock.advance(60_000);

    const registry = new InMemoryWorkflowVersionRegistry();
    registry.register(sagaFor(undone) as never);
    const errors: DistributedRunnerErrorEvent[] = [];
    const runner = new DistributedWorkflowRunner({
      storage: withoutOrphanQuery(inner),
      stepQueue: new InMemoryStepQueue({ clock }),
      registry,
      orphanGraceMs: 30_000,
      clock,
      onError: (e) => errors.push(e),
    });
    const loop = runner.startLoop();
    await waitFor(async () => (await inner.loadWorkflow("comp"))?.status === "failed");
    await runner.stopLoop();
    await loop;

    expect(undone).toEqual(["comp"]);
    expect((await inner.loadWorkflow("comp"))?.error).toContain("declined");
    // At most the adopted run's own failed outcome is reported.
    expect(errors.every((e) => e.source === "recovery" && e.workflowId === "comp")).toBe(true);
  });

  it("adopts every orphan across several listing pages, once each", async () => {
    const clock = FakeWallClock.create(0);
    const inner = new InMemoryWorkflowStorage({ clock });
    const undone: string[] = [];
    // More than one listing page of compensating runs (each adoption moves
    // one to `failed`, which would shift an offset page) plus pending runs.
    const compensating = Array.from({ length: 250 }, (_, i) => `c-${String(i).padStart(3, "0")}`);
    for (const id of compensating) await leaveCompensating(inner, id);
    for (let i = 0; i < 150; i++) {
      await inner.createWorkflow({
        workflowId: `p-${String(i).padStart(3, "0")}`,
        workflowName: "saga",
        input: { n: i },
        version: "1",
      });
    }
    clock.advance(60_000);

    const registry = new InMemoryWorkflowVersionRegistry();
    registry.register(sagaFor(undone) as never);
    const queue = new InMemoryStepQueue({ clock });
    const runner = new DistributedWorkflowRunner({
      storage: withoutOrphanQuery(inner),
      stepQueue: queue,
      registry,
      orphanGraceMs: 30_000,
      clock,
      onError: () => undefined,
    });
    const loop = runner.startLoop();
    await waitFor(async () => {
      const failed = await inner.listWorkflows({ status: "failed", limit: 1_000 });
      return failed.length === 250 && queue.getAllTasks().length === 150;
    });
    await runner.stopLoop();
    await loop;

    expect([...undone].sort()).toEqual(compensating);
    // Every pending orphan dispatched its first step exactly once.
    const dispatched = queue.getAllTasks().map((t) => t.workflowId);
    expect(new Set(dispatched).size).toBe(150);
  });
});

describe("result waits", () => {
  it("waitForResult() waits through a suspension and resolves once the run completes", async () => {
    const clock = FakeWallClock.create(0);
    const storage = new InMemoryWorkflowStorage({ clock });
    const queue = new InMemoryStepQueue({ clock });
    const runner = new DistributedWorkflowRunner({
      storage,
      stepQueue: queue,
      stepPollIntervalMs: 10,
      resultPollIntervalMs: 100,
      clock,
    });
    const worker = workerFor({ storage, queue, clock });
    void worker.start();
    await runner.submit({ workflow: waiting, workflowId: "w", input: { n: 2 } });

    let settled: unknown = "pending";
    const result = runner.waitForResult<number>("w").then(
      (r) => (settled = r),
      (e: unknown) => (settled = e),
    );
    await waitFor(async () => {
      clock.advance(10);
      return (await storage.loadWorkflow("w"))?.status === "suspended";
    });
    clock.advance(500);
    await flush();
    expect(settled).toBe("pending");

    // Another instance resumes and finishes the run.
    await storage.completeWorkflow({ workflowId: "w", result: 42 });
    clock.advance(100);
    await result;
    expect(settled).toBe(42);
    await worker.stop();
  });

  it("run() on a run another instance is driving waits for it through storage", async () => {
    const clock = FakeWallClock.create(0);
    const storage = new InMemoryWorkflowStorage({ clock });
    const queue = new InMemoryStepQueue({ clock });
    const runner = new DistributedWorkflowRunner({
      storage,
      stepQueue: queue,
      resultPollIntervalMs: 100,
      clock,
    });
    await storage.createWorkflow({ workflowId: "x", workflowName: "sleepy", input: { n: 1 } });
    // Another instance holds the run's lock.
    await storage.tryLock({ workflowId: "x", lockDurationMs: 600_000 });

    let settled: unknown = "pending";
    const running = runner.run({ workflow: sleepy, workflowId: "x", input: { n: 1 } }).then(
      (r) => (settled = r),
      (e: unknown) => (settled = e),
    );
    await waitFor(() => clock.pendingCount() > 0);
    expect(settled).toBe("pending");

    await storage.completeWorkflow({ workflowId: "x", result: "done-elsewhere" });
    clock.advance(100);
    await running;
    expect(settled).toBe("done-elsewhere");
  });

  it("waitForResult() rejects for a run that doesn't exist", async () => {
    const clock = FakeWallClock.create(0);
    const runner = new DistributedWorkflowRunner({
      storage: new InMemoryWorkflowStorage({ clock }),
      stepQueue: new InMemoryStepQueue({ clock }),
      clock,
    });
    await expect(runner.waitForResult("nope")).rejects.toThrow('Workflow "nope" not found');
  });
});

describe("fenced sweep", () => {
  it("a sweep whose lease was taken over is rejected and reported; the old leader stands down", async () => {
    const clock = FakeWallClock.create(0);
    const leases = new InMemoryLeaderLeases({ clock });
    const inner = new InMemoryStepQueue({ clock, leaderLeases: leases });
    const key = coordinatorLeaderKey();
    let stealNext = false;
    const queue = wrapQueue(inner, {
      requeueStuck: async (p) => {
        if (stealNext) {
          // The leader paused past its TTL mid-sweep; B took over.
          stealNext = false;
          clock.advance(5_000);
          await leases.tryAcquireLeader({ key, instanceId: "b", ttlMs: 5_000 });
        }
        return inner.requeueStuck(p);
      },
    });
    const errors: DistributedRunnerErrorEvent[] = [];
    const runner = new DistributedWorkflowRunner({
      storage: new InMemoryWorkflowStorage({ clock }),
      stepQueue: queue,
      leaderElection: new LeaseLeaderElection({
        store: leases,
        key,
        instanceId: "a",
        ttlMs: 3_000,
      }),
      pollIntervalMs: 1_000,
      clock,
      onError: (e) => errors.push(e),
    });

    const loop = runner.startLoop();
    await waitFor(() => clock.pendingCount() === 1);
    expect(errors).toEqual([]);

    stealNext = true;
    clock.advance(1_000);
    await waitFor(() => errors.length === 1);
    expect(isStaleLeaseError(errors[0]!.error)).toBe(true);

    // B holds a live lease now, so A's next sweeps don't lead (and don't write).
    let sweeps = 0;
    const requeue = queue.requeueStuck;
    queue.requeueStuck = (p) => (sweeps++, requeue(p));
    for (let i = 0; i < 2; i++) {
      clock.advance(2_000);
      await flush();
    }
    expect(sweeps).toBe(0);
    await runner.stopLoop();
    await loop;
  });
});

describe("step waits", () => {
  it("in-flight steps of one run share each storage read and don't sweep the queue", async () => {
    const clock = FakeWallClock.create(0);
    const storage = new InMemoryWorkflowStorage({ clock });
    const inner = new InMemoryStepQueue({ clock });
    let sweeps = 0;
    const queue = wrapQueue(inner, {
      requeueStuck: (p) => (sweeps++, inner.requeueStuck(p)),
    });
    let fanout = workflow<number>({ name: "fanout" }).step("root", ({ input }) => succeed(input));
    for (let i = 0; i < 10; i++) {
      fanout = fanout.step(`leaf-${i}`, { dependsOn: ["root"] }, () => succeed(i)) as never;
    }
    const wf = fanout.build();
    const runner = new DistributedWorkflowRunner({
      storage,
      stepQueue: queue,
      stepPollIntervalMs: 100,
      workerTimeoutMs: 200,
      clock,
    });
    const registry = new MapStepRegistry();
    registry.register("root", (ctx) => succeed(ctx.input));
    const worker = createWorker({ storage, stepQueue: queue, registry, pollIntervalMs: 10, clock });
    void worker.start();

    await runner.submit({ workflow: wf, workflowId: "f", input: 1 });
    await waitFor(async () => {
      clock.advance(10);
      return inner.getAllTasks().length === 11;
    });
    await worker.stop();

    let loads = 0;
    const load = storage.loadWorkflow.bind(storage);
    storage.loadWorkflow = (id) => (loads++, load(id));
    for (let i = 0; i < 10; i++) {
      clock.advance(100);
      await flush();
    }
    // 10 leaves waiting for 10 polls: one shared read per poll, not ten.
    expect(loads).toBeLessThanOrEqual(10);
    expect(loads).toBeGreaterThan(0);
    // Stale tasks are the leader sweep's job, not each waiting step's.
    expect(sweeps).toBe(0);
  });
});
