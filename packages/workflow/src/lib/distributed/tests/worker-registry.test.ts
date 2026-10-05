import { describe, it, expect } from "bun:test";
import { InMemoryWorkflowStorage } from "../../../index.ts";
import { InMemoryWorkerRegistry } from "../worker-registry.ts";
import { InMemoryStepQueue } from "../in-memory-step-queue.ts";
import { MapStepRegistry } from "../step-registry.ts";
import { createWorker } from "../worker.ts";
import { workerRegistryConformance } from "../worker-registry-conformance.ts";
import { FakeWallClock } from "../../shared/wall-clock.ts";

/** Yield to the event loop until `predicate` holds (bounded). */
async function waitFor(predicate: () => boolean | Promise<boolean>): Promise<void> {
  for (let i = 0; i < 2_000; i++) {
    if (await predicate()) return;
    await new Promise<void>((r) => setImmediate(r));
  }
  expect(await predicate()).toBe(true);
}

/** Let in-flight async work settle: a bounded number of macrotask turns. */
async function flush(): Promise<void> {
  for (let i = 0; i < 20; i++) await new Promise<void>((r) => setImmediate(r));
}

/** Storage, queue and worker registry on one fake clock. */
function setup() {
  const clock = FakeWallClock.create("2026-01-01T00:00:00Z");
  return {
    clock,
    storage: new InMemoryWorkflowStorage({ clock }),
    queue: new InMemoryStepQueue({ clock }),
    stepRegistry: new MapStepRegistry(),
    workerRegistry: new InMemoryWorkerRegistry({ clock }),
  };
}

// ---------------------------------------------------------------------------
// WorkerRegistry semantics — conformance suite
//
// The basics moved to worker-registry-conformance.ts so InMemory + Postgres
// (and any future backend) run the same test matrix. This file keeps the
// InMemory-specific integration tests that exercise the worker lifecycle
// alongside a real worker.
// ---------------------------------------------------------------------------

describe("InMemoryWorkerRegistry — conformance", () => {
  workerRegistryConformance({
    factory: async ({ clock }) => new InMemoryWorkerRegistry({ clock }),
  });
});

// ---------------------------------------------------------------------------
// Worker + registry integration
// ---------------------------------------------------------------------------

describe("Worker + registry integration — automatic lifecycle management", () => {
  it("worker auto-registers on start and auto-deregisters on stop", async () => {
    const { clock, storage, queue, stepRegistry, workerRegistry } = setup();

    const worker = createWorker({
      clock,
      storage,
      stepQueue: queue,
      registry: stepRegistry,
      capabilities: ["default", "gpu"],
      concurrency: 3,
      pollIntervalMs: 50,
      workerRegistry,
      heartbeatIntervalMs: 50,
      metadata: { hostname: "node-1" },
    });

    void worker.start();
    await waitFor(async () => (await workerRegistry.list({ status: "active" })).length > 0);

    // Worker should be registered
    const active = await workerRegistry.list({ status: "active" });
    expect(active).toHaveLength(1);
    expect(active[0]!.workerId).toBe(worker.workerId);
    expect(active[0]!.capabilities).toEqual(["default", "gpu"]);
    expect(active[0]!.concurrency).toBe(3);

    await worker.stop();

    // Worker is retired on a graceful stop — the row stays (for forensics
    // / the dashboard) but is no longer counted active.
    expect(await workerRegistry.list({ status: "active" })).toHaveLength(0);
    const retired = await workerRegistry.list({ status: "retired" });
    expect(retired).toHaveLength(1);
    expect(retired[0]!.workerId).toBe(worker.workerId);
    expect(retired[0]!.retiredAt?.getTime()).toBe(clock.currentTimeMs());
  });

  it("worker sends periodic heartbeats — registry knows it is healthy", async () => {
    const { clock, storage, queue, stepRegistry, workerRegistry } = setup();

    const worker = createWorker({
      clock,
      storage,
      stepQueue: queue,
      registry: stepRegistry,
      capabilities: [],
      pollIntervalMs: 50,
      workerRegistry,
      heartbeatIntervalMs: 50,
    });

    const startedAt = clock.currentTimeMs();
    void worker.start();
    await waitFor(async () => (await workerRegistry.list()).length > 0);
    expect((await workerRegistry.list())[0]!.lastHeartbeat.getTime()).toBe(startedAt);

    // Each heartbeat interval moves lastHeartbeat to the beat's clock time.
    for (let beat = 1; beat <= 4; beat++) {
      clock.advance(50);
      await flush();
      const [info] = await workerRegistry.list();
      expect(info!.lastHeartbeat.getTime()).toBe(startedAt + beat * 50);
    }
    // A healthy worker is not reported dead.
    expect(await workerRegistry.detectDead(100)).toEqual([]);

    await worker.stop();
  });

  it("worker drains in-flight tasks before fully stopping", async () => {
    const { clock, storage, queue, stepRegistry, workerRegistry } = setup();

    stepRegistry.register({
      stepName: "slow",
      handler: async () => {
        await new Promise<void>((r) => clock.setTimeout(r, 300));
        return "done";
      },
    });

    await storage.createWorkflow({ workflowId: "drain-1", workflowName: "test", input: {} });
    await queue.enqueue({
      workflowId: "drain-1",
      stepName: "slow",
      input: {},
      prevResults: {},
    });

    const worker = createWorker({
      clock,
      storage,
      stepQueue: queue,
      registry: stepRegistry,
      capabilities: [],
      pollIntervalMs: 50,
      workerRegistry,
      heartbeatIntervalMs: 50,
    });

    void worker.start();
    await waitFor(() => queue.getAllTasks()[0]?.status === "running");

    // Start stopping — the worker drains while the task finishes.
    let stopped = false;
    const stopPromise = worker.stop().then(() => (stopped = true));
    await waitFor(async () => (await workerRegistry.list({ status: "draining" })).length === 1);
    await flush();
    expect(stopped).toBe(false);
    expect(queue.getAllTasks()[0]?.status).toBe("running");

    // The task's 300ms of clock time pass; it completes and stop() resolves.
    clock.advance(300);
    await stopPromise;
    expect(queue.getAllTasks()[0]?.status).toBe("completed");
    expect((await storage.loadWorkflow("drain-1"))?.steps["slow"]?.result).toBe("done");

    // After stop — retired (row kept), no longer active.
    expect(await workerRegistry.list({ status: "active" })).toHaveLength(0);
    expect(await workerRegistry.list({ status: "retired" })).toHaveLength(1);
  });
});

// ---------------------------------------------------------------------------
// Dead worker recovery — requeueStuck
// ---------------------------------------------------------------------------

describe("Dead worker recovery — requeue stuck tasks after a worker crash", () => {
  it("tasks claimed by a crashed worker are returned to the queue for another worker", async () => {
    const queue = new InMemoryStepQueue();

    await queue.enqueue({
      workflowId: "wf-1",
      stepName: "stuck-step",
      input: {},
      prevResults: {},
    });

    // Claim — sets claimedBy to "dead-worker"
    const tasks = await queue.claim({ workerId: "dead-worker", capabilities: [], limit: 1 });
    expect(tasks).toHaveLength(1);

    // Task is now "running" claimed by "dead-worker" — simulate worker death
    const { requeued } = await queue.requeueStuck({ mode: "worker", workerId: "dead-worker" });
    expect(requeued).toBe(1);

    // Task should be claimable again
    const reclaimed = await queue.claim({ workerId: "live-worker", capabilities: [], limit: 1 });
    expect(reclaimed).toHaveLength(1);
    expect(reclaimed[0]!.stepName).toBe("stuck-step");
  });
});

// ---------------------------------------------------------------------------
// Timestamps and cutoffs follow the injected WallClock
// ---------------------------------------------------------------------------

describe("InMemoryWorkerRegistry — injected WallClock", () => {
  const T0 = Date.parse("2026-01-01T00:00:00Z");

  it("stamps startedAt / lastHeartbeat / retiredAt from the clock", async () => {
    const clock = FakeWallClock.create(T0);
    const registry = new InMemoryWorkerRegistry({ clock });

    await registry.register({ workerId: "w", capabilities: [], concurrency: 1 });
    clock.advance(5_000);
    await registry.heartbeat("w");
    clock.advance(2_000);
    await registry.deregister("w");

    const [w] = await registry.list();
    expect(w!.startedAt.getTime()).toBe(T0);
    expect(w!.lastHeartbeat.getTime()).toBe(T0 + 5_000);
    expect(w!.retiredAt!.getTime()).toBe(T0 + 7_000);
  });

  it("detectDead measures heartbeat staleness on the clock", async () => {
    const clock = FakeWallClock.create(T0);
    const registry = new InMemoryWorkerRegistry({ clock });
    await registry.register({ workerId: "stale", capabilities: [], concurrency: 1 });
    await registry.register({ workerId: "fresh", capabilities: [], concurrency: 1 });

    clock.advance(10_000);
    await registry.heartbeat("fresh");
    // Exactly at the timeout boundary: not yet dead.
    expect(await registry.detectDead(10_000)).toEqual([]);

    clock.advance(1);
    const dead = await registry.detectDead(10_000);
    expect(dead.map((w) => w.workerId)).toEqual(["stale"]);
    expect((await registry.list({ status: "active" })).map((w) => w.workerId)).toEqual(["fresh"]);
  });

  it("gc reaps on the clock using retiredAt, else lastHeartbeat", async () => {
    const clock = FakeWallClock.create(T0);
    const registry = new InMemoryWorkerRegistry({ clock });
    await registry.register({ workerId: "silent", capabilities: [], concurrency: 1 });
    await registry.register({ workerId: "retired", capabilities: [], concurrency: 1 });
    clock.advance(30_000);
    await registry.deregister("retired");

    // silent was last active at T0, retired at T0+30s. Retain 60s.
    clock.advance(30_000);
    expect(await registry.gc({ retainMs: 60_000 })).toBe(0);
    clock.advance(1);
    expect(await registry.gc({ retainMs: 60_000 })).toBe(1);
    expect((await registry.list()).map((w) => w.workerId)).toEqual(["retired"]);
    clock.advance(30_000);
    expect(await registry.gc({ retainMs: 60_000 })).toBe(1);
    expect(await registry.list()).toEqual([]);
  });
});
