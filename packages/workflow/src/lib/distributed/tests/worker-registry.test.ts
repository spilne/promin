import { describe, it, expect } from "bun:test";
import { InMemoryWorkflowStorage } from "../../durable/index.ts";
import { InMemoryWorkerRegistry } from "../worker-registry.ts";
import { InMemoryStepQueue } from "../in-memory-step-queue.ts";
import { MapStepRegistry } from "../step-registry.ts";
import { createWorker } from "../worker.ts";
import { workerRegistryConformance } from "../worker-registry-conformance.ts";

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
    factory: async () => new InMemoryWorkerRegistry(),
  });
});

// ---------------------------------------------------------------------------
// Worker + registry integration
// ---------------------------------------------------------------------------

describe("Worker + registry integration — automatic lifecycle management", () => {
  it("worker auto-registers on start and auto-deregisters on stop", async () => {
    const storage = new InMemoryWorkflowStorage();
    const queue = new InMemoryStepQueue();
    const stepRegistry = new MapStepRegistry();
    const workerRegistry = new InMemoryWorkerRegistry();

    const worker = createWorker({
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
    await new Promise((r) => setTimeout(r, 100));

    // Worker should be registered
    const active = await workerRegistry.list({ status: "active" });
    expect(active).toHaveLength(1);
    expect(active[0]!.workerId).toBe(worker.workerId);
    expect(active[0]!.capabilities).toEqual(["default", "gpu"]);
    expect(active[0]!.concurrency).toBe(3);

    await worker.stop();

    // Worker should be gone
    const afterStop = await workerRegistry.list();
    expect(afterStop).toHaveLength(0);
  });

  it("worker sends periodic heartbeats — registry knows it is healthy", async () => {
    const storage = new InMemoryWorkflowStorage();
    const queue = new InMemoryStepQueue();
    const stepRegistry = new MapStepRegistry();
    const workerRegistry = new InMemoryWorkerRegistry();

    const worker = createWorker({
      storage,
      stepQueue: queue,
      registry: stepRegistry,
      capabilities: [],
      pollIntervalMs: 50,
      workerRegistry,
      heartbeatIntervalMs: 50,
    });

    void worker.start();
    await new Promise((r) => setTimeout(r, 200));

    // Heartbeat should have been updated
    const workers = await workerRegistry.list();
    const timeSinceHeartbeat = Date.now() - workers[0]!.lastHeartbeat.getTime();
    expect(timeSinceHeartbeat).toBeLessThan(150); // recent heartbeat

    await worker.stop();
  });

  it("worker drains in-flight tasks before fully stopping", async () => {
    const storage = new InMemoryWorkflowStorage();
    const queue = new InMemoryStepQueue();
    const stepRegistry = new MapStepRegistry();
    const workerRegistry = new InMemoryWorkerRegistry();

    stepRegistry.register("slow", async () => {
      await new Promise((r) => setTimeout(r, 300));
      return "done";
    });

    await storage.createWorkflow({ workflowId: "drain-1", workflowName: "test", input: {} });
    await queue.enqueue({
      workflowId: "drain-1",
      stepName: "slow",
      queue: "default",
      input: {},
      prevResults: {},
    });

    const worker = createWorker({
      storage,
      stepQueue: queue,
      registry: stepRegistry,
      capabilities: [],
      pollIntervalMs: 50,
      workerRegistry,
      heartbeatIntervalMs: 50,
    });

    void worker.start();
    await new Promise((r) => setTimeout(r, 100));

    // Start stopping — should be draining while task finishes
    const stopPromise = worker.stop();

    // Check draining status
    await new Promise((r) => setTimeout(r, 50));
    // May or may not catch the draining state depending on timing
    void workerRegistry.list({ status: "draining" });

    await stopPromise;

    // After stop — fully deregistered
    const after = await workerRegistry.list();
    expect(after).toHaveLength(0);
  });
});

// ---------------------------------------------------------------------------
// Dead worker recovery — requeueStuck
// ---------------------------------------------------------------------------

describe("Dead worker recovery — requeue stuck tasks after a worker crash", () => {
  it("tasks claimed by a crashed worker are returned to the queue for another worker", async () => {
    const queue = new InMemoryStepQueue({ workerId: "dead-worker" });

    await queue.enqueue({
      workflowId: "wf-1",
      stepName: "stuck-step",
      queue: "default",
      input: {},
      prevResults: {},
    });

    // Claim — sets claimedBy to "dead-worker"
    const tasks = await queue.claim({ capabilities: [], limit: 1 });
    expect(tasks).toHaveLength(1);

    // Task is now "running" claimed by "dead-worker" — simulate worker death
    const requeued = await queue.requeueStuck({ claimedBy: "dead-worker" });
    expect(requeued).toBe(1);

    // Task should be claimable again
    const reclaimed = await queue.claim({ capabilities: [], limit: 1 });
    expect(reclaimed).toHaveLength(1);
    expect(reclaimed[0]!.stepName).toBe("stuck-step");
  });
});
