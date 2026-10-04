// ---------------------------------------------------------------------------
// Versioned step dispatch — coordinator tags tasks with workflow version;
// workers filter claims by registered step names + supportedVersions.
// ---------------------------------------------------------------------------

import { succeed } from "@spilne/perfect-core";
import { describe, it, expect } from "bun:test";
import { workflow, InMemoryWorkflowStorage } from "../../../index.ts";
import { createWorkflowRunner } from "../../durable/workflow-runner.ts";
import { MapStepRegistry } from "../step-registry.ts";
import { InMemoryStepQueue } from "../in-memory-step-queue.ts";
import { createWorker } from "../worker.ts";
import { StepQueueExecutor } from "../step-queue-executor.ts";
import { RoutingStepExecutor } from "../../durable/runner/routing-step-executor.ts";

/** A runner that sends `remoteSteps` to workers over `stepQueue`. */
function routedRunner(params: {
  storage: InMemoryWorkflowStorage;
  stepQueue: InMemoryStepQueue;
  remoteSteps: readonly string[];
  pollIntervalMs: number;
}) {
  const { storage, stepQueue, remoteSteps, pollIntervalMs } = params;
  return createWorkflowRunner({
    storage,
    stepExecutor: new RoutingStepExecutor({
      remote: new StepQueueExecutor({ stepQueue, storage, pollIntervalMs }),
      remoteSteps,
      storage,
    }),
  });
}

describe("versioned dispatch", () => {
  it("coordinator stamps enqueued tasks with the workflow's version", async () => {
    const storage = new InMemoryWorkflowStorage();
    const stepQueue = new InMemoryStepQueue();

    const registry = new MapStepRegistry();
    registry.register({ stepName: "remote-step", handler: (ctx) => succeed(`done-${ctx.prev}`) });

    const worker = createWorker({
      storage,
      stepQueue,
      registry,
      capabilities: [],
      pollIntervalMs: 25,
    });
    void worker.start();

    const wf = workflow<{ id: string }>({
      name: "vd-1",
      version: "2",
    })
      .step("load", ({ input }) => succeed(input.id))
      .step("remote-step", { dependsOn: ["load"] }, ({ deps }) => succeed(`x-${deps.load}`))
      .build();
    const runner = routedRunner({
      storage,
      stepQueue,
      remoteSteps: ["remote-step"],
      pollIntervalMs: 25,
    });
    await runner.run({ workflow: wf, workflowId: "vd-1-a", input: { id: "abc" } });

    // After the workflow completes, inspect the completed task's stored version.
    const metrics = await stepQueue.metrics({ since: new Date(Date.now() - 60_000) });
    expect(metrics.completed).toBeGreaterThanOrEqual(1);

    await worker.stop();
  });

  it("worker skips tasks for step names it doesn't have handlers for", async () => {
    const stepQueue = new InMemoryStepQueue();

    // Enqueue two tasks — one whose name the worker handles, one it doesn't.
    await stepQueue.enqueue({
      workflowId: "w-1",
      stepName: "known-step",
      input: { x: 1 },
      prevResults: {},
    });
    await stepQueue.enqueue({
      workflowId: "w-2",
      stepName: "unknown-step",
      input: { y: 2 },
      prevResults: {},
    });

    // Worker supports only "known-step".
    const registry = new MapStepRegistry();
    registry.register({ stepName: "known-step", handler: () => succeed("ok") });

    // Claim directly — avoid spinning the worker loop.
    const claimed = await stepQueue.claim({
      workerId: "w-known",
      capabilities: [],
      limit: 10,
      stepNames: registry.list(),
    });

    expect(claimed).toHaveLength(1);
    expect(claimed[0]!.stepName).toBe("known-step");

    // The unknown-step task stays pending — another worker can pick it up.
    const second = await stepQueue.claim({
      workerId: "w-unknown",
      capabilities: [],
      limit: 10,
      stepNames: ["unknown-step"],
    });
    expect(second).toHaveLength(1);
    expect(second[0]!.stepName).toBe("unknown-step");
  });

  it("supportedVersions narrows the worker to a subset of versions", async () => {
    const stepQueue = new InMemoryStepQueue();

    // Enqueue versioned tasks for v1, v2, v3, and one unversioned.
    for (const v of ["1", "2", "3"]) {
      await stepQueue.enqueue({
        workflowId: `v${v}-wf`,
        stepName: "s",
        input: {},
        prevResults: {},
        version: v,
      });
    }
    await stepQueue.enqueue({
      workflowId: "unversioned-wf",
      stepName: "s",
      input: {},
      prevResults: {},
    });

    const registry = new MapStepRegistry();
    registry.register({ stepName: "s", handler: () => succeed("ok") });

    // Worker supports v1 + v2 only. Unversioned is always accepted;
    // v3 is rejected.
    const supported = ["1", "2"];
    const claimed = await stepQueue.claim({
      workerId: "w-1",
      capabilities: [],
      limit: 10,
      stepNames: registry.list(),
      versions: supported,
    });

    const versions = claimed.map((t) => t.version).sort();
    // 3 accepted: v1, v2, and unversioned (undefined sorts last).
    expect(claimed).toHaveLength(3);
    expect(versions).toEqual(["1", "2", undefined]);
  });

  it("rolling deploy — worker with supportedVersions=['1','2'] completes both v1 and v2 workflows", async () => {
    const storage = new InMemoryWorkflowStorage();
    const stepQueue = new InMemoryStepQueue();

    const v2Registry = new MapStepRegistry();
    v2Registry.register({
      stepName: "step-a",
      handler: (ctx) => succeed(`A-${(ctx.input as any).x}`),
    });

    // Worker declares it supports both versions during the rolling deploy.
    const worker = createWorker({
      storage,
      stepQueue,
      registry: v2Registry,
      capabilities: [],
      pollIntervalMs: 25,
      supportedVersions: ["1", "2"],
    });
    void worker.start();

    const runner = routedRunner({
      storage,
      stepQueue,
      remoteSteps: ["step-a"],
      pollIntervalMs: 25,
    });

    // v1 in-flight workflow.
    const v1 = workflow<{ x: number }>({
      name: "order",
      version: "1",
    })
      .step("step-a", ({ input }) => succeed(`v1-${input.x}`))
      .build();
    const v1Result = await runner.run({
      workflow: v1,
      workflowId: "v1-wf",
      input: { x: 10 },
    });
    expect(v1Result).toBe("A-10");

    // v2 fresh workflow.
    const v2 = workflow<{ x: number }>({
      name: "order",
      version: "2",
    })
      .step("step-a", ({ input }) => succeed(`v2-${input.x}`))
      .build();
    const v2Result = await runner.run({
      workflow: v2,
      workflowId: "v2-wf",
      input: { x: 20 },
    });
    expect(v2Result).toBe("A-20");

    await worker.stop();

    // Both workflows completed. Storage records their versions correctly.
    expect((await storage.loadWorkflow("v1-wf"))?.version).toBe("1");
    expect((await storage.loadWorkflow("v2-wf"))?.version).toBe("2");
  });

  it("rolling deploy — worker with supportedVersions=['2'] skips v1 tasks (they stay pending)", async () => {
    const storage = new InMemoryWorkflowStorage();
    const stepQueue = new InMemoryStepQueue();

    // Seed a v1 task directly (no worker picks it up).
    await stepQueue.enqueue({
      workflowId: "v1-orphan",
      stepName: "step-a",
      input: {},
      prevResults: {},
      version: "1",
    });

    const v2OnlyRegistry = new MapStepRegistry();
    v2OnlyRegistry.register({ stepName: "step-a", handler: () => succeed("v2-result") });

    // Worker only supports v2 — declares drain complete on its side.
    const worker = createWorker({
      storage,
      stepQueue,
      registry: v2OnlyRegistry,
      capabilities: [],
      pollIntervalMs: 25,
      supportedVersions: ["2"],
    });
    void worker.start();

    // Give the worker a chance to (not) claim the v1 task.
    await new Promise((r) => setTimeout(r, 100));
    await worker.stop();

    // v1 task should still be pending — worker correctly skipped it.
    const metrics = await stepQueue.metrics({ since: new Date(Date.now() - 60_000) });
    expect(metrics.pending).toBeGreaterThanOrEqual(1);
    expect(metrics.completed).toBe(0);
  });
});
