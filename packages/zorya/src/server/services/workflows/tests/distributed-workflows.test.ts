// ---------------------------------------------------------------------------
// DistributedWorkflows tests — advertisement resolution + dispatch.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { InMemoryWorkflowStorage, InMemoryStepQueue } from "@promin/workflow";
import { InMemoryWorkflowAdvertisementRegistry } from "../../../workflow-advertisements.ts";
import { DistributedWorkflows, UnknownWorkflowError } from "../index.ts";

describe("DistributedWorkflows.canHandle", () => {
  it("returns false when no advertisement matches", async () => {
    const storage = new InMemoryWorkflowStorage();
    const stepQueue = new InMemoryStepQueue();
    const advertisements = new InMemoryWorkflowAdvertisementRegistry();
    const workflows = new DistributedWorkflows({ storage, stepQueue, advertisements });

    await expect(workflows.trigger("missing", {})).rejects.toBeInstanceOf(UnknownWorkflowError);
  });

  it("dispatches via stub workflow when advertisement is present", async () => {
    const storage = new InMemoryWorkflowStorage();
    const stepQueue = new InMemoryStepQueue();
    const advertisements = new InMemoryWorkflowAdvertisementRegistry();
    await advertisements.upsert("worker-gpu", [
      {
        name: "encode",
        steps: [{ name: "compress", kind: "single", dependsOn: [], needs: ["gpu"] }],
      },
    ]);

    const workflows = new DistributedWorkflows({ storage, stepQueue, advertisements });
    const r = await workflows.trigger("encode", { url: "x" });
    expect(r.workflowId).toBeDefined();

    // Storage row pre-created
    const state = await storage.loadWorkflow(r.workflowId);
    expect(state?.workflowName).toBe("encode");

    // Step task enqueued with the GPU need preserved
    const enqueued = await stepQueue.list?.();
    if (enqueued) {
      expect(enqueued.some((t) => t.needs?.includes("gpu"))).toBe(true);
    }
  });

  it("threads namespace + metadata + runSource onto the storage row", async () => {
    const storage = new InMemoryWorkflowStorage();
    const stepQueue = new InMemoryStepQueue();
    const advertisements = new InMemoryWorkflowAdvertisementRegistry();
    await advertisements.upsert("w1", [
      { name: "wf", steps: [{ name: "s", kind: "single", dependsOn: [] }] },
    ]);

    const workflows = new DistributedWorkflows({ storage, stepQueue, advertisements });
    const { workflowId } = await workflows.trigger(
      "wf",
      { n: 1 },
      { namespace: "tenant-a", metadata: { src: "test" }, runSource: "manual" },
    );
    const state = await storage.loadWorkflow(workflowId);
    expect(state?.namespace).toBe("tenant-a");
    expect(state?.metadata?.src).toBe("test");
    expect(state?.runSource).toBe("manual");
  });
});

describe("DistributedWorkflows lifecycle", () => {
  it("start() / stop() are clean and idempotent", async () => {
    const storage = new InMemoryWorkflowStorage();
    const stepQueue = new InMemoryStepQueue();
    const advertisements = new InMemoryWorkflowAdvertisementRegistry();
    const workflows = new DistributedWorkflows({
      storage,
      stepQueue,
      advertisements,
      pollIntervalMs: 50,
    });

    await workflows.start();
    await workflows.start(); // idempotent
    await workflows.stop();
    await workflows.stop(); // idempotent
  });
});
