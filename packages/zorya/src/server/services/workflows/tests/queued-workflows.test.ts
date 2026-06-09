// ---------------------------------------------------------------------------
// QueuedWorkflows tests — pre-create + enqueue, advertisement-gated canHandle.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { InMemoryWorkflowStorage } from "@promin/workflow";
import { InMemoryWorkflowAdvertisementRegistry } from "../../../workflow-advertisements.ts";
import { InMemoryWorkflowStartQueue } from "../../../workflow-starts.ts";
import { QueuedWorkflows, UnknownWorkflowError } from "../index.ts";

describe("QueuedWorkflows.trigger", () => {
  it("rejects unknown workflows when no advertisement and acceptAny is false", async () => {
    const storage = new InMemoryWorkflowStorage();
    const queue = new InMemoryWorkflowStartQueue();
    const advertisements = new InMemoryWorkflowAdvertisementRegistry();
    const workflows = new QueuedWorkflows({
      storage,
      workflowStarts: queue,
      advertisements,
    });

    await expect(workflows.trigger("missing", {})).rejects.toBeInstanceOf(UnknownWorkflowError);
  });

  it("accepts when an advertisement exists for the name", async () => {
    const storage = new InMemoryWorkflowStorage();
    const queue = new InMemoryWorkflowStartQueue();
    const advertisements = new InMemoryWorkflowAdvertisementRegistry();
    await advertisements.upsert("worker-1", [
      { name: "video", steps: [{ name: "encode", kind: "single", dependsOn: [] }] },
    ]);

    const workflows = new QueuedWorkflows({
      storage,
      workflowStarts: queue,
      advertisements,
    });

    const r = await workflows.trigger("video", { url: "x" });
    expect(r.workflowId).toBeDefined();

    // Storage row pre-created
    const state = await storage.loadWorkflow(r.workflowId);
    expect(state?.workflowName).toBe("video");
    expect(state?.status).toBe("pending");

    // Queued
    const list = await queue.list();
    expect(list.length).toBe(1);
    expect(list[0]?.workflowName).toBe("video");
  });

  it("accepts anything when acceptAny is true", async () => {
    const storage = new InMemoryWorkflowStorage();
    const queue = new InMemoryWorkflowStartQueue();
    const workflows = new QueuedWorkflows({
      storage,
      workflowStarts: queue,
      acceptAny: true,
    });

    const r = await workflows.trigger("anything", { x: 1 });
    expect(r.workflowId).toBeDefined();
    const list = await queue.list();
    expect(list.length).toBe(1);
  });

  it("threads namespace and metadata to the storage row", async () => {
    const storage = new InMemoryWorkflowStorage();
    const queue = new InMemoryWorkflowStartQueue();
    const workflows = new QueuedWorkflows({
      storage,
      workflowStarts: queue,
      acceptAny: true,
    });

    const { workflowId } = await workflows.trigger(
      "wf",
      { x: 1 },
      { namespace: "tenant-x", metadata: { source: "scheduled" }, runSource: "schedule" },
    );
    const state = await storage.loadWorkflow(workflowId);
    expect(state?.namespace).toBe("tenant-x");
    expect(state?.metadata?.source).toBe("scheduled");
    expect(state?.runSource).toBe("schedule");
  });
});

describe("QueuedWorkflows.rerun", () => {
  it("resets the row and re-enqueues a start", async () => {
    const storage = new InMemoryWorkflowStorage();
    const queue = new InMemoryWorkflowStartQueue();
    const workflows = new QueuedWorkflows({
      storage,
      workflowStarts: queue,
      acceptAny: true,
    });

    const { workflowId } = await workflows.trigger("wf", { x: 1 });
    // Drain the queue (simulate a worker claiming + completing)
    const claimed = await queue.claim({
      workflowSpecs: [{ name: "wf", versions: [] }],
      limit: 10,
    });
    for (const c of claimed) await queue.complete(c.id);

    await workflows.rerun(workflowId);
    const list = await queue.list();
    expect(list.some((r) => r.workflowId === workflowId)).toBe(true);
  });
});

describe("QueuedWorkflows deterministic ids", () => {
  it("fresh-runs a terminal row before enqueueing the next start", async () => {
    const storage = new InMemoryWorkflowStorage();
    const queue = new InMemoryWorkflowStartQueue();
    const workflows = new QueuedWorkflows({
      storage,
      workflowStarts: queue,
      acceptAny: true,
    });

    await storage.createWorkflow({
      workflowId: "scheduled-wf",
      workflowName: "wf",
      input: { old: true },
    });
    await storage.completeWorkflow("scheduled-wf", "old-result");

    await workflows.trigger("wf", { old: false }, { workflowId: "scheduled-wf" });

    const state = await storage.loadWorkflow("scheduled-wf");
    expect(state?.status).toBe("pending");
    expect(state?.result).toBeUndefined();
    expect(await queue.list()).toHaveLength(1);
  });
});
