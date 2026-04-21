// ---------------------------------------------------------------------------
// createWorkerApiHandler — smoke tests
//
// Calls the handler with in-process Request objects (no TCP). Verifies each
// RPC method round-trips through the wire codec and dispatches correctly.
// ---------------------------------------------------------------------------

import { describe, it, expect, beforeEach } from "bun:test";
import { InMemoryStepQueue, InMemoryWorkflowStorage } from "@promin/workflow";
import { createWorkerApiHandler } from "./worker-http-handler.ts";
import { WORKER_WIRE_CODEC } from "./worker-wire.ts";

function post(handler: (r: Request) => Promise<Response>, method: string, params: unknown) {
  const body = JSON.stringify({ method, params: WORKER_WIRE_CODEC.encode(params) });
  return handler(
    new Request("http://test.local/worker", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body,
    }),
  );
}

describe("createWorkerApiHandler", () => {
  let queue: InMemoryStepQueue;
  let storage: InMemoryWorkflowStorage;
  let handler: (r: Request) => Promise<Response>;

  beforeEach(() => {
    queue = new InMemoryStepQueue();
    storage = new InMemoryWorkflowStorage();
    handler = createWorkerApiHandler({ stepQueue: queue, storage });
  });

  it("rejects non-POST", async () => {
    const res = await handler(new Request("http://test.local/worker", { method: "GET" }));
    expect(res.status).toBe(405);
    const body = await res.json();
    expect(body.ok).toBe(false);
  });

  it("returns 404 for unknown method", async () => {
    const res = await post(handler, "nonexistent", {});
    expect(res.status).toBe(404);
    const body = await res.json();
    expect(body.ok).toBe(false);
  });

  it("claim returns available tasks", async () => {
    await queue.enqueue({ workflowId: "wf-1", stepName: "step-a", input: {}, prevResults: {} });

    const res = await post(handler, "claim", { limit: 5 });
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body.ok).toBe(true);
    const tasks = WORKER_WIRE_CODEC.decode(body.result) as any[];
    expect(tasks).toHaveLength(1);
    expect(tasks[0].stepName).toBe("step-a");
  });

  it("complete marks task completed", async () => {
    await queue.enqueue({ workflowId: "wf-1", stepName: "step-a", input: {}, prevResults: {} });
    const [task] = await queue.claim({ limit: 1 });

    const res = await post(handler, "complete", {
      taskId: task!.id,
      result: { value: 42 },
      durationMs: 10,
    });
    expect(res.status).toBe(200);
    const tasks = queue.getAllTasks();
    expect(tasks[0]!.status).toBe("completed");
  });

  it("fail marks task failed", async () => {
    await queue.enqueue({ workflowId: "wf-1", stepName: "step-a", input: {}, prevResults: {} });
    const [task] = await queue.claim({ limit: 1 });

    const res = await post(handler, "fail", {
      taskId: task!.id,
      error: "something went wrong",
      durationMs: 5,
    });
    expect(res.status).toBe(200);
    expect(queue.getAllTasks()[0]!.status).toBe("failed");
  });

  it("heartbeat succeeds for a running task", async () => {
    await queue.enqueue({ workflowId: "wf-1", stepName: "step-a", input: {}, prevResults: {} });
    const [task] = await queue.claim({ limit: 1 });

    const res = await post(handler, "heartbeat", { taskId: task!.id });
    const body = await res.json();
    expect(body.ok).toBe(true);
    expect(res.status).toBe(200);
  });

  it("requeueStuck requeues stale tasks", async () => {
    await queue.enqueue({ workflowId: "wf-1", stepName: "step-a", input: {}, prevResults: {} });
    await queue.claim({ limit: 1 });

    await new Promise((r) => setTimeout(r, 15));
    const res = await post(handler, "requeueStuck", { staleTimeoutMs: 1 });
    expect(res.status).toBe(200);
    const body = await res.json();
    const count = WORKER_WIRE_CODEC.decode(body.result);
    expect(count).toBe(1);
  });

  it("saveStepResult writes to storage", async () => {
    await storage.createWorkflow({
      workflowId: "wf-1",
      workflowName: "test",
      input: {},
      version: "1",
    });

    const res = await post(handler, "saveStepResult", {
      workflowId: "wf-1",
      stepName: "step-a",
      result: { x: 1 },
      durationMs: 10,
      startedAt: new Date(),
    });
    expect(res.status).toBe(200);

    const state = await storage.loadWorkflow("wf-1");
    expect(state?.steps["step-a"]?.status).toBe("completed");
  });

  it("saveStepFailure writes to storage", async () => {
    await storage.createWorkflow({
      workflowId: "wf-1",
      workflowName: "test",
      input: {},
      version: "1",
    });

    const res = await post(handler, "saveStepFailure", {
      workflowId: "wf-1",
      stepName: "step-a",
      error: "it broke",
      durationMs: 5,
      startedAt: new Date(),
    });
    expect(res.status).toBe(200);

    const state = await storage.loadWorkflow("wf-1");
    expect(state?.steps["step-a"]?.status).toBe("failed");
  });
});
