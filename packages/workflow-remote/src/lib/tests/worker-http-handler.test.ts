// ---------------------------------------------------------------------------
// createWorkerApiHandler — smoke tests
//
// Calls the handler with in-process Request objects (no TCP). Verifies each
// RPC method round-trips through the wire codec and dispatches correctly.
// ---------------------------------------------------------------------------

import { describe, it, expect, beforeEach } from "bun:test";
import { InMemoryStepQueue } from "@promin/workflow/distributed";
import { createWorkerApiHandler } from "../worker-http-handler.ts";
import { WORKER_WIRE_CODEC } from "../worker-wire.ts";

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
  let handler: (r: Request) => Promise<Response>;

  beforeEach(() => {
    queue = new InMemoryStepQueue();
    handler = createWorkerApiHandler({ stepQueue: queue });
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
    await queue.enqueue({ workflowId: "wf-1", stepName: "step-a", input: {} });

    const res = await post(handler, "claim", { workerId: "w-1", limit: 5 });
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body.ok).toBe(true);
    const tasks = WORKER_WIRE_CODEC.decode(body.result) as any[];
    expect(tasks).toHaveLength(1);
    expect(tasks[0].stepName).toBe("step-a");
  });

  it("complete marks task completed", async () => {
    await queue.enqueue({ workflowId: "wf-1", stepName: "step-a", input: {} });
    const [task] = await queue.claim({ workerId: "w-1", limit: 1 });

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
    await queue.enqueue({ workflowId: "wf-1", stepName: "step-a", input: {} });
    const [task] = await queue.claim({ workerId: "w-1", limit: 1 });

    const res = await post(handler, "fail", {
      taskId: task!.id,
      error: "something went wrong",
      durationMs: 5,
    });
    expect(res.status).toBe(200);
    expect(queue.getAllTasks()[0]!.status).toBe("failed");
  });

  it("heartbeat succeeds for a running task", async () => {
    await queue.enqueue({ workflowId: "wf-1", stepName: "step-a", input: {} });
    const [task] = await queue.claim({ workerId: "w-1", limit: 1 });

    const res = await post(handler, "heartbeat", { taskId: task!.id });
    const body = await res.json();
    expect(body.ok).toBe(true);
    expect(res.status).toBe(200);
  });

  it("claim routes by step name and version and records the worker id", async () => {
    await queue.enqueue({
      workflowId: "wf-1",
      stepName: "foreign",
      input: {},
      priority: 9,
    });
    await queue.enqueue({
      workflowId: "wf-2",
      stepName: "step-a",
      input: {},
      version: "1",
    });

    const res = await post(handler, "claim", {
      workerId: "remote-7",
      limit: 1,
      stepNames: ["step-a"],
      versions: ["1"],
    });
    const tasks = WORKER_WIRE_CODEC.decode((await res.json()).result) as any[];
    expect(tasks.map((t) => t.stepName)).toEqual(["step-a"]);
    expect((await queue.get(tasks[0].id))?.claimedBy).toBe("remote-7");
    expect((await queue.requeueStuck({ mode: "worker", workerId: "remote-7" })).requeued).toBe(1);
  });

  it("claim without a workerId is rejected", async () => {
    const res = await post(handler, "claim", { limit: 5 });
    expect(res.status).toBe(500);
    expect((await res.json()).ok).toBe(false);
  });

  it("release gives a claimed task back", async () => {
    const id = await queue.enqueue({
      workflowId: "wf-1",
      stepName: "step-a",
      input: {},
    });
    const [task] = await queue.claim({ workerId: "w-1", limit: 1 });

    const res = await post(handler, "release", { taskId: id, claimToken: task!.claimToken });
    expect(WORKER_WIRE_CODEC.decode((await res.json()).result)).toBe(true);
    const record = await queue.get(id);
    expect(record?.status).toBe("pending");
    expect(record?.deliveries).toBe(0);
  });

  it("requeueStuck is not on the worker wire", async () => {
    const res = await post(handler, "requeueStuck", { mode: "stale", olderThanMs: 0 });
    expect(res.status).toBe(404);
  });

  it("storage methods are not on the worker wire", async () => {
    for (const method of ["saveStepResult", "saveStepFailure"]) {
      const res = await post(handler, method, { workflowId: "wf-1", stepName: "step-a" });
      expect(res.status).toBe(404);
    }
  });

  it("complete and fail carry the outcome fields the coordinator reads back", async () => {
    await queue.enqueue({ workflowId: "wf-1", stepName: "ok", input: {} });
    await queue.enqueue({ workflowId: "wf-2", stepName: "bad", input: {} });
    const [ok, bad] = await queue.claim({ workerId: "w-1", limit: 2 });

    await post(handler, "complete", {
      taskId: ok!.id,
      claimToken: ok!.claimToken,
      result: 1,
      durationMs: 1,
      stepMetadata: { matchCase: "a" },
    });
    await post(handler, "fail", {
      taskId: bad!.id,
      claimToken: bad!.claimToken,
      error: "declined",
      errorTag: "CardDeclined",
      durationMs: 1,
    });
    expect(await queue.get(ok!.id)).toMatchObject({ stepMetadata: { matchCase: "a" } });
    expect(await queue.get(bad!.id)).toMatchObject({ error: "declined", errorTag: "CardDeclined" });
  });

  it("a stale claim token cannot settle the task", async () => {
    const id = await queue.enqueue({ workflowId: "wf-1", stepName: "step-a", input: {} });
    const [task] = await queue.claim({ workerId: "w-1", limit: 1 });
    await queue.requeueStuck({ mode: "worker", workerId: "w-1" });
    await queue.claim({ workerId: "w-2", limit: 1 });

    const res = await post(handler, "complete", {
      taskId: id,
      claimToken: task!.claimToken,
      result: "stale",
      durationMs: 1,
    });
    expect(WORKER_WIRE_CODEC.decode((await res.json()).result)).toBe(false);
    expect((await queue.get(id))?.status).toBe("running");
  });
});
