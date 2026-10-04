// ---------------------------------------------------------------------------
// The storage RPC envelope: the RPC params of every method are its storage
// params object, `guard` included.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { InMemoryWorkflowStorage, type WorkflowStorage } from "@promin/workflow";
import { RemoteWorkflowStorage } from "../remote-workflow-storage.ts";
import { createWorkflowStorageHandler } from "../storage-http-handler.ts";
import { WIRE_CODEC } from "../wire.ts";

/** A remote over `backing` that records every request body. */
function recordingRemote(backing: WorkflowStorage): {
  remote: RemoteWorkflowStorage;
  bodies: Array<{ method: string; params: Record<string, unknown> }>;
} {
  const handler = createWorkflowStorageHandler(backing);
  const bodies: Array<{ method: string; params: Record<string, unknown> }> = [];
  const remote = new RemoteWorkflowStorage({
    url: "http://test.local/storage",
    fetch: async (req) => {
      const body = (await req.clone().json()) as { method: string; params: unknown };
      bodies.push({
        method: body.method,
        params: WIRE_CODEC.decode(body.params) as Record<string, unknown>,
      });
      return handler(req);
    },
  });
  return { remote, bodies };
}

describe("storage RPC envelope", () => {
  it("sends the params object for cancelWorkflow, failWorkflow and loadRunHistory", async () => {
    const backing = new InMemoryWorkflowStorage();
    const { remote, bodies } = recordingRemote(backing);
    await remote.createWorkflow({ workflowId: "p", workflowName: "w", input: 1 });
    await remote.createWorkflow({
      workflowId: "c",
      workflowName: "w",
      input: 1,
      parentWorkflowId: "p",
    });
    await remote.createWorkflow({ workflowId: "f", workflowName: "w", input: 1 });

    await remote.cancelWorkflow({ workflowId: "p", cascade: true });
    await remote.failWorkflow({ workflowId: "f", error: "boom", errorTag: "Boom" });
    await remote.loadRunHistory({ workflowId: "f", limit: 1 });

    const sent = (method: string) => bodies.find((b) => b.method === method)?.params;
    expect(sent("cancelWorkflow")).toEqual({ workflowId: "p", cascade: true });
    expect(sent("failWorkflow")).toEqual({ workflowId: "f", error: "boom", errorTag: "Boom" });
    expect(sent("loadRunHistory")).toEqual({ workflowId: "f", limit: 1 });

    expect((await backing.loadWorkflow("c"))?.status).toBe("failed");
    expect((await backing.loadWorkflow("f"))?.errorTag).toBe("Boom");
  });

  it("sends the params object, guard included, for every other method", async () => {
    const backing = new InMemoryWorkflowStorage();
    const { remote, bodies } = recordingRemote(backing);
    await remote.createWorkflow({ workflowId: "wf", workflowName: "w", input: 1 });
    const { token } = await remote.tryLock({ workflowId: "wf", lockDurationMs: 60_000 });
    await remote.completeWorkflow({ workflowId: "wf", result: 7, guard: { fenceToken: token } });

    expect(bodies.find((b) => b.method === "tryLock")?.params).toEqual({
      workflowId: "wf",
      lockDurationMs: 60_000,
    });
    expect(bodies.find((b) => b.method === "completeWorkflow")?.params).toEqual({
      workflowId: "wf",
      result: 7,
      guard: { fenceToken: token },
    });
    expect((await backing.loadWorkflow("wf"))?.result).toBe(7);
  });
});
