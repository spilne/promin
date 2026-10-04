// ---------------------------------------------------------------------------
// The storage RPC envelope: RPC params are the storage params object, and
// the three methods whose envelope predates params objects
// (`cancelWorkflow`, `failWorkflow`, `loadRunHistory`) keep it on the wire,
// so clients and servers of either version interoperate.
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

/** POST one raw envelope to a handler over `backing`. */
async function rawCall(params: {
  backing: WorkflowStorage;
  method: string;
  rpcParams: unknown;
}): Promise<{ ok: boolean; result?: unknown }> {
  const handler = createWorkflowStorageHandler(params.backing);
  const res = await handler(
    new Request("http://test.local/storage", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ method: params.method, params: WIRE_CODEC.encode(params.rpcParams) }),
    }),
  );
  const body = (await res.json()) as { ok: boolean; result?: unknown };
  return { ok: body.ok, result: body.ok ? WIRE_CODEC.decode(body.result) : undefined };
}

describe("storage RPC envelope", () => {
  it("keeps the older envelope for cancelWorkflow, failWorkflow and loadRunHistory", async () => {
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
    expect(sent("cancelWorkflow")).toMatchObject({ workflowId: "p", options: { cascade: true } });
    expect(sent("failWorkflow")).toMatchObject({
      workflowId: "f",
      error: "boom",
      details: { errorTag: "Boom" },
    });
    expect(sent("loadRunHistory")).toMatchObject({ workflowId: "f", params: { limit: 1 } });

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

  it("serves a client that sends the older envelope", async () => {
    const backing = new InMemoryWorkflowStorage();
    await backing.createWorkflow({ workflowId: "p", workflowName: "w", input: 1 });
    await backing.createWorkflow({
      workflowId: "c",
      workflowName: "w",
      input: 1,
      parentWorkflowId: "p",
    });
    await backing.createWorkflow({ workflowId: "f", workflowName: "w", input: 1 });
    await backing.startFreshRun({ workflowId: "f" });

    expect(
      await rawCall({
        backing,
        method: "cancelWorkflow",
        rpcParams: { workflowId: "p", options: { cascade: true } },
      }),
    ).toEqual({ ok: true, result: undefined });
    expect((await backing.loadWorkflow("c"))?.status).toBe("failed");

    await rawCall({
      backing,
      method: "failWorkflow",
      rpcParams: { workflowId: "f", error: "boom", details: { errorTag: "Boom" } },
    });
    expect((await backing.loadWorkflow("f"))?.errorTag).toBe("Boom");

    const history = await rawCall({
      backing,
      method: "loadRunHistory",
      rpcParams: { workflowId: "f", params: { limit: 1 } },
    });
    expect((history.result as unknown[]).length).toBe(1);
  });
});
