// ---------------------------------------------------------------------------
// ZoryaClient.start / startByName — typed and untyped run dispatch.
//
// Stubs the Zorya server with an in-process fetch handler that:
//   - serves the storage RPC against an InMemoryWorkflowStorage,
//   - serves /api/runs/trigger by createWorkflow + completeWorkflow against
//     the same storage so the handle's status / result / cancel calls land
//     on a real workflow row without spinning up a worker.
// Lets us exercise the full `start → handle.result()` round-trip without
// running an actual workflow engine, and pins the back-compat shape of
// `triggerWorkflow`.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { InMemoryWorkflowStorage, workflow } from "@promin/workflow";
import { createWorkflowStorageHandler } from "@promin/workflow-remote";
import { ZoryaClient } from "../zorya-client.ts";

function mountTestServer(storage: InMemoryWorkflowStorage) {
  const storageHandler = createWorkflowStorageHandler(storage);
  return async (req: Request): Promise<Response> => {
    const url = new URL(req.url);
    if (url.pathname.startsWith("/rpc/storage")) {
      return storageHandler(req);
    }
    if (url.pathname.startsWith("/api/runs/trigger/")) {
      const body = (await req.json()) as {
        input?: unknown;
        workflowId?: string;
        namespace?: string;
        version?: string;
        metadata?: Record<string, unknown>;
      };
      const name = decodeURIComponent(url.pathname.replace("/api/runs/trigger/", ""));
      const workflowId = body.workflowId ?? `wf_${Math.random().toString(36).slice(2, 10)}`;
      // Synthesize a completed workflow row so handle.result() returns
      // immediately. Mirrors what a real worker would do — just inlined
      // for test ergonomics.
      await storage.createWorkflow({
        workflowId,
        workflowName: name,
        workflowType: name,
        namespace: body.namespace,
        input: body.input,
        version: body.version,
      });
      await storage.completeWorkflow(workflowId, { ok: true, echoed: body.input });
      return new Response(JSON.stringify({ workflowId }), {
        status: 200,
        headers: { "content-type": "application/json" },
      });
    }
    return new Response("not-found", { status: 404 });
  };
}

describe("ZoryaClient.start — typed dispatch", () => {
  it("triggers via name, returns a handle, and result() resolves to the workflow's Output", async () => {
    const storage = new InMemoryWorkflowStorage();
    const fetch = mountTestServer(storage);
    const client = new ZoryaClient({ url: "http://test.local", fetch });

    const wf = workflow<{ orderId: number }>({ name: "order" })
      .stepAsync("hello", async ({ input }) => ({ orderId: input.orderId, ok: true }))
      .build();

    const handle = await client.start(wf, {
      input: { orderId: 42 },
      workflowId: "wf-typed-1",
    });

    expect(handle.workflowId).toBe("wf-typed-1");
    const result = await handle.result({ timeoutMs: 1_000 });
    // The fake server's completeWorkflow call wrote { ok, echoed }, not the
    // workflow's real output — we only care that the handle.result() path
    // pulls from storage and the value round-trips.
    expect((result as { ok: boolean }).ok).toBe(true);
  });

  it("status() reads through the storage RPC", async () => {
    const storage = new InMemoryWorkflowStorage();
    const fetch = mountTestServer(storage);
    const client = new ZoryaClient({ url: "http://test.local", fetch });

    const wf = workflow<number>({ name: "noop" })
      .stepAsync("a", async ({ input }) => input)
      .build();

    const handle = await client.start(wf, { input: 1, workflowId: "wf-status-1" });
    const status = await handle.status();
    expect(status?.state).toBe("completed");
  });
});

describe("ZoryaClient.startByName — untyped dispatch", () => {
  it("returns a handle with the trigger response's workflowId", async () => {
    const storage = new InMemoryWorkflowStorage();
    const fetch = mountTestServer(storage);
    const client = new ZoryaClient({ url: "http://test.local", fetch });

    const handle = await client.startByName("dynamic", {
      input: { foo: "bar" },
      workflowId: "wf-by-name-1",
      namespace: "tenant-a",
    });

    expect(handle.workflowId).toBe("wf-by-name-1");
    const result = await handle.result({ timeoutMs: 1_000 });
    expect(result).toEqual({ ok: true, echoed: { foo: "bar" } });
  });
});

describe("ZoryaClient.triggerWorkflow — back-compat", () => {
  it("still returns { workflowId } for legacy callers", async () => {
    const storage = new InMemoryWorkflowStorage();
    const fetch = mountTestServer(storage);
    const client = new ZoryaClient({ url: "http://test.local", fetch });

    const out = await client.triggerWorkflow("legacy", {
      input: { x: 1 },
      workflowId: "wf-legacy-1",
    });
    expect(out).toEqual({ workflowId: "wf-legacy-1" });
  });
});
