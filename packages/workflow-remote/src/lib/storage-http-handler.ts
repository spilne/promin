// ---------------------------------------------------------------------------
// createWorkflowStorageHandler — server side of the storage RPC.
//
// Wraps any WorkflowStorage into a fetch-compatible handler
// (Request → Response). Embed it in whatever HTTP server you already run —
// Bun.serve, Hono, raw Node, doesn't matter.
//
// The handler accepts a single JSON-encoded RpcRequest on any POST path and
// dispatches to the matching storage method. Non-POSTs and malformed bodies
// get a 400; unknown methods get a 404; method throws become 500 with the
// error message. No routing, no middleware, no OpenAPI. If you want auth,
// put it in front of this handler.
// ---------------------------------------------------------------------------

import type { WorkflowStorage } from "@promin/workflow";
import { WIRE_CODEC, type RpcRequest, type RpcResponse, type StorageMethod } from "./wire.ts";

/**
 * Build a fetch-style handler backed by `storage`. The returned function
 * mirrors the `(req: Request) => Promise<Response>` signature that
 * `Bun.serve`, `Deno.serve`, and most edge runtimes accept.
 */
export function createWorkflowStorageHandler(
  storage: WorkflowStorage,
): (req: Request) => Promise<Response> {
  const dispatchers: Record<StorageMethod, (params: any) => Promise<unknown>> = {
    loadWorkflow: (p) => storage.loadWorkflow(p.workflowId),
    listWorkflows: (p) => storage.listWorkflows(p),
    cancelWorkflow: (p) => storage.cancelWorkflow(p.workflowId, p.options),
    createWorkflow: (p) => storage.createWorkflow(p),
    saveStepResult: (p) => storage.saveStepResult(p),
    batchSaveStepResults: (p) => storage.batchSaveStepResults(p),
    saveStepFailure: (p) => storage.saveStepFailure(p),
    saveTaskResult: (p) => storage.saveTaskResult(p),
    saveTaskFailure: (p) => storage.saveTaskFailure(p),
    completeWorkflow: (p) => storage.completeWorkflow(p.workflowId, p.result),
    failWorkflow: (p) => storage.failWorkflow(p.workflowId, p.error),
    suspendWorkflow: (p) => storage.suspendWorkflow(p.workflowId, p.stepName, p.stepUpdate),
    deliverSignal: (p) => storage.deliverSignal(p.workflowId, p.signalName, p.payload),
    loadSignals: (p) => storage.loadSignals(p.workflowId),
    tryLock: (p) => storage.tryLock(p.workflowId, p.lockDurationMs),
    tryLockAndLoad: (p) => storage.tryLockAndLoad(p.workflowId, p.lockDurationMs),
    releaseLock: (p) => storage.releaseLock(p.workflowId),
    heartbeat: (p) => storage.heartbeat(p.workflowId, p.lockDurationMs),
    startFreshRun: (p) => storage.startFreshRun(p.workflowId),
    loadRunHistory: (p) => storage.loadRunHistory(p.workflowId, p.params),
    purgeCompleted: (p) => storage.purgeCompleted(p),
  };

  return async (req) => {
    if (req.method !== "POST") {
      return jsonResponse({ ok: false, error: `Only POST is supported, got ${req.method}` }, 405);
    }

    let envelope: RpcRequest;
    try {
      // Body is LosslessJsonCodec-encoded — parse the raw JSON, then decode
      // the params back through the codec so Date / BigInt etc. come back
      // as real JS values, not their `{ __t: ... }` tags.
      const raw = (await req.json()) as { method: string; params: unknown };
      envelope = { method: raw.method as StorageMethod, params: WIRE_CODEC.decode(raw.params) };
    } catch (err) {
      return jsonResponse(
        { ok: false, error: `Invalid request body: ${(err as Error).message}` },
        400,
      );
    }

    const fn = dispatchers[envelope.method];
    if (!fn) {
      return jsonResponse({ ok: false, error: `Unknown method "${envelope.method}"` }, 404);
    }

    try {
      const result = await fn(envelope.params);
      return jsonResponse({ ok: true, result: WIRE_CODEC.encode(result) }, 200);
    } catch (err) {
      return jsonResponse(
        { ok: false, error: err instanceof Error ? err.message : String(err) },
        500,
      );
    }
  };
}

function jsonResponse(body: RpcResponse, status: number): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}
