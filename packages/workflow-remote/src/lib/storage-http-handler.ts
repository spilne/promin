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

import type {
  WorkflowStorage,
  ActivityJournalStorage,
  JournaledSuspendStorage,
} from "@promin/workflow";
import { isActivityJournalStorage, isJournaledSuspendStorage } from "@promin/workflow";
import { WIRE_CODEC, type RpcRequest, type RpcResponse, type StorageMethod } from "./wire.ts";

/**
 * Build a fetch-style handler backed by `storage`. The returned function
 * mirrors the `(req: Request) => Promise<Response>` signature that
 * `Bun.serve`, `Deno.serve`, and most edge runtimes accept.
 */
export function createWorkflowStorageHandler(
  storage: WorkflowStorage,
): (req: Request) => Promise<Response> {
  // Every write RPC carries an optional `guard: { fenceToken }` field on
  // its params object so a stale holder's stray writes get rejected
  // server-side. The param shape is exactly `{ ...originalArgs, guard? }`
  // — new clients send it, legacy clients omit it, both work.
  const dispatchers: Record<StorageMethod, (params: any) => Promise<unknown>> = {
    loadWorkflow: (p) => storage.loadWorkflow(p.workflowId),
    listWorkflows: (p) => storage.listWorkflows(p),
    distinctWorkflowNames: (p) => storage.distinctWorkflowNames(p),
    distinctWorkflowTypes: (p) => storage.distinctWorkflowTypes(p),
    distinctNamespaces: () => storage.distinctNamespaces(),
    cancelWorkflow: (p) => storage.cancelWorkflow(p.workflowId, p.options, p.guard),
    createWorkflow: (p) => storage.createWorkflow(p),
    saveStepResult: (p) => storage.saveStepResult(p, p.guard),
    batchSaveStepResults: (p) => storage.batchSaveStepResults(p.records, p.guard),
    saveStepFailure: (p) => storage.saveStepFailure(p, p.guard),
    saveTaskResult: (p) => storage.saveTaskResult(p, p.guard),
    saveTaskFailure: (p) => storage.saveTaskFailure(p, p.guard),
    completeWorkflow: (p) => storage.completeWorkflow(p.workflowId, p.result, p.guard),
    failWorkflow: (p) => storage.failWorkflow(p.workflowId, p.error, p.guard),
    tripwireWorkflow: async (p) => {
      // Forwarded only if the underlying storage implements it. The client
      // calling this op against a non-tripwire-capable storage surfaces a
      // clear 4xx-style error rather than a silent miss.
      if (!storage.tripwireWorkflow) {
        throw new Error("storage does not implement tripwireWorkflow");
      }
      await storage.tripwireWorkflow(p.workflowId, p.reason, p.guard);
    },
    suspendWorkflow: (p) =>
      storage.suspendWorkflow(p.workflowId, p.stepName, p.stepUpdate, p.guard),
    deliverSignal: (p) => storage.deliverSignal(p.workflowId, p.signalName, p.payload),
    loadSignals: (p) => storage.loadSignals(p.workflowId),
    tryLock: (p) => storage.tryLock(p.workflowId, p.lockDurationMs),
    tryLockAndLoad: (p) => storage.tryLockAndLoad(p.workflowId, p.lockDurationMs),
    releaseLock: (p) => storage.releaseLock(p.workflowId, p.guard),
    heartbeat: (p) => storage.heartbeat(p.workflowId, p.lockDurationMs, p.guard),
    startFreshRun: (p) => storage.startFreshRun(p.workflowId),
    loadRunHistory: (p) => storage.loadRunHistory(p.workflowId, p.params),
    purgeCompleted: (p) => storage.purgeCompleted(p),
    // -- Journal methods. Feature-detected so backends without journal
    // support surface a clear error instead of silently dropping calls.
    loadJournal: (p) => requireJournal(storage).loadJournal(p.workflowId, p.stepName),
    appendEntry: (p) => requireJournal(storage).appendEntry(p),
    appendPendingEntry: (p) => requireSuspend(storage).appendPendingEntry(p),
    completePendingEntry: (p) => requireSuspend(storage).completePendingEntry(p),
    findDueSleeps: (p) => requireSuspend(storage).findDueSleeps(p),
    findPendingSignal: (p) => requireSuspend(storage).findPendingSignal(p),
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
      const message = err instanceof Error ? err.message : String(err);
      // Preserve Effect tagged-error shape so the client can rebuild
      // `{ _tag, ...fields }` objects — the conformance suite relies on
      // `.toMatchObject({ _tag: "FenceTokenMismatchError" })` passing over
      // the wire the same way it does in-process.
      const tagged = err as { _tag?: string } & Record<string, unknown>;
      if (typeof tagged._tag === "string") {
        const fields: Record<string, unknown> = {};
        for (const [k, v] of Object.entries(tagged)) {
          if (k === "_tag" || k === "message" || k === "stack" || k === "name") continue;
          if (typeof v === "function") continue;
          fields[k] = v;
        }
        return jsonResponse(
          {
            ok: false,
            error: message,
            errorTag: tagged._tag,
            errorFields: WIRE_CODEC.encode(fields) as Record<string, unknown>,
          },
          500,
        );
      }
      return jsonResponse({ ok: false, error: message }, 500);
    }
  };
}

function jsonResponse(body: RpcResponse, status: number): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}

function requireJournal(storage: WorkflowStorage): ActivityJournalStorage {
  if (!isActivityJournalStorage(storage)) {
    throw new Error(
      "storage does not implement ActivityJournalStorage — .journaled() steps are unsupported on this backend",
    );
  }
  return storage;
}

function requireSuspend(storage: WorkflowStorage): JournaledSuspendStorage {
  if (!isActivityJournalStorage(storage) || !isJournaledSuspendStorage(storage)) {
    throw new Error(
      "storage does not implement JournaledSuspendStorage — ctx.sleep / ctx.signal in journaled steps are unsupported on this backend",
    );
  }
  return storage;
}
