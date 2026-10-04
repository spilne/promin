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
import { hasCapability, type StorageCapability, type StorageCapabilityMap } from "@promin/workflow";
import { WIRE_CODEC, type RpcRequest, type RpcResponse, type StorageMethod } from "./wire.ts";

/**
 * Build a fetch-style handler backed by `storage`. The returned function
 * mirrors the `(req: Request) => Promise<Response>` signature that
 * `Bun.serve`, `Deno.serve`, and most edge runtimes accept.
 */
export function createWorkflowStorageHandler(
  storage: WorkflowStorage,
): (req: Request) => Promise<Response> {
  // The RPC params of a method are its storage params object, `guard`
  // included, so the dispatchers hand them straight through.
  const dispatchers: Record<StorageMethod, (params: any) => Promise<unknown>> = {
    loadWorkflow: (p) => storage.loadWorkflow(p.workflowId),
    loadWorkflowStatus: (p) => storage.loadWorkflowStatus(p.workflowId),
    listWorkflows: (p) => storage.listWorkflows(p),
    distinctWorkflowNames: (p) => storage.distinctWorkflowNames(p),
    distinctWorkflowTypes: (p) => storage.distinctWorkflowTypes(p),
    distinctNamespaces: () => storage.distinctNamespaces(),
    cancelWorkflow: (p) => storage.cancelWorkflow(p),
    createWorkflow: (p) => storage.createWorkflow(p),
    findWorkflowByIdempotencyKey: (p) => storage.findWorkflowByIdempotencyKey(p),
    saveStepResult: (p) => storage.saveStepResult(p),
    batchSaveStepResults: (p) => storage.batchSaveStepResults(p),
    saveStepFailure: (p) => storage.saveStepFailure(p),
    saveTaskResult: (p) => storage.saveTaskResult(p),
    saveTaskFailure: (p) => storage.saveTaskFailure(p),
    completeWorkflow: (p) => storage.completeWorkflow(p),
    failWorkflow: (p) => storage.failWorkflow(p),
    tripwireWorkflow: (p) => requireCapability(storage, "tripwire").tripwireWorkflow(p),
    suspendWorkflow: (p) => storage.suspendWorkflow(p),
    deliverSignal: (p) => storage.deliverSignal(p),
    loadSignals: (p) => storage.loadSignals(p.workflowId),
    setWorkflowMetadata: (p) => storage.setWorkflowMetadata(p),
    tryLock: (p) => storage.tryLock(p),
    tryLockAndLoad: (p) => storage.tryLockAndLoad(p),
    releaseLock: (p) => storage.releaseLock(p),
    heartbeat: (p) => storage.heartbeat(p),
    startFreshRun: (p) => storage.startFreshRun(p),
    loadRunHistory: (p) => storage.loadRunHistory(p),
    resetSteps: (p) => requireCapability(storage, "resetSteps").resetSteps(p),
    purgeCompleted: (p) => storage.purgeCompleted(p),
    // -- Scanner / recovery queries.
    listDueTimers: (p) => requireCapability(storage, "dueTimers").listDueTimers(p),
    listSignalWakeups: (p) => requireCapability(storage, "signalWakeups").listSignalWakeups(p),
    listOrphanedRuns: (p) => requireCapability(storage, "orphanedRuns").listOrphanedRuns(p),
    // -- Journal.
    loadJournal: (p) => requireCapability(storage, "journal").loadJournal(p),
    appendEntry: (p) => requireCapability(storage, "journal").appendEntry(p),
    appendPendingEntry: (p) => requireCapability(storage, "journal").appendPendingEntry(p),
    completePendingEntry: (p) => requireCapability(storage, "journal").completePendingEntry(p),
    discardJournalEntries: (p) => requireCapability(storage, "journal").discardJournalEntries(p),
    findDueSleeps: (p) => requireCapability(storage, "journal").findDueSleeps(p),
    findPendingSignal: (p) => requireCapability(storage, "journal").findPendingSignal(p),
    // -- Step attempts.
    saveStepAttempt: (p) => requireCapability(storage, "stepAttempts").saveStepAttempt(p),
    loadStepAttempts: (p) => requireCapability(storage, "stepAttempts").loadStepAttempts(p),
    // -- Compensation ledger.
    beginCompensation: (p) => requireCapability(storage, "compensationLedger").beginCompensation(p),
    saveStepCompensation: (p) =>
      requireCapability(storage, "compensationLedger").saveStepCompensation(p),
    // -- Signal tokens.
    createSignalToken: (p) => storage.createSignalToken(p),
    findSignalTokenById: (p) => storage.findSignalTokenById(p.tokenId),
    markSignalTokenCompleted: (p) => storage.markSignalTokenCompleted(p),
    listSignalTokensForWorkflow: (p) => storage.listSignalTokensForWorkflow(p.workflowId),
    // -- Streams.
    appendStreamChunk: (p) => storage.appendStreamChunk(p),
    readStreamChunks: (p) => storage.readStreamChunks(p),
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
      // Preserve tagged-error shape so the client can rebuild
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

/** Why a storage lacking each capability can't serve the call. */
const MISSING_CAPABILITY: Record<StorageCapability, string> = {
  journal:
    "storage does not implement JournalStore — .journaled() steps are unsupported on this backend",
  stepAttempts:
    "storage does not implement StepAttemptStore — step attempt history is unsupported on this backend",
  stepCheckpoint: "storage does not implement checkpointStep",
  compensationLedger:
    "storage does not implement CompensationLedgerStore — durable compensation is unsupported on this backend",
  tripwire: "storage does not implement tripwireWorkflow",
  resetSteps: "storage does not implement resetSteps",
  runEvents: "storage does not implement subscribeToWorkflow",
  stepStartedEvents: "storage does not implement notifyStepStarted",
  summaries: "storage does not implement listWorkflowSummaries",
  countWorkflows: "storage does not implement countWorkflows",
  cancelStale: "storage does not implement cancelStaleWorkflows",
  dueTimers: "storage does not implement listDueTimers",
  signalWakeups: "storage does not implement listSignalWakeups",
  orphanedRuns: "storage does not implement listOrphanedRuns",
};

/** `storage` narrowed to `capability`, or a clear error when it lacks it. */
function requireCapability<S extends object, K extends StorageCapability>(
  storage: S,
  capability: K,
): S & StorageCapabilityMap[K] {
  if (!hasCapability(storage, capability)) throw new Error(MISSING_CAPABILITY[capability]);
  return storage;
}
