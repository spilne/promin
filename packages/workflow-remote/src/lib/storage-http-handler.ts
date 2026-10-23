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

import type { WorkflowStorage, ActivityJournalStorage } from "@promin/workflow";
import {
  isActivityJournalStorage,
  isCompensationLedgerStorage,
  isStepAttemptStorage,
} from "@promin/workflow";
import type { CompensationLedgerStorage, StepAttemptStorage } from "@promin/workflow";
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
    loadWorkflowStatus: (p) => storage.loadWorkflowStatus(p.workflowId),
    listWorkflows: (p) => storage.listWorkflows(p),
    distinctWorkflowNames: (p) => storage.distinctWorkflowNames(p),
    distinctWorkflowTypes: (p) => storage.distinctWorkflowTypes(p),
    distinctNamespaces: () => storage.distinctNamespaces(),
    cancelWorkflow: (p) => storage.cancelWorkflow(p.workflowId, p.options, p.guard),
    createWorkflow: (p) => {
      const { guard, ...params } = p;
      return storage.createWorkflow(params, guard);
    },
    findWorkflowByIdempotencyKey: (p) => storage.findWorkflowByIdempotencyKey(p),
    saveStepResult: (p) => {
      const { guard, ...params } = p;
      return storage.saveStepResult(params, guard);
    },
    batchSaveStepResults: (p) => storage.batchSaveStepResults(p.records, p.guard),
    saveStepFailure: (p) => {
      const { guard, ...params } = p;
      return storage.saveStepFailure(params, guard);
    },
    saveTaskResult: (p) => {
      const { guard, ...params } = p;
      return storage.saveTaskResult(params, guard);
    },
    saveTaskFailure: (p) => {
      const { guard, ...params } = p;
      return storage.saveTaskFailure(params, guard);
    },
    completeWorkflow: (p) => storage.completeWorkflow(p.workflowId, p.result, p.guard),
    failWorkflow: (p) => storage.failWorkflow(p.workflowId, p.error, p.guard, p.details),
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
    setWorkflowMetadata: (p) => storage.setWorkflowMetadata(p.workflowId, p.patch, p.guard),
    tryLock: (p) => storage.tryLock(p.workflowId, p.lockDurationMs),
    tryLockAndLoad: (p) => storage.tryLockAndLoad(p.workflowId, p.lockDurationMs),
    releaseLock: (p) => storage.releaseLock(p.workflowId, p.guard),
    heartbeat: (p) => storage.heartbeat(p.workflowId, p.lockDurationMs, p.guard),
    startFreshRun: (p) => storage.startFreshRun(p.workflowId, p.guard),
    loadRunHistory: (p) => storage.loadRunHistory(p.workflowId, p.params),
    resetSteps: async (p) => {
      // Forwarded only if the underlying storage implements it — surface
      // a clear error rather than a silent miss.
      if (!storage.resetSteps) {
        throw new Error("storage does not implement resetSteps");
      }
      await storage.resetSteps(p.workflowId, p.stepNames);
    },
    purgeCompleted: (p) => storage.purgeCompleted(p),
    // -- Scanner / recovery queries. Feature-detected: a backend without
    // them surfaces a clear error instead of a silent miss.
    listDueTimers: (p) => {
      if (!storage.listDueTimers) throw new Error("storage does not implement listDueTimers");
      return storage.listDueTimers(p);
    },
    listSignalWakeups: (p) => {
      if (!storage.listSignalWakeups) {
        throw new Error("storage does not implement listSignalWakeups");
      }
      return storage.listSignalWakeups(p);
    },
    listOrphanedRuns: (p) => {
      if (!storage.listOrphanedRuns) throw new Error("storage does not implement listOrphanedRuns");
      return storage.listOrphanedRuns(p);
    },
    // -- Journal methods. Feature-detected so backends without journal
    // support surface a clear error instead of silently dropping calls.
    loadJournal: (p) => requireJournal(storage).loadJournal(p.workflowId, p.stepName),
    appendEntry: (p) => {
      const { guard, ...params } = p;
      return requireJournal(storage).appendEntry(params, guard);
    },
    appendPendingEntry: (p) => {
      const { guard, ...params } = p;
      return requireJournal(storage).appendPendingEntry(params, guard);
    },
    completePendingEntry: (p) => {
      const { guard, ...params } = p;
      return requireJournal(storage).completePendingEntry(params, guard);
    },
    discardJournalEntries: async (p) => {
      const journal = requireJournal(storage);
      if (!journal.discardJournalEntries) {
        throw new Error("storage does not implement discardJournalEntries");
      }
      const { guard, ...params } = p;
      await journal.discardJournalEntries(params, guard);
    },
    findDueSleeps: (p) => requireJournal(storage).findDueSleeps(p),
    findPendingSignal: (p) => requireJournal(storage).findPendingSignal(p),
    // -- StepAttempt methods. Feature-detected so backends without
    // attempt-history support surface a clear error instead of silent
    // failure.
    saveStepAttempt: (p) => requireStepAttempt(storage).saveStepAttempt(p.record, p.guard),
    loadStepAttempts: (p) => requireStepAttempt(storage).loadStepAttempts(p.workflowId, p.stepName),
    // -- Compensation ledger. Feature-detected like the attempt history.
    beginCompensation: (p) => {
      const { guard, ...params } = p;
      return requireCompensationLedger(storage).beginCompensation(params, guard);
    },
    saveStepCompensation: (p) => {
      const { guard, ...params } = p;
      return requireCompensationLedger(storage).saveStepCompensation(params, guard);
    },
    // -- Signal tokens. Core methods on WorkflowStorage — no feature gate.
    createSignalToken: (p) => storage.createSignalToken(p),
    findSignalTokenById: (p) => storage.findSignalTokenById(p.tokenId),
    markSignalTokenCompleted: (p) => storage.markSignalTokenCompleted(p),
    listSignalTokensForWorkflow: (p) => storage.listSignalTokensForWorkflow(p.workflowId),
    appendStreamChunk: (p) => {
      const { guard, ...params } = p;
      return storage.appendStreamChunk(params, guard);
    },
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

function requireJournal(storage: WorkflowStorage): ActivityJournalStorage {
  if (!isActivityJournalStorage(storage)) {
    throw new Error(
      "storage does not implement ActivityJournalStorage — .journaled() steps are unsupported on this backend",
    );
  }
  return storage;
}

function requireCompensationLedger(storage: WorkflowStorage): CompensationLedgerStorage {
  if (!isCompensationLedgerStorage(storage)) {
    throw new Error(
      "storage does not implement CompensationLedgerStorage — durable compensation is unsupported on this backend",
    );
  }
  return storage;
}

function requireStepAttempt(storage: WorkflowStorage): StepAttemptStorage {
  if (!isStepAttemptStorage(storage)) {
    throw new Error(
      "storage does not implement StepAttemptStorage — step attempt history is unsupported on this backend",
    );
  }
  return storage;
}
