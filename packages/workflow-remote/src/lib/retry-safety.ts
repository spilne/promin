// ---------------------------------------------------------------------------
// Retry safety — which RPC calls may be sent again after the connection
// dropped mid-request, when the server may already have applied them.
//
// A call is retry-safe when applying it twice leaves the store as applying
// it once did and answers the same. That has to hold durably — across a
// server restart — so it rests on the store's own contract, never on a
// response cache:
//
//   - reads, and writes that are upserts or conditional transitions
//   - `tryLock` / `tryLockAndLoad` with an `attemptId`: a repeat gets the
//     lock it took back
//   - `startFreshRun` with `expectedRun`: a repeat changes nothing and
//     answers the current run number
//   - `appendStreamChunk` with a `chunkId`: a repeat answers the first index
//   - `createSignalToken` (the caller's `tokenId`) and `completeSignalToken`
//     / `completePendingEntry` (a repeat of the winning call answers like it)
//
// Everything else is resent only when the request provably never reached a
// route (connection refused, or the server's own 503 while shutting down).
// `RemoteWorkflowStorage` stamps the ids above on every call, so its
// requests qualify.
// ---------------------------------------------------------------------------

import type { StorageMethod } from "./wire.ts";
import type { WorkerMethod } from "./worker-wire.ts";

/** Storage methods safe to resend after a mid-request disconnect, whatever their params. */
const ALWAYS_SAFE_STORAGE_METHODS: ReadonlySet<StorageMethod> = new Set<StorageMethod>([
  // Reads.
  "loadWorkflow",
  "loadWorkflowStatus",
  "listWorkflows",
  "distinctWorkflowNames",
  "distinctWorkflowTypes",
  "distinctNamespaces",
  "findWorkflowByIdempotencyKey",
  "loadSignals",
  "loadRunHistory",
  "listDueTimers",
  "listSignalWakeups",
  "listOrphanedRuns",
  "listChildEndedWaits",
  "loadJournal",
  "findDueSleeps",
  "findPendingSignal",
  "loadStepAttempts",
  "findSignalTokenById",
  "listSignalTokensForWorkflow",
  "readStreamChunks",
  // Upserts and conditional transitions: a repeat changes nothing more.
  "createWorkflow",
  "saveStepResult",
  "batchSaveStepResults",
  "saveStepFailure",
  "saveTaskResult",
  "saveTaskFailure",
  "checkpointStep",
  "saveStepAttempt",
  "completeWorkflow",
  "failWorkflow",
  "tripwireWorkflow",
  "cancelWorkflow",
  "suspendWorkflow",
  "deliverSignal",
  "setWorkflowMetadata",
  "releaseLock",
  "heartbeat",
  "resetSteps",
  "appendEntry",
  "appendPendingEntry",
  "completePendingEntry",
  "discardJournalEntries",
  "createSignalToken",
  "completeSignalToken",
]);

/**
 * True when a storage RPC (`{ method, params }`, params as sent) may be
 * resent after the connection dropped mid-request.
 */
export function isRetrySafeStorageCall(call: { method: string; params: unknown }): boolean {
  if (ALWAYS_SAFE_STORAGE_METHODS.has(call.method as StorageMethod)) return true;
  const params = (call.params ?? {}) as Record<string, unknown>;
  switch (call.method as StorageMethod) {
    case "tryLock":
    case "tryLockAndLoad":
      return typeof params.attemptId === "string";
    case "startFreshRun":
      return typeof params.expectedRun === "number";
    case "appendStreamChunk":
      return typeof params.chunkId === "string";
    default:
      return false;
  }
}

/** Worker-registry RPC methods safe to resend after a mid-request disconnect. */
const SAFE_WORKER_METHODS: ReadonlySet<WorkerMethod> = new Set<WorkerMethod>([
  "registerWorker",
  "heartbeatWorker",
  "drainWorker",
  "deregisterWorker",
  "listWorkers",
]);

/** True when a worker RPC (`{ method }`) may be resent after a mid-request disconnect. */
export function isRetrySafeWorkerCall(call: { method: string }): boolean {
  return SAFE_WORKER_METHODS.has(call.method as WorkerMethod);
}
