// ---------------------------------------------------------------------------
// Wire format — JSON-RPC-style envelope for WorkflowStorage over HTTP.
//
// Single endpoint, single POST. The client serializes `{ method, params }`
// using `LosslessJsonCodec` (Date / BigInt / Map / Set / Error / undefined /
// NaN / Infinity all round-trip). The server decodes, dispatches to the
// underlying WorkflowStorage, and re-encodes the result. Keeps the wire
// schema trivial — no per-method REST design, no OpenAPI surface, no
// codegen. If WorkflowStorage grows a method, both sides auto-pick it up.
// ---------------------------------------------------------------------------

import { LosslessJsonCodec } from "@promin/core";

/** Every name on the WorkflowStorage interface we can dispatch through. */
export type StorageMethod =
  | "loadWorkflow"
  | "listWorkflows"
  | "distinctWorkflowNames"
  | "distinctWorkflowTypes"
  | "distinctNamespaces"
  | "cancelWorkflow"
  | "createWorkflow"
  | "findWorkflowByIdempotencyKey"
  | "saveStepResult"
  | "batchSaveStepResults"
  | "saveStepFailure"
  | "saveTaskResult"
  | "saveTaskFailure"
  | "completeWorkflow"
  | "failWorkflow"
  | "tripwireWorkflow"
  | "suspendWorkflow"
  | "deliverSignal"
  | "loadSignals"
  | "setWorkflowMetadata"
  | "tryLock"
  | "tryLockAndLoad"
  | "releaseLock"
  | "heartbeat"
  | "startFreshRun"
  | "loadRunHistory"
  | "purgeCompleted"
  // ActivityJournalStorage / JournaledSuspendStorage — forwarded only when
  // the underlying storage implements them. Lets `.journaled()` workflows
  // (with ctx.activity / ctx.sleep / ctx.signal) run over the wire.
  | "loadJournal"
  | "appendEntry"
  | "appendPendingEntry"
  | "completePendingEntry"
  | "findDueSleeps"
  | "findPendingSignal"
  // StepAttemptStorage — forwarded only when the underlying storage
  // implements it. Lets remote workers populate the audit trail
  // (workerId per attempt) on the central server's storage.
  | "saveStepAttempt"
  | "loadStepAttempts"
  // Signal tokens — public-bearer authz for deliverSignal. Forwarded so
  // remote workers can surface tokens through their parent storage.
  | "createSignalToken"
  | "findSignalTokenById"
  | "markSignalTokenCompleted"
  | "listSignalTokensForWorkflow"
  // Streams — generic typed channels per workflow.
  | "appendStreamChunk"
  | "readStreamChunks";

export interface RpcRequest {
  readonly method: StorageMethod;
  /** Method-specific params, encoded via `LosslessJsonCodec`. */
  readonly params: unknown;
}

export type RpcResponse =
  | { readonly ok: true; readonly result: unknown }
  | {
      readonly ok: false;
      readonly error: string;
      /**
       * Optional tag for Effect `Data.TaggedError` subclasses (e.g.
       * `"FenceTokenMismatchError"`). When present, the client rehydrates
       * a plain object carrying `_tag`, `message`, and the other public
       * fields so downstream code can branch on `_tag` the same way it
       * would for an in-process storage.
       */
      readonly errorTag?: string;
      /**
       * Additional public fields from a tagged error (e.g.
       * `{ workflowId, expected, provided }` on `FenceTokenMismatchError`).
       * Decoded via `WIRE_CODEC` server-side so Date / BigInt / Map fields
       * round-trip intact.
       */
      readonly errorFields?: Record<string, unknown>;
    };

/** Shared codec — both client and server must agree on encoding. */
export const WIRE_CODEC = LosslessJsonCodec;
