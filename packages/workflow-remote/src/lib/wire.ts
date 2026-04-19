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
  | "cancelWorkflow"
  | "createWorkflow"
  | "saveStepResult"
  | "saveStepFailure"
  | "saveTaskResult"
  | "saveTaskFailure"
  | "completeWorkflow"
  | "failWorkflow"
  | "suspendWorkflow"
  | "deliverSignal"
  | "loadSignals"
  | "tryLock"
  | "releaseLock"
  | "heartbeat"
  | "startFreshRun"
  | "loadRunHistory"
  | "purgeCompleted";

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
    };

/** Shared codec — both client and server must agree on encoding. */
export const WIRE_CODEC = LosslessJsonCodec;
