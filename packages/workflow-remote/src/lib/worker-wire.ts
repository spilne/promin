// ---------------------------------------------------------------------------
// Worker wire format — RPC envelope for the distributed worker HTTP API.
//
// Same pattern as storage-http-handler: single POST endpoint, dispatch by
// { method, params } body. Cross-language workers use this to claim tasks,
// heartbeat, and write results without importing the TypeScript SDK.
// ---------------------------------------------------------------------------

import { LosslessJsonCodec } from "@promin/core";

/** Every method a worker can call over HTTP. */
export type WorkerMethod =
  // StepQueue — task lifecycle
  | "claim"
  | "complete"
  | "fail"
  | "heartbeat"
  | "requeueStuck"
  // WorkflowStorage — result/failure shortcuts (used by cross-language
  // workers that don't also speak the storage wire)
  | "saveStepResult"
  | "saveStepFailure"
  // WorkerRegistry — worker-level liveness. `heartbeat` above is the
  // task-level heartbeat; `heartbeatWorker` keeps the registry entry alive.
  | "registerWorker"
  | "heartbeatWorker"
  | "drainWorker"
  | "deregisterWorker"
  | "listWorkers"
  | "detectDeadWorkers"
  | "gcWorkers";

export interface WorkerRpcRequest {
  readonly method: WorkerMethod;
  readonly params: unknown;
}

export type WorkerRpcResponse =
  | { readonly ok: true; readonly result: unknown }
  | { readonly ok: false; readonly error: string };

/** Shared codec — must match on both client and server. */
export const WORKER_WIRE_CODEC = LosslessJsonCodec;
