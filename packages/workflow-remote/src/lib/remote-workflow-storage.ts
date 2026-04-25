// ---------------------------------------------------------------------------
// RemoteWorkflowStorage — client-side WorkflowStorage backed by HTTP.
//
// Implements the full WorkflowStorage interface by forwarding each call to
// a server running `createWorkflowStorageHandler`. Passes the portable
// conformance suite.
//
// Accepts any `fetch`-compatible function — lets tests swap a direct
// in-process handler in place of real HTTP without spinning up a server.
// ---------------------------------------------------------------------------

import type {
  WorkflowStorage,
  WorkflowState,
  WorkflowStatus,
  WorkflowRunSummary,
  SignalState,
  FenceGuard,
} from "@promin/workflow";
import { WIRE_CODEC, type RpcResponse, type StorageMethod } from "./wire.ts";

export type FetchLike = (req: Request) => Promise<Response>;

export interface RemoteWorkflowStorageConfig {
  /**
   * URL the client POSTs to. Path doesn't matter — the server handler
   * dispatches by request body, not path.
   */
  readonly url: string;
  /**
   * Fetch implementation. Defaults to the global `fetch`. Tests (and
   * in-process adapters) pass a handler-bound fetch that skips the network.
   */
  readonly fetch?: FetchLike;
  /**
   * Extra headers applied to every request (auth tokens, tracing, etc.).
   */
  readonly headers?: Record<string, string>;
}

export class RemoteWorkflowStorage implements WorkflowStorage {
  private readonly url: string;
  private readonly fetch: FetchLike;
  private readonly headers: Record<string, string>;

  constructor(config: RemoteWorkflowStorageConfig) {
    this.url = config.url;
    this.fetch = config.fetch ?? ((req) => globalThis.fetch(req));
    this.headers = { "content-type": "application/json", ...(config.headers ?? {}) };
  }

  /**
   * Core RPC primitive. Encodes params via LosslessJsonCodec, POSTs, decodes
   * the response. Surfaces server-side errors as thrown JS errors so callers
   * see the same semantics they'd get from an in-process storage.
   */
  private async call<T>(method: StorageMethod, params: unknown): Promise<T> {
    const body = JSON.stringify({ method, params: WIRE_CODEC.encode(params) });
    const req = new Request(this.url, { method: "POST", headers: this.headers, body });
    const res = await this.fetch(req);

    let envelope: RpcResponse;
    try {
      envelope = (await res.json()) as RpcResponse;
    } catch (err) {
      throw new Error(
        `RemoteWorkflowStorage: invalid response from ${this.url} (status ${res.status}): ${
          (err as Error).message
        }`,
      );
    }

    if (!envelope.ok) {
      // Rehydrate Effect tagged errors — the server packs `_tag` + public
      // fields into the envelope so `.toMatchObject({ _tag: "..." })` and
      // downstream `err._tag === "FenceTokenMismatchError"` branches work
      // the same way they would for an in-process storage.
      if (envelope.errorTag) {
        const decoded = (envelope.errorFields ? WIRE_CODEC.decode(envelope.errorFields) : {}) as
          | Record<string, unknown>
          | undefined;
        const tagged = Object.assign(
          new Error(envelope.error),
          { _tag: envelope.errorTag },
          decoded ?? {},
        );
        throw tagged;
      }
      throw new Error(`RemoteWorkflowStorage.${method}: ${envelope.error}`);
    }
    return WIRE_CODEC.decode(envelope.result) as T;
  }

  // -------------------------------------------------------------------------
  // WorkflowStorage — thin delegates. Each method packs its params into an
  // object keyed by the target method's own named params, so the server's
  // dispatcher can unpack them without knowing arity. Keeping this uniform
  // lets us grow the interface without touching the transport.
  // -------------------------------------------------------------------------

  loadWorkflow(workflowId: string): Promise<WorkflowState | null> {
    return this.call("loadWorkflow", { workflowId });
  }

  listWorkflows(params?: {
    status?: WorkflowStatus;
    name?: string;
    type?: string;
    parentId?: string;
    namespace?: string;
    limit?: number;
    offset?: number;
  }): Promise<WorkflowState[]> {
    return this.call("listWorkflows", params ?? {});
  }

  cancelWorkflow(
    workflowId: string,
    options?: { cascade?: boolean },
    guard?: FenceGuard,
  ): Promise<void> {
    return this.call("cancelWorkflow", { workflowId, options, guard });
  }

  createWorkflow(params: {
    workflowId: string;
    workflowName: string;
    input: unknown;
    workflowType?: string;
    parentWorkflowId?: string;
    namespace?: string;
    metadata?: Record<string, unknown>;
    version?: string;
  }): Promise<{ created: true } | { created: false; existing: WorkflowState }> {
    return this.call("createWorkflow", params);
  }

  saveStepResult(
    params: {
      workflowId: string;
      stepName: string;
      result: unknown;
      durationMs: number;
      startedAt: Date;
      metadata?: Record<string, unknown>;
    },
    guard?: FenceGuard,
  ): Promise<void> {
    return this.call("saveStepResult", { ...params, guard });
  }

  batchSaveStepResults(
    records: ReadonlyArray<{
      workflowId: string;
      stepName: string;
      result: unknown;
      durationMs: number;
      startedAt: Date;
      metadata?: Record<string, unknown>;
    }>,
    guard?: FenceGuard,
  ): Promise<void> {
    return this.call("batchSaveStepResults", { records, guard });
  }

  saveStepFailure(
    params: {
      workflowId: string;
      stepName: string;
      error: string;
      durationMs: number;
      startedAt: Date;
      metadata?: Record<string, unknown>;
    },
    guard?: FenceGuard,
  ): Promise<void> {
    return this.call("saveStepFailure", { ...params, guard });
  }

  saveTaskResult(
    params: {
      workflowId: string;
      stepName: string;
      taskIndex: number;
      result: unknown;
    },
    guard?: FenceGuard,
  ): Promise<void> {
    return this.call("saveTaskResult", { ...params, guard });
  }

  saveTaskFailure(
    params: {
      workflowId: string;
      stepName: string;
      taskIndex: number;
      error: string;
    },
    guard?: FenceGuard,
  ): Promise<void> {
    return this.call("saveTaskFailure", { ...params, guard });
  }

  completeWorkflow(workflowId: string, result: unknown, guard?: FenceGuard): Promise<void> {
    return this.call("completeWorkflow", { workflowId, result, guard });
  }

  failWorkflow(workflowId: string, error: string, guard?: FenceGuard): Promise<void> {
    return this.call("failWorkflow", { workflowId, error, guard });
  }

  tripwireWorkflow(workflowId: string, reason: unknown, guard?: FenceGuard): Promise<void> {
    return this.call("tripwireWorkflow", { workflowId, reason, guard });
  }

  suspendWorkflow(
    workflowId: string,
    stepName: string,
    stepUpdate: Record<string, unknown>,
    guard?: FenceGuard,
  ): Promise<void> {
    return this.call("suspendWorkflow", { workflowId, stepName, stepUpdate, guard });
  }

  deliverSignal(workflowId: string, signalName: string, payload: unknown): Promise<void> {
    return this.call("deliverSignal", { workflowId, signalName, payload });
  }

  loadSignals(workflowId: string): Promise<SignalState[]> {
    return this.call("loadSignals", { workflowId });
  }

  tryLock(
    workflowId: string,
    lockDurationMs: number,
  ): Promise<{ acquired: boolean; token?: string }> {
    return this.call("tryLock", { workflowId, lockDurationMs });
  }

  tryLockAndLoad(
    workflowId: string,
    lockDurationMs: number,
  ): Promise<{ locked: boolean; token?: string; state: WorkflowState | null }> {
    return this.call("tryLockAndLoad", { workflowId, lockDurationMs });
  }

  releaseLock(workflowId: string, guard?: FenceGuard): Promise<void> {
    return this.call("releaseLock", { workflowId, guard });
  }

  heartbeat(workflowId: string, lockDurationMs: number, guard?: FenceGuard): Promise<void> {
    return this.call("heartbeat", { workflowId, lockDurationMs, guard });
  }

  startFreshRun(workflowId: string): Promise<number> {
    return this.call("startFreshRun", { workflowId });
  }

  loadRunHistory(
    workflowId: string,
    params?: { limit?: number; offset?: number },
  ): Promise<WorkflowRunSummary[]> {
    return this.call("loadRunHistory", { workflowId, params });
  }

  purgeCompleted(
    params: { olderThanMs: number; limit: number } | { from: Date; to: Date; limit: number },
  ): Promise<number> {
    return this.call("purgeCompleted", params);
  }
}
