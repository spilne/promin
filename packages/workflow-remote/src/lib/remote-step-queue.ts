// ---------------------------------------------------------------------------
// RemoteStepQueue — client-side StepQueue that forwards worker-side calls
// over HTTP to a server running `createWorkerApiHandler`.
//
// Only implements the worker-facing subset of StepQueue: claim, complete,
// fail, heartbeat, requeueStuck. `enqueue` throws — workers never enqueue,
// that's the coordinator's job on the server.
// ---------------------------------------------------------------------------

import type { FairnessPolicy, StepQueue, StepTask } from "@promin/workflow";
import type { FetchLike } from "./remote-workflow-storage.ts";
import { WORKER_WIRE_CODEC, type WorkerMethod, type WorkerRpcResponse } from "./worker-wire.ts";

export interface RemoteStepQueueConfig {
  /**
   * URL the client POSTs to. Path doesn't matter — the server dispatches
   * by request body, not path.
   */
  readonly url: string;
  /** Fetch impl. Defaults to the global fetch. */
  readonly fetch?: FetchLike;
  /** Extra headers applied to every request (auth tokens, tracing, etc.). */
  readonly headers?: Record<string, string>;
}

export class RemoteStepQueue implements StepQueue {
  private readonly url: string;
  private readonly fetch: FetchLike;
  private readonly headers: Record<string, string>;

  constructor(config: RemoteStepQueueConfig) {
    this.url = config.url;
    this.fetch = config.fetch ?? ((req) => globalThis.fetch(req));
    this.headers = { "content-type": "application/json", ...(config.headers ?? {}) };
  }

  private async call<T>(method: WorkerMethod, params: unknown): Promise<T> {
    const body = JSON.stringify({ method, params: WORKER_WIRE_CODEC.encode(params) });
    const req = new Request(this.url, { method: "POST", headers: this.headers, body });
    const res = await this.fetch(req);

    let envelope: WorkerRpcResponse;
    try {
      envelope = (await res.json()) as WorkerRpcResponse;
    } catch (err) {
      throw new Error(
        `RemoteStepQueue: invalid response from ${this.url} (status ${res.status}): ${(err as Error).message}`,
      );
    }

    if (!envelope.ok) {
      throw new Error(envelope.error);
    }
    return WORKER_WIRE_CODEC.decode(envelope.result) as T;
  }

  // enqueue is a coordinator concern — workers don't enqueue. Surfaced as a
  // thrown error so accidental calls surface immediately instead of silently
  // missing tasks.
  enqueue(): Promise<string> {
    throw new Error(
      "RemoteStepQueue.enqueue is not supported — workers can't enqueue, that's the coordinator's job server-side.",
    );
  }

  claim(params: {
    capabilities?: readonly string[];
    stepNames?: readonly string[];
    supportedVersions?: readonly string[];
    limit: number;
    fairness?: FairnessPolicy;
    filter?: (task: StepTask) => boolean;
  }): Promise<StepTask[]> {
    // The `filter` predicate is client-local and can't cross the wire —
    // we strip it from the server call and apply it to the returned tasks.
    // Tasks the filter rejects are lost (no way to release them from here);
    // workers that need strict routing should use `capabilities` instead.
    const { filter, ...forWire } = params;
    return this.call<StepTask[]>("claim", forWire).then((tasks) =>
      filter ? tasks.filter(filter) : tasks,
    );
  }

  complete(params: {
    taskId: string;
    claimToken?: string;
    result: unknown;
    durationMs: number;
  }): Promise<boolean> {
    return this.call("complete", params);
  }

  fail(params: {
    taskId: string;
    claimToken?: string;
    error: string;
    durationMs: number;
  }): Promise<boolean> {
    return this.call("fail", params);
  }

  heartbeat(params: { taskId: string; claimToken?: string }): Promise<boolean> {
    return this.call("heartbeat", params);
  }

  requeueStuck(params: { claimedBy?: string; staleTimeoutMs?: number }): Promise<number> {
    return this.call<number>("requeueStuck", params);
  }

  // Queue metrics aren't on the worker wire yet — queue-level observability
  // lives on the server side. Dashboard metrics should query the server's
  // local queue directly. Remote workers don't need this.
  metrics(): Promise<{
    pending: number;
    running: number;
    completed: number;
    failed: number;
    avgWaitMs: number;
    avgExecMs: number;
    p95ExecMs: number;
  }> {
    throw new Error(
      "RemoteStepQueue.metrics is not exposed over the worker wire — query the server-side queue directly.",
    );
  }
}
