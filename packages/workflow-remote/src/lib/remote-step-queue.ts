// ---------------------------------------------------------------------------
// RemoteStepQueue — client-side StepQueue that forwards worker-side calls
// over HTTP to a server running `createWorkerApiHandler`.
//
// Only implements the worker-facing subset of StepQueue: claim, release,
// complete, fail, heartbeat. The coordinator-side calls (enqueue,
// requeueStuck, get, purge, metrics) throw — they run against the server's
// queue directly.
// ---------------------------------------------------------------------------

import type {
  StepQueue,
  StepQueueClaimParams,
  StepQueueCompleteParams,
  StepQueueFailParams,
  StepQueueRequeueResult,
  StepTask,
  StepTaskRecord,
} from "@promin/workflow/distributed";
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

  // Consuming a settled outcome is the coordinator's step before it writes
  // the step row; workers never consume.
  consume(): Promise<boolean> {
    throw new Error(
      "RemoteStepQueue.consume is not exposed over the worker wire — the server-side coordinator consumes outcomes.",
    );
  }

  consumeSettled(): Promise<number> {
    throw new Error(
      "RemoteStepQueue.consumeSettled is not exposed over the worker wire — the server-side coordinator consumes outcomes.",
    );
  }

  /**
   * Claim on the server's queue. `workerId`, `stepNames` and `versions`
   * travel with the call, so routing happens inside the server-side claim
   * and the server records which worker holds each task (dead-worker
   * reclaim works for remote workers too).
   */
  claim(params: StepQueueClaimParams): Promise<StepTask[]> {
    return this.call<StepTask[]>("claim", {
      workerId: params.workerId,
      limit: params.limit,
      capabilities: params.capabilities,
      stepNames: params.stepNames,
      versions: params.versions,
    });
  }

  release(params: { taskId: string; claimToken: string }): Promise<boolean> {
    return this.call("release", params);
  }

  get(): Promise<StepTaskRecord | undefined> {
    throw new Error(
      "RemoteStepQueue.get is not exposed over the worker wire — query the server-side queue directly.",
    );
  }

  purge(): Promise<number> {
    throw new Error(
      "RemoteStepQueue.purge is not exposed over the worker wire — purge the server-side queue directly.",
    );
  }

  complete(params: StepQueueCompleteParams): Promise<boolean> {
    return this.call("complete", params);
  }

  fail(params: StepQueueFailParams): Promise<boolean> {
    return this.call("fail", params);
  }

  heartbeat(params: { taskId: string; claimToken?: string }): Promise<boolean> {
    return this.call("heartbeat", params);
  }

  // Requeueing stuck tasks is the coordinator's sweep (it knows which
  // workers are dead); a worker calling it could requeue everyone's tasks.
  requeueStuck(): Promise<StepQueueRequeueResult> {
    throw new Error(
      "RemoteStepQueue.requeueStuck is not exposed over the worker wire — the server-side coordinator sweeps stuck tasks.",
    );
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
