// ---------------------------------------------------------------------------
// RemoteWorkerRegistry — client-side WorkerRegistry over HTTP. Forwards
// register/heartbeat/drain/deregister/list/detectDead to a server running
// `createWorkerApiHandler` configured with a real WorkerRegistry.
// ---------------------------------------------------------------------------

import type { WorkerInfo, WorkerRegistry } from "@promin/workflow";
import type { FetchLike } from "./remote-workflow-storage.ts";
import { WORKER_WIRE_CODEC, type WorkerMethod, type WorkerRpcResponse } from "./worker-wire.ts";

export interface RemoteWorkerRegistryConfig {
  readonly url: string;
  readonly fetch?: FetchLike;
  readonly headers?: Record<string, string>;
}

export class RemoteWorkerRegistry implements WorkerRegistry {
  private readonly url: string;
  private readonly fetch: FetchLike;
  private readonly headers: Record<string, string>;

  constructor(config: RemoteWorkerRegistryConfig) {
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
        `RemoteWorkerRegistry: invalid response from ${this.url} (status ${res.status}): ${(err as Error).message}`,
      );
    }

    if (!envelope.ok) throw new Error(envelope.error);
    return WORKER_WIRE_CODEC.decode(envelope.result) as T;
  }

  register(params: {
    workerId: string;
    capabilities: readonly string[];
    concurrency: number;
    metadata?: Record<string, unknown>;
  }): Promise<void> {
    return this.call("registerWorker", params);
  }

  heartbeat(workerId: string): Promise<void> {
    return this.call("heartbeatWorker", { workerId });
  }

  drain(workerId: string): Promise<void> {
    return this.call("drainWorker", { workerId });
  }

  deregister(workerId: string): Promise<void> {
    return this.call("deregisterWorker", { workerId });
  }

  list(params?: { status?: "active" | "draining" | "dead" }): Promise<WorkerInfo[]> {
    return this.call<WorkerInfo[]>("listWorkers", { status: params?.status });
  }

  detectDead(timeoutMs: number): Promise<WorkerInfo[]> {
    return this.call<WorkerInfo[]>("detectDeadWorkers", { timeoutMs });
  }
}
