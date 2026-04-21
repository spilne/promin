// ---------------------------------------------------------------------------
// createWorkerApiHandler — server side of the distributed worker HTTP API.
//
// Wraps a StepQueue + WorkflowStorage into a fetch-compatible handler for
// cross-language workers. The handler accepts a single POST with a
// { method, params } body and dispatches to the matching queue / storage
// method. No routing, no middleware, no auth (put auth in front of it).
// ---------------------------------------------------------------------------

import type { StepQueue } from "@promin/workflow";
import type { WorkflowStorage } from "@promin/workflow";
import {
  WORKER_WIRE_CODEC,
  type WorkerMethod,
  type WorkerRpcRequest,
  type WorkerRpcResponse,
} from "./worker-wire.ts";

/**
 * Build a fetch-style handler that exposes `stepQueue` and `storage` to
 * remote workers. The returned function matches the `(req: Request) =>
 * Promise<Response>` signature that `Bun.serve`, `Deno.serve`, and most
 * edge runtimes accept.
 *
 * @example
 * ```ts
 * Bun.serve({
 *   port: 4001,
 *   fetch: createWorkerApiHandler({ stepQueue, storage }),
 * });
 * ```
 */
export function createWorkerApiHandler(config: {
  stepQueue: StepQueue;
  storage: WorkflowStorage;
}): (req: Request) => Promise<Response> {
  const { stepQueue, storage } = config;

  const dispatchers: Record<WorkerMethod, (params: any) => Promise<unknown>> = {
    claim: (p) =>
      stepQueue.claim({
        capabilities: p.capabilities,
        limit: p.limit,
        fairness: p.fairness,
      }),
    complete: (p) =>
      stepQueue.complete({
        taskId: p.taskId,
        result: p.result,
        durationMs: p.durationMs,
      }),
    fail: (p) =>
      stepQueue.fail({
        taskId: p.taskId,
        error: p.error,
        durationMs: p.durationMs,
      }),
    heartbeat: (p) => stepQueue.heartbeat({ taskId: p.taskId }),
    requeueStuck: (p) =>
      stepQueue.requeueStuck({
        claimedBy: p.claimedBy,
        staleTimeoutMs: p.staleTimeoutMs,
      }),
    saveStepResult: (p) => storage.saveStepResult(p),
    saveStepFailure: (p) => storage.saveStepFailure(p),
  };

  return async (req) => {
    if (req.method !== "POST") {
      return jsonResponse({ ok: false, error: `Only POST is supported, got ${req.method}` }, 405);
    }

    let envelope: WorkerRpcRequest;
    try {
      const raw = (await req.json()) as { method: string; params: unknown };
      envelope = {
        method: raw.method as WorkerMethod,
        params: WORKER_WIRE_CODEC.decode(raw.params),
      };
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
      return jsonResponse({ ok: true, result: WORKER_WIRE_CODEC.encode(result) }, 200);
    } catch (err) {
      return jsonResponse(
        { ok: false, error: err instanceof Error ? err.message : String(err) },
        500,
      );
    }
  };
}

function jsonResponse(body: WorkerRpcResponse, status: number): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}
