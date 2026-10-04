// ---------------------------------------------------------------------------
// Worker-protocol endpoints for the WorkflowStartQueue. Workers POST here
// to claim, heartbeat and complete pending start requests enqueued by the
// auto-trigger fn in ZoryaServer. Heartbeat and complete carry the claim
// token from the claim (JSON body `{ claimToken }`).
// ---------------------------------------------------------------------------

import { json, jsonError, readJson } from "../router.ts";
import type {
  WorkerWorkflowSpec,
  WorkflowStartClaimRef,
  WorkflowStartQueue,
} from "../workflow-starts.ts";

export function claimWorkflowStarts(queue: WorkflowStartQueue) {
  return async (req: Request): Promise<Response> => {
    const body = await readJson<{
      workflowSpecs?: WorkerWorkflowSpec[];
      workerId?: string;
      limit?: number;
    }>(req);
    const limit = Math.min(Math.max(body?.limit ?? 10, 1), 100);
    const specs: WorkerWorkflowSpec[] = body?.workflowSpecs ?? [];
    if (specs.length === 0) return json(200, { starts: [] });
    const starts = await queue.claim({ workflowSpecs: specs, workerId: body?.workerId, limit });
    return json(200, { starts });
  };
}

/** `POST /api/worker-protocol/heartbeat-start/:id` — `{ ok: false }` once the claim is lost. */
export function heartbeatWorkflowStart(queue: WorkflowStartQueue) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const ref = await readClaimRef({ req, params });
    if ("error" in ref) return ref.error;
    const ok = await queue.heartbeat(ref);
    return json(200, { ok });
  };
}

/** `POST /api/worker-protocol/complete-start/:id` — `{ ok: false }` when the token is stale. */
export function completeWorkflowStart(queue: WorkflowStartQueue) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const ref = await readClaimRef({ req, params });
    if ("error" in ref) return ref.error;
    const ok = await queue.complete(ref);
    return json(200, { ok });
  };
}

async function readClaimRef(args: {
  req: Request;
  params: Record<string, string>;
}): Promise<WorkflowStartClaimRef | { error: Response }> {
  const id = args.params.id;
  if (!id) return { error: jsonError(400, "missing_id") };
  const body = await readJson<{ claimToken?: unknown }>(args.req);
  const claimToken = body?.claimToken;
  if (typeof claimToken !== "string" || claimToken.length === 0) {
    return { error: jsonError(400, "missing_claim_token") };
  }
  return { id, claimToken };
}

export function listWorkflowStarts(queue: WorkflowStartQueue) {
  return async (): Promise<Response> => {
    const starts = await queue.list();
    return json(200, { starts });
  };
}
