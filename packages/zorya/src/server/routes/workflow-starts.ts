// ---------------------------------------------------------------------------
// Worker-protocol endpoints for the WorkflowStartQueue. Workers that host a
// workflow claim its starts here, keep each claim alive while they drive the
// run, then complete it (the run ended or suspended) or release it (the run
// is locked elsewhere, the worker is stopping). Heartbeat, complete and
// release carry the claim token from the claim (JSON body `{ claimToken }`).
//
//   POST /api/worker-protocol/claim-starts            { workflowSpecs, workerId?, limit?, claimId? }
//   POST /api/worker-protocol/heartbeat-start/:id     { claimToken }           → { ok }
//   POST /api/worker-protocol/complete-start/:id      { claimToken }           → { ok }
//   POST /api/worker-protocol/release-start/:id       { claimToken, delayMs? } → { ok }
//   GET  /api/worker-protocol/starts?limit=           (debugging; oldest first, ≤ 500)
//
// A start names the run, not its input: the worker drives the stored run
// with the input stored on it.
// ---------------------------------------------------------------------------

import { z } from "zod";
import { json } from "../router.ts";
import { parseBody, parseParams, parseQuery } from "../request-validation.ts";
import {
  ClaimStartsRequestSchema,
  limitQuery,
  ReleaseStartRequestSchema,
  StartClaimRequestSchema,
  StartParamsSchema,
} from "../api-types.ts";
import type { WorkflowStartClaimRef, WorkflowStartQueue } from "@promin/workflow/distributed";

/**
 * `POST /api/worker-protocol/claim-starts`. `onClaimed` hears every claimed
 * start (the wake-ups stop following up on it).
 */
export function claimWorkflowStarts(
  queue: WorkflowStartQueue,
  onClaimed?: (starts: ReadonlyArray<{ readonly workflowId: string }>) => void,
) {
  return async (req: Request): Promise<Response> => {
    const { workflowSpecs, workerId, limit, claimId } = await parseBody({
      req,
      schema: ClaimStartsRequestSchema,
    });
    if (workflowSpecs.length === 0) return json(200, { starts: [] });
    const starts = await queue.claim({
      workflowSpecs,
      ...(workerId !== undefined && { workerId }),
      ...(claimId !== undefined && { claimId }),
      limit: limit ?? 10,
    });
    if (starts.length > 0) onClaimed?.(starts);
    return json(200, { starts });
  };
}

/** `POST /api/worker-protocol/heartbeat-start/:id` — `{ ok: false }` once the claim is lost. */
export function heartbeatWorkflowStart(queue: WorkflowStartQueue) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    return json(200, { ok: await queue.heartbeat(await readClaimRef({ req, params })) });
  };
}

/** `POST /api/worker-protocol/complete-start/:id` — `{ ok: false }` when the token is stale. */
export function completeWorkflowStart(queue: WorkflowStartQueue) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    return json(200, { ok: await queue.complete(await readClaimRef({ req, params })) });
  };
}

/** `POST /api/worker-protocol/release-start/:id` — `{ ok: false }` when the token is stale. */
export function releaseWorkflowStart(queue: WorkflowStartQueue) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const { id } = parseParams({ params, schema: StartParamsSchema });
    const body = await parseBody({ req, schema: ReleaseStartRequestSchema });
    const ok = await queue.release({
      id,
      claimToken: body.claimToken,
      ...(body.delayMs !== undefined && { delayMs: body.delayMs }),
    });
    return json(200, { ok });
  };
}

async function readClaimRef(args: {
  req: Request;
  params: Record<string, string>;
}): Promise<WorkflowStartClaimRef> {
  const { id } = parseParams({ params: args.params, schema: StartParamsSchema });
  const { claimToken } = await parseBody({ req: args.req, schema: StartClaimRequestSchema });
  return { id, claimToken };
}

const ListStartsQuerySchema = z.object({ limit: limitQuery({ fallback: 100 }) });

/** `GET /api/worker-protocol/starts?limit=` — the oldest `limit` starts (default 100, max 500). */
export function listWorkflowStarts(queue: WorkflowStartQueue) {
  return async (req: Request): Promise<Response> => {
    const { limit } = parseQuery({ req, schema: ListStartsQuerySchema });
    const starts = await queue.list({ limit });
    return json(200, { starts });
  };
}
