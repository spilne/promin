// ---------------------------------------------------------------------------
// Worker-protocol endpoints for the WorkflowStartQueue. Workers POST here
// to claim and complete pending start requests enqueued by the auto-trigger
// fn in ZoryaServer.
// ---------------------------------------------------------------------------

import { json, jsonError, readJson } from "../router.ts";
import type { WorkflowStartQueue } from "../workflow-starts.ts";

export function claimWorkflowStarts(queue: WorkflowStartQueue) {
  return async (req: Request): Promise<Response> => {
    const body = await readJson<{
      workflowNames?: string[];
      workerId?: string;
      limit?: number;
    }>(req);
    const workflowNames = body?.workflowNames ?? [];
    const limit = Math.min(Math.max(body?.limit ?? 10, 1), 100);
    if (workflowNames.length === 0) return json(200, { starts: [] });
    const starts = await queue.claim({ workflowNames, workerId: body?.workerId, limit });
    return json(200, { starts });
  };
}

export function completeWorkflowStart(queue: WorkflowStartQueue) {
  return async (_req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    if (!id) return jsonError(400, "missing_id");
    await queue.complete(id);
    return json(200, { ok: true });
  };
}

export function listWorkflowStarts(queue: WorkflowStartQueue) {
  return async (): Promise<Response> => {
    const starts = await queue.list();
    return json(200, { starts });
  };
}
