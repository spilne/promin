// ---------------------------------------------------------------------------
// Approvals route — /api/approvals
//
// Read-only inbox over `listPendingApprovals` (in @promin/agent). Surfaces
// every workflow currently suspended on `approve:<callId>` so operators
// can triage pending requests from the dashboard. Decisions still flow
// through the agent loop's signal machinery — the route is read-only
// for now; resume wiring is a follow-up.
// ---------------------------------------------------------------------------

import { listPendingApprovals } from "@promin/agent";
import type { WorkflowStorage } from "@promin/workflow";
import { json } from "../router.ts";

export interface ApprovalDto {
  workflowId: string;
  toolCallId: string;
  namespace: string | null;
  toolName: string | null;
  toolInput: unknown;
  /** ISO timestamp when the suspending step started; null when unknown. */
  suspendedAt: string | null;
  workflowName: string;
  /** Step name suspended on the approve signal — operator UI can drill in. */
  stepName: string;
}

export interface ApprovalsResponse {
  approvals: ApprovalDto[];
  total: number;
}

export function listApprovals(storage: WorkflowStorage) {
  return async (req: Request) => {
    const url = new URL(req.url);
    const namespace = url.searchParams.get("namespace") ?? undefined;
    const limitRaw = url.searchParams.get("limit");
    const limit = limitRaw ? Number(limitRaw) : undefined;

    const pending = await listPendingApprovals(storage, {
      ...(namespace ? { namespace } : {}),
      ...(limit && Number.isFinite(limit) && limit > 0 ? { limit } : {}),
    });

    const approvals: ApprovalDto[] = pending.map((p) => ({
      workflowId: p.workflowId,
      toolCallId: p.toolCallId,
      namespace: p.namespace ?? null,
      toolName: p.toolName ?? null,
      toolInput: p.toolInput,
      suspendedAt: p.suspendedAt ? p.suspendedAt.toISOString() : null,
      workflowName: p.workflowName,
      stepName: p.stepName,
    }));

    const body: ApprovalsResponse = { approvals, total: approvals.length };
    return json(200, body);
  };
}
