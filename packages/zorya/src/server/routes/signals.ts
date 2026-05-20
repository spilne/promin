// ---------------------------------------------------------------------------
// Signals route — /api/signals
//
// Read-only inbox of every workflow currently suspended on `waiting_for_signal`,
// regardless of signal name. Superset of /api/approvals: agent-tool
// `approve:<callId>` rows are included (with their toolName/toolInput
// populated when an agent loop wrote the journal metadata), plus any
// other workflow suspension waiting for a custom-named signal.
//
// Decisions still flow through `WorkflowStorage.deliverSignal` (or the
// agent loop's `approve` / `reject` shortcuts). This view is the inbox.
// ---------------------------------------------------------------------------

import { listPendingSignals } from "@promin/agent";
import type { WorkflowStorage } from "@promin/workflow";
import { json } from "../router.ts";

export interface SignalDto {
  workflowId: string;
  workflowName: string;
  namespace: string | null;
  /** Step suspended on the signal. */
  stepName: string;
  /** Exact signal name the step is waiting for (e.g. `approve:<callId>`). */
  signalName: string;
  /**
   * True when the signal follows the tool-call approval convention.
   * Computed server-side via `parseApprovalSignal` so the wire-format
   * prefix never crosses the bundle boundary — UI keys its shortcut
   * Approve / Reject buttons off this discriminator, not a string check.
   */
  isApproval: boolean;
  /** ISO timestamp when the suspending step started; null when unknown. */
  suspendedAt: string | null;
  /** Parsed `<callId>` portion of an `approve:` signal. */
  toolCallId?: string;
  /** Tool name (only populated when the agent loop wrote it). */
  toolName?: string;
  /** Tool input (only populated when the agent loop wrote it). */
  toolInput?: unknown;
}

export interface SignalsResponse {
  signals: SignalDto[];
  total: number;
}

export function listSignals(storage: WorkflowStorage) {
  return async (req: Request) => {
    const url = new URL(req.url);
    const namespace = url.searchParams.get("namespace") ?? undefined;
    const limitRaw = url.searchParams.get("limit");
    const limit = limitRaw ? Number(limitRaw) : undefined;

    const pending = await listPendingSignals(storage, {
      ...(namespace ? { namespace } : {}),
      ...(limit && Number.isFinite(limit) && limit > 0 ? { limit } : {}),
    });

    const signals: SignalDto[] = pending.map((p) => ({
      workflowId: p.workflowId,
      workflowName: p.workflowName,
      namespace: p.namespace ?? null,
      stepName: p.stepName,
      signalName: p.signalName,
      isApproval: p.isApproval,
      suspendedAt: p.suspendedAt ? p.suspendedAt.toISOString() : null,
      ...(p.toolCallId !== undefined && { toolCallId: p.toolCallId }),
      ...(p.toolName !== undefined && { toolName: p.toolName }),
      ...(p.toolInput !== undefined && { toolInput: p.toolInput }),
    }));

    const body: SignalsResponse = { signals, total: signals.length };
    return json(200, body);
  };
}
