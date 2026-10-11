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
import { z } from "zod";
import { json } from "../router.ts";
import { NameSchema, limitQuery } from "../api-types.ts";
import { parseQuery } from "../request-validation.ts";

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
  /**
   * JSON Schema snapshot the suspend point waited on (from
   * `ctx.validatedSignal` / `ctx.approval`). Undefined for plain
   * `ctx.signal` callers — UI falls back to the generic JSON editor in
   * that case. The dashboard form renderer + the public share page key
   * schema-driven form rendering off this field.
   */
  jsonSchema?: unknown;
  /** Parsed `<callId>` portion of an `approve:` signal. */
  toolCallId?: string;
  /** Tool name (only populated when the agent loop wrote it). */
  toolName?: string;
  /** Tool input (only populated when the agent loop wrote it). */
  toolInput?: unknown;
  /**
   * ISO time a value for this signal was delivered and is waiting to be
   * consumed — the run resumes on the signal scanner's next pass. Absent
   * while nothing has been delivered.
   */
  deliveredAt?: string;
}

export interface SignalsResponse {
  signals: SignalDto[];
  /** Number of signal waits found among the `limit` most recent suspended runs scanned. */
  total: number;
}

/** Query of `GET /api/signals`. `limit` caps the suspended runs scanned (default 200). */
export const SignalsQuerySchema = z.object({
  namespace: z.preprocess((v) => (v === "" ? undefined : v), NameSchema.optional()),
  limit: limitQuery({ fallback: 200 }),
});

export function listSignals(storage: WorkflowStorage) {
  return async (req: Request) => {
    const { namespace, limit } = parseQuery({ req, schema: SignalsQuerySchema });

    const pending = await listPendingSignals(storage, {
      ...(namespace !== undefined && { namespace }),
      limit,
    });

    // A value already delivered but not yet consumed: the run resumes on
    // the scanner's next pass. Shown so a second operator doesn't decide
    // again (a later delivery would replace it — delivery is last-wins).
    const delivered = await Promise.all(
      pending.map(async (p) => {
        const signals = await storage.loadSignals(p.workflowId);
        return signals.find((s) => s.signalName === p.signalName)?.deliveredAt;
      }),
    );

    const signals: SignalDto[] = pending.map((p, i) => ({
      workflowId: p.workflowId,
      workflowName: p.workflowName,
      namespace: p.namespace ?? null,
      stepName: p.stepName,
      signalName: p.signalName,
      isApproval: p.isApproval,
      suspendedAt: p.suspendedAt ? p.suspendedAt.toISOString() : null,
      ...(p.signalJsonSchema !== undefined && { jsonSchema: p.signalJsonSchema }),
      ...(p.toolCallId !== undefined && { toolCallId: p.toolCallId }),
      ...(p.toolName !== undefined && { toolName: p.toolName }),
      ...(p.toolInput !== undefined && { toolInput: p.toolInput }),
      ...(delivered[i] !== undefined && { deliveredAt: delivered[i]!.toISOString() }),
    }));

    const body: SignalsResponse = { signals, total: signals.length };
    return json(200, body);
  };
}
