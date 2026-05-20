// ---------------------------------------------------------------------------
// listPendingSignals — broader sibling of `listPendingApprovals`. Surfaces
// EVERY workflow currently suspended on `waiting_for_signal`, not just the
// agent loop's `approve:<callId>` pattern.
//
// `listPendingApprovals` is for the agent-tool inbox specifically; this
// helper is for an "everything pending external delivery" inbox the
// dashboard can show. When the signal happens to be `approve:<id>` the
// tool metadata (toolName / toolInput) is still surfaced — those rows
// are a strict superset of `listPendingApprovals`.
// ---------------------------------------------------------------------------

import type { ActivityJournalStorage, JournalEntry, WorkflowStorage } from "@promin/workflow";
import { isActivityJournalStorage } from "@promin/workflow";
import { parseApprovalSignal } from "./approve-signal.ts";

const APPROVAL_START_SUFFIX = "-start";

export interface PendingSignal {
  readonly workflowId: string;
  readonly workflowName: string;
  readonly namespace: string | undefined;
  /** Step suspended on the signal — pass to `loadJournal` / `deliverSignal`. */
  readonly stepName: string;
  /** Exact signal name the step is waiting for. */
  readonly signalName: string;
  /**
   * True when the signal name matches the tool-call approval convention
   * (`approve:<callId>`). Consumers should branch on this, NOT on the
   * prefix string — see `parseApprovalSignal` in `./approve-signal.ts`.
   */
  readonly isApproval: boolean;
  readonly suspendedAt: Date | undefined;
  /** Tool-call id parsed out of an `approve:<id>` signal — undefined otherwise. */
  readonly toolCallId?: string;
  /** Tool name (only set for `approve:` signals where the agent loop wrote it). */
  readonly toolName?: string;
  /** Tool input (only set for `approve:` signals where the agent loop wrote it). */
  readonly toolInput?: unknown;
}

export interface ListPendingSignalsParams {
  readonly namespace?: string;
  /** Cap on suspended workflows scanned. Default 200. */
  readonly limit?: number;
}

/**
 * Every workflow currently suspended on any signal, most-recent first.
 * One entry per workflow — when a workflow has multiple
 * `waiting_for_signal` steps the first one is returned (agent loops only
 * suspend on one at a time; generic workflows typically do too).
 */
export async function listPendingSignals(
  storage: WorkflowStorage,
  params: ListPendingSignalsParams = {},
): Promise<PendingSignal[]> {
  const limit = params.limit ?? 200;
  const suspended = await storage.listWorkflows({
    status: "suspended",
    ...(params.namespace !== undefined && { namespace: params.namespace }),
    limit,
    orderBy: "startedAt",
    orderDir: "desc",
  });

  const journalStorage = isActivityJournalStorage(storage) ? storage : undefined;

  const out: PendingSignal[] = [];
  for (const wf of suspended) {
    let waiting: { stepName: string; signalName: string; startedAt: Date | undefined } | null =
      null;
    for (const step of Object.values(wf.steps)) {
      if (step.status === "waiting_for_signal" && step.signalName !== undefined) {
        waiting = {
          stepName: step.stepName,
          signalName: step.signalName,
          startedAt: step.startedAt,
        };
        break;
      }
    }
    if (!waiting) continue;

    const parsed = parseApprovalSignal(waiting.signalName);
    const meta =
      parsed && journalStorage
        ? await readApprovalStartMetadata(
            journalStorage,
            wf.workflowId,
            waiting.stepName,
            parsed.toolCallId,
          )
        : { toolName: undefined, toolInput: undefined };

    out.push({
      workflowId: wf.workflowId,
      workflowName: wf.workflowName,
      namespace: wf.namespace,
      stepName: waiting.stepName,
      signalName: waiting.signalName,
      isApproval: parsed !== null,
      suspendedAt: waiting.startedAt,
      ...(parsed && { toolCallId: parsed.toolCallId }),
      ...(meta.toolName !== undefined && { toolName: meta.toolName }),
      ...(meta.toolInput !== undefined && { toolInput: meta.toolInput }),
    });
  }
  return out;
}

async function readApprovalStartMetadata(
  storage: ActivityJournalStorage,
  workflowId: string,
  stepName: string,
  toolCallId: string,
): Promise<{ toolName: string | undefined; toolInput: unknown }> {
  let entries: JournalEntry[];
  try {
    entries = await storage.loadJournal(workflowId, stepName);
  } catch {
    return { toolName: undefined, toolInput: undefined };
  }
  const startName = `-approval-${toolCallId}${APPROVAL_START_SUFFIX}`;
  const entry = entries.find(
    (e) => e.activityName.endsWith(startName) && e.phase !== "pending" && e.exit?.tag === "Success",
  );
  const value = entry?.exit && "value" in entry.exit ? entry.exit.value : undefined;
  if (!value || typeof value !== "object") {
    return { toolName: undefined, toolInput: undefined };
  }
  const { toolName, toolInput } = value as { toolName?: unknown; toolInput?: unknown };
  return {
    toolName: typeof toolName === "string" ? toolName : undefined,
    toolInput,
  };
}
