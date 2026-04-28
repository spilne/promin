// ---------------------------------------------------------------------------
// listPendingApprovals — read-only helper over WorkflowStorage primitives
// that surfaces every approval the agent loop is currently suspended on.
//
// Why a helper, not a parallel storage?
// -------------------------------------
// The workflow journal already holds everything we need: a suspended
// workflow has a step (the agent loop's `conversation`) waiting on
// `signalName: "approve:<callId>"`, and the lifecycle activity that
// runs right before suspension records `{ toolName, toolInput }` as its
// journal entry exit. The helper joins those facts. No parallel side-
// table to keep in sync, no eventual-consistency story to manage.
//
// This is the corrected scope of an earlier `ApprovalStorage` attempt
// (reverted in 77877121) — the journal was already authoritative; we
// just needed a query.
//
// Operator UIs / dashboards / inbox pages call this. Decisions still go
// through `WorkflowStorage.deliverSignal` (or the loop's `approve` /
// `reject` shortcuts) — the helper is read-only.
//
// Storage capability
// ------------------
// `loadJournal` is required to surface `toolName` / `toolInput` (they
// live in the activity journal, not in `wf.steps`). When the storage
// doesn't implement `ActivityJournalStorage`, the helper still returns
// rows but with `toolName`/`toolInput` as `undefined`.
//
// Not exhaustive: only surfaces approvals waiting on the **default
// signal** path (`requireApproval: true` without a custom
// `onApprovalRequired` hook). A hook turns the gate into an inline
// activity — there's no suspension and no inbox row.
// ---------------------------------------------------------------------------

import type {
  ActivityJournalStorage,
  JournalEntry,
  StepState,
  WorkflowState,
  WorkflowStorage,
} from "@promin/workflow";
import { isActivityJournalStorage } from "@promin/workflow";

const APPROVE_SIGNAL_PREFIX = "approve:";
const APPROVAL_START_SUFFIX = "-start";

export interface PendingApproval {
  /** Workflow id — pass to `deliverSignal` / `cancelWorkflow` etc. */
  readonly workflowId: string;
  /** Tool call id — used as the signal key (`approve:<id>`). */
  readonly toolCallId: string;
  /** Tenant scope from the workflow row, when set. */
  readonly namespace: string | undefined;
  /**
   * Tool name — pulled from the lc-start activity's journal exit value.
   * `undefined` if the journal isn't accessible (no
   * `ActivityJournalStorage`) or the entry is missing.
   */
  readonly toolName: string | undefined;
  /** Tool input — pulled from the lc-start activity's journal exit value. */
  readonly toolInput: unknown;
  /**
   * When the suspending step started — proxy for "request received at".
   * `undefined` when the step hasn't recorded a startedAt (fresh
   * fixtures, exotic backends). Use this to age-rank the inbox.
   */
  readonly suspendedAt: Date | undefined;
  /** Workflow display name — useful for inbox grouping. */
  readonly workflowName: string;
  /** Step that's suspended on the approve signal — pass to `loadJournal` etc. */
  readonly stepName: string;
}

export interface ListPendingApprovalsParams {
  /** Restrict to a tenant. Empty / undefined = all namespaces. */
  readonly namespace?: string;
  /**
   * Cap the number of suspended workflows scanned. Default: 200.
   * Each workflow contributes at most one pending approval row in
   * practice — agent loops suspend on a single signal at a time —
   * so this also bounds the result size.
   */
  readonly limit?: number;
}

/**
 * Returns every pending approval the agent loop is suspended on,
 * ordered most-recent-suspension first.
 */
export async function listPendingApprovals(
  storage: WorkflowStorage,
  params: ListPendingApprovalsParams = {},
): Promise<PendingApproval[]> {
  const limit = params.limit ?? 200;
  const suspended = await storage.listWorkflows({
    status: "suspended",
    ...(params.namespace !== undefined && { namespace: params.namespace }),
    limit,
    orderBy: "startedAt",
    orderDir: "desc",
  });

  const journalStorage = isActivityJournalStorage(storage) ? storage : undefined;

  const out: PendingApproval[] = [];
  for (const wf of suspended) {
    const found = findSuspendedApprovalStep(wf);
    if (!found) continue;
    const { step, toolCallId } = found;

    const meta = journalStorage
      ? await readApprovalStartMetadata(journalStorage, wf.workflowId, step.stepName, toolCallId)
      : { toolName: undefined, toolInput: undefined };

    out.push({
      workflowId: wf.workflowId,
      toolCallId,
      namespace: wf.namespace,
      toolName: meta.toolName,
      toolInput: meta.toolInput,
      suspendedAt: step.startedAt,
      workflowName: wf.workflowName,
      stepName: step.stepName,
    });
  }
  return out;
}

function findSuspendedApprovalStep(
  wf: WorkflowState,
): { step: StepState; toolCallId: string } | null {
  for (const step of Object.values(wf.steps)) {
    if (
      step.status === "waiting_for_signal" &&
      step.signalName !== undefined &&
      step.signalName.startsWith(APPROVE_SIGNAL_PREFIX)
    ) {
      const toolCallId = step.signalName.slice(APPROVE_SIGNAL_PREFIX.length);
      if (toolCallId.length > 0) return { step, toolCallId };
    }
  }
  return null;
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
