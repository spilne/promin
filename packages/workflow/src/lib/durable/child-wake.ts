// ---------------------------------------------------------------------------
// Child wake — how a parent parked on a suspended child workflow is woken
// when that child ends.
//
// A parent step whose child (journaled `ctx.child` or `.subworkflow()`)
// suspends parks as a signal wait on a reserved per-child signal name, with
// the child's earliest wake time (its sleep or signal deadline) as the wait's
// timeout. When the child run ends (completed, failed, tripwire, cancelled),
// its runner delivers that signal to the parent. Both halves are ordinary
// storage writes, so the existing scanners resume the parent from any
// process: the sleep scanner when the child's own wake time is due, the
// signal scanner once the child has ended. A delivered signal is kept, so a
// child that ends before its parent has finished parking still wakes it.
//
// The signal name carries the child's run number: a failed child re-run
// from scratch for a retried parent step is a new run, and the earlier
// run's delivered signal must not wake a parent waiting on the new one.
// ---------------------------------------------------------------------------

import { WorkflowSuspendedError } from "./durable-pipeline-error.ts";
import { isTerminalWorkflowStatus, type WorkflowState } from "./workflow-state.ts";
import type { FenceGuard, WorkflowStorage } from "./workflow-storage.ts";

/** Prefix of the reserved signal names a parent waits on for its children. */
export const CHILD_ENDED_SIGNAL_PREFIX = "workflow.child-ended:";

/** The signal a parent waits on for run `run` of child `childWorkflowId`. */
export function childEndedSignalName(params: {
  readonly childWorkflowId: string;
  readonly run: number;
}): string {
  return `${CHILD_ENDED_SIGNAL_PREFIX}${params.childWorkflowId}#${params.run}`;
}

/**
 * Park step `stepName` of `workflowId` on its suspended child: a signal
 * wait on the child's ended-signal, timing out at the child's earliest wake
 * time (a sleeping step's `wakeAt` or a signal wait's deadline). Returns the
 * parent's `WorkflowSuspendedError`.
 */
export async function suspendOnChild(params: {
  readonly storage: WorkflowStorage;
  readonly workflowId: string;
  readonly stepName: string;
  readonly childWorkflowId: string;
  /** The child's `WorkflowSuspendedError`. */
  readonly childError: unknown;
  readonly guard?: FenceGuard | undefined;
}): Promise<WorkflowSuspendedError> {
  const { storage, workflowId, stepName, childWorkflowId, guard } = params;
  const reason =
    (params.childError as { reason?: unknown }).reason === "sleep" ? "sleep" : "signal";
  const child = await storage.loadWorkflow(childWorkflowId);
  const wakeAt = child ? earliestWakeOf(child) : undefined;
  await storage.suspendWorkflow({
    workflowId,
    stepName,
    stepUpdate: {
      status: "waiting_for_signal",
      signalName: childEndedSignalName({ childWorkflowId, run: child?.run ?? 1 }),
      ...(wakeAt && { signalTimeoutAt: wakeAt }),
    },
    guard,
  });
  return new WorkflowSuspendedError({
    workflowId,
    stepName,
    reason,
    message:
      `waiting for child workflow "${childWorkflowId}"` +
      (wakeAt ? ` (wakes at ${wakeAt.toISOString()})` : ""),
  });
}

/**
 * Wake the parent of `state` when the run has ended: deliver the parent the
 * child's ended-signal. A run without a parent, or one that has not ended,
 * is left alone. A parent that is not waiting on this child only gains a
 * stored signal that nothing waits on.
 */
export async function wakeParentOfEndedRun(params: {
  readonly storage: WorkflowStorage;
  readonly state: Pick<WorkflowState, "workflowId" | "parentWorkflowId" | "run" | "status">;
}): Promise<void> {
  const { storage, state } = params;
  if (state.parentWorkflowId === undefined) return;
  if (!isTerminalWorkflowStatus(state.status)) return;
  await storage.deliverSignal({
    workflowId: state.parentWorkflowId,
    signalName: childEndedSignalName({ childWorkflowId: state.workflowId, run: state.run }),
    payload: null,
  });
}

/** The child's earliest sleep wake time or signal deadline. */
function earliestWakeOf(child: WorkflowState): Date | undefined {
  let wakeAt: Date | undefined;
  for (const step of Object.values(child.steps)) {
    const at =
      step.status === "sleeping"
        ? step.wakeAt
        : step.status === "waiting_for_signal"
          ? step.signalTimeoutAt
          : undefined;
    if (at && (!wakeAt || at.getTime() < wakeAt.getTime())) wakeAt = at;
  }
  return wakeAt;
}
