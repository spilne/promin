// ---------------------------------------------------------------------------
// Journal wakeups — complete a suspended journaled step's pending entry from
// outside its body: a signal delivery (`completeSignal`) or the sleep
// scanner (`completeDueSleeps`). The next run of the step replays the
// completed entry and continues past the wait.
// ---------------------------------------------------------------------------

import type { ActivityJournalStorage, CompletePendingResult } from "./activity-journal.ts";
import { deliveredSignalExitValue } from "./journal-exit.ts";

/**
 * Deliver a signal value to a workflow awaiting it via `ctx.signal(name)`.
 * Finds the matching pending journal entry and completes it with the given
 * value. Subsequent replay of the journaled step unblocks at the signal and
 * continues.
 *
 * This only completes the entry: the caller re-runs the workflow afterwards
 * (the `SignalScanner` does so for signals delivered through storage).
 *
 * Returns `true` if this call completed the pending entry; `false` if no
 * matching pending signal exists (already delivered, never registered) or
 * the entry was completed concurrently — by another delivery, or by the
 * body recording the signal's timeout. On `false` the value was not
 * delivered and the workflow will not see it.
 */
export async function completeSignal(params: {
  storage: ActivityJournalStorage;
  workflowId: string;
  stepName: string;
  signalName: string;
  value: unknown;
}): Promise<boolean> {
  const hit = await params.storage.findPendingSignal({
    workflowId: params.workflowId,
    stepName: params.stepName,
    signalName: params.signalName,
  });
  if (!hit) return false;

  const result: CompletePendingResult | undefined = await params.storage.completePendingEntry({
    workflowId: params.workflowId,
    stepName: params.stepName,
    activityIndex: hit.activityIndex,
    branchPath: hit.branchPath,
    exit: { tag: "Success", value: deliveredSignalExitValue(params.value) },
  });
  // A storage written before `CompletePendingResult` reports nothing; keep
  // its old answer.
  return result?.completed ?? true;
}

/**
 * Scanner hook — complete all due sleeps up to `limit`. Returns the
 * completed entries so a caller (or test) can re-enqueue the workflows.
 *
 * Usage:
 * ```ts
 * const due = await completeDueSleeps({ storage, now, limit: 100 });
 * for (const { workflowId } of due) {
 *   await workflow.run({ workflowId }); // re-drive to consume completion
 * }
 * ```
 */
export async function completeDueSleeps(params: {
  storage: ActivityJournalStorage;
  now: Date;
  limit: number;
}): Promise<
  Array<{
    workflowId: string;
    stepName: string;
    activityIndex: number;
    branchPath: string;
    wakeAt: Date;
  }>
> {
  const due = await params.storage.findDueSleeps({
    now: params.now,
    limit: params.limit,
  });
  for (const entry of due) {
    await params.storage.completePendingEntry({
      workflowId: entry.workflowId,
      stepName: entry.stepName,
      activityIndex: entry.activityIndex,
      branchPath: entry.branchPath,
      // Store as ISO string for consistent JSON roundtrip; the generator
      // hydrates back to Date on replay.
      exit: { tag: "Success", value: entry.wakeAt.toISOString() },
    });
  }
  return due;
}
