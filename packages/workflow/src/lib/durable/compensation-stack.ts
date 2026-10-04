// ---------------------------------------------------------------------------
// Compensation stack — what happens when a failure escapes a journaled body.
//
//   - `CompensationStack` collects the `compensate` callbacks of activities
//     that succeeded and, on a genuine failure, runs them in reverse, each
//     journaled as its own `compensation` entry.
//   - `runsCompensations` is the unwind policy: control-flow and
//     engine-integrity exits propagate without unwinding.
//   - `discardFailedAttempt` drops the recorded failures and rolled-back
//     activities so the next attempt of the step re-executes them.
// ---------------------------------------------------------------------------

import type { JournalStore, JournalExit, JournalSlot } from "./activity-journal.ts";
import { errorTag, failureExit } from "./journal-exit.ts";
import type { JournalCursor } from "./journal-cursor.ts";
import { journaledBodyScope } from "./journaled-body-scope.ts";
import type { FenceGuard } from "./workflow-storage.ts";

interface Compensation {
  /** Slot index reserved at registration time. */
  readonly activityIndex: number;
  /** Slot of the activity this compensation rolls back. */
  readonly sourceActivityIndex: number;
  /** Name of that activity, for the compensation row. */
  readonly activityName: string;
  readonly run: () => Promise<void>;
}

export class CompensationStack {
  private readonly compensations: Compensation[] = [];

  constructor(private readonly cursor: JournalCursor) {}

  /**
   * Register the rollback of a succeeded top-level activity. Reserves the
   * compensation's journal slot now, before any later yield, so activity
   * and compensation indices stay deterministic across replay; the unwind
   * writes its rows later.
   */
  register(params: {
    readonly sourceActivityIndex: number;
    readonly activityName: string;
    readonly compensate: () => void | Promise<void>;
  }): void {
    const { compensate } = params;
    this.compensations.push({
      activityIndex: this.cursor.reserveTopLevelIndex(),
      sourceActivityIndex: params.sourceActivityIndex,
      activityName: params.activityName,
      run: async () => {
        // Run outside the body scope so Date.now / random inside
        // compensations aren't flagged by instrumentNonDeterminism.
        await journaledBodyScope.exit(async () => {
          await Promise.resolve(compensate());
        });
      },
    });
  }

  /**
   * Run every registered compensation in reverse. Each runs through the
   * two-phase journal record with stepType="compensation", so a crash
   * during unwind replays cleanly — completed compensations are skipped.
   *
   * Compensation failures do NOT halt the unwind — the engine records the
   * failure in the journal and continues with the remaining compensations.
   * The caller rethrows the original body error after.
   *
   * Returns the slots of every rolled-back activity together with its
   * completed compensation, so the caller can discard them and the next
   * step attempt re-executes the activity.
   */
  async unwind(): Promise<JournalSlot[]> {
    const rolledBack: JournalSlot[] = [];
    const markRolledBack = (comp: Compensation): void => {
      rolledBack.push(
        { activityIndex: comp.sourceActivityIndex, branchPath: "" },
        { activityIndex: comp.activityIndex, branchPath: "" },
      );
    };
    for (let i = this.compensations.length - 1; i >= 0; i--) {
      const comp = this.compensations[i]!;
      const slot: JournalSlot = { activityIndex: comp.activityIndex, branchPath: "" };

      // Replay: a completed row means the previous worker finished it — skip.
      const recorded = this.cursor.recorded(slot);
      if (recorded && (recorded.phase ?? "completed") === "completed") {
        if (recorded.exit?.tag === "Success") markRolledBack(comp);
        continue;
      }

      try {
        await this.cursor.writePending({
          slot,
          kind: "compensation",
          name: `compensation:${comp.activityName}`,
        });
      } catch {
        // Journal unreachable — nothing to do.
        continue;
      }
      let exit: JournalExit;
      try {
        await comp.run();
        exit = { tag: "Success", value: null };
      } catch (err) {
        exit = failureExit(err);
      }
      try {
        await this.cursor.complete({ slot, exit });
      } catch {
        // Journal unreachable — give up on this one, continue the unwind.
        continue;
      }
      if (exit.tag === "Success") markRolledBack(comp);
    }
    return rolledBack;
  }
}

/**
 * Exits that are NOT business failures and so must not trigger the
 * intra-step compensation unwind. Matched by `_tag` so an error thrown from
 * another copy of this module is still recognised.
 *
 *  - `WorkflowSuspendedError`: ctx.sleep / ctx.signal parked the workflow.
 *  - `WorkflowContinueAsNewError`: a clean restart, not a rollback.
 *  - `WorkflowTripwireError`: an intentional early end.
 *  - `JournalNonDeterminismError`: code drifted from the journal; rolling
 *    back completed work would turn a deploy problem into data loss.
 *  - `AmbiguousActivityOutcome`: the workflow halts so an operator can
 *    inspect the external system before anything else runs.
 *  - `WorkflowLockError` / `FenceTokenMismatchError` /
 *    `WorkflowLockLostError`: this worker lost the workflow; the new owner
 *    re-drives it from storage.
 *  - `CheckpointError`: a durable write failed past its retries; recovery
 *    re-drives the workflow.
 */
const NON_COMPENSATING_EXITS: ReadonlySet<string> = new Set([
  "WorkflowSuspendedError",
  "WorkflowContinueAsNewError",
  "WorkflowTripwireError",
  "JournalNonDeterminismError",
  "AmbiguousActivityOutcome",
  "WorkflowLockError",
  "FenceTokenMismatchError",
  "WorkflowLockLostError",
  "CheckpointError",
]);

/** Whether a body error is a genuine failure that unwinds compensations. */
export function runsCompensations(bodyError: unknown): boolean {
  const tag = errorTag(bodyError);
  return !(tag !== undefined && NON_COMPENSATING_EXITS.has(tag));
}

/**
 * A failure escaped the body, so this step attempt is over. Drop what the
 * next attempt must re-execute rather than replay: recorded failures of
 * activities, children and compensations, and every rolled-back activity
 * with its compensation row. Best effort: if the journal can't be read or
 * written, the next attempt replays the failure and discards it then.
 */
export async function discardFailedAttempt(params: {
  storage: JournalStore;
  workflowId: string;
  stepName: string;
  rolledBack: readonly JournalSlot[];
  guard?: FenceGuard;
}): Promise<void> {
  const { storage, workflowId, stepName, rolledBack, guard } = params;
  try {
    const journal = await storage.loadJournal({ workflowId, stepName });
    const slots = new Map<string, JournalSlot>();
    const add = (slot: JournalSlot): void => {
      slots.set(`${slot.activityIndex}:${slot.branchPath}`, slot);
    };
    for (const entry of journal) {
      const type = entry.stepType ?? "activity";
      if (type !== "activity" && type !== "child" && type !== "compensation") continue;
      if ((entry.phase ?? "completed") !== "completed" || entry.exit?.tag !== "Failure") continue;
      add({ activityIndex: entry.activityIndex, branchPath: entry.branchPath });
    }
    for (const slot of rolledBack) add(slot);
    if (slots.size === 0) return;
    await storage.discardJournalEntries({
      workflowId,
      stepName,
      slots: [...slots.values()],
      guard,
    });
  } catch {
    // Journal unreachable: the body error is what the caller needs to see.
  }
}
