// ---------------------------------------------------------------------------
// ActivityJournalStorage — optional WorkflowStorage extension for .journaled() steps
//
// Journaled steps (.journaled() builder method) record each activity
// invocation as a journal entry, enabling intra-step replay after retry/crash.
// Storages opt in by also implementing this interface; .journaled() throws at
// build time if the configured storage doesn't support journaling.
//
// Mirrors the StepAttemptStorage pattern for consistency.
// ---------------------------------------------------------------------------

import type { WorkflowStorage } from "./workflow-storage.ts";

/**
 * One entry in a journaled step's activity log.
 *
 * Exit is a tagged union so failures serialize cleanly alongside successes.
 * Success values are codec-encoded (Phase 1: plain JSON; Phase 3: per-activity
 * Zod codecs). Errors are serialized to string in Phase 1; Phase 3 upgrades
 * to tagged error unions for type-safe rethrow on replay.
 */
export interface JournalEntry {
  readonly activityIndex: number;
  readonly activityName: string;
  readonly exit:
    | { readonly tag: "Success"; readonly value: unknown }
    | { readonly tag: "Failure"; readonly error: string };
  readonly createdAt: Date;
}

/**
 * Optional storage extension for `.journaled()` steps. Implementations persist
 * activity journal entries keyed by (workflowId, stepName, activityIndex) and
 * return them in index order on load.
 *
 * The engine detects this at runtime via `isActivityJournalStorage()`.
 */
export interface ActivityJournalStorage {
  /**
   * Load all journal entries for one journaled step of one workflow run,
   * ordered by `activityIndex` ascending. Returns empty array if no entries.
   */
  loadJournal(workflowId: string, stepName: string): Promise<JournalEntry[]>;

  /**
   * Append one journal entry. Idempotent on (workflowId, stepName, activityIndex):
   * re-inserting the same index is a no-op (the engine only appends after the
   * side effect completes, so at-most-once is the target; see Phase 3 for
   * two-phase record when at-least-once is unsafe).
   */
  appendEntry(params: {
    readonly workflowId: string;
    readonly stepName: string;
    readonly activityIndex: number;
    readonly activityName: string;
    readonly exit: JournalEntry["exit"];
  }): Promise<void>;
}

/** Runtime check for whether a storage implementation supports activity journaling. */
export function isActivityJournalStorage(
  storage: WorkflowStorage,
): storage is WorkflowStorage & ActivityJournalStorage {
  return (
    "loadJournal" in storage &&
    typeof (storage as { loadJournal?: unknown }).loadJournal === "function" &&
    "appendEntry" in storage &&
    typeof (storage as { appendEntry?: unknown }).appendEntry === "function"
  );
}
