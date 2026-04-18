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

/** What kind of checkpoint an entry records. Used by replay + the sleep scanner. */
export type JournalStepType = "activity" | "sleep" | "signal" | "compensation";

/** Lifecycle phase of an entry. `pending` means suspend is in flight (sleep wake or signal delivery). */
export type JournalPhase = "pending" | "completed";

/**
 * One entry in a journaled step's activity log.
 *
 * Exit is a tagged union so failures serialize cleanly alongside successes.
 * `pending` entries have no exit yet — they're placeholders while the
 * workflow is suspended (sleep waiting for wake time, signal waiting for
 * delivery). `completed` entries have an exit and are the source of truth on
 * replay.
 *
 * Success values are codec-encoded (currently plain JSON; per-activity
 * Zod codecs are a planned follow-up). Errors serialize to string today;
 * tagged error unions for type-safe rethrow are a future refinement.
 */
export interface JournalEntry {
  readonly activityIndex: number;
  /**
   * Branch path inside a `ctx.parallel` tree. Empty string `""` means "at
   * top-level in the body" — that's the value every pre-parallel workflow
   * journal already has, so old data works unchanged. Parallel branches get
   * paths like `"0"`, `"1"`, `"2.3"` for nested parallels. The journal
   * (workflow, step, activity_index, branch_path) quadruple is unique.
   */
  readonly branchPath: string;
  readonly activityName: string;
  /** Default `"activity"` preserves backward compat for entries without stepType. */
  readonly stepType?: JournalStepType;
  /** Default `"completed"` preserves backward compat for entries without phase. */
  readonly phase?: JournalPhase;
  /** Exit is set once the entry reaches `completed` phase. `undefined` while `pending`. */
  readonly exit?:
    | { readonly tag: "Success"; readonly value: unknown }
    | { readonly tag: "Failure"; readonly error: string };
  /** For `sleep` entries: when the workflow should wake. Null for other types. */
  readonly wakeAt?: Date;
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
   * Append one journal entry. Idempotent on
   * `(workflowId, stepName, activityIndex, branchPath)`: re-inserting the
   * same quadruple is a no-op (the engine only appends after the side
   * effect completes, so at-most-once is the target). `branchPath` defaults
   * to `""` for backwards compatibility with callers that don't use
   * `ctx.parallel`.
   */
  appendEntry(params: {
    readonly workflowId: string;
    readonly stepName: string;
    readonly activityIndex: number;
    readonly branchPath?: string;
    readonly activityName: string;
    readonly exit: NonNullable<JournalEntry["exit"]>;
  }): Promise<void>;
}

// ---------------------------------------------------------------------------
// JournaledSuspendStorage — extension for ctx.sleep / ctx.signal
//
// Strict superset of ActivityJournalStorage. Storages opt in by implementing
// these four methods; the engine type-guards at first use of ctx.sleep /
// ctx.signal and throws a loud error if the configured storage doesn't
// support durable suspend/resume.
// ---------------------------------------------------------------------------

/** Optional extension for journaled steps that use `ctx.sleep` or `ctx.signal`. */
export interface JournaledSuspendStorage extends ActivityJournalStorage {
  /**
   * Append a `pending` entry — used by `ctx.sleep` / `ctx.signal` when a
   * journaled step suspends, by `ctx.activity` for the two-phase record
   * (pending row written before the side effect, completed after), and by
   * the intra-step compensation unwind for each rollback callback. For
   * sleep: carries `wakeAt`. For signal / activity / compensation: the name
   * lives in `activityName`. Idempotent on PK.
   */
  appendPendingEntry(params: {
    readonly workflowId: string;
    readonly stepName: string;
    readonly activityIndex: number;
    readonly branchPath?: string;
    readonly activityName: string;
    readonly stepType: "sleep" | "signal" | "activity" | "compensation";
    readonly wakeAt?: Date;
  }): Promise<void>;

  /**
   * Transition a `pending` entry to `completed`. Used by the sleep scanner
   * (for sleep entries, exit = `{ tag: "Success", value: actual wake time }`)
   * and by `completeSignal` (for signal entries, exit carries the delivered
   * value). No-op if already completed (idempotent on repeated delivery).
   * `branchPath` defaults to `""` for non-parallel entries.
   */
  completePendingEntry(params: {
    readonly workflowId: string;
    readonly stepName: string;
    readonly activityIndex: number;
    readonly branchPath?: string;
    readonly exit: NonNullable<JournalEntry["exit"]>;
  }): Promise<void>;

  /**
   * Scanner hook — return pending sleep entries whose `wakeAt <= now`, up to
   * `limit`. Backends use an index on `(wakeAt) WHERE step_type='sleep' AND phase='pending'`.
   *
   * Each hit includes `branchPath` so the caller can target the matching
   * pending row when completing it. Sleep yields at top level always have
   * `branchPath = ""`, but sleep inside a parallel branch (should a user
   * ever mix them) carries its branch path here.
   */
  findDueSleeps(params: { now: Date; limit: number }): Promise<
    Array<{
      workflowId: string;
      stepName: string;
      activityIndex: number;
      branchPath: string;
      wakeAt: Date;
    }>
  >;

  /**
   * Look up a pending signal entry by (workflowId, stepName, signalName).
   * Returns null if no pending entry matches (already delivered, or never
   * registered). `completeSignal` uses this to locate the entry to complete.
   */
  findPendingSignal(params: {
    workflowId: string;
    stepName: string;
    signalName: string;
  }): Promise<JournalEntry | null>;
}

/** Runtime check for whether a storage implementation supports suspend/resume. */
export function isJournaledSuspendStorage(
  storage: ActivityJournalStorage,
): storage is JournaledSuspendStorage {
  const s = storage as Partial<JournaledSuspendStorage>;
  return (
    typeof s.appendPendingEntry === "function" &&
    typeof s.completePendingEntry === "function" &&
    typeof s.findDueSleeps === "function" &&
    typeof s.findPendingSignal === "function"
  );
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
