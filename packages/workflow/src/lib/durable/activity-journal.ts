// ---------------------------------------------------------------------------
// ActivityJournalStorage — the WorkflowStorage extension behind `.journaled()`
// steps.
//
// A journaled step records each activity, sleep, signal wait, child run and
// compensation as a journal entry, so a retry or crash recovery replays the
// recorded outcomes instead of re-running them. Storages opt in by
// implementing this interface; a `.journaled()` step throws
// `JournalStorageMissingError` at execute time when the runner's storage
// doesn't.
// ---------------------------------------------------------------------------

import type { FenceGuard } from "./workflow-storage.ts";

/** What kind of checkpoint an entry records. Used by replay + the sleep scanner. */
export type JournalStepType = "activity" | "sleep" | "signal" | "compensation" | "child";

/** Every `JournalStepType`. Backends with a CHECK constraint mirror this list. */
export const JOURNAL_STEP_TYPES: readonly JournalStepType[] = [
  "activity",
  "sleep",
  "signal",
  "compensation",
  "child",
];

/** Lifecycle phase of an entry. `pending` means suspend is in flight (sleep wake or signal delivery). */
export type JournalPhase = "pending" | "completed";

/**
 * A recorded failure. `error` is the message. Entries written before tagged
 * persistence hold only `error` and replay as a plain `Error`.
 */
export interface JournalFailureExit {
  readonly tag: "Failure";
  readonly error: string;
  /** `_tag` of a tagged error (`TerminalError`, a user `TaggedError`, ...). */
  readonly errorTag?: string;
  /** `name` of an untagged error when it isn't plain `"Error"` (e.g. `"TypeError"`). */
  readonly errorName?: string;
  /** The error's other own enumerable fields, `LosslessJsonCodec`-encoded. */
  readonly errorData?: unknown;
}

/** Completed outcome of a journal entry. */
export type JournalExit = { readonly tag: "Success"; readonly value: unknown } | JournalFailureExit;

/**
 * Result of `completePendingEntry`. `completed` is `true` when this call
 * moved the entry from `pending` to `completed`; `false` when the entry was
 * already completed (another writer won) or doesn't exist. `exit` is the
 * exit stored after the call: this call's own exit when it won, the
 * winner's when it lost, `undefined` when there is no such entry.
 */
export interface CompletePendingResult {
  readonly completed: boolean;
  readonly exit: JournalExit | undefined;
}

/** Address of one journal entry inside a step. */
export interface JournalSlot {
  readonly activityIndex: number;
  readonly branchPath: string;
}

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
 * Zod codecs are a planned follow-up). Failures carry the error message plus
 * its `_tag` / `name` and public fields (see `JournalFailureExit`) so replay
 * rethrows an error of the same kind.
 */
export interface JournalEntry {
  readonly activityIndex: number;
  /**
   * Branch path inside a `ctx.parallel` tree. Empty string `""` means "at
   * top-level in the body" — that's the value every pre-parallel workflow
   * journal already has, so old data works unchanged. Yields inside parallel
   * branches get paths like `"/0.0"`, `"/1.2"`, `"/1.1/0.0"` (nested); journals
   * written before that grammar hold `"0"`, `"1.1"`-style paths and still
   * replay — see `journal-format.ts`. Opaque text to storage backends. The
   * (workflow, step, activity_index, branch_path) quadruple is unique.
   */
  readonly branchPath: string;
  readonly activityName: string;
  /** Default `"activity"` preserves backward compat for entries without stepType. */
  readonly stepType?: JournalStepType;
  /** Default `"completed"` preserves backward compat for entries without phase. */
  readonly phase?: JournalPhase;
  /**
   * Canonicalized-+-hashed fingerprint of the activity's input — only set when
   * the caller opts in via `ActivityOptions.payloadHash` (or pipeline-level
   * `payloadHash: true` with a 3-arg activity). On replay, if the stored and
   * recomputed hashes differ, the engine throws `JournalNonDeterminismError`
   * to catch silent payload drift (same activity name, different input).
   * `undefined` when hashing was never requested.
   */
  readonly payloadHash?: string;
  /** Exit is set once the entry reaches `completed` phase. `undefined` while `pending`. */
  readonly exit?: JournalExit;
  /** For `sleep` entries: when the workflow should wake. Null for other types. */
  readonly wakeAt?: Date;
  readonly createdAt: Date;
}

/**
 * Storage extension for `.journaled()` steps. Implementations persist journal
 * entries keyed by (workflowId, stepName, activityIndex, branchPath), return
 * them in index order on load, and support the two-phase record (a `pending`
 * entry before the side effect, completed after) that suspend/resume and
 * crash recovery rely on.
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
   * Append one completed journal entry. Idempotent on
   * `(workflowId, stepName, activityIndex, branchPath)`: re-inserting the
   * same quadruple is a no-op. `branchPath` defaults to `""`. The engine
   * records through `appendPendingEntry` / `completePendingEntry`; this
   * single-write form serves tooling and the conformance suites.
   *
   * Fenced by `guard`: the step body's run passes its lock token (see
   * `FenceGuard`).
   */
  appendEntry(
    params: {
      readonly workflowId: string;
      readonly stepName: string;
      readonly activityIndex: number;
      readonly branchPath?: string;
      readonly activityName: string;
      readonly payloadHash?: string;
      readonly exit: NonNullable<JournalEntry["exit"]>;
    },
    guard?: FenceGuard,
  ): Promise<void>;

  /**
   * Append a `pending` entry — used by `ctx.sleep` / `ctx.signal` when a
   * journaled step suspends, by `ctx.activity` for the two-phase record
   * (pending row written before the side effect, completed after), and by
   * the intra-step compensation unwind for each rollback callback. For
   * sleep: carries `wakeAt`. For signal / activity / compensation: the name
   * lives in `activityName`. Idempotent on PK. Fenced by `guard`.
   */
  appendPendingEntry(
    params: {
      readonly workflowId: string;
      readonly stepName: string;
      readonly activityIndex: number;
      readonly branchPath?: string;
      readonly activityName: string;
      readonly payloadHash?: string;
      readonly stepType: "sleep" | "signal" | "activity" | "compensation" | "child";
      readonly wakeAt?: Date;
    },
    guard?: FenceGuard,
  ): Promise<void>;

  /**
   * Transition a `pending` entry to `completed`. Used by the sleep scanner
   * (for sleep entries, exit = `{ tag: "Success", value: actual wake time }`),
   * by `completeSignal` (for signal entries, exit carries the delivered
   * value) and by the step body itself (activity results, sleep and signal
   * timeouts). First writer wins: if the entry is already completed the call
   * changes nothing. `branchPath` defaults to `""` for non-parallel entries.
   *
   * The check and the write must be one atomic step, and the result must
   * report who won (see `CompletePendingResult`). A signal delivery and the
   * signal's timeout race for the same entry, and the losing side adopts the
   * stored exit so the live run and the journal agree.
   *
   * Fenced by `guard` when the step body completes its own entry. The sleep
   * scanner and signal delivery complete entries unfenced: they hold no
   * lock, and first writer wins already settles their race with the body.
   * A rejected fenced call changes nothing.
   */
  completePendingEntry(
    params: {
      readonly workflowId: string;
      readonly stepName: string;
      readonly activityIndex: number;
      readonly branchPath?: string;
      readonly exit: JournalExit;
    },
    guard?: FenceGuard,
  ): Promise<CompletePendingResult>;

  /**
   * Delete the given entries of one journaled step. Missing slots are
   * ignored. Also drops them from any sleep or signal lookup index.
   *
   * The engine calls this when a failure escapes a journaled body: recorded
   * failures (and activities whose compensation ran) are removed so the next
   * attempt of the step re-executes them instead of replaying the failure.
   * On a storage without this method a step-level retry replays the
   * recorded failure. Fenced by `guard`.
   */
  discardJournalEntries?(
    params: {
      readonly workflowId: string;
      readonly stepName: string;
      readonly slots: readonly JournalSlot[];
    },
    guard?: FenceGuard,
  ): Promise<void>;

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

/** Runtime check for whether a storage implementation supports `.journaled()` steps. */
export function isActivityJournalStorage<S extends object>(
  storage: S,
): storage is S & ActivityJournalStorage {
  const s = storage as Partial<ActivityJournalStorage>;
  return (
    typeof s.loadJournal === "function" &&
    typeof s.appendEntry === "function" &&
    typeof s.appendPendingEntry === "function" &&
    typeof s.completePendingEntry === "function" &&
    typeof s.findDueSleeps === "function" &&
    typeof s.findPendingSignal === "function"
  );
}

/** @deprecated Merged into `ActivityJournalStorage`; removed in the next release. */
export type JournaledSuspendStorage = ActivityJournalStorage;

/** @deprecated Use `isActivityJournalStorage`; removed in the next release. */
export const isJournaledSuspendStorage = isActivityJournalStorage;
