// ---------------------------------------------------------------------------
// Activity journal — the entry shapes a `.journaled()` step records. The
// storage side is `JournalStore` (`./storage/journal-store.ts`).
// ---------------------------------------------------------------------------

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
 * A recorded failure. `error` is the message; a plain `Error` records
 * nothing else and replays as a plain `Error`.
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
   * top-level in the body". Yields inside parallel branches get paths like
   * `"/0.0"`, `"/1.2"`, `"/1.1/0.0"` (nested) — see `journal-format.ts`.
   * Opaque text to storage backends. The
   * (workflow, step, activity_index, branch_path) quadruple is unique.
   */
  readonly branchPath: string;
  readonly activityName: string;
  /** Kind of yield that wrote the entry. Absent means `"activity"`. */
  readonly stepType?: JournalStepType;
  /** Absent means `"completed"` (an entry written in one step by `appendEntry`). */
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

export type { JournalStore } from "./storage/journal-store.ts";
