// ---------------------------------------------------------------------------
// JournalStore — the activity journal behind `.journaled()` steps.
//
// A journaled step records each activity, sleep, signal wait, child run and
// compensation as a journal entry, so a retry or crash recovery replays the
// recorded outcomes instead of re-running them. Storages opt in by
// implementing this interface (capability `journal`); a `.journaled()` step
// throws `JournalStorageMissingError` at execute time when the runner's
// storage doesn't.
// ---------------------------------------------------------------------------

import type {
  CompletePendingResult,
  JournalEntry,
  JournalExit,
  JournalSlot,
  JournalStepType,
} from "../activity-journal.ts";
import type { FencedWrite } from "./fencing.ts";

/** Params of `loadJournal`. */
export interface LoadJournalParams {
  readonly workflowId: string;
  readonly stepName: string;
}

/** Address of one entry: step plus slot. `branchPath` defaults to `""`. */
interface JournalEntryAddress {
  readonly workflowId: string;
  readonly stepName: string;
  readonly activityIndex: number;
  readonly branchPath?: string;
}

/** Params of `appendEntry`. */
export interface AppendEntryParams extends JournalEntryAddress, FencedWrite {
  readonly activityName: string;
  readonly payloadHash?: string;
  readonly exit: JournalExit;
}

/** Params of `appendPendingEntry`. */
export interface AppendPendingEntryParams extends JournalEntryAddress, FencedWrite {
  readonly activityName: string;
  readonly payloadHash?: string;
  readonly stepType: JournalStepType;
  /** `sleep` entries: when the workflow should wake. */
  readonly wakeAt?: Date;
}

/** Params of `completePendingEntry`. */
export interface CompletePendingEntryParams extends JournalEntryAddress, FencedWrite {
  readonly exit: JournalExit;
}

/** Params of `discardJournalEntries`. */
export interface DiscardJournalEntriesParams extends FencedWrite {
  readonly workflowId: string;
  readonly stepName: string;
  readonly slots: readonly JournalSlot[];
}

/** Params of `findDueSleeps`. */
export interface FindDueSleepsParams {
  readonly now: Date;
  readonly limit: number;
}

/** One pending sleep entry whose `wakeAt` has passed. */
export interface DueSleep {
  workflowId: string;
  stepName: string;
  activityIndex: number;
  branchPath: string;
  wakeAt: Date;
}

/** Params of `findPendingSignal`. */
export interface FindPendingSignalParams {
  readonly workflowId: string;
  readonly stepName: string;
  readonly signalName: string;
}

/**
 * Storage capability for `.journaled()` steps. Implementations persist
 * journal entries keyed by (workflowId, stepName, activityIndex,
 * branchPath), return them in index order on load, and support the
 * two-phase record (a `pending` entry before the side effect, completed
 * after) that suspend/resume and crash recovery rely on.
 *
 * The journal is run-scoped: `startFreshRun` clears every step's journal
 * and `resetSteps` clears the reset steps' journals.
 */
export interface JournalStore {
  /**
   * Load all journal entries for one journaled step of one workflow run,
   * ordered by `activityIndex` then `branchPath`. Returns an empty array if
   * there are no entries.
   */
  loadJournal(params: LoadJournalParams): Promise<JournalEntry[]>;

  /**
   * Append one completed journal entry. Idempotent on
   * `(workflowId, stepName, activityIndex, branchPath)`: re-inserting the
   * same quadruple is a no-op. The engine records through
   * `appendPendingEntry` / `completePendingEntry`; this single-write form
   * serves tooling and the conformance suites. Fenced.
   */
  appendEntry(params: AppendEntryParams): Promise<void>;

  /**
   * Append a `pending` entry — used by `ctx.sleep` / `ctx.signal` when a
   * journaled step suspends, by `ctx.activity` for the two-phase record
   * (pending row written before the side effect, completed after), and by
   * the intra-step compensation unwind for each rollback callback. For
   * sleep: carries `wakeAt`. For signal / activity / compensation: the name
   * lives in `activityName`. Idempotent on the slot. Fenced.
   */
  appendPendingEntry(params: AppendPendingEntryParams): Promise<void>;

  /**
   * Transition a `pending` entry to `completed`. Used by the sleep scanner
   * (for sleep entries, exit = `{ tag: "Success", value: actual wake time }`),
   * by `completeSignal` (for signal entries, exit carries the delivered
   * value) and by the step body itself (activity results, sleep and signal
   * timeouts). First writer wins: if the entry is already completed the call
   * changes nothing.
   *
   * The check and the write must be one atomic step, and the result must
   * report who won (see `CompletePendingResult`). A signal delivery and the
   * signal's timeout race for the same entry, and the losing side adopts the
   * stored exit so the live run and the journal agree.
   *
   * Fenced when the step body completes its own entry. The sleep scanner
   * and signal delivery complete entries unfenced: they hold no lock, and
   * first writer wins already settles their race with the body. A rejected
   * fenced call changes nothing.
   */
  completePendingEntry(params: CompletePendingEntryParams): Promise<CompletePendingResult>;

  /**
   * Delete the given entries of one journaled step. Missing slots are
   * ignored. Also drops them from any sleep or signal lookup index.
   *
   * The engine calls this when a failure escapes a journaled body: recorded
   * failures (and activities whose compensation ran) are removed so the next
   * attempt of the step re-executes them instead of replaying the failure.
   * Fenced.
   */
  discardJournalEntries(params: DiscardJournalEntriesParams): Promise<void>;

  /**
   * Scanner hook — return pending sleep entries whose `wakeAt <= now`, up to
   * `limit`. Backends use an index on
   * `(wakeAt) WHERE step_type='sleep' AND phase='pending'`.
   *
   * Each hit includes `branchPath` so the caller can target the matching
   * pending row when completing it.
   */
  findDueSleeps(params: FindDueSleepsParams): Promise<DueSleep[]>;

  /**
   * Look up a pending signal entry by (workflowId, stepName, signalName).
   * Returns null if no pending entry matches (already delivered, or never
   * registered). `completeSignal` uses this to locate the entry to complete.
   */
  findPendingSignal(params: FindPendingSignalParams): Promise<JournalEntry | null>;
}
