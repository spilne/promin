// ---------------------------------------------------------------------------
// Journal cursor — one journaled body run's view of its step journal.
//
// It allocates the slot of every yield (top-level counter or the enclosing
// `ctx.parallel` branch), checks a replayed yield against what the journal
// recorded in that slot, and writes the slot's pending and completed rows.
// Every journal write a body makes goes through here, under the run's fence
// guard.
// ---------------------------------------------------------------------------

import type {
  ActivityJournalStorage,
  CompletePendingResult,
  JournalEntry,
  JournalExit,
  JournalSlot,
  JournalStepType,
} from "./activity-journal.ts";
import { failureExit } from "./journal-exit.ts";
import { JournalNonDeterminismError } from "./journal-errors.ts";
import {
  JOURNAL_FORMAT_LEGACY,
  detectJournalFormat,
  resolveUndecidedJournalFormat,
  type JournalFormatVersion,
} from "./journal-format.ts";
import { activityScope, nextPathInScope } from "./journaled-body-scope.ts";
import type { FenceGuard } from "./workflow-storage.ts";

/** Kinds of yield that check a recorded entry before replaying it. */
export type JournalYieldKind = "activity" | "sleep" | "signal" | "child";

/**
 * How a recorded outcome ends: the value or failure the journal holds, or
 * this run's own failure (recorded, and rethrown as the original error).
 */
export type RecordedOutcome =
  | { readonly kind: "stored"; readonly exit: JournalExit }
  | { readonly kind: "failed"; readonly error: unknown };

const slotKey = (slot: JournalSlot): string => `${slot.activityIndex}:${slot.branchPath}`;

export class JournalCursor {
  readonly workflowId: string;
  readonly stepName: string;
  private readonly storage: ActivityJournalStorage;
  private readonly guard: FenceGuard | undefined;
  private readonly journal: readonly JournalEntry[];
  private readonly entries: Map<string, JournalEntry>;
  /**
   * Top-level slot counter. Activities, sleeps, signals, children and
   * compensations all draw from it; a compensation reserves its index when
   * its activity succeeds, before any later yield, so a deterministic body
   * re-derives identical indices on replay.
   */
  private nextIndex = 0;
  /**
   * Branch-path grammar of this journal. `undefined` until the first
   * top-level `ctx.parallel` when the journal holds only top-level entries
   * (see `journal-format.ts`).
   */
  private format: JournalFormatVersion | undefined;

  constructor(params: {
    readonly workflowId: string;
    readonly stepName: string;
    readonly journal: readonly JournalEntry[];
    readonly storage: ActivityJournalStorage;
    readonly guard?: FenceGuard | undefined;
  }) {
    this.workflowId = params.workflowId;
    this.stepName = params.stepName;
    this.storage = params.storage;
    this.guard = params.guard;
    this.journal = params.journal;
    this.entries = new Map(params.journal.map((e) => [slotKey(e), e]));
    this.format = detectJournalFormat(params.journal);
  }

  /** Whether the body is re-running on top of recorded entries. */
  get isReplay(): boolean {
    return this.journal.length > 0;
  }

  /**
   * Allocate the slot of one yield. Inside a parallel branch the branch
   * scope supplies it; at top level the step's counter does.
   *
   * `ctx.sleep` / `ctx.signal` / `ctx.child` pass `suspendOrChild: true`:
   * a format 1 journal recorded those on the top-level counter even inside
   * a branch, so replaying one keeps that allocation.
   */
  allocateSlot(params: { readonly suspendOrChild: boolean }): JournalSlot {
    const scope = activityScope.getStore();
    if (scope && !(params.suspendOrChild && scope.format === JOURNAL_FORMAT_LEGACY)) {
      return { activityIndex: scope.parallelActivityIndex, branchPath: nextPathInScope(scope) };
    }
    return { activityIndex: this.nextIndex++, branchPath: "" };
  }

  /** Reserve the next top-level slot index (a compensation's row). */
  reserveTopLevelIndex(): number {
    return this.nextIndex++;
  }

  /**
   * Branch-path grammar for the `ctx.parallel` occupying `parallelIndex`. A
   * journal with only top-level entries settles its format at the first
   * top-level parallel; nested parallels inherit it from their scope.
   */
  formatForParallel(parallelIndex: number): JournalFormatVersion {
    return (this.format ??= resolveUndecidedJournalFormat({
      journal: this.journal,
      parallelIndex,
    }));
  }

  /** The entry recorded in `slot`, if any. */
  recorded(slot: JournalSlot): JournalEntry | undefined {
    return this.entries.get(slotKey(slot));
  }

  /**
   * The entry recorded in `slot`, after checking that the same kind of
   * yield recorded it under the same name. Throws
   * `JournalNonDeterminismError` on drift.
   */
  expectRecorded(params: {
    readonly slot: JournalSlot;
    readonly kind: JournalYieldKind;
    readonly name: string;
  }): JournalEntry | undefined {
    const { slot, kind, name } = params;
    const recorded = this.recorded(slot);
    if (!recorded) return undefined;
    const nonDeterminism = (expected: string, actual: string): JournalNonDeterminismError =>
      new JournalNonDeterminismError(this.stepName, slot.activityIndex, expected, actual);
    // An activity reports a renamed activity before a changed kind.
    if (kind === "activity" && recorded.activityName !== name) {
      throw nonDeterminism(recorded.activityName, name);
    }
    const recordedKind = recorded.stepType ?? "activity";
    if (recordedKind !== kind) {
      throw nonDeterminism(
        `${recordedKind}:${recorded.activityName}`,
        kind === "sleep" ? "sleep" : `${kind}:${name}`,
      );
    }
    // Sleeps are unnamed.
    if (kind !== "sleep" && recorded.activityName !== name) {
      throw kind === "child"
        ? nonDeterminism(`child:${recorded.activityName}`, `child:${name}`)
        : nonDeterminism(recorded.activityName, name);
    }
    return recorded;
  }

  /**
   * Write the `pending` row of `slot`. Idempotent: re-writing an existing
   * pending row is a no-op.
   */
  async writePending(params: {
    readonly slot: JournalSlot;
    readonly kind: JournalStepType;
    readonly name: string;
    readonly payloadHash?: string | undefined;
    readonly wakeAt?: Date | undefined;
  }): Promise<void> {
    const { slot, kind, name, payloadHash, wakeAt } = params;
    await this.storage.appendPendingEntry(
      {
        workflowId: this.workflowId,
        stepName: this.stepName,
        activityIndex: slot.activityIndex,
        branchPath: slot.branchPath,
        activityName: name,
        stepType: kind,
        ...(payloadHash !== undefined && { payloadHash }),
        ...(wakeAt !== undefined && { wakeAt }),
      },
      this.guard,
    );
  }

  /**
   * Complete the pending row of `slot` and return the exit the journal now
   * holds. When another writer completed the slot first (a signal delivery
   * beating the timeout, or a second worker), that writer's exit is
   * returned and `won` is false: the caller must continue with it so the
   * live run and replay agree.
   *
   * A storage that reports nothing (written before `CompletePendingResult`)
   * is assumed to have taken this exit, unless `readBack` asks to read the
   * row back; only worth the read where a race is expected.
   */
  async complete(params: {
    readonly slot: JournalSlot;
    readonly exit: JournalExit;
    readonly readBack: boolean;
  }): Promise<{ won: boolean; exit: JournalExit }> {
    const { slot, exit, readBack } = params;
    const result: CompletePendingResult | undefined = await this.storage.completePendingEntry(
      {
        workflowId: this.workflowId,
        stepName: this.stepName,
        activityIndex: slot.activityIndex,
        branchPath: slot.branchPath,
        exit,
      },
      this.guard,
    );
    if (result) {
      // No stored exit means the entry is gone (purged under us): nothing to
      // follow, keep the local outcome.
      if (result.completed || result.exit === undefined) return { won: true, exit };
      return { won: false, exit: result.exit };
    }
    if (!readBack) return { won: true, exit };
    const stored = (await this.storage.loadJournal(this.workflowId, this.stepName)).find(
      (e) => e.activityIndex === slot.activityIndex && e.branchPath === slot.branchPath,
    );
    if (stored?.exit && (stored.phase ?? "completed") === "completed") {
      return { won: false, exit: stored.exit };
    }
    return { won: true, exit };
  }

  /**
   * Two-phase record of one outcome: write the pending row, `run` the side
   * effect, then complete the row with its success (`encode`d) or failure.
   *
   * A failure is recorded and reported as `failed` so the caller rethrows
   * the original error, unless another writer completed the slot first: then
   * the stored exit is returned and the caller follows the journal.
   * `passThrough` sees each failure before it is recorded and throws to
   * propagate it unrecorded (control flow, not an outcome).
   */
  async recordOutcome<T>(params: {
    readonly slot: JournalSlot;
    readonly kind: "activity" | "child";
    readonly name: string;
    readonly payloadHash?: string | undefined;
    readonly run: () => Promise<T>;
    readonly encode: (value: T) => unknown;
    readonly passThrough?: (err: unknown) => Promise<void>;
  }): Promise<RecordedOutcome> {
    const { slot, kind, name, payloadHash, run, encode, passThrough } = params;
    await this.writePending({ slot, kind, name, payloadHash });
    let value: T;
    try {
      value = await run();
    } catch (err) {
      await passThrough?.(err);
      const stored = await this.complete({ slot, exit: failureExit(err), readBack: false });
      // Another writer completed the slot first: follow the journal.
      return stored.won ? { kind: "failed", error: err } : { kind: "stored", exit: stored.exit };
    }
    const stored = await this.complete({
      slot,
      exit: { tag: "Success", value: encode(value) },
      readBack: false,
    });
    return { kind: "stored", exit: stored.exit };
  }
}
