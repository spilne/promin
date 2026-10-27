// ---------------------------------------------------------------------------
// InMemoryJournal — the activity journal of `.journaled()` steps.
//
// Entries are kept per (workflow, step) in write order with a position index
// by slot, so a write is a lookup plus an in-place set or push; the sorted
// view `loadJournal` returns is built on the first load after a write.
// Fencing is the caller's job.
// ---------------------------------------------------------------------------

import type {
  CompletePendingResult,
  JournalEntry,
  JournalExit,
  JournalSlot,
  JournalStepType,
} from "../activity-journal.ts";
import type { DueSleep } from "../storage/journal-store.ts";
import type { WallClock } from "../../shared/wall-clock.ts";

/** One step's activity journal: entries in write order, positions by slot. */
interface JournalSlots {
  entries: JournalEntry[];
  /** Position in `entries` by `slotKey(activityIndex, branchPath)`. */
  position: Map<string, number>;
  /** `entries` sorted by slot, built on the first load after a write. */
  sorted?: JournalEntry[];
}

function slotKey(slot: { activityIndex: number; branchPath: string }): string {
  return `${slot.activityIndex}:${slot.branchPath}`;
}

function journalKey(step: { workflowId: string; stepName: string }): string {
  return `${step.workflowId}::${step.stepName}`;
}

/** Address of one journal entry. `branchPath` defaults to `""`. */
interface EntryAddress {
  readonly workflowId: string;
  readonly stepName: string;
  readonly activityIndex: number;
  readonly branchPath?: string;
}

export class InMemoryJournal {
  /** Journal keyed by `journalKey(workflowId, stepName)`. */
  private readonly journal = new Map<string, JournalSlots>();
  /** Journal keys by workflow id, so a workflow's journal is dropped without a scan. */
  private readonly journalKeys = new Map<string, Set<string>>();
  /** Journal keys that have (or had) a pending sleep entry: what `findDueSleeps` scans. */
  private readonly sleepKeys = new Set<string>();

  constructor(private readonly clock: WallClock) {}

  /** Entries of one step, ordered by `activityIndex` then `branchPath`. */
  load(params: { workflowId: string; stepName: string }): JournalEntry[] {
    const slots = this.journalOf(params);
    if (!slots) return [];
    // Defensive copy + stable sort: by activityIndex primarily, then by
    // branchPath so `ctx.parallel` branches have a deterministic replay
    // order when a consumer iterates the journal directly. The sorted
    // order is kept until the next write.
    slots.sorted ??= [...slots.entries].sort((a, b) => {
      if (a.activityIndex !== b.activityIndex) return a.activityIndex - b.activityIndex;
      return a.branchPath.localeCompare(b.branchPath);
    });
    return [...slots.sorted];
  }

  /** `appendEntry`: idempotent on a completed slot; completes a pending one. */
  append(
    params: EntryAddress & { activityName: string; payloadHash?: string; exit: JournalExit },
  ): void {
    const branchPath = params.branchPath ?? "";
    const slots = this.openJournal(params);
    const at = slots.position.get(slotKey({ activityIndex: params.activityIndex, branchPath }));
    const existing = at !== undefined ? slots.entries[at] : undefined;
    if (existing !== undefined && existing.phase !== "pending") return;
    // Preserve payloadHash from the prior pending row if the completer didn't
    // pass one — pending→completed transition shouldn't drop the fingerprint.
    putEntry({
      slots,
      entry: {
        activityIndex: params.activityIndex,
        branchPath,
        activityName: params.activityName,
        stepType: "activity",
        phase: "completed",
        payloadHash: params.payloadHash ?? existing?.payloadHash,
        exit: params.exit,
        createdAt: this.clock.now(),
      },
    });
  }

  /** `appendPendingEntry`: idempotent on the slot. */
  appendPending(
    params: EntryAddress & {
      activityName: string;
      payloadHash?: string;
      stepType: JournalStepType;
      wakeAt?: Date;
    },
  ): void {
    const branchPath = params.branchPath ?? "";
    const slots = this.openJournal(params);
    if (slots.position.has(slotKey({ activityIndex: params.activityIndex, branchPath }))) return;
    putEntry({
      slots,
      entry: {
        activityIndex: params.activityIndex,
        branchPath,
        activityName: params.activityName,
        stepType: params.stepType,
        phase: "pending",
        payloadHash: params.payloadHash,
        wakeAt: params.wakeAt,
        createdAt: this.clock.now(),
      },
    });
    if (params.stepType === "sleep") {
      this.sleepKeys.add(journalKey(params));
    }
  }

  /** `completePendingEntry`: first writer wins, the loser gets the stored exit. */
  completePending(params: EntryAddress & { exit: JournalExit }): CompletePendingResult {
    const branchPath = params.branchPath ?? "";
    const slots = this.journalOf(params);
    const at = slots?.position.get(slotKey({ activityIndex: params.activityIndex, branchPath }));
    if (slots === undefined || at === undefined) return { completed: false, exit: undefined };
    const existing = slots.entries[at]!;
    if (existing.phase !== "pending") return { completed: false, exit: existing.exit };
    putEntry({ slots, entry: { ...existing, phase: "completed", exit: params.exit } });
    return { completed: true, exit: params.exit };
  }

  /** `discardJournalEntries`: drop the given slots of one step. */
  discard(params: { workflowId: string; stepName: string; slots: readonly JournalSlot[] }): void {
    const slots = this.journalOf(params);
    if (!slots) return;
    const drop = new Set(params.slots.map(slotKey));
    filterEntries({ slots, keep: (e) => !drop.has(slotKey(e)) });
  }

  /** Drop every entry of one step at `activityIndex` (any branch). */
  discardIndex(params: { workflowId: string; stepName: string; activityIndex: number }): void {
    const slots = this.journalOf(params);
    if (!slots) return;
    filterEntries({ slots, keep: (e) => e.activityIndex !== params.activityIndex });
  }

  /** Pending sleep entries due at `now`, at most `limit`. */
  findDueSleeps(params: { now: Date; limit: number }): DueSleep[] {
    const due: DueSleep[] = [];
    // Only journals that ever held a pending sleep; one found without any
    // is dropped from the scan set.
    for (const key of this.sleepKeys) {
      const slots = this.journal.get(key);
      const [workflowId, stepName] = key.split("::") as [string, string];
      let pendingSleeps = 0;
      for (const e of slots?.entries ?? []) {
        if (e.stepType !== "sleep" || e.phase !== "pending") continue;
        pendingSleeps++;
        if (e.wakeAt && e.wakeAt.getTime() <= params.now.getTime()) {
          due.push({
            workflowId,
            stepName,
            activityIndex: e.activityIndex,
            branchPath: e.branchPath,
            wakeAt: e.wakeAt,
          });
          if (due.length >= params.limit) return due;
        }
      }
      if (pendingSleeps === 0) this.sleepKeys.delete(key);
    }
    return due;
  }

  /** The pending signal entry of one step waiting on `signalName`. */
  findPendingSignal(params: {
    workflowId: string;
    stepName: string;
    signalName: string;
  }): JournalEntry | null {
    const entries = this.journalOf(params)?.entries;
    if (!entries) return null;
    const hit = entries.find(
      (e) =>
        e.stepType === "signal" && e.phase === "pending" && e.activityName === params.signalName,
    );
    return hit ?? null;
  }

  /** Drop one step's journal (`resetSteps`). */
  deleteStep(params: { workflowId: string; stepName: string }): void {
    this.journal.delete(journalKey(params));
  }

  /** Drop every journal entry of one workflow, across all steps. */
  deleteWorkflow(workflowId: string): void {
    for (const key of this.journalKeys.get(workflowId) ?? []) this.journal.delete(key);
    this.journalKeys.delete(workflowId);
  }

  clear(): void {
    this.journal.clear();
    this.journalKeys.clear();
    this.sleepKeys.clear();
  }

  private journalOf(params: { workflowId: string; stepName: string }): JournalSlots | undefined {
    return this.journal.get(journalKey(params));
  }

  /** The step's journal, created empty when it has none. */
  private openJournal(params: { workflowId: string; stepName: string }): JournalSlots {
    const key = journalKey(params);
    let slots = this.journal.get(key);
    if (slots === undefined) {
      slots = { entries: [], position: new Map() };
      this.journal.set(key, slots);
      const keys = this.journalKeys.get(params.workflowId) ?? new Set<string>();
      keys.add(key);
      this.journalKeys.set(params.workflowId, keys);
    }
    return slots;
  }
}

/** Put `entry` at its slot: replaces the entry there, or appends. */
function putEntry(params: { slots: JournalSlots; entry: JournalEntry }): void {
  const { slots, entry } = params;
  const key = slotKey(entry);
  const at = slots.position.get(key);
  if (at !== undefined) slots.entries[at] = entry;
  else {
    slots.position.set(key, slots.entries.length);
    slots.entries.push(entry);
  }
  slots.sorted = undefined;
}

/** Keep only the entries `keep` accepts, re-indexing the rest. */
function filterEntries(params: { slots: JournalSlots; keep: (e: JournalEntry) => boolean }): void {
  const { slots } = params;
  slots.entries = slots.entries.filter(params.keep);
  slots.position = new Map();
  for (let i = 0; i < slots.entries.length; i++) {
    slots.position.set(slotKey(slots.entries[i]!), i);
  }
  slots.sorted = undefined;
}
