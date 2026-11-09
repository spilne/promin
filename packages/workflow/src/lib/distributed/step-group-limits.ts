// ---------------------------------------------------------------------------
// Step groups — named rate-limit / concurrency groups with fair scheduling.
//
// A step opts into a group (`queue: { group: { name: "openai", ... } }`).
// Every task of the group carries the group's limits; `StepQueue.claim()`
// enforces them across all workers:
//
// - `maxConcurrent`: at most N tasks of the group `running` at once.
// - `maxStarts` per `windowMs`: at most N claims of the group within any
//   sliding window. A claim is a start; a `release()` gives the start back.
// - fairness: among pending tasks of one group, claim order interleaves the
//   group's `fairnessKey`s round-robin (a key's n-th queued task ranks
//   behind every other key's n-th) and counts what a key already has
//   running, so one noisy tenant cannot starve the others.
//
// A throttled task is never failed and never loses an attempt: it simply
// stays `pending` until the group has room. The pure rules live here so the
// bundled backends (and any other) share one definition.
// ---------------------------------------------------------------------------

/** The group fields a task carries. All absent = not in a group. */
export interface StepGroupFields {
  readonly groupName?: string;
  readonly groupMaxConcurrent?: number;
  readonly groupMaxStarts?: number;
  readonly groupWindowMs?: number;
  readonly fairnessKey?: string;
}

/** Why a pending task is held back. */
export type StepThrottleReason = "concurrency" | "rate";

/** A pending task a group is currently holding back (`StepQueue.throttled`). */
export interface ThrottledTask {
  readonly taskId: string;
  readonly workflowId: string;
  readonly run: number;
  readonly stepName: string;
  readonly attempt: number;
  readonly groupName: string;
  readonly reason: StepThrottleReason;
  /** When the task was enqueued. */
  readonly since: Date;
  /**
   * Earliest time the rate window frees a slot (`reason: "rate"`). A hint:
   * other claimers may take the slot first. Absent for `"concurrency"`,
   * which frees when a running task settles.
   */
  readonly retryAt?: Date;
}

/** Throws on a group the queue could not enforce. */
export function assertValidGroupLimits(group: {
  readonly name: string;
  readonly maxConcurrent?: number;
  readonly maxStarts?: number;
  readonly windowMs?: number;
}): void {
  if (group.name.length === 0) throw new Error("step group: name must not be empty");
  const posInt = (label: string, v: number | undefined) => {
    if (v !== undefined && !(Number.isInteger(v) && v >= 1)) {
      throw new Error(`step group "${group.name}": ${label} must be a positive integer, got ${v}`);
    }
  };
  posInt("maxConcurrent", group.maxConcurrent);
  posInt("maxStarts", group.maxStarts);
  posInt("windowMs", group.windowMs);
  if ((group.maxStarts === undefined) !== (group.windowMs === undefined)) {
    throw new Error(`step group "${group.name}": maxStarts and windowMs go together`);
  }
}

/** True when `task` is in a group (it has a name). */
export function inGroup(task: StepGroupFields): task is StepGroupFields & { groupName: string } {
  return task.groupName !== undefined && task.groupName !== null && task.groupName !== "";
}

/** Group state a claimer reads before admitting tasks. */
export interface GroupUsage {
  readonly running: number;
  /** Start times (ms) of the group's claims inside the window of the task asked about. */
  readonly startsInWindow: readonly number[];
}

/**
 * Whether one more task of `task`'s group may start. `usage` must already
 * include admissions made earlier in the same claim. Returns the reason it
 * may not, with the rate window's `retryAt` (ms) when that is the reason.
 */
export function admission(params: {
  task: StepGroupFields;
  usage: GroupUsage;
}): { ok: true } | { ok: false; reason: StepThrottleReason; retryAtMs?: number } {
  const { task, usage } = params;
  if (task.groupMaxConcurrent != null && usage.running >= task.groupMaxConcurrent) {
    return { ok: false, reason: "concurrency" };
  }
  if (
    task.groupMaxStarts != null &&
    task.groupWindowMs != null &&
    usage.startsInWindow.length >= task.groupMaxStarts
  ) {
    const sorted = [...usage.startsInWindow].sort((a, b) => a - b);
    const freeing = sorted[sorted.length - task.groupMaxStarts]!;
    return { ok: false, reason: "rate", retryAtMs: freeing + task.groupWindowMs };
  }
  return { ok: true };
}

/** Start times that still count inside `windowMs` at `nowMs` (strictly newer than the cutoff). */
export function startsWithin(params: {
  starts: readonly number[];
  windowMs: number;
  nowMs: number;
}): number[] {
  const cutoff = params.nowMs - params.windowMs;
  return params.starts.filter((s) => s > cutoff);
}

/**
 * Claim order for pending tasks: priority (high first), then fairness rank,
 * then age. A grouped task with a `fairnessKey` ranks at its position among
 * its `(group, key)` pending peers plus the number of that key's tasks
 * already running, so keys interleave round-robin and a key that is already
 * using the group yields to one that is not. Everything else ranks 0.
 *
 * `pending` must be in the queue's natural FIFO order (oldest first, ties in
 * enqueue order): the sort is stable and `createdAt` ties keep that order.
 * `runningByKey` maps `fairKeyId({group, key})` to its running count.
 */
export function claimOrder<
  T extends StepGroupFields & { priority: number; createdAt: Date },
>(params: { pending: readonly T[]; runningByKey: ReadonlyMap<string, number> }): T[] {
  const byAge = (a: T, b: T) => a.createdAt.getTime() - b.createdAt.getTime();
  const ranks = new Map<T, number>();
  const seen = new Map<string, number>();
  for (const task of [...params.pending].sort(byAge)) {
    if (!inGroup(task) || task.fairnessKey == null) continue;
    const k = fairKeyId({ group: task.groupName, key: task.fairnessKey });
    const pos = seen.get(k) ?? 0;
    seen.set(k, pos + 1);
    ranks.set(task, pos + (params.runningByKey.get(k) ?? 0));
  }
  return [...params.pending].sort(
    (a, b) => b.priority - a.priority || (ranks.get(a) ?? 0) - (ranks.get(b) ?? 0) || byAge(a, b),
  );
}

/** Stable id of a `(group, fairnessKey)` pair (length-prefixed, so it cannot collide). */
export function fairKeyId(params: { group: string; key: string }): string {
  return `${[...params.group].length}:${params.group}:${params.key}`;
}

/**
 * In-process group usage for backends that claim in application code
 * (in-memory, SQLite): fed one row per task of a group that is running or
 * has started, it answers `admission` for a task and counts admissions made
 * during one claim so a single `claim()` cannot itself break a cap.
 */
export class GroupLedger {
  private readonly running = new Map<string, number>();
  private readonly starts = new Map<string, number[]>();
  private readonly runningByFair = new Map<string, number>();

  constructor(private readonly nowMs: number) {}

  /** Record an existing task: `running`, and/or its `startedAtMs` when it has one. */
  record(task: StepGroupFields & { running: boolean; startedAtMs?: number }): void {
    if (!inGroup(task)) return;
    if (task.running) this.addRunning(task);
    if (task.startedAtMs !== undefined)
      this.addStart({ group: task.groupName, atMs: task.startedAtMs });
  }

  /** Running tasks per `fairKeyId`, for `claimOrder`. */
  runningByFairKey(): ReadonlyMap<string, number> {
    return this.runningByFair;
  }

  /** The verdict for one more task of `task`'s group right now. */
  check(task: StepGroupFields & { groupName: string }) {
    return admission({
      task,
      usage: {
        running: this.running.get(task.groupName) ?? 0,
        startsInWindow:
          task.groupWindowMs != null
            ? startsWithin({
                starts: this.starts.get(task.groupName) ?? [],
                windowMs: task.groupWindowMs,
                nowMs: this.nowMs,
              })
            : [],
      },
    });
  }

  /** Count a task admitted by this claim. */
  admit(task: StepGroupFields & { groupName: string }): void {
    this.addRunning(task);
    this.addStart({ group: task.groupName, atMs: this.nowMs });
  }

  private addRunning(task: StepGroupFields & { groupName: string }): void {
    this.running.set(task.groupName, (this.running.get(task.groupName) ?? 0) + 1);
    if (task.fairnessKey != null) {
      const k = fairKeyId({ group: task.groupName, key: task.fairnessKey });
      this.runningByFair.set(k, (this.runningByFair.get(k) ?? 0) + 1);
    }
  }

  private addStart(params: { group: string; atMs: number }): void {
    const list = this.starts.get(params.group) ?? [];
    list.push(params.atMs);
    this.starts.set(params.group, list);
  }
}
