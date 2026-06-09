// ---------------------------------------------------------------------------
// InMemoryStepQueue — for testing distributed workflows without Postgres
// ---------------------------------------------------------------------------

import type { StepQueue, StepTask, FairnessPolicy } from "./step-queue.ts";
import { SystemClock, type Clock } from "@promin/core";

type MutableTask = {
  -readonly [K in keyof StepTask]: StepTask[K];
} & {
  result?: unknown;
  error?: string;
  claimedBy?: string;
  claimedAt?: Date;
  claimToken?: string;
  heartbeatAt?: Date;
  completedAt?: Date;
  durationMs?: number;
  // Internal — StepTask hides namespace from consumers, but we need it to
  // clear the activeByKey slot on complete/fail.
  namespace?: string;
};

export class InMemoryStepQueue implements StepQueue {
  private tasks = new Map<string, MutableTask>();
  /**
   * `${namespace}::${workflowId}::${stepName}` → active taskId. Drives the
   * idempotent-enqueue contract: while a prior task for the triple is
   * pending/running, re-enqueue returns the existing id. Cleared on
   * complete/fail so retries + fresh runs can re-enqueue cleanly.
   */
  private activeByKey = new Map<string, string>();
  private counter = 0;
  private readonly workerId: string;
  /** Time source — drives createdAt/claimedAt/completedAt + metrics window. */
  private readonly clock: Clock;

  constructor(params?: { workerId?: string; clock?: Clock }) {
    this.workerId = params?.workerId ?? "in-memory";
    this.clock = params?.clock ?? SystemClock;
  }

  private activeKey(namespace: string | undefined, workflowId: string, stepName: string): string {
    return `${namespace ?? ""}::${workflowId}::${stepName}`;
  }

  async enqueue(params: {
    workflowId: string;
    stepName: string;
    input: unknown;
    prevResults: Record<string, unknown>;
    needs?: readonly string[];
    priority?: number;
    namespace?: string;
    version?: string;
    metadata?: Record<string, unknown>;
    concurrencyKey?: string;
    concurrencyScope?: string;
    concurrencyLimit?: number;
  }): Promise<string> {
    const key = this.activeKey(params.namespace, params.workflowId, params.stepName);
    const existing = this.activeByKey.get(key);
    if (existing !== undefined) return existing;

    const id = `task-${++this.counter}`;
    this.tasks.set(id, {
      id,
      workflowId: params.workflowId,
      stepName: params.stepName,
      needs: params.needs ?? [],
      priority: params.priority ?? 5,
      input: params.input,
      prevResults: params.prevResults,
      attempt: 1,
      status: "pending",
      createdAt: this.clock.now(),
      version: params.version,
      namespace: params.namespace,
      metadata: params.metadata,
      concurrencyKey: params.concurrencyKey,
      concurrencyScope: params.concurrencyScope,
      concurrencyLimit: params.concurrencyLimit,
    });
    this.activeByKey.set(key, id);
    return id;
  }

  async claim(params: {
    capabilities?: readonly string[];
    stepNames?: readonly string[];
    supportedVersions?: readonly string[];
    limit: number;
    fairness?: FairnessPolicy;
    filter?: (task: StepTask) => boolean;
  }): Promise<StepTask[]> {
    const caps = new Set(params.capabilities ?? []);
    const stepNames = params.stepNames ? new Set(params.stepNames) : undefined;
    const supportedVersions = params.supportedVersions
      ? new Set(params.supportedVersions)
      : undefined;
    const fairness = params.fairness ?? "strict-priority";

    // Subset check: task.needs ⊆ capabilities. Empty needs matches anyone.
    const canHandle = (task: MutableTask): boolean => {
      for (const n of task.needs) {
        if (!caps.has(n)) return false;
      }
      if (stepNames && !stepNames.has(task.stepName)) return false;
      if (supportedVersions && task.version !== undefined && !supportedVersions.has(task.version)) {
        return false;
      }
      return true;
    };

    const pending = [...this.tasks.values()].filter((t) => t.status === "pending" && canHandle(t));

    let ordered: MutableTask[];

    switch (fairness) {
      case "strict-priority":
        ordered = pending.sort(
          (a, b) =>
            (b.priority ?? 5) - (a.priority ?? 5) || a.createdAt.getTime() - b.createdAt.getTime(),
        );
        break;

      case "round-robin": {
        const byWorkflow = new Map<string, MutableTask[]>();
        for (const t of pending.sort((a, b) => a.createdAt.getTime() - b.createdAt.getTime())) {
          if (!byWorkflow.has(t.workflowId)) byWorkflow.set(t.workflowId, []);
          byWorkflow.get(t.workflowId)!.push(t);
        }
        ordered = [];
        const queues = [...byWorkflow.values()];
        let round = 0;
        while (ordered.length < pending.length) {
          let added = false;
          for (const wfTasks of queues) {
            if (round < wfTasks.length) {
              ordered.push(wfTasks[round]!);
              added = true;
            }
          }
          if (!added) break;
          round++;
        }
        break;
      }

      case "weighted": {
        ordered = pending
          .map((t) => ({ t, score: (t.priority ?? 5) * (0.5 + Math.random()) }))
          .sort((a, b) => b.score - a.score)
          .map((x) => x.t);
        break;
      }

      default:
        ordered = pending;
    }

    // Per-(scope,key) running counter — built once per claim() call so we
    // can decide if claiming a task would push past its concurrencyLimit.
    // Counts both already-running tasks and tasks claimed earlier in this
    // same batch (so a single claim call can't itself violate the cap).
    const runningPerKey = new Map<string, number>();
    for (const t of this.tasks.values()) {
      if (t.status !== "running") continue;
      if (!t.concurrencyKey || !t.concurrencyScope) continue;
      const k = `${t.concurrencyScope}::${t.concurrencyKey}`;
      runningPerKey.set(k, (runningPerKey.get(k) ?? 0) + 1);
    }

    const claimed: StepTask[] = [];
    for (const task of ordered) {
      if (claimed.length >= params.limit) break;
      if (task.status !== "pending") continue;
      if (params.filter && !params.filter({ ...task } as StepTask)) continue;
      // Concurrency cap check — only when all three fields are set.
      if (task.concurrencyKey && task.concurrencyScope && task.concurrencyLimit !== undefined) {
        const k = `${task.concurrencyScope}::${task.concurrencyKey}`;
        const running = runningPerKey.get(k) ?? 0;
        if (running >= task.concurrencyLimit) continue;
        runningPerKey.set(k, running + 1);
      }
      task.status = "running";
      task.claimedBy = this.workerId;
      task.claimedAt = this.clock.now();
      task.claimToken = `claim-${this.workerId}-${++this.counter}`;
      claimed.push({ ...task });
    }

    return claimed;
  }

  async complete(params: {
    taskId: string;
    claimToken?: string;
    result: unknown;
    durationMs: number;
  }): Promise<boolean> {
    const task = this.tasks.get(params.taskId);
    if (!this.isCurrentClaim(task, params.claimToken)) return false;
    task.status = "completed";
    task.result = params.result;
    task.durationMs = params.durationMs;
    task.completedAt = this.clock.now();
    this.activeByKey.delete(this.activeKey(task.namespace, task.workflowId, task.stepName));
    return true;
  }

  async heartbeat(params: { taskId: string; claimToken?: string }): Promise<boolean> {
    const task = this.tasks.get(params.taskId);
    if (!this.isCurrentClaim(task, params.claimToken)) return false;
    task.heartbeatAt = this.clock.now();
    return true;
  }

  async fail(params: {
    taskId: string;
    claimToken?: string;
    error: string;
    durationMs: number;
  }): Promise<boolean> {
    const task = this.tasks.get(params.taskId);
    if (!this.isCurrentClaim(task, params.claimToken)) return false;
    task.status = "failed";
    task.error = params.error;
    task.durationMs = params.durationMs;
    task.completedAt = this.clock.now();
    this.activeByKey.delete(this.activeKey(task.namespace, task.workflowId, task.stepName));
    return true;
  }

  async requeueStuck(params: { claimedBy?: string; staleTimeoutMs?: number }): Promise<number> {
    let count = 0;
    const cutoff = params.staleTimeoutMs
      ? this.clock.currentTimeMs() - params.staleTimeoutMs
      : undefined;

    for (const task of this.tasks.values()) {
      if (task.status !== "running") continue;

      const matchesByWorker = params.claimedBy && task.claimedBy === params.claimedBy;
      const lastActivity = task.heartbeatAt ?? task.claimedAt;
      const matchesByTimeout = cutoff && lastActivity && lastActivity.getTime() < cutoff;

      if (matchesByWorker || matchesByTimeout) {
        task.status = "pending";
        task.claimedBy = undefined;
        task.claimedAt = undefined;
        task.claimToken = undefined;
        count++;
      }
    }
    return count;
  }

  async metrics(params: { since: Date; until?: Date }): Promise<{
    pending: number;
    running: number;
    completed: number;
    failed: number;
    avgWaitMs: number;
    avgExecMs: number;
    p95ExecMs: number;
  }> {
    const sinceMs = params.since.getTime();
    const untilMs = (params.until ?? this.clock.now()).getTime();
    const inWindow = (d: Date | undefined): boolean =>
      d !== undefined && d.getTime() >= sinceMs && d.getTime() <= untilMs;

    let pending = 0;
    let running = 0;
    let completed = 0;
    let failed = 0;
    let waitSum = 0;
    let waitN = 0;
    let execSum = 0;
    const execTimes: number[] = [];

    for (const task of this.tasks.values()) {
      // Each status uses the timestamp that defines its current membership
      // in the window: createdAt for pending, claimedAt for running,
      // completedAt for terminal. Tasks that don't fit the window don't
      // count — this is the whole point of the mandatory window.
      if (task.status === "pending" && inWindow(task.createdAt)) {
        pending++;
      } else if (task.status === "running" && inWindow(task.claimedAt)) {
        running++;
      } else if (
        (task.status === "completed" || task.status === "failed") &&
        inWindow(task.completedAt)
      ) {
        if (task.status === "completed") completed++;
        else failed++;
        // Latency pool spans both completed and failed — ops wants exec
        // distribution regardless of outcome.
        if (task.claimedAt) {
          waitSum += task.claimedAt.getTime() - task.createdAt.getTime();
          waitN++;
        }
        if (task.durationMs !== undefined) {
          execSum += task.durationMs;
          execTimes.push(task.durationMs);
        }
      }
    }

    const terminalN = execTimes.length;
    return {
      pending,
      running,
      completed,
      failed,
      avgWaitMs: waitN > 0 ? waitSum / waitN : 0,
      avgExecMs: terminalN > 0 ? execSum / terminalN : 0,
      p95ExecMs: terminalN > 0 ? percentile(execTimes, 0.95) : 0,
    };
  }

  /** Test helper: get all tasks. */
  getAllTasks(): StepTask[] {
    return [...this.tasks.values()];
  }

  private isCurrentClaim(
    task: MutableTask | undefined,
    claimToken: string | undefined,
  ): task is MutableTask {
    if (!task || task.status !== "running") return false;
    return claimToken === undefined || task.claimToken === claimToken;
  }
}

/**
 * Linear-interpolation percentile (matches SQL `PERCENTILE_CONT`). Sorts a
 * copy so callers keep their ordering. Handles the degenerate cases
 * cleanly: single value returns itself, empty array is guarded by callers.
 */
function percentile(values: number[], p: number): number {
  const sorted = [...values].sort((a, b) => a - b);
  const rank = p * (sorted.length - 1);
  const lo = Math.floor(rank);
  const hi = Math.ceil(rank);
  if (lo === hi) return sorted[lo]!;
  return sorted[lo]! + (sorted[hi]! - sorted[lo]!) * (rank - lo);
}
