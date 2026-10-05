// ---------------------------------------------------------------------------
// InMemoryStepQueue — for testing distributed workflows without Postgres
// ---------------------------------------------------------------------------

import {
  DEFAULT_MAX_DELIVERIES,
  deadLetterError,
  percentileCont,
  type StepQueue,
  type StepQueueClaimParams,
  type StepQueueCompleteParams,
  type StepQueueEnqueueParams,
  type StepQueueFailParams,
  type StepQueueRequeueParams,
  type StepQueueRequeueResult,
  type StepTask,
  type StepTaskRecord,
} from "./step-queue.ts";
import { SystemWallClock, type WallClock } from "../shared/wall-clock.ts";
import type { InMemoryLeaderLeases } from "../scheduler/leader-lease.ts";

type MutableTask = {
  -readonly [K in keyof StepTaskRecord]: StepTaskRecord[K];
} & {
  /** Internal — StepTask hides namespace from consumers. */
  namespace?: string;
};

export interface InMemoryStepQueueConfig {
  /** Time source — drives createdAt/claimedAt/completedAt + metrics window. */
  clock?: WallClock;
  /**
   * Deliveries after which `requeueStuck` dead-letters a task instead of
   * requeueing it. Default: `DEFAULT_MAX_DELIVERIES` (10).
   */
  maxDeliveries?: number;
  /**
   * Lease store that `requeueStuck({ lease })` fences against: the sweep
   * rejects with `StaleLeaseError` and changes nothing unless the lease is
   * still current here. Without it the `lease` param is ignored.
   */
  leaderLeases?: InMemoryLeaderLeases;
}

export class InMemoryStepQueue implements StepQueue {
  private tasks = new Map<string, MutableTask>();
  /**
   * `${workflowId}::${stepName}` → unconsumed taskId. Drives the
   * idempotent-enqueue contract: while a prior task for the pair is
   * pending, running or settled-but-unconsumed, re-enqueue returns the
   * existing id. Cleared by `consume` (and when the task is purged).
   */
  private activeByKey = new Map<string, string>();
  private counter = 0;
  private readonly clock: WallClock;
  private readonly maxDeliveries: number;
  private readonly leaderLeases: InMemoryLeaderLeases | undefined;

  constructor(config?: InMemoryStepQueueConfig) {
    this.clock = config?.clock ?? SystemWallClock;
    this.maxDeliveries = config?.maxDeliveries ?? DEFAULT_MAX_DELIVERIES;
    this.leaderLeases = config?.leaderLeases;
  }

  private activeKey(workflowId: string, stepName: string): string {
    return `${workflowId}::${stepName}`;
  }

  async enqueue(params: StepQueueEnqueueParams): Promise<string> {
    const key = this.activeKey(params.workflowId, params.stepName);
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
      deps: params.deps ?? {},
      dependsOn: params.dependsOn ?? [],
      ...(params.timeoutMs !== undefined && { timeoutMs: params.timeoutMs }),
      attempt: params.attempt ?? 1,
      run: params.run ?? 1,
      deliveries: 0,
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

  async claim(params: StepQueueClaimParams): Promise<StepTask[]> {
    const caps = new Set(params.capabilities ?? []);
    const stepNames = params.stepNames ? new Set(params.stepNames) : undefined;
    const versions = params.versions ? new Set(params.versions) : undefined;

    const claimable = (task: MutableTask): boolean => {
      if (task.status !== "pending") return false;
      if (stepNames && !stepNames.has(task.stepName)) return false;
      if (versions && task.version !== undefined && !versions.has(task.version)) return false;
      // Subset check: task.needs ⊆ capabilities. Empty needs matches anyone.
      for (const n of task.needs) {
        if (!caps.has(n)) return false;
      }
      return true;
    };

    const ordered = [...this.tasks.values()]
      .filter(claimable)
      .sort((a, b) => b.priority - a.priority || a.createdAt.getTime() - b.createdAt.getTime());

    // Per-(scope,key) running counter, counting both already-running tasks
    // and tasks claimed earlier in this batch (so one claim() can't itself
    // violate the cap).
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
      if (task.concurrencyKey && task.concurrencyScope && task.concurrencyLimit !== undefined) {
        const k = `${task.concurrencyScope}::${task.concurrencyKey}`;
        const running = runningPerKey.get(k) ?? 0;
        if (running >= task.concurrencyLimit) continue;
        runningPerKey.set(k, running + 1);
      }
      task.status = "running";
      task.claimedBy = params.workerId;
      task.claimedAt = this.clock.now();
      // A fresh claim starts a fresh lease: a heartbeat left over from an
      // earlier claim must not make the new one look stale.
      task.heartbeatAt = undefined;
      task.claimToken = `claim-${++this.counter}`;
      task.deliveries += 1;
      claimed.push(this.toTask(task));
    }

    return claimed;
  }

  async release(params: { taskId: string; claimToken: string }): Promise<boolean> {
    const task = this.tasks.get(params.taskId);
    if (!this.isCurrentClaim(task, params.claimToken)) return false;
    this.backToPending(task);
    task.deliveries = Math.max(0, task.deliveries - 1);
    return true;
  }

  async get(taskId: string): Promise<StepTaskRecord | undefined> {
    const task = this.tasks.get(taskId);
    if (!task) return undefined;
    const { namespace: _ns, ...record } = task;
    return { ...record };
  }

  async complete(params: StepQueueCompleteParams): Promise<boolean> {
    const task = this.tasks.get(params.taskId);
    if (!this.isCurrentClaim(task, params.claimToken)) return false;
    task.status = "completed";
    task.result = params.result;
    task.stepMetadata = params.stepMetadata;
    task.durationMs = params.durationMs;
    task.completedAt = this.clock.now();
    return true;
  }

  async heartbeat(params: { taskId: string; claimToken?: string }): Promise<boolean> {
    const task = this.tasks.get(params.taskId);
    if (!this.isCurrentClaim(task, params.claimToken)) return false;
    task.heartbeatAt = this.clock.now();
    return true;
  }

  async fail(params: StepQueueFailParams): Promise<boolean> {
    const task = this.tasks.get(params.taskId);
    if (!this.isCurrentClaim(task, params.claimToken)) return false;
    task.status = "failed";
    task.error = params.error;
    task.errorTag = params.errorTag;
    task.stepMetadata = params.stepMetadata;
    task.durationMs = params.durationMs;
    task.completedAt = this.clock.now();
    return true;
  }

  async requeueStuck(params: StepQueueRequeueParams): Promise<StepQueueRequeueResult> {
    // Synchronous with the sweep below, so no takeover can land in between.
    if (params.lease && this.leaderLeases) this.leaderLeases.assertCurrent(params.lease);
    let requeued = 0;
    let deadLettered = 0;
    const cutoff =
      params.mode === "stale" ? this.clock.currentTimeMs() - params.olderThanMs : undefined;

    for (const task of this.tasks.values()) {
      if (task.status !== "running") continue;

      if (params.mode === "worker") {
        if (task.claimedBy !== params.workerId) continue;
      } else {
        const lastActivity = task.heartbeatAt ?? task.claimedAt;
        if (!lastActivity || lastActivity.getTime() >= cutoff!) continue;
      }

      if (task.deliveries >= this.maxDeliveries) {
        task.status = "failed";
        task.error = deadLetterError(this.maxDeliveries);
        task.completedAt = this.clock.now();
        task.claimToken = undefined;
        deadLettered++;
      } else {
        this.backToPending(task);
        requeued++;
      }
    }
    return { requeued, deadLettered };
  }

  async purge(params: { completedBefore: Date }): Promise<number> {
    const cutoff = params.completedBefore.getTime();
    let purged = 0;
    for (const [id, task] of this.tasks) {
      if (task.status !== "completed" && task.status !== "failed") continue;
      if (!task.completedAt || task.completedAt.getTime() >= cutoff) continue;
      this.tasks.delete(id);
      this.freeSlot(task);
      purged++;
    }
    return purged;
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
      // completedAt for terminal.
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
      p95ExecMs: terminalN > 0 ? percentileCont({ values: execTimes, p: 0.95 }) : 0,
    };
  }

  async consume(params: { taskId: string }): Promise<boolean> {
    const task = this.tasks.get(params.taskId);
    if (!task || task.consumedAt !== undefined) return false;
    if (task.status !== "completed" && task.status !== "failed") return false;
    task.consumedAt = this.clock.now();
    this.freeSlot(task);
    return true;
  }

  async consumeSettled(params: {
    workflowId: string;
    stepNames: readonly string[];
  }): Promise<number> {
    let consumed = 0;
    for (const stepName of params.stepNames) {
      const id = this.activeByKey.get(this.activeKey(params.workflowId, stepName));
      if (id !== undefined && (await this.consume({ taskId: id }))) consumed++;
    }
    return consumed;
  }

  /** Test helper: get all tasks. */
  getAllTasks(): StepTaskRecord[] {
    return [...this.tasks.values()].map((t) => {
      const { namespace: _ns, ...record } = t;
      return { ...record };
    });
  }

  /** Give up the task's `(workflowId, stepName)` slot, if it holds it. */
  private freeSlot(task: MutableTask): void {
    const key = this.activeKey(task.workflowId, task.stepName);
    if (this.activeByKey.get(key) === task.id) this.activeByKey.delete(key);
  }

  private backToPending(task: MutableTask): void {
    task.status = "pending";
    task.claimedBy = undefined;
    task.claimedAt = undefined;
    task.claimToken = undefined;
    task.heartbeatAt = undefined;
  }

  private toTask(task: MutableTask): StepTask {
    return {
      id: task.id,
      workflowId: task.workflowId,
      stepName: task.stepName,
      needs: task.needs,
      priority: task.priority,
      input: task.input,
      deps: task.deps,
      dependsOn: task.dependsOn,
      ...(task.timeoutMs !== undefined && { timeoutMs: task.timeoutMs }),
      attempt: task.attempt,
      run: task.run,
      deliveries: task.deliveries,
      status: task.status,
      createdAt: task.createdAt,
      claimToken: task.claimToken,
      version: task.version,
      metadata: task.metadata,
      concurrencyKey: task.concurrencyKey,
      concurrencyScope: task.concurrencyScope,
      concurrencyLimit: task.concurrencyLimit,
    };
  }

  private isCurrentClaim(
    task: MutableTask | undefined,
    claimToken: string | undefined,
  ): task is MutableTask {
    if (!task || task.status !== "running") return false;
    return claimToken === undefined || task.claimToken === claimToken;
  }
}
