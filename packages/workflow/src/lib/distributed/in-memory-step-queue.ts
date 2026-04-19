// ---------------------------------------------------------------------------
// InMemoryStepQueue — for testing distributed workflows without Postgres
// ---------------------------------------------------------------------------

import type { StepQueue, StepTask, FairnessPolicy } from "./step-queue.ts";

type MutableTask = {
  -readonly [K in keyof StepTask]: StepTask[K];
} & { result?: unknown; error?: string; claimedBy?: string; claimedAt?: Date };

export class InMemoryStepQueue implements StepQueue {
  private tasks = new Map<string, MutableTask>();
  /**
   * `${workflowId}::${stepName}` → taskId of the currently-active (pending or
   * running) task for that step. Used to dedupe re-enqueues — see
   * `StepQueue.enqueue` for the full semantics. Cleared when a task hits a
   * terminal state (complete / fail) so a subsequent enqueue for the same
   * step can create a fresh task (step retry, fresh workflow runs, etc).
   */
  private activeByKey = new Map<string, string>();
  private counter = 0;
  private readonly workerId: string;

  constructor(params?: { workerId?: string }) {
    this.workerId = params?.workerId ?? "in-memory";
  }

  private activeKey(workflowId: string, stepName: string): string {
    return `${workflowId}::${stepName}`;
  }

  async enqueue(params: {
    workflowId: string;
    stepName: string;
    queue: string;
    input: unknown;
    prevResults: Record<string, unknown>;
    priority?: number;
    namespace?: string;
    version?: string;
  }): Promise<string> {
    // Idempotency: if a task for this (workflowId, stepName) is already
    // pending or running, hand back its id instead of creating a second.
    // The active map is authoritative because we clear it on complete/fail.
    const key = this.activeKey(params.workflowId, params.stepName);
    const existing = this.activeByKey.get(key);
    if (existing !== undefined) return existing;

    const id = `task-${++this.counter}`;
    this.tasks.set(id, {
      id,
      workflowId: params.workflowId,
      stepName: params.stepName,
      queue: params.queue,
      priority: params.priority ?? 5,
      input: params.input,
      prevResults: params.prevResults,
      attempt: 1,
      status: "pending",
      createdAt: new Date(),
      version: params.version,
    });
    this.activeByKey.set(key, id);
    return id;
  }

  async claim(params: {
    queues: string[];
    limit: number;
    fairness?: FairnessPolicy;
    filter?: (task: StepTask) => boolean;
  }): Promise<StepTask[]> {
    const claimed: StepTask[] = [];
    const queueSet = new Set(params.queues);
    const fairness = params.fairness ?? "strict-priority";

    const pending = [...this.tasks.values()].filter(
      (t) => t.status === "pending" && queueSet.has(t.queue),
    );

    let ordered: MutableTask[];

    switch (fairness) {
      case "strict-priority":
        // Highest priority first, FIFO within same priority
        ordered = pending.sort(
          (a, b) =>
            (b.priority ?? 5) - (a.priority ?? 5) || a.createdAt.getTime() - b.createdAt.getTime(),
        );
        break;

      case "round-robin": {
        // Interleave across workflowIds — one task per workflow, then cycle
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
        // Weighted random — higher priority tasks have proportionally higher chance
        // Shuffle pending, then sort with randomized priority weight
        ordered = pending
          .map((t) => ({ t, score: (t.priority ?? 5) * (0.5 + Math.random()) }))
          .sort((a, b) => b.score - a.score)
          .map((x) => x.t);
        break;
      }

      default:
        ordered = pending;
    }

    for (const task of ordered) {
      if (claimed.length >= params.limit) break;
      if (task.status !== "pending") continue;
      // Apply filter predicate — rejected tasks stay pending for other workers.
      if (params.filter && !params.filter({ ...task } as StepTask)) continue;
      task.status = "running";
      task.claimedBy = this.workerId;
      task.claimedAt = new Date();
      claimed.push({ ...task });
    }

    return claimed;
  }

  async complete(params: { taskId: string; result: unknown; durationMs: number }): Promise<void> {
    const task = this.tasks.get(params.taskId);
    if (task) {
      task.status = "completed";
      task.result = params.result;
      // Free the dedupe slot so a retry / fresh-run re-enqueue can succeed.
      this.activeByKey.delete(this.activeKey(task.workflowId, task.stepName));
    }
  }

  async fail(params: { taskId: string; error: string; durationMs: number }): Promise<void> {
    const task = this.tasks.get(params.taskId);
    if (task) {
      task.status = "failed";
      task.error = params.error;
      this.activeByKey.delete(this.activeKey(task.workflowId, task.stepName));
    }
  }

  async requeueStuck(params: { claimedBy?: string; staleTimeoutMs?: number }): Promise<number> {
    let count = 0;
    const cutoff = params.staleTimeoutMs ? Date.now() - params.staleTimeoutMs : undefined;

    for (const task of this.tasks.values()) {
      if (task.status !== "running") continue;

      const matchesByWorker = params.claimedBy && task.claimedBy === params.claimedBy;
      const matchesByTimeout = cutoff && task.claimedAt && task.claimedAt.getTime() < cutoff;

      if (matchesByWorker || matchesByTimeout) {
        task.status = "pending";
        task.claimedBy = undefined;
        task.claimedAt = undefined;
        count++;
      }
    }
    return count;
  }

  async metrics(): Promise<
    Record<string, { pending: number; running: number; completed: number; failed: number }>
  > {
    const result: Record<
      string,
      { pending: number; running: number; completed: number; failed: number }
    > = {};
    for (const task of this.tasks.values()) {
      if (!result[task.queue]) {
        result[task.queue] = { pending: 0, running: 0, completed: 0, failed: 0 };
      }
      result[task.queue]![task.status]++;
    }
    return result;
  }

  /** Test helper: get all tasks. */
  getAllTasks(): StepTask[] {
    return [...this.tasks.values()];
  }
}
