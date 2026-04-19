// ---------------------------------------------------------------------------
// InMemoryStepQueue — for testing distributed workflows without Postgres
// ---------------------------------------------------------------------------

import type { StepQueue, StepTask, FairnessPolicy } from "./step-queue.ts";

type MutableTask = {
  -readonly [K in keyof StepTask]: StepTask[K];
} & {
  result?: unknown;
  error?: string;
  claimedBy?: string;
  claimedAt?: Date;
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

  constructor(params?: { workerId?: string }) {
    this.workerId = params?.workerId ?? "in-memory";
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
      createdAt: new Date(),
      version: params.version,
      namespace: params.namespace,
    });
    this.activeByKey.set(key, id);
    return id;
  }

  async claim(params: {
    capabilities?: readonly string[];
    limit: number;
    fairness?: FairnessPolicy;
    filter?: (task: StepTask) => boolean;
  }): Promise<StepTask[]> {
    const caps = new Set(params.capabilities ?? []);
    const fairness = params.fairness ?? "strict-priority";

    // Subset check: task.needs ⊆ capabilities. Empty needs matches anyone.
    const canHandle = (task: MutableTask): boolean => {
      for (const n of task.needs) {
        if (!caps.has(n)) return false;
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

    const claimed: StepTask[] = [];
    for (const task of ordered) {
      if (claimed.length >= params.limit) break;
      if (task.status !== "pending") continue;
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
      this.activeByKey.delete(this.activeKey(task.namespace, task.workflowId, task.stepName));
    }
  }

  async fail(params: { taskId: string; error: string; durationMs: number }): Promise<void> {
    const task = this.tasks.get(params.taskId);
    if (task) {
      task.status = "failed";
      task.error = params.error;
      this.activeByKey.delete(this.activeKey(task.namespace, task.workflowId, task.stepName));
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

  async metrics(): Promise<{
    pending: number;
    running: number;
    completed: number;
    failed: number;
  }> {
    const counts = { pending: 0, running: 0, completed: 0, failed: 0 };
    for (const task of this.tasks.values()) counts[task.status]++;
    return counts;
  }

  /** Test helper: get all tasks. */
  getAllTasks(): StepTask[] {
    return [...this.tasks.values()];
  }
}
