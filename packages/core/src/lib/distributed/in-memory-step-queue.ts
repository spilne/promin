// ---------------------------------------------------------------------------
// InMemoryStepQueue — for testing distributed workflows without Postgres
// ---------------------------------------------------------------------------

import type { StepQueue, StepTask } from "./step-queue.ts";

type MutableTask = {
  -readonly [K in keyof StepTask]: StepTask[K];
} & { result?: unknown; error?: string };

export class InMemoryStepQueue implements StepQueue {
  private tasks = new Map<string, MutableTask>();
  private counter = 0;

  async enqueue(params: {
    workflowId: string;
    stepName: string;
    queue: string;
    input: unknown;
    prevResults: Record<string, unknown>;
  }): Promise<string> {
    const id = `task-${++this.counter}`;
    this.tasks.set(id, {
      id,
      workflowId: params.workflowId,
      stepName: params.stepName,
      queue: params.queue,
      input: params.input,
      prevResults: params.prevResults,
      attempt: 1,
      status: "pending",
      createdAt: new Date(),
    });
    return id;
  }

  async claim(params: { queues: string[]; limit: number }): Promise<StepTask[]> {
    const claimed: StepTask[] = [];
    const queueSet = new Set(params.queues);

    for (const [, task] of this.tasks) {
      if (claimed.length >= params.limit) break;
      if (task.status === "pending" && queueSet.has(task.queue)) {
        task.status = "running";
        claimed.push({ ...task });
      }
    }

    return claimed;
  }

  async complete(params: { taskId: string; result: unknown; durationMs: number }): Promise<void> {
    const task = this.tasks.get(params.taskId);
    if (task) {
      task.status = "completed";
      task.result = params.result;
    }
  }

  async fail(params: { taskId: string; error: string; durationMs: number }): Promise<void> {
    const task = this.tasks.get(params.taskId);
    if (task) {
      task.status = "failed";
      task.error = params.error;
    }
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
