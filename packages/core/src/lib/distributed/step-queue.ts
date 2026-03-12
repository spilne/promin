// ---------------------------------------------------------------------------
// StepQueue — interface for dispatching and claiming step tasks
//
// The coordinator enqueues steps; workers dequeue and execute them.
// Postgres implementation uses SKIP LOCKED for exactly-once delivery.
// ---------------------------------------------------------------------------

export interface StepTask {
  readonly id: string;
  readonly workflowId: string;
  readonly stepName: string;
  readonly queue: string;
  readonly input: unknown;
  readonly prevResults: Record<string, unknown>;
  readonly attempt: number;
  readonly status: "pending" | "running" | "completed" | "failed";
  readonly createdAt: Date;
}

export interface StepQueue {
  /** Enqueue a step for execution on a named queue. */
  enqueue(params: {
    workflowId: string;
    stepName: string;
    queue: string;
    input: unknown;
    prevResults: Record<string, unknown>;
  }): Promise<string>;

  /** Claim up to `limit` pending tasks from the given queues (SKIP LOCKED). */
  claim(params: { queues: string[]; limit: number }): Promise<StepTask[]>;

  /** Mark a task as completed with a result. */
  complete(params: { taskId: string; result: unknown; durationMs: number }): Promise<void>;

  /** Mark a task as failed with an error. */
  fail(params: { taskId: string; error: string; durationMs: number }): Promise<void>;

  /** Re-enqueue tasks stuck in "running" by a dead worker. Returns count re-enqueued. */
  requeueStuck(params: { claimedBy: string }): Promise<number>;

  /** Get pending task count per queue. */
  metrics(): Promise<
    Record<string, { pending: number; running: number; completed: number; failed: number }>
  >;
}
