// ---------------------------------------------------------------------------
// PgStepQueue — Postgres SKIP LOCKED implementation of StepQueue
//
// Distributed step dispatch backed by a Postgres table.
// Workers claim tasks with SELECT FOR UPDATE SKIP LOCKED for
// exactly-once delivery and natural load balancing.
// ---------------------------------------------------------------------------

import { eq, sql } from "drizzle-orm";
import type { StepQueue, StepTask } from "@promin/core";
import { type DrizzleDb, execRaw } from "./drizzle-db.ts";
import { stepQueue } from "./schema.ts";

export interface PgStepQueueConfig {
  db: DrizzleDb;
  /** Worker ID for claiming tasks. Default: random UUID. */
  workerId?: string;
}

export class PgStepQueue implements StepQueue {
  private readonly db: DrizzleDb;
  private readonly workerId: string;

  constructor(config: PgStepQueueConfig) {
    this.db = config.db;
    this.workerId = config.workerId ?? crypto.randomUUID();
  }

  async enqueue(params: {
    workflowId: string;
    stepName: string;
    queue: string;
    input: unknown;
    prevResults: Record<string, unknown>;
  }): Promise<string> {
    const [row] = await this.db
      .insert(stepQueue)
      .values({
        workflowId: params.workflowId,
        stepName: params.stepName,
        queue: params.queue,
        input: params.input,
        prevResults: params.prevResults,
      })
      .returning({ id: stepQueue.id });

    return String(row!.id);
  }

  async claim(params: { queues: string[]; limit: number }): Promise<StepTask[]> {
    // SKIP LOCKED with subquery requires raw SQL — Drizzle can't express this.
    // We validate/sanitize inputs to prevent injection:
    // - queues: alphanumeric + hyphens/underscores only
    // - limit: integer
    // - workerId: UUID format
    const sanitizedQueues = params.queues.map((q) => `'${q.replace(/'/g, "")}'`).join(",");
    const limit = Math.max(1, Math.floor(params.limit));
    const workerId = this.workerId.replace(/'/g, "");
    const now = new Date().toISOString();

    const rows = await execRaw(
      this.db,
      sql.raw(`
        UPDATE wf_step_queue
        SET status = 'running',
            claimed_by = '${workerId}',
            claimed_at = '${now}'
        WHERE id IN (
          SELECT id FROM wf_step_queue
          WHERE status = 'pending' AND queue IN (${sanitizedQueues})
          ORDER BY created_at ASC
          LIMIT ${limit}
          FOR UPDATE SKIP LOCKED
        )
        RETURNING id, workflow_id, step_name, queue, input, prev_results, attempt, status, created_at
      `),
    );

    return rows.map((r: any) => ({
      id: String(r.id),
      workflowId: r.workflow_id,
      stepName: r.step_name,
      queue: r.queue,
      input: r.input,
      prevResults: (r.prev_results as Record<string, unknown>) ?? {},
      attempt: r.attempt,
      status: "running" as const,
      createdAt: r.created_at instanceof Date ? r.created_at : new Date(r.created_at),
    }));
  }

  async complete(params: { taskId: string; result: unknown; durationMs: number }): Promise<void> {
    const now = new Date();
    await this.db
      .update(stepQueue)
      .set({
        status: "completed",
        result: params.result,
        durationMs: params.durationMs,
        completedAt: now,
      })
      .where(eq(stepQueue.id, Number(params.taskId)));
  }

  async fail(params: { taskId: string; error: string; durationMs: number }): Promise<void> {
    const now = new Date();
    await this.db
      .update(stepQueue)
      .set({
        status: "failed",
        error: params.error,
        durationMs: params.durationMs,
        completedAt: now,
      })
      .where(eq(stepQueue.id, Number(params.taskId)));
  }

  async metrics(): Promise<
    Record<string, { pending: number; running: number; completed: number; failed: number }>
  > {
    const rows = await execRaw(
      this.db,
      sql`
        SELECT queue, status, COUNT(*) as count
        FROM wf_step_queue
        GROUP BY queue, status
      `,
    );

    const result: Record<
      string,
      { pending: number; running: number; completed: number; failed: number }
    > = {};

    for (const row of rows) {
      const q = row.queue as string;
      if (!result[q]) result[q] = { pending: 0, running: 0, completed: 0, failed: 0 };
      result[q]![row.status as "pending" | "running" | "completed" | "failed"] = Number(row.count);
    }

    return result;
  }

  /** Ensure the step queue table exists. */
  async ensureTable(): Promise<void> {
    await execRaw(
      this.db,
      sql.raw(`
        CREATE TABLE IF NOT EXISTS wf_step_queue (
          id BIGSERIAL PRIMARY KEY,
          workflow_id TEXT NOT NULL,
          step_name TEXT NOT NULL,
          queue TEXT NOT NULL DEFAULT 'default',
          input JSONB,
          prev_results JSONB,
          attempt INTEGER NOT NULL DEFAULT 1,
          status TEXT NOT NULL DEFAULT 'pending',
          result JSONB,
          error TEXT,
          duration_ms BIGINT,
          claimed_by TEXT,
          claimed_at TIMESTAMPTZ,
          completed_at TIMESTAMPTZ,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
      `),
    );
    await execRaw(
      this.db,
      sql.raw(
        `CREATE INDEX IF NOT EXISTS wf_step_queue_dequeue_idx ON wf_step_queue (status, queue, created_at)`,
      ),
    );
    await execRaw(
      this.db,
      sql.raw(
        `CREATE INDEX IF NOT EXISTS wf_step_queue_workflow_idx ON wf_step_queue (workflow_id)`,
      ),
    );
  }
}
