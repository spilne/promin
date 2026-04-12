// ---------------------------------------------------------------------------
// PgStepQueue — Postgres SKIP LOCKED implementation of StepQueue
//
// Distributed step dispatch backed by a Postgres table.
// Workers claim tasks with SELECT FOR UPDATE SKIP LOCKED for
// exactly-once delivery and natural load balancing.
// ---------------------------------------------------------------------------

import { eq, and, lt, sql } from "drizzle-orm";
import type { StepQueue, StepTask, FairnessPolicy } from "@promin/workflow";
import { type DrizzleDb, execRaw } from "./drizzle-db.ts";
import { stepQueue } from "./schema.ts";
import { ensureTable as ensureTableFromSchema } from "./schema-utils.ts";

export interface PgStepQueueConfig {
  db: DrizzleDb;
  /** Worker ID for claiming tasks. Default: random UUID. */
  workerId?: string;
  /** Default namespace for task isolation. Null means unscoped. Default: null. */
  namespace?: string | null;
}

export class PgStepQueue implements StepQueue {
  private readonly db: DrizzleDb;
  private readonly workerId: string;
  private readonly namespace: string | null;

  constructor(config: PgStepQueueConfig) {
    this.db = config.db;
    this.workerId = config.workerId ?? crypto.randomUUID();
    this.namespace = config.namespace ?? null;
  }

  /**
   * Get the Drizzle schema for the step queue table.
   * Use this to include in your migration pipeline.
   *
   * @example
   * ```ts
   * // In your drizzle schema file:
   * export { stepQueue } from "@promin/postgres";
   * // Or:
   * export const stepQueue = PgStepQueue.schema;
   * ```
   */
  static readonly schema = stepQueue;

  async enqueue(params: {
    workflowId: string;
    stepName: string;
    queue: string;
    input: unknown;
    prevResults: Record<string, unknown>;
    priority?: number;
    namespace?: string;
  }): Promise<string> {
    const ns = params.namespace ?? this.namespace;
    const [row] = await this.db
      .insert(stepQueue)
      .values({
        workflowId: params.workflowId,
        stepName: params.stepName,
        namespace: ns,
        queue: params.queue,
        priority: params.priority ?? 5,
        input: params.input,
        prevResults: params.prevResults,
      })
      .returning({ id: stepQueue.id });

    return String(row!.id);
  }

  async claim(params: {
    queues: string[];
    limit: number;
    fairness?: FairnessPolicy;
  }): Promise<StepTask[]> {
    const sanitizedQueues = params.queues.map((q) => `'${q.replace(/'/g, "")}'`).join(",");
    const limit = Math.max(1, Math.floor(params.limit));
    const workerId = this.workerId.replace(/'/g, "");
    const now = new Date().toISOString();
    const nsFilter = this.namespace ? `AND namespace = '${this.namespace.replace(/'/g, "")}'` : "";
    const fairness = params.fairness ?? "strict-priority";

    // Build ORDER BY clause based on fairness policy
    let orderBy: string;
    switch (fairness) {
      case "round-robin":
        // Round-robin across workflows: interleave by row number within each workflow
        // ROW_NUMBER() can't be used inside FOR UPDATE SKIP LOCKED, so we use a
        // two-layer approach: inner selects with SKIP LOCKED, outer orders by interleave
        orderBy = `rn, created_at ASC`;
        break;
      case "weighted":
        orderBy = `(priority * random()) DESC, created_at ASC`;
        break;
      case "strict-priority":
      default:
        orderBy = `priority DESC, created_at ASC`;
        break;
    }

    let query: string;
    if (fairness === "round-robin") {
      // Round-robin: select all pending with SKIP LOCKED, then apply window function outside
      query = `
        UPDATE wf_step_queue
        SET status = 'running', claimed_by = '${workerId}', claimed_at = '${now}'
        WHERE id IN (
          SELECT id FROM (
            SELECT id, ROW_NUMBER() OVER (PARTITION BY workflow_id ORDER BY created_at ASC) as rn
            FROM wf_step_queue
            WHERE status = 'pending' AND queue IN (${sanitizedQueues}) ${nsFilter}
          ) ranked
          ORDER BY ${orderBy}
          LIMIT ${limit}
          FOR UPDATE SKIP LOCKED
        )
        RETURNING id, workflow_id, step_name, queue, priority, input, prev_results, attempt, status, created_at
      `;
    } else {
      // strict-priority and weighted: simple ORDER BY with SKIP LOCKED
      query = `
        UPDATE wf_step_queue
        SET status = 'running', claimed_by = '${workerId}', claimed_at = '${now}'
        WHERE id IN (
          SELECT id FROM wf_step_queue
          WHERE status = 'pending' AND queue IN (${sanitizedQueues}) ${nsFilter}
          ORDER BY ${orderBy}
          LIMIT ${limit}
          FOR UPDATE SKIP LOCKED
        )
        RETURNING id, workflow_id, step_name, queue, priority, input, prev_results, attempt, status, created_at
      `;
    }

    const rows = await execRaw(this.db, sql.raw(query));

    return rows
      .map((r: any) => ({
        id: String(r.id),
        workflowId: r.workflow_id,
        stepName: r.step_name,
        queue: r.queue,
        priority: r.priority ?? 5,
        input: r.input,
        prevResults: (r.prev_results as Record<string, unknown>) ?? {},
        attempt: r.attempt,
        status: "running" as const,
        createdAt: r.created_at instanceof Date ? r.created_at : new Date(r.created_at),
      }))
      .sort((a, b) => b.priority - a.priority || a.createdAt.getTime() - b.createdAt.getTime());
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

  async requeueStuck(params: { claimedBy?: string; staleTimeoutMs?: number }): Promise<number> {
    const conditions = [eq(stepQueue.status, "running")];

    if (this.namespace) {
      conditions.push(eq(stepQueue.namespace, this.namespace));
    }

    if (params.claimedBy) {
      conditions.push(eq(stepQueue.claimedBy, params.claimedBy));
    } else if (params.staleTimeoutMs) {
      conditions.push(lt(stepQueue.claimedAt, new Date(Date.now() - params.staleTimeoutMs)));
    } else {
      return 0;
    }

    const rows = await this.db
      .update(stepQueue)
      .set({ status: "pending", claimedBy: null, claimedAt: null })
      .where(and(...conditions))
      .returning({ id: stepQueue.id });

    return rows.length;
  }

  async metrics(): Promise<
    Record<string, { pending: number; running: number; completed: number; failed: number }>
  > {
    const nsFilter = this.namespace ? sql` WHERE namespace = ${this.namespace}` : sql``;
    const rows = await execRaw(
      this.db,
      sql`
        SELECT queue, status, COUNT(*) as count
        FROM wf_step_queue${nsFilter}
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

  /**
   * Ensure the step queue table exists with all columns.
   * Derived from the Drizzle schema — single source of truth.
   * For production, prefer using migrations instead.
   */
  async ensureTable(): Promise<void> {
    await ensureTableFromSchema(this.db, stepQueue);
  }
}
