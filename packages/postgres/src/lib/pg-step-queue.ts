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
import { SystemClock, type Clock } from "@promin/core";

/**
 * Render a JS string[] as a Postgres `text[]` literal:
 *   ["foo", "bar"] → `'{"foo","bar"}'::text[]`
 *
 * Drizzle's parameter binding doesn't round-trip string[] cleanly for text[]
 * columns (it serializes the array into a single delimited string at bind
 * time). Embedding the literal as raw SQL avoids the binder entirely.
 * Values are escaped for the Postgres array-literal syntax — double quotes
 * and backslashes are the only escape targets.
 */
function textArrayLiteral(arr: readonly string[]): string {
  if (arr.length === 0) return `'{}'::text[]`;
  const escaped = arr.map((s) => `"${s.replace(/\\/g, "\\\\").replace(/"/g, '\\"')}"`);
  return `'{${escaped.join(",")}}'::text[]`;
}

export interface PgStepQueueConfig {
  db: DrizzleDb;
  /** Worker ID for claiming tasks. Default: random UUID. */
  workerId?: string;
  /** Default namespace for task isolation. Null means unscoped. Default: null. */
  namespace?: string | null;
  /**
   * Time source for client-side timestamps — claimedAt on claim, completedAt
   * on complete/fail, staleTimeoutMs cutoff on requeue. Default: `SystemClock`.
   * The `metrics()` default `until` stays server-side (`NOW()` in SQL) so it
   * remains skew-immune independent of this clock.
   */
  clock?: Clock;
}

export class PgStepQueue implements StepQueue {
  private readonly db: DrizzleDb;
  private readonly workerId: string;
  private readonly namespace: string | null;
  private readonly clock: Clock;

  constructor(config: PgStepQueueConfig) {
    this.db = config.db;
    this.workerId = config.workerId ?? crypto.randomUUID();
    this.namespace = config.namespace ?? null;
    this.clock = config.clock ?? SystemClock;
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
    input: unknown;
    prevResults: Record<string, unknown>;
    needs?: readonly string[];
    priority?: number;
    namespace?: string;
    version?: string;
  }): Promise<string> {
    const ns = params.namespace ?? this.namespace;
    const priority = params.priority ?? 5;
    const needs = params.needs ?? [];
    const inputJson = params.input === undefined ? null : JSON.stringify(params.input);
    const prevResultsJson = JSON.stringify(params.prevResults);
    // Raw SQL literal for text[] — drizzle's binder doesn't handle JS
    // arrays cleanly for this column type. Step names / capability names
    // come from code (readonly string[] on the interface), so the literal
    // is safe as long as textArrayLiteral escapes the two special chars
    // (" and \).
    const needsLiteral = sql.raw(textArrayLiteral(needs));
    // Idempotent on (workflow_id, step_name) via the partial unique index
    // `wf_step_queue_active_uniq`. DO UPDATE is a no-op self-assignment
    // that lets RETURNING surface the existing task id on conflict, so
    // callers always get an id back.
    const result = await this.db.execute(sql`
      INSERT INTO wf_step_queue (
        workflow_id, step_name, namespace, needs, priority, input, prev_results, version
      )
      VALUES (
        ${params.workflowId},
        ${params.stepName},
        ${ns},
        ${needsLiteral},
        ${priority},
        ${inputJson}::jsonb,
        ${prevResultsJson}::jsonb,
        ${params.version ?? null}
      )
      ON CONFLICT (workflow_id, step_name) WHERE status IN ('pending', 'running')
      DO UPDATE SET workflow_id = wf_step_queue.workflow_id
      RETURNING id
    `);
    const rows =
      (result as unknown as { rows?: Array<{ id: number | string }> }).rows ??
      (result as unknown as Array<{ id: number | string }>);
    const id = Array.isArray(rows) ? rows[0]?.id : undefined;
    if (id === undefined) {
      throw new Error("PgStepQueue.enqueue: no row returned from INSERT/UPSERT");
    }
    return String(id);
  }

  async claim(params: {
    capabilities?: readonly string[];
    limit: number;
    fairness?: FairnessPolicy;
    filter?: (task: StepTask) => boolean;
  }): Promise<StepTask[]> {
    const caps = params.capabilities ?? [];
    const limit = Math.max(1, Math.floor(params.limit));
    const workerId = this.workerId.replace(/'/g, "");
    const now = this.clock.now().toISOString();
    const fairness = params.fairness ?? "strict-priority";

    // Capability filter: `needs <@ caps` = "every element of needs is in
    // caps." Empty caps still matches tasks with empty needs (∅ ⊆ ∅).
    // Use raw literal for the same reason as enqueue — drizzle's
    // parameter binding doesn't round-trip JS string[] to text[].
    const capsLiteral = sql.raw(textArrayLiteral(caps));
    const nsFilter = this.namespace ? sql` AND namespace = ${this.namespace}` : sql``;

    // ORDER BY fragment per fairness policy.
    let orderBy;
    switch (fairness) {
      case "round-robin":
        orderBy = sql`rn, created_at ASC`;
        break;
      case "weighted":
        orderBy = sql`(priority * random()) DESC, created_at ASC`;
        break;
      case "strict-priority":
      default:
        orderBy = sql`priority DESC, created_at ASC`;
        break;
    }

    const claimSql =
      fairness === "round-robin"
        ? sql`
            UPDATE wf_step_queue
            SET status = 'running', claimed_by = ${workerId}, claimed_at = ${now}
            WHERE id IN (
              SELECT id FROM (
                SELECT id, ROW_NUMBER() OVER (PARTITION BY workflow_id ORDER BY created_at ASC) as rn
                FROM wf_step_queue
                WHERE status = 'pending'
                  AND needs <@ ${capsLiteral}
                  ${nsFilter}
              ) ranked
              ORDER BY ${orderBy}
              LIMIT ${limit}
              FOR UPDATE SKIP LOCKED
            )
            RETURNING id, workflow_id, step_name, needs, priority, input, prev_results, attempt, status, created_at, version
          `
        : sql`
            UPDATE wf_step_queue
            SET status = 'running', claimed_by = ${workerId}, claimed_at = ${now}
            WHERE id IN (
              SELECT id FROM wf_step_queue
              WHERE status = 'pending'
                AND needs <@ ${capsLiteral}
                ${nsFilter}
              ORDER BY ${orderBy}
              LIMIT ${limit}
              FOR UPDATE SKIP LOCKED
            )
            RETURNING id, workflow_id, step_name, needs, priority, input, prev_results, attempt, status, created_at, version
          `;

    const rows = await execRaw(this.db, claimSql);

    const claimed: StepTask[] = rows
      .map((r: any) => ({
        id: String(r.id),
        workflowId: r.workflow_id,
        stepName: r.step_name,
        needs: (r.needs as string[]) ?? [],
        priority: r.priority ?? 5,
        input: r.input,
        prevResults: (r.prev_results as Record<string, unknown>) ?? {},
        attempt: r.attempt,
        status: "running" as const,
        createdAt: r.created_at instanceof Date ? r.created_at : new Date(r.created_at),
        version: r.version ?? undefined,
      }))
      .sort((a, b) => b.priority - a.priority || a.createdAt.getTime() - b.createdAt.getTime());

    // Apply filter AFTER the claim — reject rows by releasing the lock (mark
    // back to 'pending'). This is less efficient than filtering pre-UPDATE,
    // but simpler and fine because worker filter rejections should be rare
    // (a misconfigured worker that rejects most tasks is the real problem).
    if (params.filter) {
      const accepted: StepTask[] = [];
      const released: string[] = [];
      for (const t of claimed) {
        if (params.filter(t)) accepted.push(t);
        else released.push(t.id);
      }
      if (released.length > 0) {
        await execRaw(
          this.db,
          sql.raw(
            `UPDATE wf_step_queue SET status = 'pending', claimed_by = NULL, claimed_at = NULL ` +
              `WHERE id IN (${released
                .map((id) => parseInt(id, 10))
                .filter(Number.isFinite)
                .join(",")})`,
          ),
        );
      }
      return accepted;
    }

    return claimed;
  }

  async complete(params: { taskId: string; result: unknown; durationMs: number }): Promise<void> {
    const now = this.clock.now();
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
    const now = this.clock.now();
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
      conditions.push(
        lt(stepQueue.claimedAt, new Date(this.clock.currentTimeMs() - params.staleTimeoutMs)),
      );
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

  async metrics(params: { since: Date; until?: Date }): Promise<{
    pending: number;
    running: number;
    completed: number;
    failed: number;
    avgWaitMs: number;
    avgExecMs: number;
    p95ExecMs: number;
  }> {
    // postgres-js refuses to bind Date directly against an untyped
    // parameter; pass ISO strings and let Postgres cast via ::timestamptz.
    const since = params.since.toISOString();
    // When caller omits `until`, use the DB's `NOW()` inside the query
    // instead of an app-side `this.clock.now()`. Rows are inserted with the
    // DB's own `created_at` — pulling `until` from the same clock
    // avoids the app-vs-DB skew that previously dropped just-inserted
    // rows out of the BETWEEN filter (the 5s buffer this replaces).
    const untilExpr = params.until ? sql`${params.until.toISOString()}::timestamptz` : sql`NOW()`;
    // Status uses the column that defines membership-in-window: createdAt
    // for pending, claimedAt for running, completedAt for terminal. A
    // single window-aware query per status keeps Postgres-side work minimal.
    // Namespace filter stays AND'd on top — no double-scope confusion.
    const nsFilter = this.namespace ? sql` AND namespace = ${this.namespace}` : sql``;

    const [counts, latency] = await Promise.all([
      execRaw(
        this.db,
        sql`
          SELECT status, COUNT(*) as count
          FROM wf_step_queue
          WHERE (
            (status = 'pending'   AND created_at   BETWEEN ${since}::timestamptz AND ${untilExpr}) OR
            (status = 'running'   AND claimed_at   BETWEEN ${since}::timestamptz AND ${untilExpr}) OR
            (status IN ('completed', 'failed') AND completed_at BETWEEN ${since}::timestamptz AND ${untilExpr})
          )${nsFilter}
          GROUP BY status
        `,
      ),
      // Latency stats pool completed + failed in the window. EXTRACT EPOCH
      // returns seconds — multiply by 1000 for ms. percentile_cont is the
      // SQL standard linear-interp percentile, matching the in-memory
      // implementation.
      execRaw(
        this.db,
        sql`
          SELECT
            AVG(EXTRACT(EPOCH FROM (claimed_at - created_at)) * 1000) AS avg_wait_ms,
            AVG(duration_ms)                                           AS avg_exec_ms,
            PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration_ms)  AS p95_exec_ms
          FROM wf_step_queue
          WHERE status IN ('completed', 'failed')
            AND completed_at BETWEEN ${since}::timestamptz AND ${untilExpr}
            ${nsFilter}
        `,
      ),
    ]);

    const result = {
      pending: 0,
      running: 0,
      completed: 0,
      failed: 0,
      avgWaitMs: 0,
      avgExecMs: 0,
      p95ExecMs: 0,
    };
    for (const row of counts) {
      const s = row.status as "pending" | "running" | "completed" | "failed";
      if (s === "pending" || s === "running" || s === "completed" || s === "failed") {
        result[s] = Number(row.count);
      }
    }
    const lat = latency[0];
    if (lat) {
      result.avgWaitMs = lat.avg_wait_ms != null ? Number(lat.avg_wait_ms) : 0;
      result.avgExecMs = lat.avg_exec_ms != null ? Number(lat.avg_exec_ms) : 0;
      result.p95ExecMs = lat.p95_exec_ms != null ? Number(lat.p95_exec_ms) : 0;
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
