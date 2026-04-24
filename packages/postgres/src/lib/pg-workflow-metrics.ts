// ---------------------------------------------------------------------------
// PgWorkflowMetrics — workflow state duration metrics derived from existing data
//
// No new tables or instrumentation required. Derives all durations from the
// existing wf_workflows and wf_workflow_steps rows:
//
//   pendingMs   = startedAt - createdAt          (exact, from wf_workflows)
//   suspendedMs = SUM of sleep + signal wait durations (from wf_workflow_steps)
//   runningMs   = totalMs - pendingMs - suspendedMs
//   totalMs     = completedAt - createdAt (or NOW() for in-progress workflows)
//
// Sleep suspended time: step.wake_at - step.started_at
// Signal suspended time: step.completed_at - step.started_at  (waiting_for_signal steps)
// Both use NOW() as the upper bound while the step is still active.
// ---------------------------------------------------------------------------

import { sql } from "drizzle-orm";
import { type DrizzleDb, execRaw } from "./drizzle-db.ts";
import { StepTypeIds } from "./workflow-lookups.ts";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface WorkflowMetrics {
  workflowId: string;
  workflowName: string;
  workflowType?: string;
  status: string;
  /** Time from creation to first step execution (ms). 0 if workflow has not started. */
  pendingMs: number;
  /** Cumulative time actively executing steps (ms). */
  runningMs: number;
  /** Cumulative time waiting in sleep or signal-wait steps (ms). */
  suspendedMs: number;
  /** Wall-clock end-to-end duration: completedAt - createdAt, or NOW() - createdAt (ms). */
  totalMs: number;
  /** runningMs / totalMs — fraction of wall time spent doing real work (0–1). */
  activeRatio: number;
}

export interface MetricsQuery {
  workflowId?: string;
  workflowType?: string;
  workflowName?: string;
  /** Filter to workflows whose completedAt >= from. */
  from?: Date;
  /** Filter to workflows whose completedAt < to. */
  to?: Date;
  /** Filter by status name: 'pending' | 'running' | 'suspended' | 'completed' | 'failed'. */
  status?: string;
  limit?: number;
  offset?: number;
}

export interface WorkflowMetricsSummary {
  group: string;
  count: number;
  avgPendingMs: number;
  avgRunningMs: number;
  avgSuspendedMs: number;
  p50TotalMs: number;
  p95TotalMs: number;
  p99TotalMs: number;
}

// ---------------------------------------------------------------------------
// Core SQL fragments
// ---------------------------------------------------------------------------

// Suspended time per workflow: sum of sleep + signal-wait durations.
// Sleep: wake_at - started_at  (or NOW() if still sleeping)
// Signal: completed_at - started_at  (or NOW() if still waiting)
const SUSPENDED_SUBQUERY = (workflowIdExpr: string) => `
  SELECT COALESCE(SUM(
    EXTRACT(EPOCH FROM (
      CASE
        WHEN s.step_type_id = ${StepTypeIds.id.sleep}
          THEN COALESCE(s.wake_at, NOW()) - s.started_at
        WHEN s.step_type_id = ${StepTypeIds.id.signal}
          THEN COALESCE(s.completed_at, NOW()) - s.started_at
        ELSE INTERVAL '0'
      END
    )) * 1000
  ), 0)::BIGINT
  FROM wf_workflow_steps s
  WHERE s.workflow_id = ${workflowIdExpr}
    AND s.step_type_id IN (${StepTypeIds.id.sleep}, ${StepTypeIds.id.signal})
    AND s.started_at IS NOT NULL
`;

// ---------------------------------------------------------------------------
// PgWorkflowMetrics
// ---------------------------------------------------------------------------

/**
 * Read-only metrics derived from existing workflow and step data.
 * Requires no additional tables or configuration changes.
 *
 * @example
 * ```ts
 * const m = await PgWorkflowMetrics.getMetrics(db, "wf-123");
 * console.log(`pending ${m.pendingMs}ms, running ${m.runningMs}ms`);
 *
 * const summary = await PgWorkflowMetrics.queryMetricsSummary(db, { groupBy: "type" });
 * ```
 */
export class PgWorkflowMetrics {
  /** Get detailed duration metrics for a single workflow. Returns null if not found. */
  static async getMetrics(db: DrizzleDb, workflowId: string): Promise<WorkflowMetrics | null> {
    const rows = await execRaw(
      db,
      sql`
        SELECT
          w.workflow_id,
          w.workflow_name,
          w.workflow_type,
          ws.name                                                         AS status,
          COALESCE(
            EXTRACT(EPOCH FROM (w.started_at - w.created_at)) * 1000, 0
          )::BIGINT                                                       AS pending_ms,
          (${sql.raw(SUSPENDED_SUBQUERY("w.workflow_id"))})               AS suspended_ms,
          (EXTRACT(EPOCH FROM (
            COALESCE(w.completed_at, NOW()) - w.created_at
          )) * 1000)::BIGINT                                              AS total_ms
        FROM wf_workflows w
        JOIN wf_workflow_status ws ON ws.id = w.status_id
        WHERE w.workflow_id = ${workflowId}
      `,
    );
    if (rows.length === 0) return null;
    return rowToMetrics(rows[0]);
  }

  /**
   * Query metrics for multiple workflows with optional filters.
   * Ordered by createdAt descending (newest first).
   */
  static async queryMetrics(db: DrizzleDb, query: MetricsQuery = {}): Promise<WorkflowMetrics[]> {
    const conditions: SQL[] = [];
    if (query.workflowId) conditions.push(sql`w.workflow_id = ${query.workflowId}`);
    if (query.workflowType) conditions.push(sql`w.workflow_type = ${query.workflowType}`);
    if (query.workflowName) conditions.push(sql`w.workflow_name = ${query.workflowName}`);
    if (query.from) conditions.push(sql`w.completed_at >= ${query.from}`);
    if (query.to) conditions.push(sql`w.completed_at < ${query.to}`);
    if (query.status) conditions.push(sql`ws.name = ${query.status}`);

    const where =
      conditions.length > 0 ? sql`WHERE ${conditions.reduce((a, b) => sql`${a} AND ${b}`)}` : sql``;

    const limit = query.limit ?? 100;
    const offset = query.offset ?? 0;

    const rows = await execRaw(
      db,
      sql`
        SELECT
          w.workflow_id,
          w.workflow_name,
          w.workflow_type,
          ws.name                                                         AS status,
          COALESCE(
            EXTRACT(EPOCH FROM (w.started_at - w.created_at)) * 1000, 0
          )::BIGINT                                                       AS pending_ms,
          (${sql.raw(SUSPENDED_SUBQUERY("w.workflow_id"))})               AS suspended_ms,
          (EXTRACT(EPOCH FROM (
            COALESCE(w.completed_at, NOW()) - w.created_at
          )) * 1000)::BIGINT                                              AS total_ms
        FROM wf_workflows w
        JOIN wf_workflow_status ws ON ws.id = w.status_id
        ${where}
        ORDER BY w.created_at DESC
        LIMIT ${limit} OFFSET ${offset}
      `,
    );
    return rows.map(rowToMetrics);
  }

  /**
   * Aggregate metrics grouped by workflow type or name.
   * Includes p50 / p95 / p99 percentiles on total wall-clock duration.
   */
  static async queryMetricsSummary(
    db: DrizzleDb,
    query: MetricsQuery & { groupBy: "type" | "name" },
  ): Promise<WorkflowMetricsSummary[]> {
    const groupCol = query.groupBy === "type" ? sql`w.workflow_type` : sql`w.workflow_name`;

    const conditions: SQL[] = [];
    if (query.workflowType) conditions.push(sql`w.workflow_type = ${query.workflowType}`);
    if (query.workflowName) conditions.push(sql`w.workflow_name = ${query.workflowName}`);
    if (query.from) conditions.push(sql`w.completed_at >= ${query.from}`);
    if (query.to) conditions.push(sql`w.completed_at < ${query.to}`);
    if (query.status) conditions.push(sql`ws.name = ${query.status}`);

    const where =
      conditions.length > 0 ? sql`WHERE ${conditions.reduce((a, b) => sql`${a} AND ${b}`)}` : sql``;

    const rows = await execRaw(
      db,
      sql`
        WITH per_wf AS (
          SELECT
            COALESCE(${groupCol}, 'unknown')                               AS grp,
            COALESCE(
              EXTRACT(EPOCH FROM (w.started_at - w.created_at)) * 1000, 0
            )::BIGINT                                                       AS pending_ms,
            (${sql.raw(SUSPENDED_SUBQUERY("w.workflow_id"))})               AS suspended_ms,
            (EXTRACT(EPOCH FROM (
              COALESCE(w.completed_at, NOW()) - w.created_at
            )) * 1000)::BIGINT                                              AS total_ms
          FROM wf_workflows w
          JOIN wf_workflow_status ws ON ws.id = w.status_id
          ${where}
        )
        SELECT
          grp,
          COUNT(*)                                                          AS count,
          AVG(pending_ms)::BIGINT                                           AS avg_pending_ms,
          AVG(total_ms - pending_ms - suspended_ms)::BIGINT                 AS avg_running_ms,
          AVG(suspended_ms)::BIGINT                                         AS avg_suspended_ms,
          PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY total_ms)::BIGINT    AS p50_total_ms,
          PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY total_ms)::BIGINT    AS p95_total_ms,
          PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY total_ms)::BIGINT    AS p99_total_ms
        FROM per_wf
        GROUP BY grp
        ORDER BY count DESC
      `,
    );

    return rows.map((r: any) => ({
      group: r.grp as string,
      count: Number(r.count),
      avgPendingMs: Number(r.avg_pending_ms ?? 0),
      avgRunningMs: Number(r.avg_running_ms ?? 0),
      avgSuspendedMs: Number(r.avg_suspended_ms ?? 0),
      p50TotalMs: Number(r.p50_total_ms ?? 0),
      p95TotalMs: Number(r.p95_total_ms ?? 0),
      p99TotalMs: Number(r.p99_total_ms ?? 0),
    }));
  }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

type SQL = ReturnType<typeof sql>;

function rowToMetrics(r: any): WorkflowMetrics {
  const totalMs = Math.max(1, Number(r.total_ms ?? 0));
  const pendingMs = Number(r.pending_ms ?? 0);
  const suspendedMs = Number(r.suspended_ms ?? 0);
  const runningMs = Math.max(0, totalMs - pendingMs - suspendedMs);
  return {
    workflowId: r.workflow_id as string,
    workflowName: r.workflow_name as string,
    workflowType: r.workflow_type ?? undefined,
    status: r.status as string,
    pendingMs,
    runningMs,
    suspendedMs,
    totalMs,
    activeRatio: Math.min(1, runningMs / totalMs),
  };
}
