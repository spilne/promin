import { randomUUID } from "node:crypto";
import type { StepQueue, StepTask, FairnessPolicy } from "@promin/workflow";
import type { SqliteDatabase } from "./sqlite-database.ts";

/**
 * Persistent step queue backed by SQLite.
 *
 * Implements idempotent enqueue on (workflowId, stepName), capability-based
 * routing, priority ordering, heartbeat, and stuck-task requeue.
 *
 * An `active_key` column with a partial unique index enforces the
 * single-active-task-per-(workflowId, stepName) invariant without a
 * separate lookup table.
 *
 * Schema (auto-created on first use):
 *   CREATE TABLE promin_step_tasks (
 *     id TEXT PRIMARY KEY, workflow_id, step_name, needs TEXT (JSON),
 *     priority, input TEXT (JSON), prev_results TEXT (JSON),
 *     attempt, status, version, namespace, created_at, claimed_at,
 *     completed_at, result TEXT (JSON), error, duration_ms,
 *     last_heartbeat, active_key TEXT UNIQUE WHERE NOT NULL
 *   )
 *
 * @example
 * ```ts
 * import { Database } from "bun:sqlite";
 * const db = new Database("tasks.db");
 * const queue = SqliteStepQueue.make({ db });
 * const id = await queue.enqueue({ workflowId: "wf-1", stepName: "charge", input: {}, prevResults: {} });
 * const [task] = await queue.claim({ limit: 1 });
 * await queue.complete({ taskId: task.id, result: "ok", durationMs: 50 });
 * ```
 */
export class SqliteStepQueue implements StepQueue {
  private readonly _table: string;

  private constructor(
    private readonly db: SqliteDatabase,
    table: string,
  ) {
    this._table = table;
    this._setup();
  }

  static make(params: {
    db: SqliteDatabase;
    /** Override the table name (default: `promin_step_tasks`). */
    table?: string;
  }): SqliteStepQueue {
    return new SqliteStepQueue(params.db, params.table ?? "promin_step_tasks");
  }

  private _setup(): void {
    const t = this._table;
    this.db.run(`
      CREATE TABLE IF NOT EXISTS ${t} (
        id             TEXT    NOT NULL PRIMARY KEY,
        workflow_id    TEXT    NOT NULL,
        step_name      TEXT    NOT NULL,
        needs          TEXT    NOT NULL DEFAULT '[]',
        priority       INTEGER NOT NULL DEFAULT 5,
        input          TEXT    NOT NULL,
        prev_results   TEXT    NOT NULL DEFAULT '{}',
        attempt        INTEGER NOT NULL DEFAULT 1,
        status         TEXT    NOT NULL DEFAULT 'pending',
        version        TEXT,
        namespace      TEXT,
        created_at     INTEGER NOT NULL,
        claimed_at     INTEGER,
        completed_at   INTEGER,
        result         TEXT,
        error          TEXT,
        duration_ms    INTEGER,
        last_heartbeat INTEGER,
        active_key     TEXT
      )
    `);
    this.db.run(
      `CREATE UNIQUE INDEX IF NOT EXISTS ${t}_active ON ${t} (active_key) WHERE active_key IS NOT NULL`,
    );
    this.db.run(
      `CREATE INDEX IF NOT EXISTS ${t}_status_pri ON ${t} (status, priority DESC, created_at ASC)`,
    );
  }

  private _activeKey(namespace: string | undefined, workflowId: string, stepName: string): string {
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
    const key = this._activeKey(params.namespace, params.workflowId, params.stepName);

    return this.db.transaction((): string => {
      const existing = this.db
        .query<{ id: string }>(`SELECT id FROM ${this._table} WHERE active_key = ?`)
        .get(key);
      if (existing) return existing.id;

      const id = randomUUID();
      this.db
        .query(
          `INSERT INTO ${this._table}
           (id, workflow_id, step_name, needs, priority, input, prev_results,
            attempt, status, version, namespace, created_at, active_key)
           VALUES (?, ?, ?, ?, ?, ?, ?, 1, 'pending', ?, ?, ?, ?)`,
        )
        .run(
          id,
          params.workflowId,
          params.stepName,
          JSON.stringify(params.needs ?? []),
          params.priority ?? 5,
          JSON.stringify(params.input),
          JSON.stringify(params.prevResults),
          params.version ?? null,
          params.namespace ?? null,
          Date.now(),
          key,
        );
      return id;
    })();
  }

  async claim(params: {
    capabilities?: readonly string[];
    limit: number;
    fairness?: FairnessPolicy;
    filter?: (task: StepTask) => boolean;
  }): Promise<StepTask[]> {
    const caps = new Set(params.capabilities ?? []);
    const fairness = params.fairness ?? "strict-priority";

    // Load all pending tasks — SQLite is local so this is fine for reasonable queue sizes.
    const rows = this.db
      .query<TaskRow>(
        `SELECT * FROM ${this._table} WHERE status = 'pending'
         ORDER BY priority DESC, created_at ASC`,
      )
      .all();

    // Capability subset check: task.needs ⊆ caps
    const canHandle = (needs: string[]): boolean => {
      for (const n of needs) {
        if (!caps.has(n)) return false;
      }
      return true;
    };

    const eligible = rows.filter((r) => canHandle(JSON.parse(r.needs) as string[]));

    let ordered: TaskRow[];
    switch (fairness) {
      case "round-robin": {
        const byWf = new Map<string, TaskRow[]>();
        for (const r of eligible) {
          if (!byWf.has(r.workflow_id)) byWf.set(r.workflow_id, []);
          byWf.get(r.workflow_id)!.push(r);
        }
        ordered = [];
        const queues = [...byWf.values()];
        let round = 0;
        while (ordered.length < eligible.length) {
          let added = false;
          for (const wfRows of queues) {
            if (round < wfRows.length) {
              ordered.push(wfRows[round]!);
              added = true;
            }
          }
          if (!added) break;
          round++;
        }
        break;
      }
      case "weighted":
        ordered = eligible
          .map((r) => ({ r, score: r.priority * (0.5 + Math.random()) }))
          .sort((a, b) => b.score - a.score)
          .map((x) => x.r);
        break;
      default:
        ordered = eligible;
    }

    const claimed: StepTask[] = [];
    const now = Date.now();

    for (const row of ordered) {
      if (claimed.length >= params.limit) break;

      const task = rowToTask(row);
      if (params.filter && !params.filter(task)) continue;

      this.db
        .query(
          `UPDATE ${this._table}
           SET status = 'running', claimed_at = ?, last_heartbeat = ?
           WHERE id = ? AND status = 'pending'`,
        )
        .run(now, now, row.id);

      claimed.push({ ...task, status: "running" });
    }

    return claimed;
  }

  async complete(params: { taskId: string; result: unknown; durationMs: number }): Promise<void> {
    const now = Date.now();
    this.db
      .query(
        `UPDATE ${this._table}
         SET status = 'completed', result = ?, duration_ms = ?,
             completed_at = ?, active_key = NULL
         WHERE id = ?`,
      )
      .run(JSON.stringify(params.result), params.durationMs, now, params.taskId);
  }

  async fail(params: { taskId: string; error: string; durationMs: number }): Promise<void> {
    const now = Date.now();
    this.db
      .query(
        `UPDATE ${this._table}
         SET status = 'failed', error = ?, duration_ms = ?,
             completed_at = ?, active_key = NULL
         WHERE id = ?`,
      )
      .run(params.error, params.durationMs, now, params.taskId);
  }

  async heartbeat(params: { taskId: string }): Promise<void> {
    this.db
      .query(`UPDATE ${this._table} SET last_heartbeat = ? WHERE id = ? AND status = 'running'`)
      .run(Date.now(), params.taskId);
  }

  async requeueStuck(params: { claimedBy?: string; staleTimeoutMs?: number }): Promise<number> {
    const now = Date.now();
    let count = 0;

    if (params.claimedBy !== undefined) {
      // Not tracked in SQLite (no claimed_by column) — skip worker-based requeue
    }

    if (params.staleTimeoutMs !== undefined) {
      const cutoff = now - params.staleTimeoutMs;
      // last_heartbeat falls back to claimed_at when no heartbeat has been sent
      const rows = this.db
        .query<{ id: string }>(
          `SELECT id FROM ${this._table}
           WHERE status = 'running'
             AND COALESCE(last_heartbeat, claimed_at) < ?`,
        )
        .all(cutoff);

      for (const { id } of rows) {
        this.db
          .query(
            `UPDATE ${this._table}
             SET status = 'pending', claimed_at = NULL, last_heartbeat = NULL
             WHERE id = ? AND status = 'running'`,
          )
          .run(id);
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
    const untilMs = (params.until ?? new Date()).getTime();

    const pending =
      this.db
        .query<{ n: number }>(
          `SELECT COUNT(*) AS n FROM ${this._table}
           WHERE status = 'pending' AND created_at >= ? AND created_at <= ?`,
        )
        .get(sinceMs, untilMs)?.n ?? 0;

    const running =
      this.db
        .query<{ n: number }>(
          `SELECT COUNT(*) AS n FROM ${this._table}
           WHERE status = 'running' AND claimed_at >= ? AND claimed_at <= ?`,
        )
        .get(sinceMs, untilMs)?.n ?? 0;

    const completed =
      this.db
        .query<{ n: number }>(
          `SELECT COUNT(*) AS n FROM ${this._table}
           WHERE status = 'completed' AND completed_at >= ? AND completed_at <= ?`,
        )
        .get(sinceMs, untilMs)?.n ?? 0;

    const failed =
      this.db
        .query<{ n: number }>(
          `SELECT COUNT(*) AS n FROM ${this._table}
           WHERE status = 'failed' AND completed_at >= ? AND completed_at <= ?`,
        )
        .get(sinceMs, untilMs)?.n ?? 0;

    // Latency stats over terminal tasks in the window
    const terminalRows = this.db
      .query<{ created_at: number; claimed_at: number | null; duration_ms: number | null }>(
        `SELECT created_at, claimed_at, duration_ms FROM ${this._table}
         WHERE status IN ('completed', 'failed')
           AND completed_at >= ? AND completed_at <= ?`,
      )
      .all(sinceMs, untilMs);

    if (terminalRows.length === 0) {
      return { pending, running, completed, failed, avgWaitMs: 0, avgExecMs: 0, p95ExecMs: 0 };
    }

    let waitSum = 0;
    let waitN = 0;
    let execSum = 0;
    const execTimes: number[] = [];

    for (const r of terminalRows) {
      if (r.claimed_at != null) {
        waitSum += r.claimed_at - r.created_at;
        waitN++;
      }
      if (r.duration_ms != null) {
        execSum += r.duration_ms;
        execTimes.push(r.duration_ms);
      }
    }

    return {
      pending,
      running,
      completed,
      failed,
      avgWaitMs: waitN > 0 ? waitSum / waitN : 0,
      avgExecMs: execTimes.length > 0 ? execSum / execTimes.length : 0,
      p95ExecMs: execTimes.length > 0 ? percentile(execTimes, 0.95) : 0,
    };
  }
}

// ---------------------------------------------------------------------------
// Row helpers
// ---------------------------------------------------------------------------

interface TaskRow {
  id: string;
  workflow_id: string;
  step_name: string;
  needs: string;
  priority: number;
  input: string;
  prev_results: string;
  attempt: number;
  status: string;
  version: string | null;
  namespace: string | null;
  created_at: number;
  claimed_at: number | null;
  completed_at: number | null;
  result: string | null;
  error: string | null;
  duration_ms: number | null;
  last_heartbeat: number | null;
  active_key: string | null;
}

function rowToTask(row: TaskRow): StepTask {
  return {
    id: row.id,
    workflowId: row.workflow_id,
    stepName: row.step_name,
    needs: JSON.parse(row.needs) as string[],
    priority: row.priority,
    input: JSON.parse(row.input),
    prevResults: JSON.parse(row.prev_results),
    attempt: row.attempt,
    status: row.status as StepTask["status"],
    createdAt: new Date(row.created_at),
    version: row.version ?? undefined,
  };
}

function percentile(values: number[], p: number): number {
  const sorted = [...values].sort((a, b) => a - b);
  const rank = p * (sorted.length - 1);
  const lo = Math.floor(rank);
  const hi = Math.ceil(rank);
  if (lo === hi) return sorted[lo]!;
  return sorted[lo]! + (sorted[hi]! - sorted[lo]!) * (rank - lo);
}
