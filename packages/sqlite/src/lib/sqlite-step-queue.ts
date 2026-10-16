import { randomUUID } from "node:crypto";
import {
  DEFAULT_MAX_DELIVERIES,
  deadLetterError,
  percentileCont,
  SystemWallClock,
  type StepQueue,
  type StepQueueClaimParams,
  type StepQueueEnqueueParams,
  type StepQueueRequeueParams,
  type StepQueueRequeueResult,
  type StepTask,
  type StepTaskRecord,
  type WallClock,
} from "@promin/workflow";
import type { SqliteDatabase } from "./sqlite-database.ts";

/**
 * Persistent step queue backed by SQLite.
 *
 * Implements idempotent enqueue on (workflowId, stepName), capability /
 * step-name / version routing inside the claim, priority ordering,
 * concurrency keys, heartbeat, release, delivery counting with
 * dead-lettering, and purge.
 *
 * An `active_key` column with a partial unique index enforces the
 * single-active-task-per-(workflowId, stepName) invariant without a
 * separate lookup table.
 *
 * Schema (auto-created on first use):
 *   CREATE TABLE promin_step_tasks (
 *     id TEXT PRIMARY KEY, workflow_id, step_name, needs TEXT (JSON),
 *     priority, input TEXT (JSON), prev_results TEXT (JSON),
 *     attempt, deliveries, status, version, namespace, created_at,
 *     claimed_at, claimed_by, claim_token,
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
 * const [task] = await queue.claim({ workerId: "w-1", limit: 1 });
 * await queue.complete({ taskId: task.id, claimToken: task.claimToken, result: "ok", durationMs: 50 });
 * ```
 */
export class SqliteStepQueue implements StepQueue {
  private readonly _table: string;
  private readonly clock: WallClock;
  private readonly maxDeliveries: number;

  private constructor(
    private readonly db: SqliteDatabase,
    params: { table: string; clock: WallClock; maxDeliveries: number },
  ) {
    this._table = params.table;
    this.clock = params.clock;
    this.maxDeliveries = params.maxDeliveries;
    this._setup();
  }

  static make(params: {
    db: SqliteDatabase;
    /** Override the table name (default: `promin_step_tasks`). */
    table?: string;
    /**
     * Time source for `created_at` / `claimed_at` / heartbeat / completion
     * timestamps. Defaults to `SystemWallClock`. Tests pass a `FakeWallClock` so
     * `clock.advance(ms)` drives the queue's time math deterministically.
     */
    clock?: WallClock;
    /**
     * Deliveries after which `requeueStuck` dead-letters a task instead of
     * requeueing it. Default: `DEFAULT_MAX_DELIVERIES` (10).
     */
    maxDeliveries?: number;
  }): SqliteStepQueue {
    return new SqliteStepQueue(params.db, {
      table: params.table ?? "promin_step_tasks",
      clock: params.clock ?? SystemWallClock,
      maxDeliveries: params.maxDeliveries ?? DEFAULT_MAX_DELIVERIES,
    });
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
        metadata       TEXT,
        created_at     INTEGER NOT NULL,
        claimed_at     INTEGER,
        claim_token    TEXT,
        completed_at   INTEGER,
        result         TEXT,
        error          TEXT,
        duration_ms    INTEGER,
        last_heartbeat INTEGER,
        active_key     TEXT
      )
    `);
    // Migrate tables that predate a column. sqlite has no ADD COLUMN IF
    // NOT EXISTS before 3.35; older dbs throw "duplicate column" — we
    // swallow that exact failure mode and let any other error propagate.
    for (const stmt of [
      `ALTER TABLE ${t} ADD COLUMN metadata TEXT`,
      `ALTER TABLE ${t} ADD COLUMN concurrency_key TEXT`,
      `ALTER TABLE ${t} ADD COLUMN concurrency_scope TEXT`,
      `ALTER TABLE ${t} ADD COLUMN concurrency_limit INTEGER`,
      `ALTER TABLE ${t} ADD COLUMN claim_token TEXT`,
      `ALTER TABLE ${t} ADD COLUMN claimed_by TEXT`,
      `ALTER TABLE ${t} ADD COLUMN deliveries INTEGER NOT NULL DEFAULT 0`,
    ]) {
      try {
        this.db.run(stmt);
      } catch (e) {
        if (!String(e).includes("duplicate column")) throw e;
      }
    }
    // Active keys used to include the namespace; the dedupe key is now
    // (workflowId, stepName). Rows whose rewrite would collide keep the old
    // key until they settle.
    this.db.run(
      `UPDATE OR IGNORE ${t} SET active_key = workflow_id || '::' || step_name
       WHERE active_key IS NOT NULL AND active_key <> workflow_id || '::' || step_name`,
    );
    this.db.run(
      `CREATE INDEX IF NOT EXISTS ${t}_concurrency_running ON ${t} (concurrency_scope, concurrency_key) WHERE status = 'running' AND concurrency_key IS NOT NULL`,
    );
    this.db.run(
      `CREATE UNIQUE INDEX IF NOT EXISTS ${t}_active ON ${t} (active_key) WHERE active_key IS NOT NULL`,
    );
    this.db.run(
      `CREATE INDEX IF NOT EXISTS ${t}_status_pri ON ${t} (status, priority DESC, created_at ASC)`,
    );
    this.db.run(
      `CREATE INDEX IF NOT EXISTS ${t}_terminal ON ${t} (completed_at) WHERE status IN ('completed', 'failed')`,
    );
  }

  private _activeKey(workflowId: string, stepName: string): string {
    return `${workflowId}::${stepName}`;
  }

  async enqueue(params: StepQueueEnqueueParams): Promise<string> {
    const key = this._activeKey(params.workflowId, params.stepName);

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
            attempt, deliveries, status, version, namespace, metadata,
            concurrency_key, concurrency_scope, concurrency_limit,
            created_at, active_key)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, 'pending', ?, ?, ?, ?, ?, ?, ?, ?)`,
        )
        .run(
          id,
          params.workflowId,
          params.stepName,
          JSON.stringify(params.needs ?? []),
          params.priority ?? 5,
          JSON.stringify(params.input),
          JSON.stringify(params.prevResults),
          params.attempt ?? 1,
          params.version ?? null,
          params.namespace ?? null,
          params.metadata !== undefined ? JSON.stringify(params.metadata) : null,
          params.concurrencyKey ?? null,
          params.concurrencyScope ?? null,
          params.concurrencyLimit ?? null,
          this.clock.currentTimeMs(),
          key,
        );
      return id;
    })();
  }

  async claim(params: StepQueueClaimParams): Promise<StepTask[]> {
    const caps = new Set(params.capabilities ?? []);
    const stepNames = params.stepNames ? new Set(params.stepNames) : undefined;
    const versions = params.versions ? new Set(params.versions) : undefined;
    if (stepNames && stepNames.size === 0) return [];

    const claimable = (row: TaskRow): boolean => {
      if (stepNames && !stepNames.has(row.step_name)) return false;
      if (versions && row.version !== null && !versions.has(row.version)) return false;
      for (const n of JSON.parse(row.needs) as string[]) {
        if (!caps.has(n)) return false;
      }
      return true;
    };

    // One write transaction: no other connection can claim between our
    // read of the pending rows and the updates below.
    return this.db.transaction((): StepTask[] => {
      const rows = this.db
        .query<TaskRow>(
          `SELECT * FROM ${this._table} WHERE status = 'pending'
           ORDER BY priority DESC, created_at ASC`,
        )
        .all();

      // Per-(scope, key) running counter — counts both already-running
      // tasks and tasks claimed earlier in this call so a single `claim()`
      // batch can't itself violate a limit.
      const runningPerKey = new Map<string, number>();
      const runningRows = this.db
        .query<{ concurrency_scope: string | null; concurrency_key: string | null }>(
          `SELECT concurrency_scope, concurrency_key FROM ${this._table}
           WHERE status = 'running' AND concurrency_key IS NOT NULL`,
        )
        .all();
      for (const r of runningRows) {
        if (!r.concurrency_scope || !r.concurrency_key) continue;
        const k = `${r.concurrency_scope}::${r.concurrency_key}`;
        runningPerKey.set(k, (runningPerKey.get(k) ?? 0) + 1);
      }

      const claimed: StepTask[] = [];
      const now = this.clock.currentTimeMs();

      for (const row of rows) {
        if (claimed.length >= params.limit) break;
        if (!claimable(row)) continue;
        if (row.concurrency_key && row.concurrency_scope && row.concurrency_limit != null) {
          const k = `${row.concurrency_scope}::${row.concurrency_key}`;
          const running = runningPerKey.get(k) ?? 0;
          if (running >= row.concurrency_limit) continue;
          runningPerKey.set(k, running + 1);
        }

        const claimToken = randomUUID();
        this.db
          .query(
            `UPDATE ${this._table}
             SET status = 'running', claimed_at = ?, claimed_by = ?, last_heartbeat = ?,
                 claim_token = ?, deliveries = deliveries + 1
             WHERE id = ? AND status = 'pending'`,
          )
          .run(now, params.workerId, now, claimToken, row.id);
        if (this._changes() === 0) continue;

        claimed.push({
          ...rowToTask(row),
          status: "running",
          claimToken,
          deliveries: row.deliveries + 1,
        });
      }
      return claimed;
    })();
  }

  async release(params: { taskId: string; claimToken: string }): Promise<boolean> {
    this.db
      .query(
        `UPDATE ${this._table}
         SET status = 'pending', claimed_at = NULL, claimed_by = NULL, last_heartbeat = NULL,
             claim_token = NULL, deliveries = MAX(deliveries - 1, 0)
         WHERE id = ? AND status = 'running' AND claim_token = ?`,
      )
      .run(params.taskId, params.claimToken);
    return this._changes() > 0;
  }

  async get(taskId: string): Promise<StepTaskRecord | undefined> {
    const row = this.db.query<TaskRow>(`SELECT * FROM ${this._table} WHERE id = ?`).get(taskId);
    return row ? rowToRecord(row) : undefined;
  }

  async complete(params: {
    taskId: string;
    claimToken?: string;
    result: unknown;
    durationMs: number;
  }): Promise<boolean> {
    const now = this.clock.currentTimeMs();
    this.db
      .query(
        `UPDATE ${this._table}
         SET status = 'completed', result = ?, duration_ms = ?,
             completed_at = ?, active_key = NULL
         WHERE id = ? AND status = 'running'
           AND (? IS NULL OR claim_token = ?)`,
      )
      .run(
        JSON.stringify(params.result),
        params.durationMs,
        now,
        params.taskId,
        params.claimToken ?? null,
        params.claimToken ?? null,
      );
    return this._changes() > 0;
  }

  async fail(params: {
    taskId: string;
    claimToken?: string;
    error: string;
    durationMs: number;
  }): Promise<boolean> {
    const now = this.clock.currentTimeMs();
    this.db
      .query(
        `UPDATE ${this._table}
         SET status = 'failed', error = ?, duration_ms = ?,
             completed_at = ?, active_key = NULL
         WHERE id = ? AND status = 'running'
           AND (? IS NULL OR claim_token = ?)`,
      )
      .run(
        params.error,
        params.durationMs,
        now,
        params.taskId,
        params.claimToken ?? null,
        params.claimToken ?? null,
      );
    return this._changes() > 0;
  }

  async heartbeat(params: { taskId: string; claimToken?: string }): Promise<boolean> {
    this.db
      .query(
        `UPDATE ${this._table}
         SET last_heartbeat = ?
         WHERE id = ? AND status = 'running'
           AND (? IS NULL OR claim_token = ?)`,
      )
      .run(
        this.clock.currentTimeMs(),
        params.taskId,
        params.claimToken ?? null,
        params.claimToken ?? null,
      );
    return this._changes() > 0;
  }

  async requeueStuck(params: StepQueueRequeueParams): Promise<StepQueueRequeueResult> {
    const now = this.clock.currentTimeMs();
    const t = this._table;
    const [match, arg] =
      params.mode === "worker"
        ? [`claimed_by = ?`, params.workerId]
        : // last_heartbeat falls back to claimed_at when no heartbeat was sent
          [`COALESCE(last_heartbeat, claimed_at) < ?`, now - params.olderThanMs];

    return this.db.transaction((): StepQueueRequeueResult => {
      this.db
        .query(
          `UPDATE ${t}
           SET status = 'failed', error = ?, completed_at = ?, active_key = NULL,
               claim_token = NULL, last_heartbeat = NULL
           WHERE status = 'running' AND ${match} AND deliveries >= ?`,
        )
        .run(deadLetterError(this.maxDeliveries), now, arg, this.maxDeliveries);
      const deadLettered = this._changes();

      this.db
        .query(
          `UPDATE ${t}
           SET status = 'pending', claimed_at = NULL, claimed_by = NULL, last_heartbeat = NULL,
               claim_token = NULL
           WHERE status = 'running' AND ${match}`,
        )
        .run(arg);
      return { requeued: this._changes(), deadLettered };
    })();
  }

  async purge(params: { completedBefore: Date }): Promise<number> {
    this.db
      .query(
        `DELETE FROM ${this._table}
         WHERE status IN ('completed', 'failed') AND completed_at < ?`,
      )
      .run(params.completedBefore.getTime());
    return this._changes();
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
      p95ExecMs: execTimes.length > 0 ? percentileCont({ values: execTimes, p: 0.95 }) : 0,
    };
  }

  private _changes(): number {
    return this.db.query<{ changes: number }>(`SELECT changes() AS changes`).get()?.changes ?? 0;
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
  deliveries: number;
  status: string;
  version: string | null;
  namespace: string | null;
  metadata: string | null;
  concurrency_key: string | null;
  concurrency_scope: string | null;
  concurrency_limit: number | null;
  created_at: number;
  claimed_at: number | null;
  claimed_by: string | null;
  claim_token: string | null;
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
    deliveries: row.deliveries,
    status: row.status as StepTask["status"],
    createdAt: new Date(row.created_at),
    claimToken: row.claim_token ?? undefined,
    version: row.version ?? undefined,
    metadata:
      row.metadata != null ? (JSON.parse(row.metadata) as Record<string, unknown>) : undefined,
    concurrencyKey: row.concurrency_key ?? undefined,
    concurrencyScope: row.concurrency_scope ?? undefined,
    concurrencyLimit: row.concurrency_limit ?? undefined,
  };
}

function rowToRecord(row: TaskRow): StepTaskRecord {
  return {
    ...rowToTask(row),
    claimedBy: row.claimed_by ?? undefined,
    claimedAt: row.claimed_at != null ? new Date(row.claimed_at) : undefined,
    heartbeatAt: row.last_heartbeat != null ? new Date(row.last_heartbeat) : undefined,
    completedAt: row.completed_at != null ? new Date(row.completed_at) : undefined,
    result: row.result != null ? JSON.parse(row.result) : undefined,
    error: row.error ?? undefined,
    durationMs: row.duration_ms ?? undefined,
  };
}
