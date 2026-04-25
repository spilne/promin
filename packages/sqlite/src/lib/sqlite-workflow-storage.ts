import {
  FenceTokenMismatchError,
  type WorkflowStorage,
  type FenceToken,
  type FenceGuard,
  type WorkflowOrderBy,
} from "@promin/workflow";
import type {
  WorkflowState,
  WorkflowStatus,
  WorkflowRunSummary,
  SignalState,
  StepState,
} from "@promin/workflow";
import type {
  ActivityJournalStorage,
  JournaledSuspendStorage,
  JournalEntry,
  JournalStepType,
  JournalPhase,
} from "@promin/workflow";
import type { SqliteDatabase } from "./sqlite-database.ts";

/**
 * Persistent workflow storage backed by SQLite.
 *
 * Implements the full `WorkflowStorage` contract plus the optional
 * `ActivityJournalStorage` and `JournaledSuspendStorage` extensions.
 * Steps and tasks are stored as JSON blobs on the workflow row for
 * simplicity; run history is archived to a separate table on `startFreshRun`.
 *
 * Schema (auto-created on first use):
 *   promin_wf          — workflow records (steps as JSON)
 *   promin_wf_signals  — delivered signals
 *   promin_wf_locks    — advisory workflow locks with fence tokens
 *   promin_wf_runs     — archived run history
 *   promin_wf_journal  — activity journal for .journaled() steps
 *
 * @example
 * ```ts
 * import { Database } from "bun:sqlite";
 * const db = new Database("state.db");
 * const storage = SqliteWorkflowStorage.make({ db });
 * await storage.createWorkflow({ workflowId: "wf-1", workflowName: "onboard", input: {} });
 * ```
 */
export class SqliteWorkflowStorage
  implements WorkflowStorage, ActivityJournalStorage, JournaledSuspendStorage
{
  private readonly _t: string;
  private _nextToken = 1;

  private constructor(
    private readonly db: SqliteDatabase,
    table: string,
  ) {
    this._t = table;
    this._setup();
  }

  static make(params: {
    db: SqliteDatabase;
    /** Override the table prefix (default: `promin_wf`). */
    tablePrefix?: string;
  }): SqliteWorkflowStorage {
    return new SqliteWorkflowStorage(params.db, params.tablePrefix ?? "promin_wf");
  }

  private _setup(): void {
    const t = this._t;
    this.db.run(`
      CREATE TABLE IF NOT EXISTS ${t} (
        workflow_id          TEXT    NOT NULL PRIMARY KEY,
        workflow_name        TEXT    NOT NULL,
        workflow_type        TEXT,
        parent_workflow_id   TEXT,
        namespace            TEXT,
        status               TEXT    NOT NULL DEFAULT 'pending',
        version              TEXT,
        run                  INTEGER NOT NULL DEFAULT 1,
        input                TEXT    NOT NULL,
        result               TEXT,
        error                TEXT,
        metadata             TEXT,
        steps                TEXT    NOT NULL DEFAULT '{}',
        created_at           INTEGER NOT NULL,
        started_at           INTEGER,
        updated_at           INTEGER NOT NULL,
        completed_at         INTEGER
      )
    `);
    this.db.run(`CREATE INDEX IF NOT EXISTS ${t}_status ON ${t} (status)`);
    this.db.run(`CREATE INDEX IF NOT EXISTS ${t}_parent ON ${t} (parent_workflow_id)`);
    this.db.run(`
      CREATE TABLE IF NOT EXISTS ${t}_signals (
        id          INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
        workflow_id TEXT    NOT NULL,
        signal_name TEXT    NOT NULL,
        payload     TEXT    NOT NULL,
        delivered_at INTEGER NOT NULL
      )
    `);
    this.db.run(`CREATE INDEX IF NOT EXISTS ${t}_signals_wfid ON ${t}_signals (workflow_id)`);
    this.db.run(`
      CREATE TABLE IF NOT EXISTS ${t}_locks (
        workflow_id TEXT    NOT NULL PRIMARY KEY,
        expires_at  INTEGER NOT NULL,
        token       TEXT    NOT NULL
      )
    `);
    this.db.run(`
      CREATE TABLE IF NOT EXISTS ${t}_runs (
        workflow_id  TEXT    NOT NULL,
        run          INTEGER NOT NULL,
        version      TEXT,
        status       TEXT    NOT NULL,
        result       TEXT,
        error        TEXT,
        steps        TEXT    NOT NULL DEFAULT '{}',
        created_at   INTEGER NOT NULL,
        started_at   INTEGER,
        completed_at INTEGER,
        PRIMARY KEY (workflow_id, run)
      )
    `);
    this.db.run(`
      CREATE TABLE IF NOT EXISTS ${t}_journal (
        workflow_id    TEXT    NOT NULL,
        step_name      TEXT    NOT NULL,
        activity_index INTEGER NOT NULL,
        branch_path    TEXT    NOT NULL DEFAULT '',
        activity_name  TEXT    NOT NULL,
        step_type      TEXT    NOT NULL DEFAULT 'activity',
        phase          TEXT    NOT NULL DEFAULT 'completed',
        payload_hash   TEXT,
        exit           TEXT,
        wake_at        INTEGER,
        created_at     INTEGER NOT NULL,
        PRIMARY KEY (workflow_id, step_name, activity_index, branch_path)
      )
    `);
    this.db.run(
      `CREATE INDEX IF NOT EXISTS ${t}_journal_wfid ON ${t}_journal (workflow_id, step_name)`,
    );
    // Restore fence token counter from max stored token
    const row = this.db
      .query<{ maxToken: string | null }>(
        `SELECT MAX(CAST(token AS INTEGER)) AS maxToken FROM ${t}_locks`,
      )
      .get();
    if (row?.maxToken != null) {
      this._nextToken = parseInt(row.maxToken, 10) + 1;
    }
  }

  // ---------------------------------------------------------------------------
  // Fence helpers
  // ---------------------------------------------------------------------------

  private _checkFence(workflowId: string, guard?: FenceGuard): void {
    if (!guard?.fenceToken) return;
    const lock = this.db
      .query<{ token: string; expires_at: number }>(
        `SELECT token, expires_at FROM ${this._t}_locks WHERE workflow_id = ?`,
      )
      .get(workflowId);
    if (!lock) {
      throw new FenceTokenMismatchError({
        workflowId,
        expected: "(no lock)",
        provided: guard.fenceToken,
        message: `Fenced write for "${workflowId}" rejected — no active lock`,
      });
    }
    if (lock.token !== guard.fenceToken) {
      throw new FenceTokenMismatchError({
        workflowId,
        expected: lock.token,
        provided: guard.fenceToken,
        message: `Fenced write for "${workflowId}" rejected — token mismatch (expected "${lock.token}", got "${guard.fenceToken}")`,
      });
    }
  }

  // ---------------------------------------------------------------------------
  // Row serialization helpers
  // ---------------------------------------------------------------------------

  private _rowToState(row: WfRow): WorkflowState {
    return {
      workflowId: row.workflow_id,
      workflowName: row.workflow_name,
      workflowType: row.workflow_type ?? undefined,
      parentWorkflowId: row.parent_workflow_id ?? undefined,
      namespace: row.namespace ?? undefined,
      status: row.status as WorkflowStatus,
      version: row.version ?? undefined,
      run: row.run,
      input: JSON.parse(row.input),
      result: row.result != null ? JSON.parse(row.result) : undefined,
      error: row.error ?? undefined,
      metadata: row.metadata != null ? JSON.parse(row.metadata) : undefined,
      steps: parseSteps(row.steps),
      createdAt: new Date(row.created_at),
      startedAt: row.started_at != null ? new Date(row.started_at) : undefined,
      updatedAt: new Date(row.updated_at),
      completedAt: row.completed_at != null ? new Date(row.completed_at) : undefined,
    };
  }

  private _runRowToSummary(row: RunRow): WorkflowRunSummary {
    return {
      run: row.run,
      version: row.version ?? undefined,
      status: row.status as WorkflowStatus,
      result: row.result != null ? JSON.parse(row.result) : undefined,
      error: row.error ?? undefined,
      steps: parseSteps(row.steps),
      createdAt: new Date(row.created_at),
      startedAt: row.started_at != null ? new Date(row.started_at) : undefined,
      completedAt: row.completed_at != null ? new Date(row.completed_at) : undefined,
    };
  }

  // ---------------------------------------------------------------------------
  // WorkflowStorage — CRUD
  // ---------------------------------------------------------------------------

  async loadWorkflow(workflowId: string): Promise<WorkflowState | null> {
    const row = this.db
      .query<WfRow>(`SELECT * FROM ${this._t} WHERE workflow_id = ?`)
      .get(workflowId);
    return row ? this._rowToState(row) : null;
  }

  async listWorkflows(params?: {
    status?: WorkflowStatus;
    name?: string;
    type?: string;
    parentId?: string;
    namespace?: string;
    limit?: number;
    offset?: number;
    orderBy?: WorkflowOrderBy;
    orderDir?: "asc" | "desc";
  }): Promise<WorkflowState[]> {
    const conditions: string[] = [];
    const args: unknown[] = [];

    if (params?.status) {
      conditions.push(`status = ?`);
      args.push(params.status);
    }
    if (params?.name) {
      conditions.push(`workflow_name = ?`);
      args.push(params.name);
    }
    if (params?.type) {
      conditions.push(`workflow_type = ?`);
      args.push(params.type);
    }
    if (params?.parentId) {
      conditions.push(`parent_workflow_id = ?`);
      args.push(params.parentId);
    }
    if (params?.namespace) {
      conditions.push(`namespace = ?`);
      args.push(params.namespace);
    }

    const where = conditions.length > 0 ? ` WHERE ${conditions.join(" AND ")}` : "";
    const orderClause = sqliteOrderByClause(params?.orderBy, params?.orderDir);
    let sql = `SELECT * FROM ${this._t}${where} ORDER BY ${orderClause}`;
    if (params?.limit != null) {
      sql += ` LIMIT ?`;
      args.push(params.limit);
    }
    if (params?.offset != null) {
      sql += ` OFFSET ?`;
      args.push(params.offset);
    }

    return this.db
      .query<WfRow>(sql)
      .all(...args)
      .map((r) => this._rowToState(r));
  }

  async distinctWorkflowNames(params?: { namespace?: string }): Promise<string[]> {
    const rows = params?.namespace
      ? this.db
          .query<{ workflow_name: string }>(
            `SELECT DISTINCT workflow_name FROM ${this._t} WHERE namespace = ? ORDER BY workflow_name ASC`,
          )
          .all(params.namespace)
      : this.db
          .query<{ workflow_name: string }>(
            `SELECT DISTINCT workflow_name FROM ${this._t} ORDER BY workflow_name ASC`,
          )
          .all();
    return rows.map((r) => r.workflow_name);
  }

  async distinctWorkflowTypes(params?: { namespace?: string }): Promise<string[]> {
    const rows = params?.namespace
      ? this.db
          .query<{ workflow_type: string }>(
            `SELECT DISTINCT workflow_type FROM ${this._t}
             WHERE workflow_type IS NOT NULL AND namespace = ?
             ORDER BY workflow_type ASC`,
          )
          .all(params.namespace)
      : this.db
          .query<{ workflow_type: string }>(
            `SELECT DISTINCT workflow_type FROM ${this._t}
             WHERE workflow_type IS NOT NULL
             ORDER BY workflow_type ASC`,
          )
          .all();
    return rows.map((r) => r.workflow_type);
  }

  async distinctNamespaces(): Promise<string[]> {
    const rows = this.db
      .query<{ namespace: string }>(
        `SELECT DISTINCT namespace FROM ${this._t}
         WHERE namespace IS NOT NULL
         ORDER BY namespace ASC`,
      )
      .all();
    return rows.map((r) => r.namespace);
  }

  async cancelWorkflow(
    workflowId: string,
    options?: { cascade?: boolean },
    guard?: FenceGuard,
  ): Promise<void> {
    this._checkFence(workflowId, guard);
    const now = Date.now();
    this.db
      .query(
        `UPDATE ${this._t}
         SET status = 'failed', error = 'Cancelled', completed_at = ?, updated_at = ?
         WHERE workflow_id = ? AND status IN ('pending', 'running', 'suspended')`,
      )
      .run(now, now, workflowId);

    if (options?.cascade) {
      const children = this.db
        .query<{ workflow_id: string }>(
          `SELECT workflow_id FROM ${this._t} WHERE parent_workflow_id = ?`,
        )
        .all(workflowId);
      for (const child of children) {
        await this.cancelWorkflow(child.workflow_id, { cascade: true });
      }
    }
  }

  async createWorkflow(params: {
    workflowId: string;
    workflowName: string;
    input: unknown;
    workflowType?: string;
    parentWorkflowId?: string;
    namespace?: string;
    metadata?: Record<string, unknown>;
    version?: string;
  }): Promise<{ created: true } | { created: false; existing: WorkflowState }> {
    return this.db.transaction(
      (): { created: true } | { created: false; existing: WorkflowState } => {
        const existing = this.db
          .query<WfRow>(`SELECT * FROM ${this._t} WHERE workflow_id = ?`)
          .get(params.workflowId);
        if (existing) return { created: false, existing: this._rowToState(existing) };

        const now = Date.now();
        this.db
          .query(
            `INSERT INTO ${this._t}
           (workflow_id, workflow_name, workflow_type, parent_workflow_id, namespace, status,
            version, run, input, metadata, steps, created_at, updated_at)
           VALUES (?, ?, ?, ?, ?, 'pending', ?, 1, ?, ?, '{}', ?, ?)`,
          )
          .run(
            params.workflowId,
            params.workflowName,
            params.workflowType ?? null,
            params.parentWorkflowId ?? null,
            params.namespace ?? null,
            params.version ?? null,
            JSON.stringify(params.input),
            params.metadata != null ? JSON.stringify(params.metadata) : null,
            now,
            now,
          );
        return { created: true };
      },
    )();
  }

  // ---------------------------------------------------------------------------
  // WorkflowStorage — step results
  // ---------------------------------------------------------------------------

  private _markRunning(workflowId: string, now: number): void {
    this.db
      .query(
        `UPDATE ${this._t} SET status = 'running', started_at = ?, updated_at = ?
         WHERE workflow_id = ? AND status = 'pending'`,
      )
      .run(now, now, workflowId);
  }

  async saveStepResult(
    params: {
      workflowId: string;
      stepName: string;
      result: unknown;
      durationMs: number;
      startedAt: Date;
      metadata?: Record<string, unknown>;
    },
    guard?: FenceGuard,
  ): Promise<void> {
    this._checkFence(params.workflowId, guard);
    const now = Date.now();
    this.db.transaction((): void => {
      this._markRunning(params.workflowId, now);
      const row = this.db
        .query<{ steps: string; run: number }>(
          `SELECT steps, run FROM ${this._t} WHERE workflow_id = ?`,
        )
        .get(params.workflowId);
      if (!row) return;

      const steps: Record<string, StepState> = JSON.parse(row.steps);
      const existing = steps[params.stepName];
      steps[params.stepName] = {
        stepName: params.stepName,
        run: row.run,
        status: "completed",
        dependsOn: existing?.dependsOn ?? [],
        stepType: existing?.stepType ?? "single",
        result: params.result,
        metadata: params.metadata ?? existing?.metadata,
        startedAt: params.startedAt,
        completedAt: new Date(now),
        durationMs: params.durationMs,
        attempt: (existing?.attempt ?? 0) + 1,
        tasks: existing?.tasks,
      };
      this.db
        .query(`UPDATE ${this._t} SET steps = ?, updated_at = ? WHERE workflow_id = ?`)
        .run(JSON.stringify(steps), now, params.workflowId);
    })();
  }

  async batchSaveStepResults(
    records: ReadonlyArray<{
      workflowId: string;
      stepName: string;
      result: unknown;
      durationMs: number;
      startedAt: Date;
      metadata?: Record<string, unknown>;
    }>,
    guard?: FenceGuard,
  ): Promise<void> {
    for (const r of records) await this.saveStepResult(r, guard);
  }

  async saveStepFailure(
    params: {
      workflowId: string;
      stepName: string;
      error: string;
      durationMs: number;
      startedAt: Date;
      metadata?: Record<string, unknown>;
    },
    guard?: FenceGuard,
  ): Promise<void> {
    this._checkFence(params.workflowId, guard);
    const now = Date.now();
    this.db.transaction((): void => {
      this._markRunning(params.workflowId, now);
      const row = this.db
        .query<{ steps: string; run: number }>(
          `SELECT steps, run FROM ${this._t} WHERE workflow_id = ?`,
        )
        .get(params.workflowId);
      if (!row) return;

      const steps: Record<string, StepState> = JSON.parse(row.steps);
      const existing = steps[params.stepName];
      steps[params.stepName] = {
        stepName: params.stepName,
        run: row.run,
        status: "failed",
        dependsOn: existing?.dependsOn ?? [],
        stepType: existing?.stepType ?? "single",
        error: params.error,
        metadata: params.metadata ?? existing?.metadata,
        startedAt: params.startedAt,
        completedAt: new Date(now),
        durationMs: params.durationMs,
        attempt: (existing?.attempt ?? 0) + 1,
        tasks: existing?.tasks,
      };
      this.db
        .query(`UPDATE ${this._t} SET steps = ?, updated_at = ? WHERE workflow_id = ?`)
        .run(JSON.stringify(steps), now, params.workflowId);
    })();
  }

  async saveTaskResult(
    params: {
      workflowId: string;
      stepName: string;
      taskIndex: number;
      result: unknown;
    },
    guard?: FenceGuard,
  ): Promise<void> {
    this._checkFence(params.workflowId, guard);
    const now = Date.now();
    this.db.transaction((): void => {
      const row = this.db
        .query<{ steps: string; run: number }>(
          `SELECT steps, run FROM ${this._t} WHERE workflow_id = ?`,
        )
        .get(params.workflowId);
      if (!row) return;

      const steps: Record<string, StepState> = JSON.parse(row.steps);
      const existing = steps[params.stepName];
      const tasks = existing?.tasks ? [...existing.tasks] : [];

      const idx = tasks.findIndex((t) => t.taskIndex === params.taskIndex);
      const prev = idx >= 0 ? tasks[idx] : undefined;
      const task = {
        taskIndex: params.taskIndex,
        status: "completed" as const,
        result: params.result,
        startedAt: prev?.startedAt ?? new Date(now),
        completedAt: new Date(now),
        attempt: (prev?.attempt ?? 0) + 1,
      };
      if (idx >= 0) tasks[idx] = task;
      else tasks.push(task);

      steps[params.stepName] = {
        ...(existing ?? {
          stepName: params.stepName,
          run: row.run,
          status: "running" as const,
          dependsOn: [],
          stepType: "map" as const,
          attempt: 1,
        }),
        tasks,
      };
      this.db
        .query(`UPDATE ${this._t} SET steps = ?, updated_at = ? WHERE workflow_id = ?`)
        .run(JSON.stringify(steps), now, params.workflowId);
    })();
  }

  async saveTaskFailure(
    params: {
      workflowId: string;
      stepName: string;
      taskIndex: number;
      error: string;
    },
    guard?: FenceGuard,
  ): Promise<void> {
    this._checkFence(params.workflowId, guard);
    const now = Date.now();
    this.db.transaction((): void => {
      const row = this.db
        .query<{ steps: string; run: number }>(
          `SELECT steps, run FROM ${this._t} WHERE workflow_id = ?`,
        )
        .get(params.workflowId);
      if (!row) return;

      const steps: Record<string, StepState> = JSON.parse(row.steps);
      const existing = steps[params.stepName];
      const tasks = existing?.tasks ? [...existing.tasks] : [];

      const idx = tasks.findIndex((t) => t.taskIndex === params.taskIndex);
      const prev = idx >= 0 ? tasks[idx] : undefined;
      const task = {
        taskIndex: params.taskIndex,
        status: "failed" as const,
        error: params.error,
        startedAt: prev?.startedAt ?? new Date(now),
        completedAt: new Date(now),
        attempt: (prev?.attempt ?? 0) + 1,
      };
      if (idx >= 0) tasks[idx] = task;
      else tasks.push(task);

      steps[params.stepName] = {
        ...(existing ?? {
          stepName: params.stepName,
          run: row.run,
          status: "running" as const,
          dependsOn: [],
          stepType: "map" as const,
          attempt: 1,
        }),
        tasks,
      };
      this.db
        .query(`UPDATE ${this._t} SET steps = ?, updated_at = ? WHERE workflow_id = ?`)
        .run(JSON.stringify(steps), now, params.workflowId);
    })();
  }

  // ---------------------------------------------------------------------------
  // WorkflowStorage — workflow lifecycle
  // ---------------------------------------------------------------------------

  async completeWorkflow(workflowId: string, result: unknown, guard?: FenceGuard): Promise<void> {
    this._checkFence(workflowId, guard);
    const now = Date.now();
    this.db
      .query(
        `UPDATE ${this._t}
         SET status = 'completed', result = ?, completed_at = ?, updated_at = ?
         WHERE workflow_id = ?`,
      )
      .run(JSON.stringify(result), now, now, workflowId);
  }

  async failWorkflow(workflowId: string, error: string, guard?: FenceGuard): Promise<void> {
    this._checkFence(workflowId, guard);
    const now = Date.now();
    this.db
      .query(
        `UPDATE ${this._t}
         SET status = 'failed', error = ?, completed_at = ?, updated_at = ?
         WHERE workflow_id = ?`,
      )
      .run(error, now, now, workflowId);
  }

  async suspendWorkflow(
    workflowId: string,
    stepName: string,
    stepUpdate: Record<string, unknown>,
    guard?: FenceGuard,
  ): Promise<void> {
    this._checkFence(workflowId, guard);
    const now = Date.now();
    this.db.transaction((): void => {
      const row = this.db
        .query<{ steps: string; run: number }>(
          `SELECT steps, run FROM ${this._t} WHERE workflow_id = ?`,
        )
        .get(workflowId);
      if (!row) return;

      const steps: Record<string, StepState> = JSON.parse(row.steps);
      const existing = steps[stepName];
      steps[stepName] = {
        stepName,
        run: row.run,
        dependsOn: existing?.dependsOn ?? [],
        stepType: existing?.stepType ?? "single",
        attempt: existing?.attempt ?? 1,
        startedAt: existing?.startedAt ?? new Date(now),
        ...stepUpdate,
      } as StepState;

      this.db
        .query(
          `UPDATE ${this._t} SET status = 'suspended', steps = ?, updated_at = ? WHERE workflow_id = ?`,
        )
        .run(JSON.stringify(steps), now, workflowId);
    })();
  }

  // ---------------------------------------------------------------------------
  // WorkflowStorage — signals
  // ---------------------------------------------------------------------------

  async deliverSignal(workflowId: string, signalName: string, payload: unknown): Promise<void> {
    this.db
      .query(
        `INSERT INTO ${this._t}_signals (workflow_id, signal_name, payload, delivered_at)
         VALUES (?, ?, ?, ?)`,
      )
      .run(workflowId, signalName, JSON.stringify(payload), Date.now());
  }

  async loadSignals(workflowId: string): Promise<SignalState[]> {
    const rows = this.db
      .query<{ signal_name: string; payload: string; delivered_at: number }>(
        `SELECT signal_name, payload, delivered_at FROM ${this._t}_signals
         WHERE workflow_id = ? ORDER BY id ASC`,
      )
      .all(workflowId);
    return rows.map((r) => ({
      signalName: r.signal_name,
      payload: JSON.parse(r.payload),
      deliveredAt: new Date(r.delivered_at),
    }));
  }

  // ---------------------------------------------------------------------------
  // WorkflowStorage — locking
  // ---------------------------------------------------------------------------

  async tryLock(
    workflowId: string,
    lockDurationMs: number,
  ): Promise<{ acquired: boolean; token?: FenceToken }> {
    return this.db.transaction((): { acquired: boolean; token?: FenceToken } => {
      const now = Date.now();
      const existing = this.db
        .query<{ expires_at: number; token: string }>(
          `SELECT expires_at, token FROM ${this._t}_locks WHERE workflow_id = ?`,
        )
        .get(workflowId);

      if (existing && existing.expires_at > now) return { acquired: false };

      const token = String(this._nextToken++);
      const expiresAt = now + lockDurationMs;

      if (existing) {
        this.db
          .query(`UPDATE ${this._t}_locks SET expires_at = ?, token = ? WHERE workflow_id = ?`)
          .run(expiresAt, token, workflowId);
      } else {
        this.db
          .query(`INSERT INTO ${this._t}_locks (workflow_id, expires_at, token) VALUES (?, ?, ?)`)
          .run(workflowId, expiresAt, token);
      }
      return { acquired: true, token };
    })();
  }

  async tryLockAndLoad(
    workflowId: string,
    lockDurationMs: number,
  ): Promise<{ locked: boolean; token?: FenceToken; state: WorkflowState | null }> {
    const { acquired, token } = await this.tryLock(workflowId, lockDurationMs);
    const state = await this.loadWorkflow(workflowId);
    return { locked: acquired, token, state };
  }

  async releaseLock(workflowId: string, guard?: FenceGuard): Promise<void> {
    if (guard?.fenceToken) {
      this.db
        .query(`DELETE FROM ${this._t}_locks WHERE workflow_id = ? AND token = ?`)
        .run(workflowId, guard.fenceToken);
    } else {
      this.db.query(`DELETE FROM ${this._t}_locks WHERE workflow_id = ?`).run(workflowId);
    }
  }

  async heartbeat(workflowId: string, lockDurationMs: number, guard?: FenceGuard): Promise<void> {
    const now = Date.now();
    if (guard?.fenceToken) {
      this.db
        .query(
          `UPDATE ${this._t}_locks SET expires_at = ?
           WHERE workflow_id = ? AND token = ?`,
        )
        .run(now + lockDurationMs, workflowId, guard.fenceToken);
    } else {
      this.db
        .query(`UPDATE ${this._t}_locks SET expires_at = ? WHERE workflow_id = ?`)
        .run(now + lockDurationMs, workflowId);
    }
  }

  // ---------------------------------------------------------------------------
  // WorkflowStorage — run history
  // ---------------------------------------------------------------------------

  async startFreshRun(workflowId: string): Promise<number> {
    return this.db.transaction((): number => {
      const row = this.db
        .query<WfRow>(`SELECT * FROM ${this._t} WHERE workflow_id = ?`)
        .get(workflowId);
      if (!row) throw new Error(`Workflow ${workflowId} not found`);

      // Archive current run
      this.db
        .query(
          `INSERT OR REPLACE INTO ${this._t}_runs
           (workflow_id, run, version, status, result, error, steps, created_at, started_at, completed_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        )
        .run(
          workflowId,
          row.run,
          row.version ?? null,
          row.status,
          row.result,
          row.error ?? null,
          row.steps,
          row.created_at,
          row.started_at ?? null,
          row.completed_at ?? null,
        );

      const newRun = row.run + 1;
      const now = Date.now();
      this.db
        .query(
          `UPDATE ${this._t}
           SET run = ?, status = 'pending', result = NULL, error = NULL,
               started_at = NULL, completed_at = NULL, steps = '{}', updated_at = ?
           WHERE workflow_id = ?`,
        )
        .run(newRun, now, workflowId);
      return newRun;
    })();
  }

  async loadRunHistory(
    workflowId: string,
    params?: { limit?: number; offset?: number },
  ): Promise<WorkflowRunSummary[]> {
    const current = this.db
      .query<WfRow>(`SELECT * FROM ${this._t} WHERE workflow_id = ?`)
      .get(workflowId);
    if (!current) return [];

    const archived = this.db
      .query<RunRow>(`SELECT * FROM ${this._t}_runs WHERE workflow_id = ? ORDER BY run DESC`)
      .all(workflowId);

    const all: WorkflowRunSummary[] = [
      {
        run: current.run,
        version: current.version ?? undefined,
        status: current.status as WorkflowStatus,
        result: current.result != null ? JSON.parse(current.result) : undefined,
        error: current.error ?? undefined,
        steps: JSON.parse(current.steps),
        createdAt: new Date(current.created_at),
        startedAt: current.started_at != null ? new Date(current.started_at) : undefined,
        completedAt: current.completed_at != null ? new Date(current.completed_at) : undefined,
      },
      ...archived.map((r) => this._runRowToSummary(r)),
    ];

    all.sort((a, b) => b.run - a.run);

    const offset = params?.offset ?? 0;
    const limit = params?.limit ?? all.length;
    return all.slice(offset, offset + limit);
  }

  // ---------------------------------------------------------------------------
  // WorkflowStorage — purge
  // ---------------------------------------------------------------------------

  async purgeCompleted(
    params: { olderThanMs: number; limit: number } | { from: Date; to: Date; limit: number },
  ): Promise<number> {
    let fromMs: number;
    let toMs: number;

    if ("olderThanMs" in params) {
      fromMs = 0;
      toMs = Date.now() - params.olderThanMs;
    } else {
      fromMs = params.from.getTime();
      toMs = params.to.getTime();
    }

    return this.db.transaction((): number => {
      const ids = this.db
        .query<{ workflow_id: string }>(
          `SELECT workflow_id FROM ${this._t}
           WHERE status IN ('completed', 'failed')
             AND completed_at IS NOT NULL
             AND completed_at >= ? AND completed_at < ?
           LIMIT ?`,
        )
        .all(fromMs, toMs, params.limit);

      for (const { workflow_id } of ids) {
        this.db.query(`DELETE FROM ${this._t} WHERE workflow_id = ?`).run(workflow_id);
        this.db.query(`DELETE FROM ${this._t}_signals WHERE workflow_id = ?`).run(workflow_id);
        this.db.query(`DELETE FROM ${this._t}_locks WHERE workflow_id = ?`).run(workflow_id);
        this.db.query(`DELETE FROM ${this._t}_runs WHERE workflow_id = ?`).run(workflow_id);
        this.db.query(`DELETE FROM ${this._t}_journal WHERE workflow_id = ?`).run(workflow_id);
      }
      return ids.length;
    })();
  }

  // ---------------------------------------------------------------------------
  // ActivityJournalStorage
  // ---------------------------------------------------------------------------

  async loadJournal(workflowId: string, stepName: string): Promise<JournalEntry[]> {
    const rows = this.db
      .query<JournalRow>(
        `SELECT * FROM ${this._t}_journal
         WHERE workflow_id = ? AND step_name = ?
         ORDER BY activity_index ASC, branch_path ASC`,
      )
      .all(workflowId, stepName);
    return rows.map(rowToJournalEntry);
  }

  async appendEntry(params: {
    workflowId: string;
    stepName: string;
    activityIndex: number;
    branchPath?: string;
    activityName: string;
    payloadHash?: string;
    exit: NonNullable<JournalEntry["exit"]>;
  }): Promise<void> {
    const branchPath = params.branchPath ?? "";
    // Idempotent: skip if already completed
    const existing = this.db
      .query<{ phase: string }>(
        `SELECT phase FROM ${this._t}_journal
         WHERE workflow_id = ? AND step_name = ? AND activity_index = ? AND branch_path = ?`,
      )
      .get(params.workflowId, params.stepName, params.activityIndex, branchPath);

    if (existing && existing.phase !== "pending") return;

    const now = Date.now();
    this.db
      .query(
        `INSERT INTO ${this._t}_journal
         (workflow_id, step_name, activity_index, branch_path, activity_name,
          step_type, phase, payload_hash, exit, created_at)
         VALUES (?, ?, ?, ?, ?, 'activity', 'completed', ?, ?, ?)
         ON CONFLICT (workflow_id, step_name, activity_index, branch_path)
         DO UPDATE SET
           activity_name = excluded.activity_name,
           phase = 'completed',
           payload_hash = COALESCE(excluded.payload_hash, payload_hash),
           exit = excluded.exit`,
      )
      .run(
        params.workflowId,
        params.stepName,
        params.activityIndex,
        branchPath,
        params.activityName,
        params.payloadHash ?? null,
        JSON.stringify(params.exit),
        now,
      );
  }

  // ---------------------------------------------------------------------------
  // JournaledSuspendStorage
  // ---------------------------------------------------------------------------

  async appendPendingEntry(params: {
    workflowId: string;
    stepName: string;
    activityIndex: number;
    branchPath?: string;
    activityName: string;
    payloadHash?: string;
    stepType: "sleep" | "signal" | "activity" | "compensation";
    wakeAt?: Date;
  }): Promise<void> {
    const branchPath = params.branchPath ?? "";
    // Idempotent: leave as-is if already exists
    const existing = this.db
      .query<{ phase: string }>(
        `SELECT phase FROM ${this._t}_journal
         WHERE workflow_id = ? AND step_name = ? AND activity_index = ? AND branch_path = ?`,
      )
      .get(params.workflowId, params.stepName, params.activityIndex, branchPath);
    if (existing) return;

    this.db
      .query(
        `INSERT INTO ${this._t}_journal
         (workflow_id, step_name, activity_index, branch_path, activity_name,
          step_type, phase, payload_hash, wake_at, created_at)
         VALUES (?, ?, ?, ?, ?, ?, 'pending', ?, ?, ?)`,
      )
      .run(
        params.workflowId,
        params.stepName,
        params.activityIndex,
        branchPath,
        params.activityName,
        params.stepType,
        params.payloadHash ?? null,
        params.wakeAt != null ? params.wakeAt.getTime() : null,
        Date.now(),
      );
  }

  async completePendingEntry(params: {
    workflowId: string;
    stepName: string;
    activityIndex: number;
    branchPath?: string;
    exit: NonNullable<JournalEntry["exit"]>;
  }): Promise<void> {
    const branchPath = params.branchPath ?? "";
    // Idempotent: no-op if already completed
    this.db
      .query(
        `UPDATE ${this._t}_journal
         SET phase = 'completed', exit = ?
         WHERE workflow_id = ? AND step_name = ? AND activity_index = ? AND branch_path = ?
           AND phase = 'pending'`,
      )
      .run(
        JSON.stringify(params.exit),
        params.workflowId,
        params.stepName,
        params.activityIndex,
        branchPath,
      );
  }

  async findDueSleeps(params: { now: Date; limit: number }): Promise<
    Array<{
      workflowId: string;
      stepName: string;
      activityIndex: number;
      branchPath: string;
      wakeAt: Date;
    }>
  > {
    const rows = this.db
      .query<{
        workflow_id: string;
        step_name: string;
        activity_index: number;
        branch_path: string;
        wake_at: number;
      }>(
        `SELECT workflow_id, step_name, activity_index, branch_path, wake_at
         FROM ${this._t}_journal
         WHERE step_type = 'sleep' AND phase = 'pending' AND wake_at IS NOT NULL AND wake_at <= ?
         LIMIT ?`,
      )
      .all(params.now.getTime(), params.limit);
    return rows.map((r) => ({
      workflowId: r.workflow_id,
      stepName: r.step_name,
      activityIndex: r.activity_index,
      branchPath: r.branch_path,
      wakeAt: new Date(r.wake_at),
    }));
  }

  async findPendingSignal(params: {
    workflowId: string;
    stepName: string;
    signalName: string;
  }): Promise<JournalEntry | null> {
    const row = this.db
      .query<JournalRow>(
        `SELECT * FROM ${this._t}_journal
         WHERE workflow_id = ? AND step_name = ? AND step_type = 'signal'
           AND phase = 'pending' AND activity_name = ?`,
      )
      .get(params.workflowId, params.stepName, params.signalName);
    return row ? rowToJournalEntry(row) : null;
  }
}

// ---------------------------------------------------------------------------
// Row type helpers
// ---------------------------------------------------------------------------

interface WfRow {
  workflow_id: string;
  workflow_name: string;
  workflow_type: string | null;
  parent_workflow_id: string | null;
  namespace: string | null;
  status: string;
  version: string | null;
  run: number;
  input: string;
  result: string | null;
  error: string | null;
  metadata: string | null;
  steps: string;
  created_at: number;
  started_at: number | null;
  updated_at: number;
  completed_at: number | null;
}

interface RunRow {
  workflow_id: string;
  run: number;
  version: string | null;
  status: string;
  result: string | null;
  error: string | null;
  steps: string;
  created_at: number;
  started_at: number | null;
  completed_at: number | null;
}

interface JournalRow {
  workflow_id: string;
  step_name: string;
  activity_index: number;
  branch_path: string;
  activity_name: string;
  step_type: string;
  phase: string;
  payload_hash: string | null;
  exit: string | null;
  wake_at: number | null;
  created_at: number;
}

/**
 * Build the ORDER BY clause for `listWorkflows`. NULL values always sort
 * last so still-running rows (no `started_at` / `completed_at` /
 * `duration`) don't push real data off the first page in either direction.
 * Default: `created_at DESC`, matching the prior behavior.
 */
function sqliteOrderByClause(orderBy?: WorkflowOrderBy, orderDir?: "asc" | "desc"): string {
  const dir = orderDir === "asc" ? "ASC" : "DESC";
  switch (orderBy) {
    case "startedAt":
      return `started_at IS NULL, started_at ${dir}`;
    case "completedAt":
      return `completed_at IS NULL, completed_at ${dir}`;
    case "duration":
      return `completed_at IS NULL, (completed_at - created_at) ${dir}`;
    case "status":
      return `status ${dir}`;
    case "name":
      return `workflow_name ${dir}`;
    case "createdAt":
    default:
      return `created_at ${dir}`;
  }
}

/** Revive date strings in JSON-parsed StepState objects (JSON.parse gives strings, not Dates). */
function parseSteps(json: string): Record<string, StepState> {
  const raw: Record<string, Record<string, unknown>> = JSON.parse(json);
  const result: Record<string, StepState> = {};
  for (const [key, step] of Object.entries(raw)) {
    const tasks = step.tasks as Array<Record<string, unknown>> | undefined;
    result[key] = {
      ...step,
      startedAt: step.startedAt != null ? new Date(step.startedAt as string) : undefined,
      completedAt: step.completedAt != null ? new Date(step.completedAt as string) : undefined,
      wakeAt: step.wakeAt != null ? new Date(step.wakeAt as string) : undefined,
      signalTimeoutAt:
        step.signalTimeoutAt != null ? new Date(step.signalTimeoutAt as string) : undefined,
      compensatedAt:
        step.compensatedAt != null ? new Date(step.compensatedAt as string) : undefined,
      tasks: tasks?.map((t) => ({
        ...t,
        startedAt: t.startedAt != null ? new Date(t.startedAt as string) : undefined,
        completedAt: t.completedAt != null ? new Date(t.completedAt as string) : undefined,
      })),
    } as StepState;
  }
  return result;
}

function rowToJournalEntry(row: JournalRow): JournalEntry {
  return {
    activityIndex: row.activity_index,
    branchPath: row.branch_path,
    activityName: row.activity_name,
    stepType: row.step_type as JournalStepType,
    phase: row.phase as JournalPhase,
    payloadHash: row.payload_hash ?? undefined,
    exit: row.exit ? JSON.parse(row.exit) : undefined,
    wakeAt: row.wake_at != null ? new Date(row.wake_at) : undefined,
    createdAt: new Date(row.created_at),
  };
}
