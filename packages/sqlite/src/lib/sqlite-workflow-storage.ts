import {
  FenceTokenMismatchError,
  workflowMetadataMatches,
  encodeRunSource,
  decodeRunSource,
  type WorkflowStorage,
  type FenceToken,
  type FenceGuard,
  type WorkflowOrderBy,
  type RunSource,
  type SignalTokenRecord,
  type StreamChunk,
} from "@promin/workflow";
import type {
  WorkflowState,
  WorkflowSummary,
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
  StepAttemptStorage,
  StepAttemptRecord,
  StepAttemptType,
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
  implements WorkflowStorage, ActivityJournalStorage, JournaledSuspendStorage, StepAttemptStorage
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
        run_source           INTEGER,
        run_source_id        TEXT,
        created_at           INTEGER NOT NULL,
        started_at           INTEGER,
        updated_at           INTEGER NOT NULL,
        completed_at         INTEGER
      )
    `);
    // Migrate any existing table that predates the runSource columns.
    // sqlite ALTER TABLE ADD COLUMN IF NOT EXISTS landed in 3.35; older
    // dbs throw "duplicate column" — we swallow that exact failure mode
    // and let any other error propagate.
    for (const stmt of [
      `ALTER TABLE ${t} ADD COLUMN run_source INTEGER`,
      `ALTER TABLE ${t} ADD COLUMN run_source_id TEXT`,
      `ALTER TABLE ${t} ADD COLUMN idempotency_key TEXT`,
      `ALTER TABLE ${t} ADD COLUMN idempotency_expires_at INTEGER`,
    ]) {
      try {
        this.db.run(stmt);
      } catch (e) {
        if (!String(e).includes("duplicate column")) throw e;
      }
    }
    // Partial unique index on namespace-scoped idempotency keys for atomic claim-or-attach.
    // Rebuild the pre-namespace index if it exists under the same historical name.
    this.db.run(`DROP INDEX IF EXISTS ${t}_idempotency_key`);
    this.db.run(
      `CREATE UNIQUE INDEX IF NOT EXISTS ${t}_idempotency_key ON ${t} (COALESCE(namespace, ''), workflow_name, idempotency_key) WHERE idempotency_key IS NOT NULL`,
    );
    this.db.run(`CREATE INDEX IF NOT EXISTS ${t}_status ON ${t} (status)`);
    // Metrics and dashboard pages filter by status before sorting by recency.
    // The composite index avoids a temporary B-tree sort over the whole run
    // table for completed-duration samples.
    this.db.run(
      `CREATE INDEX IF NOT EXISTS ${t}_status_started_at ON ${t} (status, started_at DESC)`,
    );
    this.db.run(
      `CREATE INDEX IF NOT EXISTS ${t}_status_completed_at ON ${t} (status, completed_at DESC)`,
    );
    this.db.run(`CREATE INDEX IF NOT EXISTS ${t}_parent ON ${t} (parent_workflow_id)`);
    this.db.run(`CREATE INDEX IF NOT EXISTS ${t}_run_source ON ${t} (run_source, run_source_id)`);
    // Sort-order indexes so listWorkflows ORDER BY clauses can use index
    // traversal instead of a full-table sort. DESC matches the default
    // direction; SQLite uses the same index for ASC scans in reverse.
    this.db.run(`CREATE INDEX IF NOT EXISTS ${t}_started_at ON ${t} (started_at DESC)`);
    this.db.run(`CREATE INDEX IF NOT EXISTS ${t}_created_at ON ${t} (created_at DESC)`);
    this.db.run(`CREATE INDEX IF NOT EXISTS ${t}_completed_at ON ${t} (completed_at DESC)`);
    // Covers SELECT DISTINCT workflow_name ORDER BY workflow_name
    this.db.run(`CREATE INDEX IF NOT EXISTS ${t}_name ON ${t} (workflow_name)`);
    // Dashboard list/filter queries normally scope by namespace and then
    // order by recency. These composite indexes avoid scanning the entire
    // persistent demo database for each 25-row page.
    this.db.run(
      `CREATE INDEX IF NOT EXISTS ${t}_namespace_created_at ON ${t} (namespace, created_at DESC)`,
    );
    this.db.run(
      `CREATE INDEX IF NOT EXISTS ${t}_namespace_name ON ${t} (namespace, workflow_name)`,
    );
    this.db.run(
      `CREATE INDEX IF NOT EXISTS ${t}_namespace_type ON ${t} (namespace, workflow_type)`,
    );
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
    // Step attempt history — append-only audit log of every execution +
    // compensation attempt. Surfaces "which worker ran this?" + retry
    // analysis on the dashboard. (PRIMARY KEY (workflow_id, step_name,
    // attempt, type) makes saveStepAttempt naturally idempotent on retry.)
    this.db.run(`
      CREATE TABLE IF NOT EXISTS ${t}_attempts (
        workflow_id  TEXT    NOT NULL,
        step_name    TEXT    NOT NULL,
        attempt      INTEGER NOT NULL,
        type         TEXT    NOT NULL,            -- 'execution' | 'compensation'
        status       TEXT    NOT NULL,            -- 'completed' | 'failed'
        result       TEXT,                        -- JSON
        error        TEXT,
        duration_ms  INTEGER NOT NULL,
        started_at   INTEGER NOT NULL,
        completed_at INTEGER NOT NULL,
        executor_id  TEXT,
        PRIMARY KEY (workflow_id, step_name, attempt, type)
      )
    `);
    // Public-bearer signal tokens — authz sidecar for deliverSignal. Tokens
    // grant one-shot delivery rights to an external completer, scoped to a
    // specific (workflow_id, signal_name).
    this.db.run(`
      CREATE TABLE IF NOT EXISTS ${t}_signal_tokens (
        token_id        TEXT    NOT NULL PRIMARY KEY,
        workflow_id     TEXT    NOT NULL,
        signal_name     TEXT    NOT NULL,
        bearer          TEXT    NOT NULL,
        tags            TEXT    NOT NULL DEFAULT '[]',
        idempotency_key TEXT,
        expires_at      INTEGER NOT NULL,
        completed_at    INTEGER,
        completed_value TEXT,
        created_at      INTEGER NOT NULL
      )
    `);
    this.db.run(
      `CREATE INDEX IF NOT EXISTS ${t}_signal_tokens_wfid ON ${t}_signal_tokens (workflow_id)`,
    );
    this.db.run(
      `CREATE UNIQUE INDEX IF NOT EXISTS ${t}_signal_tokens_idemp ON ${t}_signal_tokens (workflow_id, idempotency_key) WHERE idempotency_key IS NOT NULL`,
    );
    // Generic typed streams — append-only chunks per (workflow, stream).
    this.db.run(`
      CREATE TABLE IF NOT EXISTS ${t}_streams (
        workflow_id  TEXT    NOT NULL,
        stream_id    TEXT    NOT NULL,
        chunk_index  INTEGER NOT NULL,
        payload      TEXT    NOT NULL,
        appended_by  TEXT    NOT NULL,
        appended_at  INTEGER NOT NULL,
        PRIMARY KEY (workflow_id, stream_id, chunk_index)
      )
    `);
    this.db.run(
      `CREATE INDEX IF NOT EXISTS ${t}_streams_wfid ON ${t}_streams (workflow_id, stream_id, chunk_index)`,
    );
    this.db.run(
      `CREATE INDEX IF NOT EXISTS ${t}_attempts_wfid ON ${t}_attempts (workflow_id, step_name)`,
    );
    // Migrate older databases that had the column named `worker_id`
    // (renamed to `executor_id` so non-worker contexts — in-process
    // runs, scheduler-loop, scripts — can populate it too without
    // misleading naming). SQLite RENAME COLUMN is no-op when the
    // column doesn't exist; we swallow that exact failure.
    try {
      this.db.run(`ALTER TABLE ${t}_attempts RENAME COLUMN worker_id TO executor_id`);
    } catch (e) {
      const msg = String(e);
      if (!msg.includes("no such column") && !msg.includes("already exists")) throw e;
    }
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
      runSource: decodeRunSource(row.run_source),
      runSourceId: row.run_source_id ?? undefined,
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
    runSource?: RunSource;
    runSourceId?: string;
    metadata?: Record<string, unknown>;
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
    if (params?.runSource !== undefined) {
      conditions.push(`run_source = ?`);
      args.push(encodeRunSource(params.runSource));
    }
    if (params?.runSourceId !== undefined) {
      conditions.push(`run_source_id = ?`);
      args.push(params.runSourceId);
    }

    // Metadata filter: push primitive equality checks down via json_extract
    // so the SQL still does the work for the common case (key=value). Object
    // / array values fall through to JS post-filter — SQLite's JSON1
    // doesn't have a containment operator and rolling our own JSON-equality
    // SQL would be slower than just loading + comparing.
    const metadataFilter = params?.metadata;
    let needsPostFilter = false;
    if (metadataFilter) {
      for (const [k, v] of Object.entries(metadataFilter)) {
        if (v === null || ["string", "number", "boolean"].includes(typeof v)) {
          conditions.push(`json_extract(metadata, ?) = ?`);
          // SQLite returns booleans as 0/1 from json_extract — match that.
          const sqlValue = typeof v === "boolean" ? (v ? 1 : 0) : v;
          args.push(jsonPathFor(k), sqlValue);
        } else {
          needsPostFilter = true;
        }
      }
    }

    const where = conditions.length > 0 ? ` WHERE ${conditions.join(" AND ")}` : "";
    const orderClause = sqliteOrderByClause(params?.orderBy, params?.orderDir);
    let sql = `SELECT * FROM ${this._t}${where} ORDER BY ${orderClause}`;

    // When the metadata filter has object/array values, do the LIMIT/OFFSET
    // in JS after post-filtering. The SQL pre-filter still cuts the row set
    // down using the primitive checks; we just can't trust pagination until
    // the JS pass narrows it further.
    if (!needsPostFilter) {
      if (params?.limit != null) {
        sql += ` LIMIT ?`;
        args.push(params.limit);
      }
      if (params?.offset != null) {
        sql += ` OFFSET ?`;
        args.push(params.offset);
      }
    }

    let rows = this.db
      .query<WfRow>(sql)
      .all(...args)
      .map((r) => this._rowToState(r));

    if (needsPostFilter && metadataFilter) {
      rows = rows.filter((r) => workflowMetadataMatches(r.metadata, metadataFilter));
      const offset = params?.offset ?? 0;
      const limit = params?.limit ?? rows.length;
      rows = rows.slice(offset, offset + limit);
    }
    return rows;
  }

  async listWorkflowSummaries(
    params?: Parameters<WorkflowStorage["listWorkflows"]>[0],
  ): Promise<WorkflowSummary[]> {
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
    if (params?.runSource !== undefined) {
      conditions.push(`run_source = ?`);
      args.push(encodeRunSource(params.runSource));
    }
    if (params?.runSourceId !== undefined) {
      conditions.push(`run_source_id = ?`);
      args.push(params.runSourceId);
    }

    const metadataFilter = params?.metadata;
    let needsPostFilter = false;
    if (metadataFilter) {
      for (const [k, v] of Object.entries(metadataFilter)) {
        if (v === null || ["string", "number", "boolean"].includes(typeof v)) {
          conditions.push(`json_extract(metadata, ?) = ?`);
          const sqlValue = typeof v === "boolean" ? (v ? 1 : 0) : v;
          args.push(jsonPathFor(k), sqlValue);
        } else {
          needsPostFilter = true;
        }
      }
    }

    const where = conditions.length > 0 ? ` WHERE ${conditions.join(" AND ")}` : "";
    const orderClause = sqliteOrderByClause(params?.orderBy, params?.orderDir);
    // Lean SELECT — intentionally omits `steps`, `input`, `result`, `error`
    // so the engine never deserialises those JSON blobs for list-view queries.
    let sql = `SELECT workflow_id, workflow_name, workflow_type, namespace, status, version, run,
                      metadata, run_source, run_source_id, created_at, started_at, updated_at, completed_at
               FROM ${this._t}${where} ORDER BY ${orderClause}`;

    if (!needsPostFilter) {
      if (params?.limit != null) {
        sql += ` LIMIT ?`;
        args.push(params.limit);
      }
      if (params?.offset != null) {
        sql += ` OFFSET ?`;
        args.push(params.offset);
      }
    }

    let rows = this.db
      .query<SummaryRow>(sql)
      .all(...args)
      .map((r) => this._summaryRowToSummary(r));

    if (needsPostFilter && metadataFilter) {
      rows = rows.filter((r) => workflowMetadataMatches(r.metadata, metadataFilter));
      const offset = params?.offset ?? 0;
      const limit = params?.limit ?? rows.length;
      rows = rows.slice(offset, offset + limit);
    }
    return rows;
  }

  private _summaryRowToSummary(row: SummaryRow): WorkflowSummary {
    return {
      workflowId: row.workflow_id,
      workflowName: row.workflow_name,
      workflowType: row.workflow_type ?? undefined,
      namespace: row.namespace ?? undefined,
      status: row.status as WorkflowStatus,
      version: row.version ?? undefined,
      run: row.run,
      runSource: decodeRunSource(row.run_source),
      runSourceId: row.run_source_id ?? undefined,
      metadata: row.metadata != null ? JSON.parse(row.metadata) : undefined,
      createdAt: new Date(row.created_at),
      startedAt: row.started_at != null ? new Date(row.started_at) : undefined,
      updatedAt: new Date(row.updated_at),
      completedAt: row.completed_at != null ? new Date(row.completed_at) : undefined,
    };
  }

  async countWorkflows(params?: {
    status?: WorkflowStatus;
    name?: string;
    type?: string;
    parentId?: string;
    namespace?: string;
    runSource?: RunSource;
    runSourceId?: string;
    metadata?: Record<string, unknown>;
  }): Promise<number> {
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
    if (params?.runSource !== undefined) {
      conditions.push(`run_source = ?`);
      args.push(encodeRunSource(params.runSource));
    }
    if (params?.runSourceId !== undefined) {
      conditions.push(`run_source_id = ?`);
      args.push(params.runSourceId);
    }
    if (params?.metadata) {
      for (const [k, v] of Object.entries(params.metadata)) {
        if (v === null || ["string", "number", "boolean"].includes(typeof v)) {
          conditions.push(`json_extract(metadata, ?) = ?`);
          const sqlValue = typeof v === "boolean" ? (v ? 1 : 0) : v;
          args.push(jsonPathFor(k), sqlValue);
        }
      }
    }

    const where = conditions.length > 0 ? ` WHERE ${conditions.join(" AND ")}` : "";
    const row = this.db
      .query<{ c: number }>(`SELECT COUNT(*) AS c FROM ${this._t}${where}`)
      .get(...args);
    return Number(row?.c ?? 0);
  }

  /**
   * Bulk-fail all pending/running/suspended workflows whose `created_at` is
   * older than `olderThanMs` milliseconds. Returns the number of rows updated.
   * Single UPDATE statement — safe to call at startup even against large DBs.
   */
  cancelStaleWorkflows(params: {
    olderThanMs: number;
    error?: string;
    statuses?: Array<"pending" | "running" | "suspended">;
  }): number {
    const cutoff = Date.now() - params.olderThanMs;
    const statuses = params.statuses ?? ["pending", "running", "suspended"];
    const placeholders = statuses.map(() => "?").join(", ");
    const now = Date.now();
    // Count first (the interface's run() returns void, not a changes count).
    const before = this.db
      .query<{ c: number }>(
        `SELECT COUNT(*) AS c FROM ${this._t}
         WHERE status IN (${placeholders}) AND created_at < ?`,
      )
      .get(...statuses, cutoff);
    const count = Number(before?.c ?? 0);
    if (count === 0) return 0;
    this.db
      .query(
        `UPDATE ${this._t}
         SET status = 'failed', error = ?, completed_at = ?, updated_at = ?
         WHERE status IN (${placeholders}) AND created_at < ?`,
      )
      .run(params.error ?? "Stale run cancelled on restart", now, now, ...statuses, cutoff);
    return count;
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
    runSource?: RunSource;
    runSourceId?: string;
    idempotencyKey?: string;
    idempotencyExpiresAt?: Date;
  }): Promise<{ created: true } | { created: false; existing: WorkflowState }> {
    return this.db.transaction(
      (): { created: true } | { created: false; existing: WorkflowState } => {
        const now = Date.now();

        // Idempotency-key path: if (namespace, workflow_name, idempotency_key) exists
        // and is unexpired, attach to it. Inside the transaction so the
        // unique-index conflict resolves atomically.
        if (params.idempotencyKey) {
          const keyHit = this.db
            .query<WfRow>(
              `SELECT * FROM ${this._t}
	               WHERE COALESCE(namespace, '') = COALESCE(?, '')
	                 AND workflow_name = ? AND idempotency_key = ?
	                 AND idempotency_expires_at IS NOT NULL
	                 AND idempotency_expires_at > ?
	               LIMIT 1`,
            )
            .get(params.namespace ?? null, params.workflowName, params.idempotencyKey, now);
          if (keyHit) return { created: false, existing: this._rowToState(keyHit) };
        }

        const existing = this.db
          .query<WfRow>(`SELECT * FROM ${this._t} WHERE workflow_id = ?`)
          .get(params.workflowId);
        if (existing) return { created: false, existing: this._rowToState(existing) };

        this.db
          .query(
            `INSERT INTO ${this._t}
           (workflow_id, workflow_name, workflow_type, parent_workflow_id, namespace, status,
            version, run, input, metadata, steps, run_source, run_source_id,
            idempotency_key, idempotency_expires_at,
            created_at, updated_at)
           VALUES (?, ?, ?, ?, ?, 'pending', ?, 1, ?, ?, '{}', ?, ?, ?, ?, ?, ?)`,
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
            encodeRunSource(params.runSource),
            params.runSourceId ?? null,
            params.idempotencyKey ?? null,
            params.idempotencyExpiresAt ? params.idempotencyExpiresAt.getTime() : null,
            now,
            now,
          );
        return { created: true };
      },
    )();
  }

  async findWorkflowByIdempotencyKey(params: {
    workflowName: string;
    namespace?: string;
    idempotencyKey: string;
    now: Date;
  }): Promise<{ workflowId: string } | null> {
    const row = this.db
      .query<{ workflow_id: string }>(
        `SELECT workflow_id FROM ${this._t}
	         WHERE COALESCE(namespace, '') = COALESCE(?, '')
	           AND workflow_name = ? AND idempotency_key = ?
	           AND idempotency_expires_at IS NOT NULL
	           AND idempotency_expires_at > ?
	         LIMIT 1`,
      )
      .get(
        params.namespace ?? null,
        params.workflowName,
        params.idempotencyKey,
        params.now.getTime(),
      );
    return row ? { workflowId: row.workflow_id } : null;
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

  async setWorkflowMetadata(workflowId: string, patch: Record<string, unknown>): Promise<void> {
    // SQLite has json_patch but support is recent + spotty; do read-modify-
    // write inside a transaction so concurrent body re-runs don't lose
    // updates. Same shape as the in-memory + redis impls.
    this.db.transaction((): void => {
      const row = this.db
        .query<{ metadata: string | null }>(`SELECT metadata FROM ${this._t} WHERE workflow_id = ?`)
        .get(workflowId);
      if (!row) return;
      const current: Record<string, unknown> = row.metadata != null ? JSON.parse(row.metadata) : {};
      const merged: Record<string, unknown> = { ...current };
      for (const [k, v] of Object.entries(patch)) {
        if (v === null) delete merged[k];
        else merged[k] = v;
      }
      this.db
        .query(`UPDATE ${this._t} SET metadata = ?, updated_at = ? WHERE workflow_id = ?`)
        .run(JSON.stringify(merged), Date.now(), workflowId);
    })();
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
  // Signal tokens — public-bearer authorization for deliverSignal
  // ---------------------------------------------------------------------------

  async createSignalToken(params: {
    tokenId: string;
    workflowId: string;
    signalName: string;
    bearer: string;
    tags: ReadonlyArray<string>;
    idempotencyKey?: string | null;
    expiresAt: Date;
  }): Promise<{ record: SignalTokenRecord; isCached: boolean }> {
    return this.db.transaction((): { record: SignalTokenRecord; isCached: boolean } => {
      if (params.idempotencyKey) {
        const existing = this.db
          .query<SignalTokenRow>(
            `SELECT * FROM ${this._t}_signal_tokens WHERE workflow_id = ? AND idempotency_key = ?`,
          )
          .get(params.workflowId, params.idempotencyKey);
        if (existing) {
          return { record: rowToSignalToken(existing), isCached: true };
        }
      }
      const now = Date.now();
      this.db
        .query(
          `INSERT INTO ${this._t}_signal_tokens
           (token_id, workflow_id, signal_name, bearer, tags, idempotency_key, expires_at, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
        )
        .run(
          params.tokenId,
          params.workflowId,
          params.signalName,
          params.bearer,
          JSON.stringify([...params.tags]),
          params.idempotencyKey ?? null,
          params.expiresAt.getTime(),
          now,
        );
      const inserted = this.db
        .query<SignalTokenRow>(`SELECT * FROM ${this._t}_signal_tokens WHERE token_id = ?`)
        .get(params.tokenId);
      if (!inserted) throw new Error("createSignalToken: insert disappeared");
      return { record: rowToSignalToken(inserted), isCached: false };
    })();
  }

  async findSignalTokenById(tokenId: string): Promise<SignalTokenRecord | null> {
    const row = this.db
      .query<SignalTokenRow>(`SELECT * FROM ${this._t}_signal_tokens WHERE token_id = ?`)
      .get(tokenId);
    return row ? rowToSignalToken(row) : null;
  }

  async markSignalTokenCompleted(params: {
    tokenId: string;
    value: unknown;
    now: Date;
  }): Promise<
    | { outcome: "delivered"; record: SignalTokenRecord }
    | { outcome: "already_completed"; record: SignalTokenRecord }
  > {
    return this.db.transaction(
      ():
        | { outcome: "delivered"; record: SignalTokenRecord }
        | { outcome: "already_completed"; record: SignalTokenRecord } => {
        const row = this.db
          .query<SignalTokenRow>(`SELECT * FROM ${this._t}_signal_tokens WHERE token_id = ?`)
          .get(params.tokenId);
        if (!row) throw new Error(`signal token ${params.tokenId} not found`);
        if (row.completed_at !== null) {
          return { outcome: "already_completed", record: rowToSignalToken(row) };
        }
        this.db
          .query(
            `UPDATE ${this._t}_signal_tokens
             SET completed_at = ?, completed_value = ?
             WHERE token_id = ? AND completed_at IS NULL`,
          )
          .run(params.now.getTime(), JSON.stringify(params.value), params.tokenId);
        const updated = this.db
          .query<SignalTokenRow>(`SELECT * FROM ${this._t}_signal_tokens WHERE token_id = ?`)
          .get(params.tokenId);
        if (!updated) throw new Error("markSignalTokenCompleted: row disappeared");
        return { outcome: "delivered", record: rowToSignalToken(updated) };
      },
    )();
  }

  async listSignalTokensForWorkflow(workflowId: string): Promise<ReadonlyArray<SignalTokenRecord>> {
    const rows = this.db
      .query<SignalTokenRow>(
        `SELECT * FROM ${this._t}_signal_tokens WHERE workflow_id = ? ORDER BY created_at DESC`,
      )
      .all(workflowId);
    return rows.map(rowToSignalToken);
  }

  // ---------------------------------------------------------------------------
  // Streams — append-only chunks per (workflow, stream).
  // ---------------------------------------------------------------------------

  async appendStreamChunk(params: {
    workflowId: string;
    streamId: string;
    payload: unknown;
    appendedBy: "workflow" | "external";
  }): Promise<{ chunkIndex: number }> {
    return this.db.transaction((): { chunkIndex: number } => {
      const row = this.db
        .query<{ next: number | null }>(
          `SELECT MAX(chunk_index) AS next FROM ${this._t}_streams
           WHERE workflow_id = ? AND stream_id = ?`,
        )
        .get(params.workflowId, params.streamId);
      const chunkIndex = row?.next == null ? 0 : row.next + 1;
      this.db
        .query(
          `INSERT INTO ${this._t}_streams (workflow_id, stream_id, chunk_index, payload, appended_by, appended_at)
           VALUES (?, ?, ?, ?, ?, ?)`,
        )
        .run(
          params.workflowId,
          params.streamId,
          chunkIndex,
          JSON.stringify(params.payload),
          params.appendedBy,
          Date.now(),
        );
      return { chunkIndex };
    })();
  }

  async readStreamChunks(params: {
    workflowId: string;
    streamId: string;
    since?: number;
    limit?: number;
  }): Promise<ReadonlyArray<StreamChunk>> {
    let sql = `SELECT chunk_index, payload, appended_by, appended_at FROM ${this._t}_streams
               WHERE workflow_id = ? AND stream_id = ?`;
    const args: Array<string | number> = [params.workflowId, params.streamId];
    if (params.since !== undefined) {
      sql += ` AND chunk_index > ?`;
      args.push(params.since);
    }
    sql += ` ORDER BY chunk_index ASC`;
    if (params.limit !== undefined) {
      sql += ` LIMIT ?`;
      args.push(params.limit);
    }
    const rows = this.db
      .query<{
        chunk_index: number;
        payload: string;
        appended_by: string;
        appended_at: number;
      }>(sql)
      .all(...args);
    return rows.map((r) => ({
      chunkIndex: r.chunk_index,
      payload: JSON.parse(r.payload),
      appendedBy: r.appended_by as "workflow" | "external",
      appendedAt: new Date(r.appended_at),
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

  async resetSteps(workflowId: string, stepNames: readonly string[]): Promise<void> {
    if (stepNames.length === 0) return;
    const names = [...stepNames];
    this.db.transaction((): void => {
      // Unknown workflow → throw, matching InMemoryWorkflowStorage so
      // every backend shares one contract (the runner guards existence
      // before calling, so this is a defensive check).
      const row = this.db
        .query<{ status: string; steps: string }>(
          `SELECT status, steps FROM ${this._t} WHERE workflow_id = ?`,
        )
        .get(workflowId);
      if (!row) throw new Error(`Workflow ${workflowId} not found`);

      // Drop the listed steps from the JSON step map — a removed entry
      // reads back as "never ran", same shape as InMemoryWorkflowStorage.
      // Map-step tasks live nested under the step, so they go with it.
      const steps = JSON.parse(row.steps) as Record<string, unknown>;
      for (const name of names) delete steps[name];

      // Clear journal entries so the activities re-fire on replay rather
      // than returning stale recorded values.
      const placeholders = names.map(() => "?").join(", ");
      this.db
        .query(
          `DELETE FROM ${this._t}_journal WHERE workflow_id = ? AND step_name IN (${placeholders})`,
        )
        .run(workflowId, ...names);

      // Flip a terminal workflow back to running so the runner resumes
      // it; a still-running / suspended workflow keeps its status.
      const terminal =
        row.status === "completed" || row.status === "failed" || row.status === "tripwire";
      const now = Date.now();
      if (terminal) {
        this.db
          .query(
            `UPDATE ${this._t}
             SET steps = ?, status = 'running', result = NULL, error = NULL,
                 completed_at = NULL, updated_at = ?
             WHERE workflow_id = ?`,
          )
          .run(JSON.stringify(steps), now, workflowId);
      } else {
        this.db
          .query(`UPDATE ${this._t} SET steps = ?, updated_at = ? WHERE workflow_id = ?`)
          .run(JSON.stringify(steps), now, workflowId);
      }
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
        this.db.query(`DELETE FROM ${this._t}_attempts WHERE workflow_id = ?`).run(workflow_id);
        this.db
          .query(`DELETE FROM ${this._t}_signal_tokens WHERE workflow_id = ?`)
          .run(workflow_id);
        this.db.query(`DELETE FROM ${this._t}_streams WHERE workflow_id = ?`).run(workflow_id);
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

  // ---------------------------------------------------------------------------
  // StepAttemptStorage — append-only audit trail of step execution +
  // compensation attempts. Surfaces "which worker handled this attempt?" +
  // retry analysis. Implemented as a separate table with (workflow_id,
  // step_name, attempt, type) composite PK so retries are idempotent and
  // execution / compensation rows for the same (step, attempt) can coexist.
  // ---------------------------------------------------------------------------

  async saveStepAttempt(record: StepAttemptRecord, _guard?: FenceGuard): Promise<void> {
    this.db
      .query(
        `INSERT INTO ${this._t}_attempts
           (workflow_id, step_name, attempt, type, status, result, error,
            duration_ms, started_at, completed_at, executor_id)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT (workflow_id, step_name, attempt, type) DO UPDATE SET
           status       = excluded.status,
           result       = excluded.result,
           error        = excluded.error,
           duration_ms  = excluded.duration_ms,
           started_at   = excluded.started_at,
           completed_at = excluded.completed_at,
           executor_id  = excluded.executor_id`,
      )
      .run(
        record.workflowId,
        record.stepName,
        record.attempt,
        record.type,
        record.status,
        record.result !== undefined ? JSON.stringify(record.result) : null,
        record.error ?? null,
        record.durationMs,
        record.startedAt.getTime(),
        record.completedAt.getTime(),
        record.executorId ?? null,
      );
  }

  async loadStepAttempts(workflowId: string, stepName?: string): Promise<StepAttemptRecord[]> {
    interface Row {
      workflow_id: string;
      step_name: string;
      attempt: number;
      type: string;
      status: string;
      result: string | null;
      error: string | null;
      duration_ms: number;
      started_at: number;
      completed_at: number;
      executor_id: string | null;
    }
    const rows = stepName
      ? this.db
          .query<Row>(
            `SELECT * FROM ${this._t}_attempts
             WHERE workflow_id = ? AND step_name = ?
             ORDER BY attempt ASC, type ASC`,
          )
          .all(workflowId, stepName)
      : this.db
          .query<Row>(
            `SELECT * FROM ${this._t}_attempts
             WHERE workflow_id = ?
             ORDER BY step_name ASC, attempt ASC, type ASC`,
          )
          .all(workflowId);
    return rows.map((r) => {
      const rec: StepAttemptRecord = {
        workflowId: r.workflow_id,
        stepName: r.step_name,
        attempt: r.attempt,
        type: r.type as StepAttemptType,
        status: r.status as "completed" | "failed",
        durationMs: r.duration_ms,
        startedAt: new Date(r.started_at),
        completedAt: new Date(r.completed_at),
        ...(r.result !== null && { result: JSON.parse(r.result) as unknown }),
        ...(r.error !== null && { error: r.error }),
        ...(r.executor_id !== null && { executorId: r.executor_id }),
      };
      return rec;
    });
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
  run_source: number | null;
  run_source_id: string | null;
  created_at: number;
  started_at: number | null;
  updated_at: number;
  completed_at: number | null;
}

/** Subset returned by `listWorkflowSummaries` — no blob columns. */
interface SummaryRow {
  workflow_id: string;
  workflow_name: string;
  workflow_type: string | null;
  namespace: string | null;
  status: string;
  version: string | null;
  run: number;
  metadata: string | null;
  run_source: number | null;
  run_source_id: string | null;
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
 * Build a SQLite JSON path for a metadata key. Bare alphanumeric / underscore
 * keys take the dot form `$.foo`; anything else (dots, dashes, spaces, quotes)
 * uses the bracket-quoted form `$."key"` with embedded quotes doubled, which is
 * SQLite JSON1's escape convention.
 */
function jsonPathFor(key: string): string {
  if (/^[A-Za-z_][A-Za-z0-9_]*$/.test(key)) return `$.${key}`;
  return `$."${key.replace(/"/g, '""')}"`;
}

/**
 * Build the ORDER BY clause for `listWorkflows`. NULL values always sort
 * last so still-running rows (no `started_at` / `completed_at` /
 * `duration`) don't push real data off the first page in either direction.
 *
 * In SQLite, NULLs sort as the smallest value, which means DESC already
 * places them last — no expression prefix needed. Bare-column expressions
 * let the query planner use the sort-order indexes added in `_setup`.
 * For ASC sorts we need explicit `NULLS LAST` (SQLite ≥ 3.30, shipped
 * with Bun).
 */
function sqliteOrderByClause(orderBy?: WorkflowOrderBy, orderDir?: "asc" | "desc"): string {
  const asc = orderDir === "asc";
  switch (orderBy) {
    case "createdAt":
      // created_at is NOT NULL — no null-handling needed.
      return asc ? "created_at ASC" : "created_at DESC";
    case "completedAt":
      return asc ? "completed_at ASC NULLS LAST" : "completed_at DESC";
    case "duration":
      // Expression — no index possible, but null handling is correct:
      // NULL result (in-flight rows) sorts last in both directions.
      return asc
        ? "(completed_at - created_at) ASC NULLS LAST"
        : "(completed_at - created_at) DESC";
    case "status":
      return asc ? "status ASC" : "status DESC";
    case "name":
      return asc ? "workflow_name ASC" : "workflow_name DESC";
    case "startedAt":
    default:
      return asc ? "started_at ASC NULLS LAST" : "started_at DESC";
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

interface SignalTokenRow {
  token_id: string;
  workflow_id: string;
  signal_name: string;
  bearer: string;
  tags: string;
  idempotency_key: string | null;
  expires_at: number;
  completed_at: number | null;
  completed_value: string | null;
  created_at: number;
}

function rowToSignalToken(row: SignalTokenRow): SignalTokenRecord {
  return {
    tokenId: row.token_id,
    workflowId: row.workflow_id,
    signalName: row.signal_name,
    bearer: row.bearer,
    tags: JSON.parse(row.tags),
    idempotencyKey: row.idempotency_key,
    expiresAt: new Date(row.expires_at),
    completedAt: row.completed_at != null ? new Date(row.completed_at) : null,
    completedValue: row.completed_value != null ? JSON.parse(row.completed_value) : null,
    createdAt: new Date(row.created_at),
  };
}
