import {
  FenceTokenMismatchError,
  SystemWallClock,
  type WallClock,
  type WorkflowStorage,
  type CompensationLedgerStore,
  type FenceToken,
  type FenceGuard,
  type WorkflowOrderBy,
  type RunSource,
  type SignalTokenRecord,
  type StreamChunk,
  type WorkflowWakeup,
  type OrphanedRun,
} from "@promin/workflow";
import {
  CANCELLED_ERROR,
  CANCELLED_ERROR_TAG,
  workflowMetadataMatches,
  encodeRunSource,
  decodeRunSource,
  withoutCompensationLedger,
} from "@promin/workflow/storage-kit";
import type {
  WorkflowState,
  WorkflowStatusSnapshot,
  WorkflowSummary,
  WorkflowStatus,
  WorkflowRunSummary,
  SignalState,
  StepState,
  AppendEntryParams,
  AppendPendingEntryParams,
  AppendStreamChunkParams,
  BatchSaveStepResultsParams,
  BeginCompensationParams,
  CancelWorkflowParams,
  CheckpointStepParams,
  CompletePendingEntryParams,
  CompleteWorkflowParams,
  CreateWorkflowParams,
  DeliverSignalParams,
  DiscardJournalEntriesParams,
  FailWorkflowParams,
  HeartbeatParams,
  LoadJournalParams,
  LoadRunHistoryParams,
  LoadStepAttemptsParams,
  ReleaseLockParams,
  ResetStepsParams,
  SaveStepAttemptParams,
  SaveStepCompensationParams,
  SaveStepFailureParams,
  SaveStepResultParams,
  SaveTaskFailureParams,
  SaveTaskResultParams,
  SetWorkflowMetadataParams,
  StartFreshRunParams,
  SuspendWorkflowParams,
  TryLockParams,
} from "@promin/workflow";
import type {
  JournalStore,
  JournalEntry,
  JournalExit,
  CompletePendingResult,
  JournalStepType,
  JournalPhase,
  StepAttemptStore,
  StepAttemptRecord,
  StepAttemptType,
  StepCheckpointStore,
} from "@promin/workflow";
import { applyMetadataPatch } from "@promin/workflow/storage-kit";
import type { SqliteDatabase } from "./sqlite-database.ts";

/**
 * Persistent workflow storage backed by SQLite.
 *
 * Implements the full `WorkflowStorage` contract plus the optional
 * `JournalStore` extension.
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
  implements
    WorkflowStorage,
    JournalStore,
    StepAttemptStore,
    StepCheckpointStore,
    CompensationLedgerStore
{
  private readonly _t: string;
  private readonly clock: WallClock;

  private constructor(
    private readonly db: SqliteDatabase,
    table: string,
    clock: WallClock,
  ) {
    this._t = table;
    this.clock = clock;
    this._setup();
  }

  static make(params: {
    db: SqliteDatabase;
    /** Override the table prefix (default: `promin_wf`). */
    tablePrefix?: string;
    /**
     * Time source for every stored timestamp, lock expiry, idempotency
     * window and age cutoff. Default: `SystemWallClock`. Tests pass a
     * `FakeWallClock`.
     */
    clock?: WallClock;
  }): SqliteWorkflowStorage {
    return new SqliteWorkflowStorage(
      params.db,
      params.tablePrefix ?? "promin_wf",
      params.clock ?? SystemWallClock,
    );
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
        error_tag            TEXT,
        metadata             TEXT,
        steps                TEXT    NOT NULL DEFAULT '{}',
        run_source           INTEGER,
        run_source_id        TEXT,
        created_at           INTEGER NOT NULL,
        started_at           INTEGER,
        updated_at           INTEGER NOT NULL,
        completed_at         INTEGER,
        idempotency_key      TEXT,
        idempotency_expires_at INTEGER
      )
    `);
    // Partial unique index on namespace-scoped idempotency keys for atomic claim-or-attach.
    this.db.run(
      `CREATE UNIQUE INDEX IF NOT EXISTS ${t}_idempotency_key ON ${t} (COALESCE(namespace, ''), workflow_name, idempotency_key) WHERE idempotency_key IS NOT NULL`,
    );
    this.db.run(`CREATE INDEX IF NOT EXISTS ${t}_status ON ${t} (status)`);
    // Scanners (`listDueTimers` / `listSignalWakeups`) page suspended runs,
    // and coordinator recovery (`listOrphanedRuns`) pages pending / running /
    // compensating runs, in workflow-id order.
    this.db.run(
      `CREATE INDEX IF NOT EXISTS ${t}_suspended ON ${t} (workflow_id) WHERE status = 'suspended'`,
    );
    this.db.run(
      `CREATE INDEX IF NOT EXISTS ${t}_active_runs ON ${t} (workflow_id) WHERE status IN ('pending', 'running', 'compensating')`,
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
    // Fence tokens come from one shared counter row so every storage
    // instance on the database mints strictly increasing, never-reused
    // tokens — a per-instance counter would hand out the same token twice.
    this.db.run(`
      CREATE TABLE IF NOT EXISTS ${t}_fence (
        id    INTEGER NOT NULL PRIMARY KEY CHECK (id = 1),
        value INTEGER NOT NULL
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
    this.db.run(`INSERT OR IGNORE INTO ${t}_fence (id, value) VALUES (1, 0)`);
  }

  /** Mint the next fence token. Call inside the lock transaction. */
  private _nextFenceToken(): string {
    const row = this.db
      .query<{ value: number }>(
        `UPDATE ${this._t}_fence SET value = value + 1 WHERE id = 1 RETURNING value`,
      )
      .get();
    if (!row) throw new Error("SqliteWorkflowStorage: fence counter row missing");
    return String(row.value);
  }

  // ---------------------------------------------------------------------------
  // Fence helpers
  // ---------------------------------------------------------------------------

  /**
   * Reject a fenced write unless `guard.fenceToken` is the workflow's
   * current, unexpired lock token. Call it inside the write's transaction,
   * before the first write: SQLite serializes writers, so no other
   * connection can move the lock between the check and the commit (a
   * writer that committed first makes this transaction fail with BUSY
   * instead of writing). Without a token the write is unfenced.
   */
  private _checkFence(params: { workflowId: string; guard?: FenceGuard }): void {
    const { workflowId, guard } = params;
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
    if (lock.expires_at <= this.clock.currentTimeMs()) {
      throw new FenceTokenMismatchError({
        workflowId,
        expected: "(expired)",
        provided: guard.fenceToken,
        message: `Fenced write for "${workflowId}" rejected — the lock for token "${guard.fenceToken}" expired`,
      });
    }
  }

  /** Run `write` in one transaction behind `_checkFence`. */
  private _fenced<T>(params: { workflowId: string; guard?: FenceGuard; write: () => T }): T {
    return this.db.transaction((): T => {
      this._checkFence({ workflowId: params.workflowId, guard: params.guard });
      return params.write();
    })();
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
      errorTag: row.error_tag ?? undefined,
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

  async loadWorkflowStatus(workflowId: string): Promise<WorkflowStatusSnapshot | null> {
    return this._loadStatus(workflowId);
  }

  private _loadStatus(workflowId: string): WorkflowStatusSnapshot | null {
    const row = this.db
      .query<{ status: string; error: string | null; error_tag: string | null }>(
        `SELECT status, error, error_tag FROM ${this._t} WHERE workflow_id = ?`,
      )
      .get(workflowId);
    if (!row) return null;
    return {
      status: row.status as WorkflowStatus,
      ...(row.error !== null && { error: row.error }),
      ...(row.error_tag !== null && { errorTag: row.error_tag }),
    };
  }

  async loadWorkflow(workflowId: string): Promise<WorkflowState | null> {
    const row = this.db
      .query<WfRow>(`SELECT * FROM ${this._t} WHERE workflow_id = ?`)
      .get(workflowId);
    return row ? this._rowToState(row) : null;
  }

  async listWorkflows(params?: {
    status?: WorkflowStatus;
    name?: string;
    version?: string;
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
    if (params?.version !== undefined) {
      conditions.push(`version = ?`);
      args.push(params.version);
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
    if (params?.version !== undefined) {
      conditions.push(`version = ?`);
      args.push(params.version);
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
    version?: string;
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
    if (params?.version !== undefined) {
      conditions.push(`version = ?`);
      args.push(params.version);
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
    const cutoff = this.clock.currentTimeMs() - params.olderThanMs;
    const statuses = params.statuses ?? ["pending", "running", "suspended"];
    const placeholders = statuses.map(() => "?").join(", ");
    const now = this.clock.currentTimeMs();
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

  async cancelWorkflow({ workflowId, cascade, guard }: CancelWorkflowParams): Promise<void> {
    const now = this.clock.currentTimeMs();
    this._fenced({
      workflowId,
      guard,
      write: () =>
        this.db
          .query(
            `UPDATE ${this._t}
             SET status = 'failed', error = ?, error_tag = ?, completed_at = ?, updated_at = ?
             WHERE workflow_id = ? AND status IN ('pending', 'running', 'suspended')`,
          )
          .run(CANCELLED_ERROR, CANCELLED_ERROR_TAG, now, now, workflowId),
    });

    if (cascade) {
      const children = this.db
        .query<{ workflow_id: string }>(
          `SELECT workflow_id FROM ${this._t} WHERE parent_workflow_id = ?`,
        )
        .all(workflowId);
      for (const child of children) {
        await this.cancelWorkflow({ workflowId: child.workflow_id, cascade: true });
      }
    }
  }

  async createWorkflow({
    guard,
    ...params
  }: CreateWorkflowParams): Promise<
    { created: true } | { created: false; existing: WorkflowState }
  > {
    if (guard?.fenceToken && params.parentWorkflowId === undefined) {
      throw new Error("createWorkflow: a fenced create needs parentWorkflowId");
    }
    return this.db.transaction(
      (): { created: true } | { created: false; existing: WorkflowState } => {
        // A child create is fenced on the parent's lock.
        if (params.parentWorkflowId !== undefined) {
          this._checkFence({ workflowId: params.parentWorkflowId, guard });
        }
        const now = this.clock.currentTimeMs();

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
          // An expired key no longer owns its slot in the unique index —
          // release it so this create can claim the key.
          this.db
            .query(
              `UPDATE ${this._t} SET idempotency_key = NULL
               WHERE COALESCE(namespace, '') = COALESCE(?, '')
                 AND workflow_name = ? AND idempotency_key = ?
                 AND (idempotency_expires_at IS NULL OR idempotency_expires_at <= ?)`,
            )
            .run(params.namespace ?? null, params.workflowName, params.idempotencyKey, now);
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

  async saveStepResult({ guard, ...params }: SaveStepResultParams): Promise<void> {
    this._saveStepResult({ ...params, guard });
  }

  /** Synchronous body of `saveStepResult`, so a batch can run it in one transaction. */
  private _saveStepResult(params: {
    workflowId: string;
    stepName: string;
    result: unknown;
    durationMs: number;
    startedAt: Date;
    metadata?: Record<string, unknown>;
    guard?: FenceGuard;
  }): void {
    const now = this.clock.currentTimeMs();
    this.db.transaction((): void => {
      this._checkFence(params);
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

  async batchSaveStepResults({ records, guard }: BatchSaveStepResultsParams): Promise<void> {
    // One transaction: every fence is checked before the first write, and
    // the batch commits whole or not at all.
    this.db.transaction((): void => {
      for (const id of new Set(records.map((r) => r.workflowId)))
        this._checkFence({ workflowId: id, guard });
      for (const r of records) this._saveStepResult(r);
    })();
  }

  async saveStepFailure({ guard, ...params }: SaveStepFailureParams): Promise<void> {
    this._saveStepFailure({ ...params, guard });
  }

  /** Synchronous body of `saveStepFailure`, so `checkpointStep` can run it in its transaction. */
  private _saveStepFailure(params: {
    workflowId: string;
    stepName: string;
    error: string;
    errorTag?: string;
    durationMs: number;
    startedAt: Date;
    metadata?: Record<string, unknown>;
    guard?: FenceGuard;
  }): void {
    const now = this.clock.currentTimeMs();
    this.db.transaction((): void => {
      this._checkFence(params);
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
        ...(params.errorTag !== undefined && { errorTag: params.errorTag }),
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

  async saveTaskResult({ result, ...params }: SaveTaskResultParams): Promise<void> {
    this._saveTask({ ...params, outcome: { status: "completed", result } });
  }

  async saveTaskFailure({ error, ...params }: SaveTaskFailureParams): Promise<void> {
    this._saveTask({ ...params, outcome: { status: "failed", error } });
  }

  /**
   * Upsert one map task row with `outcome`, creating a `running` map step
   * row when the step has none, in one fenced transaction.
   */
  private _saveTask(params: {
    workflowId: string;
    stepName: string;
    taskIndex: number;
    guard?: FenceGuard;
    outcome:
      | { readonly status: "completed"; readonly result: unknown }
      | { readonly status: "failed"; readonly error: string };
  }): void {
    const { outcome } = params;
    const now = this.clock.currentTimeMs();
    this.db.transaction((): void => {
      this._checkFence(params);
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
        status: outcome.status,
        ...(outcome.status === "completed" ? { result: outcome.result } : { error: outcome.error }),
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

  async completeWorkflow({ workflowId, result, guard }: CompleteWorkflowParams): Promise<void> {
    const now = this.clock.currentTimeMs();
    this._fenced({
      workflowId,
      guard,
      write: () =>
        this.db
          .query(
            `UPDATE ${this._t}
             SET status = 'completed', result = ?, completed_at = ?, updated_at = ?
             WHERE workflow_id = ? AND status NOT IN ('completed', 'failed', 'tripwire')`,
          )
          .run(JSON.stringify(result), now, now, workflowId),
    });
  }

  async failWorkflow({ workflowId, error, errorTag, guard }: FailWorkflowParams): Promise<void> {
    const now = this.clock.currentTimeMs();
    this._fenced({
      workflowId,
      guard,
      write: () =>
        this.db
          .query(
            `UPDATE ${this._t}
             SET status = 'failed', error = ?, error_tag = ?, completed_at = ?, updated_at = ?
             WHERE workflow_id = ? AND status NOT IN ('completed', 'failed', 'tripwire')`,
          )
          .run(error, errorTag ?? null, now, now, workflowId),
    });
  }

  async suspendWorkflow({
    workflowId,
    stepName,
    stepUpdate,
    guard,
  }: SuspendWorkflowParams): Promise<void> {
    const now = this.clock.currentTimeMs();
    this.db.transaction((): void => {
      this._checkFence({ workflowId, guard });
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

  async deliverSignal({ workflowId, signalName, payload }: DeliverSignalParams): Promise<void> {
    // Last delivery per name wins (one row per (workflow, name)).
    this.db.transaction((): void => {
      this.db
        .query(`DELETE FROM ${this._t}_signals WHERE workflow_id = ? AND signal_name = ?`)
        .run(workflowId, signalName);
      this.db
        .query(
          `INSERT INTO ${this._t}_signals (workflow_id, signal_name, payload, delivered_at)
           VALUES (?, ?, ?, ?)`,
        )
        .run(workflowId, signalName, JSON.stringify(payload), this.clock.currentTimeMs());
    })();
  }

  async setWorkflowMetadata({
    workflowId,
    patch,
    guard,
  }: SetWorkflowMetadataParams): Promise<void> {
    // SQLite has json_patch but support is recent + spotty; do read-modify-
    // write inside a transaction so concurrent body re-runs don't lose
    // updates. Same shape as the in-memory + redis impls.
    this.db.transaction((): void => {
      this._checkFence({ workflowId, guard });
      const row = this.db
        .query<{ metadata: string | null }>(`SELECT metadata FROM ${this._t} WHERE workflow_id = ?`)
        .get(workflowId);
      if (!row) return;
      const merged = applyMetadataPatch({
        current: row.metadata != null ? JSON.parse(row.metadata) : {},
        patch,
      });
      this.db
        .query(`UPDATE ${this._t} SET metadata = ?, updated_at = ? WHERE workflow_id = ?`)
        .run(JSON.stringify(merged), this.clock.currentTimeMs(), workflowId);
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
      const now = this.clock.currentTimeMs();
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

  async appendStreamChunk({
    guard,
    ...params
  }: AppendStreamChunkParams): Promise<{ chunkIndex: number }> {
    return this.db.transaction((): { chunkIndex: number } => {
      this._checkFence({ workflowId: params.workflowId, guard });
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
          this.clock.currentTimeMs(),
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

  async tryLock({
    workflowId,
    lockDurationMs,
  }: TryLockParams): Promise<{ acquired: boolean; token?: FenceToken }> {
    return this.db.transaction((): { acquired: boolean; token?: FenceToken } => {
      const now = this.clock.currentTimeMs();
      const existing = this.db
        .query<{ expires_at: number; token: string }>(
          `SELECT expires_at, token FROM ${this._t}_locks WHERE workflow_id = ?`,
        )
        .get(workflowId);

      if (existing && existing.expires_at > now) return { acquired: false };

      const token = this._nextFenceToken();
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

  async tryLockAndLoad({
    workflowId,
    lockDurationMs,
  }: TryLockParams): Promise<{ locked: boolean; token?: FenceToken; state: WorkflowState | null }> {
    const { acquired, token } = await this.tryLock({ workflowId, lockDurationMs });
    const state = await this.loadWorkflow(workflowId);
    return { locked: acquired, token, state };
  }

  async releaseLock({ workflowId, guard }: ReleaseLockParams): Promise<void> {
    if (guard?.fenceToken) {
      this.db
        .query(`DELETE FROM ${this._t}_locks WHERE workflow_id = ? AND token = ?`)
        .run(workflowId, guard.fenceToken);
    } else {
      this.db.query(`DELETE FROM ${this._t}_locks WHERE workflow_id = ?`).run(workflowId);
    }
  }

  async heartbeat({ workflowId, lockDurationMs, guard }: HeartbeatParams): Promise<void> {
    const now = this.clock.currentTimeMs();
    if (guard?.fenceToken) {
      // A token holder whose lock is gone, expired or re-taken learns it
      // lost the run.
      this._fenced({
        workflowId,
        guard,
        write: () =>
          this.db
            .query(
              `UPDATE ${this._t}_locks SET expires_at = ?
               WHERE workflow_id = ? AND token = ?`,
            )
            .run(now + lockDurationMs, workflowId, guard.fenceToken),
      });
    } else {
      this.db
        .query(`UPDATE ${this._t}_locks SET expires_at = ? WHERE workflow_id = ?`)
        .run(now + lockDurationMs, workflowId);
    }
  }

  // ---------------------------------------------------------------------------
  // WorkflowStorage — run history
  // ---------------------------------------------------------------------------

  async startFreshRun({ workflowId, guard }: StartFreshRunParams): Promise<number> {
    return this.db.transaction((): number => {
      this._checkFence({ workflowId, guard });
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
      const now = this.clock.currentTimeMs();
      this.db
        .query(
          `UPDATE ${this._t}
           SET run = ?, status = 'pending', result = NULL, error = NULL, error_tag = NULL,
               started_at = NULL, completed_at = NULL, steps = '{}', updated_at = ?
           WHERE workflow_id = ?`,
        )
        .run(newRun, now, workflowId);
      // The new run replays from nothing: drop the old run's journal and
      // its delivered signals in the same transaction as the run bump.
      this.db.query(`DELETE FROM ${this._t}_journal WHERE workflow_id = ?`).run(workflowId);
      this.db.query(`DELETE FROM ${this._t}_signals WHERE workflow_id = ?`).run(workflowId);
      return newRun;
    })();
  }

  async resetSteps({ workflowId, stepNames }: ResetStepsParams): Promise<void> {
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
      const steps = JSON.parse(row.steps) as Record<string, StepState>;
      for (const name of names) delete steps[name];
      // The kept steps start a fresh compensation ledger.
      for (const [name, step] of Object.entries(steps)) {
        steps[name] = withoutCompensationLedger(step);
      }

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
      const now = this.clock.currentTimeMs();
      if (terminal) {
        this.db
          .query(
            `UPDATE ${this._t}
             SET steps = ?, status = 'running', result = NULL, error = NULL, error_tag = NULL,
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

  // ---------------------------------------------------------------------------
  // Scanner / recovery queries
  // ---------------------------------------------------------------------------
  //
  // Steps live as a JSON object on the workflow row, so the scanners expand
  // them with `json_each` over the suspended rows (partial index on
  // workflow_id) and keep the smallest matching step name per run with
  // ROW_NUMBER(). Step timestamps are ISO strings; `julianday` compares them.

  async listDueTimers(params: {
    now: Date;
    limit: number;
    afterWorkflowId?: string;
  }): Promise<WorkflowWakeup[]> {
    const now = params.now.toISOString();
    const after = params.afterWorkflowId !== undefined ? "AND w.workflow_id > ?" : "";
    const args: unknown[] = params.afterWorkflowId !== undefined ? [params.afterWorkflowId] : [];
    args.push(now, now, Math.max(0, Math.trunc(params.limit)));
    const rows = this.db
      .query<ScanWakeupRow>(
        `SELECT workflow_id, workflow_name, version, input, step_name, reason, signal_name
         FROM (
           SELECT w.workflow_id, w.workflow_name, w.version, w.input,
             j.key AS step_name,
             CASE json_extract(j.value, '$.status')
               WHEN 'sleeping' THEN 'sleep' ELSE 'signal-timeout' END AS reason,
             json_extract(j.value, '$.signalName') AS signal_name,
             ROW_NUMBER() OVER (PARTITION BY w.workflow_id ORDER BY j.key) AS rn
           FROM ${this._t} w, json_each(w.steps) j
           WHERE w.status = 'suspended' ${after}
             AND (
               (json_extract(j.value, '$.status') = 'sleeping'
                 AND julianday(json_extract(j.value, '$.wakeAt')) <= julianday(?))
               OR (json_extract(j.value, '$.status') = 'waiting_for_signal'
                 AND json_extract(j.value, '$.signalTimeoutAt') IS NOT NULL
                 AND julianday(json_extract(j.value, '$.signalTimeoutAt')) <= julianday(?))
             )
         )
         WHERE rn = 1
         ORDER BY workflow_id
         LIMIT ?`,
      )
      .all(...args);
    return rows.map((r) => ({
      workflowId: r.workflow_id,
      workflowName: r.workflow_name,
      ...(r.version != null ? { version: r.version } : {}),
      input: JSON.parse(r.input),
      stepName: r.step_name,
      reason: r.reason as "sleep" | "signal-timeout",
      ...(r.reason === "signal-timeout" && r.signal_name != null
        ? { signalName: r.signal_name }
        : {}),
    }));
  }

  async listSignalWakeups(params: {
    limit: number;
    afterWorkflowId?: string;
  }): Promise<WorkflowWakeup[]> {
    const after = params.afterWorkflowId !== undefined ? "AND w.workflow_id > ?" : "";
    const args: unknown[] = params.afterWorkflowId !== undefined ? [params.afterWorkflowId] : [];
    args.push(Math.max(0, Math.trunc(params.limit)));
    const rows = this.db
      .query<ScanWakeupRow & { payload: string }>(
        `SELECT workflow_id, workflow_name, version, input, step_name, signal_name, payload
         FROM (
           SELECT w.workflow_id, w.workflow_name, w.version, w.input,
             j.key AS step_name, g.signal_name, g.payload,
             ROW_NUMBER() OVER (PARTITION BY w.workflow_id ORDER BY j.key) AS rn
           FROM ${this._t} w, json_each(w.steps) j
           JOIN ${this._t}_signals g
             ON g.workflow_id = w.workflow_id
            AND g.signal_name = json_extract(j.value, '$.signalName')
           WHERE w.status = 'suspended' ${after}
             AND json_extract(j.value, '$.status') = 'waiting_for_signal'
             AND g.id = (
               SELECT MAX(g2.id) FROM ${this._t}_signals g2
               WHERE g2.workflow_id = g.workflow_id AND g2.signal_name = g.signal_name
             )
         )
         WHERE rn = 1
         ORDER BY workflow_id
         LIMIT ?`,
      )
      .all(...args);
    return rows.map((r) => ({
      workflowId: r.workflow_id,
      workflowName: r.workflow_name,
      ...(r.version != null ? { version: r.version } : {}),
      input: JSON.parse(r.input),
      stepName: r.step_name,
      reason: "signal" as const,
      signalName: r.signal_name!,
      signalPayload: JSON.parse(r.payload),
    }));
  }

  async listOrphanedRuns(params: {
    now: Date;
    updatedBefore: Date;
    limit: number;
    afterWorkflowId?: string;
  }): Promise<OrphanedRun[]> {
    const after = params.afterWorkflowId !== undefined ? "AND w.workflow_id > ?" : "";
    const args: unknown[] = [params.updatedBefore.getTime()];
    if (params.afterWorkflowId !== undefined) args.push(params.afterWorkflowId);
    args.push(params.now.getTime(), Math.max(0, Math.trunc(params.limit)));
    const rows = this.db
      .query<{
        workflow_id: string;
        workflow_name: string;
        version: string | null;
        status: string;
        input: string;
        metadata: string | null;
      }>(
        `SELECT w.workflow_id, w.workflow_name, w.version, w.status, w.input, w.metadata
         FROM ${this._t} w
         WHERE w.status IN ('pending', 'running', 'compensating')
           AND w.updated_at < ? ${after}
           AND NOT EXISTS (
             SELECT 1 FROM ${this._t}_locks l
             WHERE l.workflow_id = w.workflow_id AND l.expires_at > ?
           )
         ORDER BY w.workflow_id
         LIMIT ?`,
      )
      .all(...args);
    return rows.map((r) => ({
      workflowId: r.workflow_id,
      workflowName: r.workflow_name,
      ...(r.version != null ? { version: r.version } : {}),
      status: r.status as OrphanedRun["status"],
      input: JSON.parse(r.input),
      ...(r.metadata != null ? { metadata: JSON.parse(r.metadata) } : {}),
    }));
  }

  async loadRunHistory({
    workflowId,
    ...params
  }: LoadRunHistoryParams): Promise<WorkflowRunSummary[]> {
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
      toMs = this.clock.currentTimeMs() - params.olderThanMs;
    } else {
      fromMs = params.from.getTime();
      toMs = params.to.getTime();
    }

    return this.db.transaction((): number => {
      const ids = this.db
        .query<{ workflow_id: string }>(
          `SELECT workflow_id FROM ${this._t}
           WHERE status IN ('completed', 'failed', 'tripwire')
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
  // JournalStore
  // ---------------------------------------------------------------------------

  async loadJournal({ workflowId, stepName }: LoadJournalParams): Promise<JournalEntry[]> {
    const rows = this.db
      .query<JournalRow>(
        `SELECT * FROM ${this._t}_journal
         WHERE workflow_id = ? AND step_name = ?
         ORDER BY activity_index ASC, branch_path ASC`,
      )
      .all(workflowId, stepName);
    return rows.map(rowToJournalEntry);
  }

  async appendEntry({ guard, ...params }: AppendEntryParams): Promise<void> {
    this._fenced({ workflowId: params.workflowId, guard, write: () => this._appendEntry(params) });
  }

  private _appendEntry(params: {
    workflowId: string;
    stepName: string;
    activityIndex: number;
    branchPath?: string;
    activityName: string;
    payloadHash?: string;
    exit: NonNullable<JournalEntry["exit"]>;
  }): void {
    const branchPath = params.branchPath ?? "";
    // Idempotent: skip if already completed
    const existing = this.db
      .query<{ phase: string }>(
        `SELECT phase FROM ${this._t}_journal
         WHERE workflow_id = ? AND step_name = ? AND activity_index = ? AND branch_path = ?`,
      )
      .get(params.workflowId, params.stepName, params.activityIndex, branchPath);

    if (existing && existing.phase !== "pending") return;

    const now = this.clock.currentTimeMs();
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
  // JournalStore — pending entries (ctx.sleep / ctx.signal)
  // ---------------------------------------------------------------------------

  async appendPendingEntry({ guard, ...params }: AppendPendingEntryParams): Promise<void> {
    this._fenced({
      workflowId: params.workflowId,
      guard,
      write: () => this._appendPendingEntry(params),
    });
  }

  private _appendPendingEntry(params: {
    workflowId: string;
    stepName: string;
    activityIndex: number;
    branchPath?: string;
    activityName: string;
    payloadHash?: string;
    stepType: JournalStepType;
    wakeAt?: Date;
  }): void {
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
        this.clock.currentTimeMs(),
      );
  }

  async completePendingEntry({
    guard,
    ...params
  }: CompletePendingEntryParams): Promise<CompletePendingResult> {
    return this._fenced({
      workflowId: params.workflowId,
      guard,
      write: () => this._completePendingEntry(params),
    });
  }

  private _completePendingEntry(params: {
    workflowId: string;
    stepName: string;
    activityIndex: number;
    branchPath?: string;
    exit: JournalExit;
  }): CompletePendingResult {
    const branchPath = params.branchPath ?? "";
    // First writer wins: only a still-pending row is updated.
    const won = this.db
      .query<{ won: number }>(
        `UPDATE ${this._t}_journal
         SET phase = 'completed', exit = ?
         WHERE workflow_id = ? AND step_name = ? AND activity_index = ? AND branch_path = ?
           AND phase = 'pending'
         RETURNING 1 AS won`,
      )
      .get(
        JSON.stringify(params.exit),
        params.workflowId,
        params.stepName,
        params.activityIndex,
        branchPath,
      );
    if (won) return { completed: true, exit: params.exit };
    const row = this.db
      .query<{ exit: string | null }>(
        `SELECT exit FROM ${this._t}_journal
         WHERE workflow_id = ? AND step_name = ? AND activity_index = ? AND branch_path = ?`,
      )
      .get(params.workflowId, params.stepName, params.activityIndex, branchPath);
    return {
      completed: false,
      exit: row?.exit ? (JSON.parse(row.exit) as JournalExit) : undefined,
    };
  }

  async discardJournalEntries({ guard, ...params }: DiscardJournalEntriesParams): Promise<void> {
    const del = this.db.query(
      `DELETE FROM ${this._t}_journal
       WHERE workflow_id = ? AND step_name = ? AND activity_index = ? AND branch_path = ?`,
    );
    this.db.transaction(() => {
      this._checkFence({ workflowId: params.workflowId, guard });
      for (const slot of params.slots) {
        del.run(params.workflowId, params.stepName, slot.activityIndex, slot.branchPath);
      }
    })();
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
  // StepAttemptStore — append-only audit trail of step execution +
  // compensation attempts. Surfaces "which worker handled this attempt?" +
  // retry analysis. Implemented as a separate table with (workflow_id,
  // step_name, attempt, type) composite PK so retries are idempotent and
  // execution / compensation rows for the same (step, attempt) can coexist.
  // ---------------------------------------------------------------------------

  async saveStepAttempt({ record, guard }: SaveStepAttemptParams): Promise<void> {
    this._fenced({
      workflowId: record.workflowId,
      guard,
      write: () => this._insertAttempt(record),
    });
  }

  // ---------------------------------------------------------------------------
  // StepCheckpointStore — a settled step's row and attempt rows in one
  // transaction, behind one fence check.
  // ---------------------------------------------------------------------------

  async checkpointStep({
    guard,
    ...checkpoint
  }: CheckpointStepParams): Promise<WorkflowStatusSnapshot | null> {
    const { workflowId, stepName, outcome } = checkpoint;
    return this._fenced({
      workflowId,
      guard,
      write: (): WorkflowStatusSnapshot | null => {
        const exists = this.db
          .query<{ one: number }>(`SELECT 1 AS one FROM ${this._t} WHERE workflow_id = ?`)
          .get(workflowId);
        if (!exists) return null;
        for (const attempt of checkpoint.attempts) this._insertAttempt(attempt);
        const row = {
          workflowId,
          stepName,
          durationMs: outcome.durationMs,
          startedAt: outcome.startedAt,
          ...(outcome.metadata !== undefined && { metadata: outcome.metadata }),
        };
        if (outcome.kind === "completed") {
          this._saveStepResult({ ...row, result: outcome.result });
        } else {
          this._saveStepFailure({
            ...row,
            error: outcome.error,
            ...(outcome.errorTag !== undefined && { errorTag: outcome.errorTag }),
          });
        }
        return this._loadStatus(workflowId);
      },
    });
  }

  /** Upsert one attempt row; the caller fences it. */
  private _insertAttempt(record: StepAttemptRecord): void {
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

  async loadStepAttempts({
    workflowId,
    stepName,
  }: LoadStepAttemptsParams): Promise<StepAttemptRecord[]> {
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

  // ---------------------------------------------------------------------------
  // CompensationLedgerStore
  // ---------------------------------------------------------------------------

  async beginCompensation({ guard, ...params }: BeginCompensationParams): Promise<boolean> {
    const now = this.clock.currentTimeMs();
    return this._fenced({
      workflowId: params.workflowId,
      guard,
      write: () => {
        this.db
          .query(
            `UPDATE ${this._t}
             SET status = 'compensating', error = ?, error_tag = ?, updated_at = ?
             WHERE workflow_id = ? AND status IN ('pending', 'running', 'suspended')`,
          )
          .run(params.error, params.errorTag ?? null, now, params.workflowId);
        const row = this.db
          .query<{ status: string }>(`SELECT status FROM ${this._t} WHERE workflow_id = ?`)
          .get(params.workflowId);
        return row?.status === "compensating";
      },
    });
  }

  async saveStepCompensation({ guard, ...params }: SaveStepCompensationParams): Promise<void> {
    const now = this.clock.currentTimeMs();
    this._fenced({
      workflowId: params.workflowId,
      guard,
      write: () => {
        const row = this.db
          .query<{ steps: string }>(`SELECT steps FROM ${this._t} WHERE workflow_id = ?`)
          .get(params.workflowId);
        if (!row) return;
        const steps: Record<string, StepState> = JSON.parse(row.steps);
        const step = steps[params.stepName];
        if (!step) return;
        steps[params.stepName] = {
          ...withoutCompensationLedger(step),
          compensationStatus: params.status,
          ...(params.error !== undefined && { compensationError: params.error }),
          compensatedAt: new Date(now),
        };
        this.db
          .query(`UPDATE ${this._t} SET steps = ?, updated_at = ? WHERE workflow_id = ?`)
          .run(JSON.stringify(steps), now, params.workflowId);
      },
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
  error_tag: string | null;
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

/** Row shape shared by the scanner queries. */
interface ScanWakeupRow {
  workflow_id: string;
  workflow_name: string;
  version: string | null;
  input: string;
  step_name: string;
  reason?: string;
  signal_name: string | null;
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
