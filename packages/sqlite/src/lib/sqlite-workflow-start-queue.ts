// ---------------------------------------------------------------------------
// `SqliteWorkflowStartQueue` — `WorkflowStartQueue` over SQLite.
//
// Persistent across restarts so a trigger fired before a worker connects
// stays in the queue. Claim semantics use `BEGIN IMMEDIATE` to serialise
// the SELECT-then-UPDATE atomically — SQLite has no SKIP LOCKED, but at
// the demo's concurrency level the immediate-lock pattern is sufficient
// (the same shape SqliteSchedulerStorage uses for its leader path).
//
// Stale-claim recovery: rows in `status='claimed'` past `reclaimAfterMs`
// drop back to pending on the next claim() call so a crashed worker
// doesn't strand a start indefinitely.
//
// Schema (auto-created on first use):
//
//   CREATE TABLE promin_workflow_starts (
//     id            TEXT PRIMARY KEY,
//     workflow_id   TEXT NOT NULL,
//     workflow_name TEXT NOT NULL,
//     version       TEXT,
//     input         TEXT NOT NULL,
//     metadata      TEXT,
//     enqueued_at   INTEGER NOT NULL,
//     claimed_at    INTEGER,
//     claimed_by    TEXT,
//     status        TEXT NOT NULL DEFAULT 'pending'
//   );
// ---------------------------------------------------------------------------

import type { WorkerWorkflowSpec, WorkflowStartQueue, WorkflowStartRecord } from "@promin/workflow";
import type { SqliteDatabase } from "./sqlite-database.ts";

interface Row {
  id: string;
  workflow_id: string;
  workflow_name: string;
  version: string | null;
  input: string;
  metadata: string | null;
  enqueued_at: number;
  claimed_at: number | null;
  claimed_by: string | null;
  status: string;
}

export interface SqliteWorkflowStartQueueOptions {
  db: SqliteDatabase;
  /** Override the table name (default: `promin_workflow_starts`). */
  tableName?: string;
  /** Worker stuck mid-execution — re-claimable after this many ms. Default 60s. */
  reclaimAfterMs?: number;
}

export class SqliteWorkflowStartQueue implements WorkflowStartQueue {
  private readonly _t: string;
  private readonly _reclaimAfterMs: number;

  private constructor(
    private readonly db: SqliteDatabase,
    table: string,
    reclaimAfterMs: number,
  ) {
    this._t = table;
    this._reclaimAfterMs = reclaimAfterMs;
    this._setup();
  }

  static make(opts: SqliteWorkflowStartQueueOptions): SqliteWorkflowStartQueue {
    return new SqliteWorkflowStartQueue(
      opts.db,
      opts.tableName ?? "promin_workflow_starts",
      opts.reclaimAfterMs ?? 60_000,
    );
  }

  private _setup(): void {
    const t = this._t;
    this.db.run(`
      CREATE TABLE IF NOT EXISTS ${t} (
        id            TEXT PRIMARY KEY,
        workflow_id   TEXT NOT NULL,
        workflow_name TEXT NOT NULL,
        version       TEXT,
        input         TEXT NOT NULL,
        metadata      TEXT,
        enqueued_at   INTEGER NOT NULL,
        claimed_at    INTEGER,
        claimed_by    TEXT,
        status        TEXT NOT NULL DEFAULT 'pending'
      )
    `);
    // Most claims hit the pending partition — partial index keeps the
    // scan tight even when the queue accumulates completed history.
    this.db.run(
      `CREATE INDEX IF NOT EXISTS ${t}_pending ON ${t} (workflow_name, enqueued_at) WHERE status = 'pending'`,
    );
    this.db.run(
      `CREATE INDEX IF NOT EXISTS ${t}_claimed ON ${t} (claimed_at) WHERE status = 'claimed'`,
    );
  }

  async enqueue(params: {
    workflowId: string;
    workflowName: string;
    input: unknown;
    metadata?: Record<string, unknown>;
    version?: string;
  }): Promise<{ id: string }> {
    const id = `start-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
    this.db
      .query(
        `INSERT INTO ${this._t}
           (id, workflow_id, workflow_name, version, input, metadata, enqueued_at, status)
         VALUES (?, ?, ?, ?, ?, ?, ?, 'pending')`,
      )
      .run(
        id,
        params.workflowId,
        params.workflowName,
        params.version ?? null,
        JSON.stringify(params.input),
        params.metadata !== undefined ? JSON.stringify(params.metadata) : null,
        Date.now(),
      );
    return { id };
  }

  async claim(params: {
    workflowSpecs: readonly WorkerWorkflowSpec[];
    workerId?: string;
    limit: number;
  }): Promise<WorkflowStartRecord[]> {
    if (params.workflowSpecs.length === 0 || params.limit <= 0) return [];

    const t = this._t;
    const claimed: WorkflowStartRecord[] = [];
    const now = Date.now();

    // BEGIN IMMEDIATE acquires the write lock up front so the
    // SELECT → UPDATE pair runs without another writer slipping in
    // between. SQLite without this would let two concurrent claimers
    // race the same row.
    this.db.run("BEGIN IMMEDIATE");
    try {
      // First, recover stale claims back to pending.
      const cutoff = now - this._reclaimAfterMs;
      this.db
        .query(
          `UPDATE ${t}
             SET status = 'pending', claimed_at = NULL, claimed_by = NULL
           WHERE status = 'claimed' AND claimed_at < ?`,
        )
        .run(cutoff);

      // Then find pending starts matching any of the worker's specs.
      // We can't push the version-set match into SQL portably, so we
      // grab a generous window of pending rows and filter in JS.
      const names = params.workflowSpecs.map((s) => s.name);
      const placeholders = names.map(() => "?").join(", ");
      const candidates = this.db
        .query<Row, string[]>(
          `SELECT * FROM ${t}
           WHERE status = 'pending' AND workflow_name IN (${placeholders})
           ORDER BY enqueued_at ASC
           LIMIT ?`,
        )
        .all(...names, String(Math.min(params.limit * 4, 200)));

      const specByName = new Map<string, WorkerWorkflowSpec>();
      for (const spec of params.workflowSpecs) specByName.set(spec.name, spec);

      const idsToClaim: string[] = [];
      for (const row of candidates) {
        if (claimed.length >= params.limit) break;
        const spec = specByName.get(row.workflow_name);
        if (!spec) continue;
        // Version match: versionless rows always match; pinned rows
        // only match when the spec advertises that exact version OR
        // an empty versions list ("any").
        if (
          row.version !== null &&
          spec.versions.length > 0 &&
          !spec.versions.includes(row.version)
        ) {
          continue;
        }
        idsToClaim.push(row.id);
        claimed.push(rowToRecord({ ...row, claimed_at: now, claimed_by: params.workerId ?? null }));
      }

      if (idsToClaim.length > 0) {
        const ph = idsToClaim.map(() => "?").join(", ");
        this.db
          .query(
            `UPDATE ${t}
               SET status = 'claimed', claimed_at = ?, claimed_by = ?
             WHERE id IN (${ph})`,
          )
          .run(now, params.workerId ?? null, ...idsToClaim);
      }
      this.db.run("COMMIT");
    } catch (err) {
      this.db.run("ROLLBACK");
      throw err;
    }
    return claimed;
  }

  async complete(id: string): Promise<void> {
    // Match the in-memory contract: complete() is a hard delete (the
    // record is no longer in inflight after complete). Predates a
    // 'completed' status so list() doesn't surface long-finished rows.
    this.db.query(`DELETE FROM ${this._t} WHERE id = ?`).run(id);
  }

  async list(): Promise<WorkflowStartRecord[]> {
    // Sweep stale claims first so the snapshot reflects the current
    // claimable set, matching the in-memory impl's behaviour.
    const cutoff = Date.now() - this._reclaimAfterMs;
    this.db
      .query(
        `UPDATE ${this._t}
           SET status = 'pending', claimed_at = NULL, claimed_by = NULL
         WHERE status = 'claimed' AND claimed_at < ?`,
      )
      .run(cutoff);

    const rows = this.db.query<Row, []>(`SELECT * FROM ${this._t} ORDER BY enqueued_at ASC`).all();
    return rows.map(rowToRecord);
  }
}

function rowToRecord(r: Row): WorkflowStartRecord {
  const out: WorkflowStartRecord = {
    id: r.id,
    workflowId: r.workflow_id,
    workflowName: r.workflow_name,
    input: JSON.parse(r.input),
    enqueuedAt: r.enqueued_at,
  };
  if (r.version !== null) (out as { version?: string }).version = r.version;
  if (r.metadata !== null) {
    (out as { metadata?: Record<string, unknown> }).metadata = JSON.parse(r.metadata);
  }
  if (r.claimed_at !== null) (out as { claimedAt?: number }).claimedAt = r.claimed_at;
  if (r.claimed_by !== null) (out as { claimedBy?: string }).claimedBy = r.claimed_by;
  return out;
}
