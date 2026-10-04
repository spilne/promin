// ---------------------------------------------------------------------------
// `SqliteWorkflowStartQueue` — `WorkflowStartQueue` over SQLite.
//
// Persistent across restarts so a trigger fired before a worker connects
// stays in the queue. Claim semantics use `BEGIN IMMEDIATE` to serialise
// the SELECT-then-UPDATE atomically — SQLite has no SKIP LOCKED, but at
// the demo's concurrency level the immediate-lock pattern is sufficient
// (the same shape SqliteSchedulerStorage uses for its leader path).
//
// Stale-claim recovery: rows in `status='claimed'` whose last heartbeat
// (the claim time until the first one) is older than `reclaimAfterMs`
// drop back to pending on the next claim() call so a crashed worker
// doesn't strand a start indefinitely. Every claim gets a fresh
// `claim_token`; heartbeat and complete only act on the current token.
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
//     claim_token   TEXT,
//     heartbeat_at  INTEGER,
//     status        TEXT NOT NULL DEFAULT 'pending'
//   );
// ---------------------------------------------------------------------------

import { SystemWallClock, type WallClock } from "@promin/workflow";
import {
  type WorkerWorkflowSpec,
  type WorkflowStartClaimRef,
  type WorkflowStartQueue,
  type WorkflowStartRecord,
} from "@promin/workflow/distributed";
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
  claim_token: string | null;
  heartbeat_at: number | null;
  status: string;
}

export interface SqliteWorkflowStartQueueOptions {
  db: SqliteDatabase;
  /** Override the table name (default: `promin_workflow_starts`). */
  tableName?: string;
  /** A claim with no heartbeat for this many ms is re-claimable. Default 60s. */
  reclaimAfterMs?: number;
  /** Time source for enqueue / claim timestamps and the reclaim cutoff. Default: `SystemWallClock`. */
  clock?: WallClock;
}

export class SqliteWorkflowStartQueue implements WorkflowStartQueue {
  private readonly _t: string;
  private readonly _reclaimAfterMs: number;
  private readonly clock: WallClock;

  private constructor(
    private readonly db: SqliteDatabase,
    table: string,
    reclaimAfterMs: number,
    clock: WallClock,
  ) {
    this._t = table;
    this._reclaimAfterMs = reclaimAfterMs;
    this.clock = clock;
    this._setup();
  }

  static make(opts: SqliteWorkflowStartQueueOptions): SqliteWorkflowStartQueue {
    return new SqliteWorkflowStartQueue(
      opts.db,
      opts.tableName ?? "promin_workflow_starts",
      opts.reclaimAfterMs ?? 60_000,
      opts.clock ?? SystemWallClock,
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
        claim_token   TEXT,
        heartbeat_at  INTEGER,
        status        TEXT NOT NULL DEFAULT 'pending'
      )
    `);
    // Most claims hit the pending partition — partial index keeps the
    // scan tight even when the queue accumulates completed history.
    this.db.run(
      `CREATE INDEX IF NOT EXISTS ${t}_pending ON ${t} (workflow_name, enqueued_at) WHERE status = 'pending'`,
    );
    this.db.run(
      `CREATE INDEX IF NOT EXISTS ${t}_heartbeat ON ${t} (heartbeat_at) WHERE status = 'claimed'`,
    );
  }

  async enqueue(params: {
    workflowId: string;
    workflowName: string;
    input: unknown;
    metadata?: Record<string, unknown>;
    version?: string;
  }): Promise<{ id: string }> {
    const id = `start-${this.clock.currentTimeMs().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
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
        this.clock.currentTimeMs(),
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
    const now = this.clock.currentTimeMs();

    // BEGIN IMMEDIATE acquires the write lock up front so the
    // SELECT → UPDATE pair runs without another writer slipping in
    // between. SQLite without this would let two concurrent claimers
    // race the same row.
    this.db.run("BEGIN IMMEDIATE");
    try {
      // First, recover stale claims back to pending.
      this._sweepStale(now);

      // Then find pending starts matching any of the worker's specs.
      // We can't push the version-set match into SQL portably, so we
      // grab a generous window of pending rows and filter in JS.
      const names = params.workflowSpecs.map((s) => s.name);
      const placeholders = names.map(() => "?").join(", ");
      const candidates = this.db
        .query<Row>(
          `SELECT * FROM ${t}
           WHERE status = 'pending' AND workflow_name IN (${placeholders})
           ORDER BY enqueued_at ASC
           LIMIT ?`,
        )
        .all(...names, String(Math.min(params.limit * 4, 200)));

      const specByName = new Map<string, WorkerWorkflowSpec>();
      for (const spec of params.workflowSpecs) specByName.set(spec.name, spec);

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
        claimed.push(
          rowToRecord({
            ...row,
            claimed_at: now,
            claimed_by: params.workerId ?? null,
            claim_token: crypto.randomUUID(),
            heartbeat_at: now,
          }),
        );
      }

      const stamp = this.db.query(
        `UPDATE ${t}
           SET status = 'claimed', claimed_at = ?, claimed_by = ?, claim_token = ?, heartbeat_at = ?
         WHERE id = ?`,
      );
      for (const rec of claimed) {
        stamp.run(now, params.workerId ?? null, rec.claimToken!, now, rec.id);
      }
      this.db.run("COMMIT");
    } catch (err) {
      this.db.run("ROLLBACK");
      throw err;
    }
    return claimed;
  }

  async heartbeat(params: WorkflowStartClaimRef): Promise<boolean> {
    const now = this.clock.currentTimeMs();
    this.db
      .query(
        `UPDATE ${this._t}
           SET heartbeat_at = ?
         WHERE id = ? AND status = 'claimed' AND claim_token = ?
           AND COALESCE(heartbeat_at, claimed_at) >= ?`,
      )
      .run(now, params.id, params.claimToken, now - this._reclaimAfterMs);
    return this._changes() > 0;
  }

  async complete(params: WorkflowStartClaimRef): Promise<boolean> {
    // Hard delete, fenced by the claim token: a stale claimant can't
    // remove the record a newer claimant is running.
    this.db
      .query(`DELETE FROM ${this._t} WHERE id = ? AND status = 'claimed' AND claim_token = ?`)
      .run(params.id, params.claimToken);
    return this._changes() > 0;
  }

  async list(): Promise<WorkflowStartRecord[]> {
    // Sweep stale claims first so the snapshot reflects the current
    // claimable set, matching the in-memory impl's behaviour.
    this._sweepStale(this.clock.currentTimeMs());

    const rows = this.db.query<Row>(`SELECT * FROM ${this._t} ORDER BY enqueued_at ASC`).all();
    return rows.map(rowToRecord);
  }

  /** Rows touched by the last write on this connection. */
  private _changes(): number {
    return this.db.query<{ changes: number }>(`SELECT changes() AS changes`).get()?.changes ?? 0;
  }

  /** Move claims whose last heartbeat is past the reclaim window back to pending. */
  private _sweepStale(nowMs: number): void {
    this.db
      .query(
        `UPDATE ${this._t}
           SET status = 'pending', claimed_at = NULL, claimed_by = NULL,
               claim_token = NULL, heartbeat_at = NULL
         WHERE status = 'claimed' AND COALESCE(heartbeat_at, claimed_at) < ?`,
      )
      .run(nowMs - this._reclaimAfterMs);
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
  if (r.claim_token !== null) (out as { claimToken?: string }).claimToken = r.claim_token;
  if (r.heartbeat_at !== null) (out as { heartbeatAt?: number }).heartbeatAt = r.heartbeat_at;
  return out;
}
