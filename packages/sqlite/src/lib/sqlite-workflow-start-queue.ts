// ---------------------------------------------------------------------------
// `SqliteWorkflowStartQueue` — `WorkflowStartQueue` over SQLite.
//
// Persistent across restarts so a trigger fired before a worker connects
// stays in the queue. Claim semantics use `BEGIN IMMEDIATE` to serialise
// the SELECT-then-UPDATE atomically — SQLite has no SKIP LOCKED, but at
// single-host concurrency the immediate-lock pattern is sufficient (the
// same shape SqliteSchedulerStorage uses for its leader path).
//
// One record per run: `workflow_id` is unique, so `enqueue` of a run that
// is already queued adds nothing; on a claimed record it sets `requeued`,
// and the claim's `complete` then puts the start back to pending.
//
// Stale-claim recovery: rows in `status='claimed'` whose last heartbeat
// (the claim time until the first one) is older than `reclaimAfterMs`
// drop back to pending on the next claim() call so a crashed worker
// doesn't strand a start indefinitely. Every claim gets a fresh
// `claim_token`; heartbeat, complete and release only act on the current
// token. `release({ delayMs })` sets `available_at`, before which the
// pending row is not claimable.
//
// Schema (auto-created on first use):
//
//   CREATE TABLE promin_workflow_starts (
//     id            TEXT PRIMARY KEY,
//     workflow_id   TEXT NOT NULL UNIQUE,
//     workflow_name TEXT NOT NULL,
//     version       TEXT,
//     metadata      TEXT,
//     enqueued_at   INTEGER NOT NULL,
//     available_at  INTEGER,
//     claimed_at    INTEGER,
//     claimed_by    TEXT,
//     claim_token   TEXT,
//     heartbeat_at  INTEGER,
//     requeued      INTEGER NOT NULL DEFAULT 0,
//     status        TEXT NOT NULL DEFAULT 'pending'
//   );
// ---------------------------------------------------------------------------

import { SystemWallClock, type WallClock } from "@promin/workflow";
import {
  DEFAULT_START_RECLAIM_AFTER_MS,
  claimTokenFor,
  startListLimit,
  startMatchesSpec,
  type WorkerWorkflowSpec,
  type WorkflowStartClaimParams,
  type WorkflowStartClaimRef,
  type WorkflowStartEnqueueParams,
  type WorkflowStartEnqueueResult,
  type WorkflowStartListParams,
  type WorkflowStartQueue,
  type WorkflowStartRecord,
  type WorkflowStartReleaseParams,
} from "@promin/workflow/distributed";
import type { SqliteDatabase } from "./sqlite-database.ts";

interface Row {
  id: string;
  workflow_id: string;
  workflow_name: string;
  version: string | null;
  metadata: string | null;
  enqueued_at: number;
  available_at: number | null;
  claimed_at: number | null;
  claimed_by: string | null;
  claim_token: string | null;
  heartbeat_at: number | null;
  requeued: number;
  status: string;
}

export interface SqliteWorkflowStartQueueOptions {
  db: SqliteDatabase;
  /** Override the table name (default: `promin_workflow_starts`). */
  tableName?: string;
  /**
   * A claim with no heartbeat for this many ms is re-claimable. Default:
   * `DEFAULT_START_RECLAIM_AFTER_MS` (the default run-lock lease).
   */
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
      opts.reclaimAfterMs ?? DEFAULT_START_RECLAIM_AFTER_MS,
      opts.clock ?? SystemWallClock,
    );
  }

  private _setup(): void {
    const t = this._t;
    this.db.run(`
      CREATE TABLE IF NOT EXISTS ${t} (
        id            TEXT PRIMARY KEY,
        workflow_id   TEXT NOT NULL UNIQUE,
        workflow_name TEXT NOT NULL,
        version       TEXT,
        metadata      TEXT,
        enqueued_at   INTEGER NOT NULL,
        available_at  INTEGER,
        claimed_at    INTEGER,
        claimed_by    TEXT,
        claim_token   TEXT,
        heartbeat_at  INTEGER,
        requeued      INTEGER NOT NULL DEFAULT 0,
        status        TEXT NOT NULL DEFAULT 'pending'
      )
    `);
    // Most claims hit the pending partition — partial index keeps the
    // scan tight even when the queue accumulates claimed rows.
    this.db.run(
      `CREATE INDEX IF NOT EXISTS ${t}_pending ON ${t} (workflow_name, enqueued_at) WHERE status = 'pending'`,
    );
    this.db.run(
      `CREATE INDEX IF NOT EXISTS ${t}_heartbeat ON ${t} (heartbeat_at) WHERE status = 'claimed'`,
    );
  }

  async enqueue(params: WorkflowStartEnqueueParams): Promise<WorkflowStartEnqueueResult> {
    const now = this.clock.currentTimeMs();
    const id = `start-${now.toString(36)}-${crypto.randomUUID().slice(0, 8)}`;
    // One statement: the insert, or on an existing record the requeue mark
    // (claimed) or the immediate availability (pending) — atomic with a
    // concurrent `complete`, in this process or another on the same file.
    const availableAt = params.keepAvailableAt
      ? "available_at"
      : "CASE WHEN status = 'pending' THEN NULL ELSE available_at END";
    const claimableNow = (r: { status: string; available_at: number | null } | null) =>
      r !== null && r.status === "pending" && (r.available_at === null || r.available_at <= now);
    return this.db.transaction((): WorkflowStartEnqueueResult => {
      // The record before this enqueue: did it make the start claimable?
      const prev = this.db
        .query<{ status: string; available_at: number | null }>(
          `SELECT status, available_at FROM ${this._t} WHERE workflow_id = ?`,
        )
        .get(params.workflowId);
      const row = this.db
        .query<{ id: string; status: string; available_at: number | null }>(
          `INSERT INTO ${this._t}
             (id, workflow_id, workflow_name, version, metadata, enqueued_at, status)
           VALUES (?, ?, ?, ?, ?, ?, 'pending')
           ON CONFLICT (workflow_id) DO UPDATE SET
             requeued = (status = 'claimed'),
             available_at = ${availableAt}
           RETURNING id, status, available_at`,
        )
        .get(
          id,
          params.workflowId,
          params.workflowName,
          params.version ?? null,
          params.metadata !== undefined ? JSON.stringify(params.metadata) : null,
          now,
        );
      const stored = row?.id ?? id;
      const created = stored === id;
      const claimable = claimableNow(row ?? null) && (created || !claimableNow(prev ?? null));
      return { id: stored, created, claimable };
    })();
  }

  async claim(params: WorkflowStartClaimParams): Promise<WorkflowStartRecord[]> {
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

      // A retried call: the starts an earlier call with this id claimed.
      if (params.claimId !== undefined) {
        claimTokenFor({ claimId: params.claimId, index: 0 }); // validates the id
        const already = this.db
          .query<Row>(
            `SELECT * FROM ${t} WHERE status = 'claimed' AND claim_token LIKE ?
             ORDER BY enqueued_at ASC, id ASC`,
          )
          .all(`${params.claimId}:%`);
        if (already.length > 0) {
          this.db.run("COMMIT");
          return already.map(rowToRecord);
        }
      }

      const specByName = new Map<string, WorkerWorkflowSpec>();
      for (const spec of params.workflowSpecs) specByName.set(spec.name, spec);
      const names = [...specByName.keys()];
      const placeholders = names.map(() => "?").join(", ");
      // The version-set match runs in JS, so page through the claimable
      // rows until `limit` matched: a run of starts this worker can't
      // serve never hides the ones behind it.
      const pageSize = Math.max(params.limit * 4, 50);
      for (let offset = 0; claimed.length < params.limit; offset += pageSize) {
        const page = this.db
          .query<Row>(
            `SELECT * FROM ${t}
             WHERE status = 'pending' AND workflow_name IN (${placeholders})
               AND (available_at IS NULL OR available_at <= ?)
             ORDER BY enqueued_at ASC, id ASC
             LIMIT ? OFFSET ?`,
          )
          .all(...names, now, pageSize, offset);
        for (const row of page) {
          if (claimed.length >= params.limit) break;
          const spec = specByName.get(row.workflow_name);
          if (!spec || !startMatchesSpec({ version: row.version, spec })) continue;
          claimed.push(
            rowToRecord({
              ...row,
              available_at: null,
              claimed_at: now,
              claimed_by: params.workerId ?? null,
              claim_token: claimTokenFor({
                ...(params.claimId !== undefined && { claimId: params.claimId }),
                index: claimed.length,
              }),
              heartbeat_at: now,
            }),
          );
        }
        if (page.length < pageSize) break;
      }

      const stamp = this.db.query(
        `UPDATE ${t}
           SET status = 'claimed', available_at = NULL, claimed_at = ?, claimed_by = ?,
               claim_token = ?, heartbeat_at = ?
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
    // Fenced by the claim token: a stale claimant can't touch the record a
    // newer claimant is running. A requeued start goes back to pending; any
    // other is deleted. One write transaction, so an enqueue from another
    // process lands before or after it, never between the two checks.
    this.db.run("BEGIN IMMEDIATE");
    try {
      this.db
        .query(
          `UPDATE ${this._t}
             SET status = 'pending', requeued = 0, available_at = NULL, claimed_at = NULL,
                 claimed_by = NULL, claim_token = NULL, heartbeat_at = NULL
           WHERE id = ? AND status = 'claimed' AND claim_token = ? AND requeued = 1`,
        )
        .run(params.id, params.claimToken);
      let done = this._changes() > 0;
      if (!done) {
        this.db
          .query(
            `DELETE FROM ${this._t}
             WHERE id = ? AND status = 'claimed' AND claim_token = ? AND requeued = 0`,
          )
          .run(params.id, params.claimToken);
        done = this._changes() > 0;
      }
      this.db.run("COMMIT");
      return done;
    } catch (err) {
      this.db.run("ROLLBACK");
      throw err;
    }
  }

  async release(params: WorkflowStartReleaseParams): Promise<boolean> {
    const delayMs = Math.max(0, params.delayMs ?? 0);
    const availableAt = delayMs > 0 ? this.clock.currentTimeMs() + delayMs : null;
    // A run enqueued again during the claim (`requeued`) is claimable at
    // once: the delay was chosen from the state before that enqueue.
    this.db
      .query(
        `UPDATE ${this._t}
           SET status = 'pending', requeued = 0,
               available_at = CASE WHEN requeued = 1 THEN NULL ELSE ? END, claimed_at = NULL,
               claimed_by = NULL, claim_token = NULL, heartbeat_at = NULL
         WHERE id = ? AND status = 'claimed' AND claim_token = ?`,
      )
      .run(availableAt, params.id, params.claimToken);
    return this._changes() > 0;
  }

  async list(params?: WorkflowStartListParams): Promise<WorkflowStartRecord[]> {
    // Sweep stale claims first so the snapshot reflects the current
    // claimable set, matching the in-memory impl's behaviour.
    this._sweepStale(this.clock.currentTimeMs());

    const rows = this.db
      .query<Row>(`SELECT * FROM ${this._t} ORDER BY enqueued_at ASC, id ASC LIMIT ?`)
      .all(startListLimit(params));
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
           SET status = 'pending', requeued = 0, claimed_at = NULL, claimed_by = NULL,
               claim_token = NULL, heartbeat_at = NULL
         WHERE status = 'claimed' AND COALESCE(heartbeat_at, claimed_at) < ?`,
      )
      .run(nowMs - this._reclaimAfterMs);
  }
}

function rowToRecord(r: Row): WorkflowStartRecord {
  return {
    id: r.id,
    workflowId: r.workflow_id,
    workflowName: r.workflow_name,
    enqueuedAt: r.enqueued_at,
    ...(r.version !== null && { version: r.version }),
    ...(r.metadata !== null && { metadata: JSON.parse(r.metadata) as Record<string, unknown> }),
    ...(r.available_at !== null && { availableAt: r.available_at }),
    ...(r.claimed_at !== null && { claimedAt: r.claimed_at }),
    ...(r.claimed_by !== null && { claimedBy: r.claimed_by }),
    ...(r.claim_token !== null && { claimToken: r.claim_token }),
    ...(r.heartbeat_at !== null && { heartbeatAt: r.heartbeat_at }),
    ...(r.requeued === 1 && { requeued: true }),
  };
}
