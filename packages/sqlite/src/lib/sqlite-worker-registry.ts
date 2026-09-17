import type { WorkerRegistry, WorkerInfo, WorkerStatus } from "@promin/workflow";
import type { SqliteDatabase } from "./sqlite-database.ts";

/**
 * Persistent WorkerRegistry backed by SQLite.
 *
 * Matches InMemoryWorkerRegistry's semantics exactly — idempotent
 * re-register, silent no-ops on unknown workers for heartbeat / drain /
 * deregister, atomic transition-to-dead in `detectDead` (covers both
 * active and draining workers whose heartbeat is stale).
 *
 * Uses the same `bun:sqlite`-compatible `SqliteDatabase` shape as the
 * other SQLite adapters in this package. Schema auto-creates on first use
 * via `CREATE TABLE IF NOT EXISTS`; no migration pipeline needed.
 *
 * SQLite-specific encodings:
 *   - `capabilities` is stored as a JSON string (no native TEXT[])
 *   - `metadata` is stored as JSON or NULL
 *   - timestamps are milliseconds-since-epoch INTEGERs, converted to Date
 *     on read — consistent with SqliteWorkflowStorage
 *
 * @example
 * ```ts
 * import { Database } from "bun:sqlite";
 * const registry = SqliteWorkerRegistry.make({ db: new Database("state.db") });
 * await registry.register({ workerId: "w-1", capabilities: ["gpu"], concurrency: 4 });
 * ```
 */
export class SqliteWorkerRegistry implements WorkerRegistry {
  private readonly _t: string;

  private constructor(
    private readonly db: SqliteDatabase,
    table: string,
  ) {
    this._t = table;
    this._setup();
  }

  static make(params: {
    db: SqliteDatabase;
    /** Override the table prefix (default: `promin_wf`). The worker table is `<prefix>_workers`. */
    tablePrefix?: string;
  }): SqliteWorkerRegistry {
    return new SqliteWorkerRegistry(params.db, params.tablePrefix ?? "promin_wf");
  }

  private _setup(): void {
    const t = this._t;
    const tbl = `${t}_workers`;

    // Migrate a pre-`retired` table: the old schema CHECK-constrained
    // status to ('active','draining','dead') and has no `retired_at`
    // column. SQLite can't ALTER a CHECK constraint, so rebuild — rename
    // the old table aside, create the new one, copy rows, drop the old.
    const existing = this.db
      .query<{ sql: string }>(`SELECT sql FROM sqlite_master WHERE type='table' AND name = ?`)
      .get(tbl);
    const legacy = existing != null && !existing.sql.includes("retired_at");
    if (legacy) {
      this.db.run(`ALTER TABLE ${tbl} RENAME TO ${tbl}_legacy`);
    }

    // Status is left unconstrained (validated by the WorkerStatus type) —
    // a CHECK can't be ALTER'd, and the four values are app-controlled.
    this.db.run(`
      CREATE TABLE IF NOT EXISTS ${tbl} (
        worker_id         TEXT    NOT NULL PRIMARY KEY,
        status            TEXT    NOT NULL DEFAULT 'active',
        capabilities      TEXT    NOT NULL DEFAULT '[]',
        concurrency       INTEGER NOT NULL DEFAULT 1,
        metadata          TEXT,
        started_at        INTEGER NOT NULL,
        last_heartbeat_at INTEGER NOT NULL,
        retired_at        INTEGER
      )
    `);

    if (legacy) {
      this.db.run(
        `INSERT INTO ${tbl}
           (worker_id, status, capabilities, concurrency, metadata, started_at, last_heartbeat_at)
         SELECT worker_id, status, capabilities, concurrency, metadata, started_at, last_heartbeat_at
         FROM ${tbl}_legacy`,
      );
      this.db.run(`DROP TABLE ${tbl}_legacy`);
    }

    this.db.run(`CREATE INDEX IF NOT EXISTS ${tbl}_status ON ${tbl} (status)`);
    this.db.run(`CREATE INDEX IF NOT EXISTS ${tbl}_heartbeat ON ${tbl} (last_heartbeat_at)`);
  }

  async register(params: {
    workerId: string;
    capabilities: readonly string[];
    concurrency: number;
    metadata?: Record<string, unknown>;
  }): Promise<void> {
    // Matches InMemory + Postgres: re-registering the same workerId
    // replaces the row and refreshes startedAt + heartbeat. A re-register
    // is conceptually a new worker lifecycle.
    const now = Date.now();
    this.db.run(
      `INSERT INTO ${this._t}_workers
         (worker_id, status, capabilities, concurrency, metadata, started_at, last_heartbeat_at)
       VALUES (?, 'active', ?, ?, ?, ?, ?)
       ON CONFLICT(worker_id) DO UPDATE SET
         status = 'active',
         capabilities = excluded.capabilities,
         concurrency = excluded.concurrency,
         metadata = excluded.metadata,
         started_at = excluded.started_at,
         last_heartbeat_at = excluded.last_heartbeat_at`,
      params.workerId,
      JSON.stringify([...params.capabilities]),
      params.concurrency,
      params.metadata != null ? JSON.stringify(params.metadata) : null,
      now,
      now,
    );
  }

  async heartbeat(workerId: string): Promise<void> {
    // Silently no-ops on missing worker (matches InMemory: heartbeats
    // from unknown workers are dropped, not an error).
    this.db.run(
      `UPDATE ${this._t}_workers SET last_heartbeat_at = ? WHERE worker_id = ?`,
      Date.now(),
      workerId,
    );
  }

  async drain(workerId: string): Promise<void> {
    this.db.run(`UPDATE ${this._t}_workers SET status = 'draining' WHERE worker_id = ?`, workerId);
  }

  async deregister(workerId: string): Promise<void> {
    // Retire, don't delete — the row stays for forensics until gc() reaps
    // it. No-ops on a missing worker (zero rows updated).
    this.db.run(
      `UPDATE ${this._t}_workers SET status = 'retired', retired_at = ? WHERE worker_id = ?`,
      Date.now(),
      workerId,
    );
  }

  async list(params?: { status?: WorkerStatus }): Promise<WorkerInfo[]> {
    const rows = params?.status
      ? this.db
          .query<WorkerRow>(`SELECT * FROM ${this._t}_workers WHERE status = ? ORDER BY worker_id`)
          .all(params.status)
      : this.db.query<WorkerRow>(`SELECT * FROM ${this._t}_workers ORDER BY worker_id`).all();
    return rows.map(rowToWorkerInfo);
  }

  async detectDead(timeoutMs: number): Promise<WorkerInfo[]> {
    // Atomic transition in one statement. Modern SQLite (bun:sqlite ships
    // 3.45+) supports UPDATE ... RETURNING, matching the Postgres impl's
    // single-round-trip sweep. Skips 'dead' and 'retired' — a retired
    // worker stopped on purpose and must not be relabelled a crash.
    const cutoff = Date.now() - timeoutMs;
    const rows = this.db
      .query<WorkerRow>(
        `UPDATE ${this._t}_workers
         SET status = 'dead'
         WHERE status NOT IN ('dead', 'retired') AND last_heartbeat_at < ?
         RETURNING *`,
      )
      .all(cutoff);
    return rows.map(rowToWorkerInfo);
  }

  async gc(params: { retainMs: number }): Promise<number> {
    // Reap on the most-recent activity: retired_at when the worker
    // retired, otherwise its last heartbeat. RETURNING gives the count.
    const cutoff = Date.now() - params.retainMs;
    const rows = this.db
      .query<{ worker_id: string }>(
        `DELETE FROM ${this._t}_workers
         WHERE COALESCE(retired_at, last_heartbeat_at) < ?
         RETURNING worker_id`,
      )
      .all(cutoff);
    return rows.length;
  }
}

interface WorkerRow {
  worker_id: string;
  status: string;
  capabilities: string;
  concurrency: number;
  metadata: string | null;
  started_at: number;
  last_heartbeat_at: number;
  retired_at: number | null;
}

function rowToWorkerInfo(row: WorkerRow): WorkerInfo {
  return {
    workerId: row.worker_id,
    status: row.status as WorkerStatus,
    capabilities: JSON.parse(row.capabilities) as string[],
    concurrency: row.concurrency,
    lastHeartbeat: new Date(row.last_heartbeat_at),
    startedAt: new Date(row.started_at),
    ...(row.retired_at != null ? { retiredAt: new Date(row.retired_at) } : {}),
    metadata:
      row.metadata != null ? (JSON.parse(row.metadata) as Record<string, unknown>) : undefined,
  };
}
