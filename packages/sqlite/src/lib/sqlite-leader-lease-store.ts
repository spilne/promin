// ---------------------------------------------------------------------------
// SqliteLeaderLeaseStore — `LeaderLeaseStore` over one SQLite table.
//
// Acquire/refresh is a single `INSERT … ON CONFLICT DO UPDATE … WHERE …
// RETURNING epoch`, so it is atomic across every connection to the file.
// SQLite has no server clock: expiry uses the injected `WallClock`, so the
// processes sharing a database file must agree on time.
//
// Fencing: `assertCurrent` reads the key's epoch synchronously. Call it
// inside the `db.transaction(...)` that does the fenced writes.
// ---------------------------------------------------------------------------

import { SystemWallClock, type WallClock } from "@promin/workflow";
import {
  StaleLeaseError,
  type LeaderLease,
  type LeaderLeaseStore,
} from "@promin/workflow/scheduler";
import type { SqliteDatabase } from "./sqlite-database.ts";

export class SqliteLeaderLeaseStore implements LeaderLeaseStore {
  private constructor(
    private readonly db: SqliteDatabase,
    private readonly table: string,
    private readonly clock: WallClock,
  ) {
    this.db.run(`
      CREATE TABLE IF NOT EXISTS ${table} (
        lease_key  TEXT    NOT NULL PRIMARY KEY,
        holder     TEXT,
        epoch      INTEGER NOT NULL,
        expires_at INTEGER NOT NULL
      )
    `);
  }

  /**
   * Create the store, creating its table if needed.
   * Default table: `promin_leader_leases`; default clock: `SystemWallClock`.
   */
  static make(params: {
    db: SqliteDatabase;
    table?: string;
    clock?: WallClock;
  }): SqliteLeaderLeaseStore {
    return new SqliteLeaderLeaseStore(
      params.db,
      params.table ?? "promin_leader_leases",
      params.clock ?? SystemWallClock,
    );
  }

  async tryAcquireLeader(params: {
    key: string;
    instanceId: string;
    ttlMs: number;
  }): Promise<LeaderLease | null> {
    const now = this.clock.currentTimeMs();
    const expiresAt = now + Math.max(0, params.ttlMs);
    // A live lease of the same holder is refreshed under its epoch; a new
    // lease (first, after expiry, after release, or a takeover) bumps it.
    const row = this.db
      .query<{ epoch: number }>(
        `INSERT INTO ${this.table} (lease_key, holder, epoch, expires_at) VALUES (?, ?, 1, ?)
         ON CONFLICT (lease_key) DO UPDATE SET
           epoch = CASE WHEN holder = excluded.holder AND expires_at > ? THEN epoch ELSE epoch + 1 END,
           holder = excluded.holder,
           expires_at = excluded.expires_at
         WHERE holder IS NULL OR holder = excluded.holder OR expires_at <= ?
         RETURNING epoch`,
      )
      .get(params.key, params.instanceId, expiresAt, now, now);
    if (!row) return null;
    return { key: params.key, instanceId: params.instanceId, epoch: Number(row.epoch) };
  }

  async releaseLeader(params: { lease: LeaderLease }): Promise<void> {
    const { lease } = params;
    this.db
      .query(
        `UPDATE ${this.table} SET holder = NULL, expires_at = ?
          WHERE lease_key = ? AND holder = ? AND epoch = ?`,
      )
      .run(this.clock.currentTimeMs(), lease.key, lease.instanceId, lease.epoch);
  }

  /** Throw `StaleLeaseError` unless `lease` carries its key's current epoch. */
  assertCurrent(lease: LeaderLease): void {
    const row = this.db
      .query<{ epoch: number }>(`SELECT epoch FROM ${this.table} WHERE lease_key = ?`)
      .get(lease.key);
    const current = row ? Number(row.epoch) : null;
    if (current !== lease.epoch) throw new StaleLeaseError({ lease, currentEpoch: current });
  }
}
