// ---------------------------------------------------------------------------
// PgLeaderLeaseStore — `LeaderLeaseStore` over the `wf_leader_leases` table.
//
// One row per key: holder, epoch (fencing token) and expiry, all decided by
// a single `INSERT … ON CONFLICT DO UPDATE … WHERE` against the server clock
// (`NOW()`), so app-server clock skew never shortens or stretches a lease.
// Unlike a session advisory lock through a pool, the lease belongs to the
// instance id rather than to whichever connection ran the statement, honours
// its TTL and is released explicitly.
//
// Fencing: `assertPgLeaseCurrent` locks the lease row `FOR SHARE` and checks
// the epoch. Run it inside the transaction that does the fenced writes: a
// concurrent takeover then waits for that transaction, so a write either
// commits before the new lease starts or is rejected.
// ---------------------------------------------------------------------------

import { sql } from "drizzle-orm";
import {
  StaleLeaseError,
  type LeaderLease,
  type LeaderLeaseStore,
} from "@promin/workflow/scheduler";
import type { DrizzleDb } from "@spilne/perfect-postgres";
import { execRaw } from "./exec-raw.ts";

export interface PgLeaderLeaseStoreConfig {
  db: DrizzleDb;
}

export class PgLeaderLeaseStore implements LeaderLeaseStore {
  private readonly db: DrizzleDb;

  constructor(config: PgLeaderLeaseStoreConfig) {
    this.db = config.db;
  }

  async tryAcquireLeader(params: {
    key: string;
    instanceId: string;
    ttlMs: number;
  }): Promise<LeaderLease | null> {
    const ttlMs = Math.max(0, Math.trunc(params.ttlMs));
    // A live lease of the same holder is refreshed under its epoch; a new
    // lease (first, after expiry, after release, or a takeover) bumps it.
    const rows = await execRaw(
      this.db,
      sql`
      INSERT INTO wf_leader_leases AS l (lease_key, holder, epoch, expires_at)
      VALUES (
        ${params.key}, ${params.instanceId}, 1,
        NOW() + (${ttlMs}::double precision * INTERVAL '1 millisecond')
      )
      ON CONFLICT (lease_key) DO UPDATE SET
        epoch = CASE
          WHEN l.holder = EXCLUDED.holder AND l.expires_at > NOW() THEN l.epoch
          ELSE l.epoch + 1
        END,
        holder = EXCLUDED.holder,
        expires_at = EXCLUDED.expires_at
      WHERE l.holder IS NULL OR l.holder = EXCLUDED.holder OR l.expires_at <= NOW()
      RETURNING l.epoch
    `,
    );
    const row = rows[0];
    if (!row) return null;
    return { key: params.key, instanceId: params.instanceId, epoch: Number(row.epoch) };
  }

  async releaseLeader(params: { lease: LeaderLease }): Promise<void> {
    const { lease } = params;
    await execRaw(
      this.db,
      sql`
      UPDATE wf_leader_leases SET holder = NULL, expires_at = NOW()
      WHERE lease_key = ${lease.key} AND holder = ${lease.instanceId} AND epoch = ${lease.epoch}
    `,
    );
  }
}

/**
 * Fence check for Postgres writers: throw `StaleLeaseError` unless `lease`
 * carries the current epoch of its key. Pass the transaction the fenced
 * writes run in; the lease row stays share-locked until it ends, so no
 * takeover can slip in between this check and the writes.
 */
export async function assertPgLeaseCurrent(params: {
  db: DrizzleDb;
  lease: LeaderLease;
}): Promise<void> {
  const { lease } = params;
  const rows = await execRaw(
    params.db,
    sql`SELECT epoch FROM wf_leader_leases WHERE lease_key = ${lease.key} FOR SHARE`,
  );
  const current = rows[0] ? Number(rows[0].epoch) : null;
  if (current !== lease.epoch) throw new StaleLeaseError({ lease, currentEpoch: current });
}
