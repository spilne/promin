// ---------------------------------------------------------------------------
// PgLeaderElection — Postgres advisory lock based leader election
// ---------------------------------------------------------------------------

import { sql } from "drizzle-orm";
import type { LeaderElection } from "@promin/workflow";
import { type DrizzleDb, execRaw } from "./drizzle-db.ts";

export interface PgLeaderElectionConfig {
  db: DrizzleDb;
  /** Advisory lock ID. Default: hash of "promin-coordinator". */
  lockId?: number;
}

export class PgLeaderElection implements LeaderElection {
  private readonly db: DrizzleDb;
  private readonly lockId: number;

  constructor(config: PgLeaderElectionConfig) {
    this.db = config.db;
    this.lockId = config.lockId ?? hashToInt32("promin-coordinator");
  }

  async tryAcquire(): Promise<boolean> {
    const [result] = await execRaw(
      this.db,
      sql`SELECT pg_try_advisory_lock(${this.lockId}) as acquired`,
    );
    return result?.acquired === true;
  }

  async release(): Promise<void> {
    await execRaw(this.db, sql`SELECT pg_advisory_unlock(${this.lockId})`);
  }
}

function hashToInt32(str: string): number {
  let hash = 0;
  for (let i = 0; i < str.length; i++) {
    const char = str.charCodeAt(i);
    hash = (hash << 5) - hash + char;
    hash |= 0;
  }
  return hash;
}
