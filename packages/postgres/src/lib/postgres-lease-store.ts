// ---------------------------------------------------------------------------
// PostgresLeaseStore — `LeaseStore` over the `agent_thread_lease` table.
//
// Atomic claim via `INSERT ... ON CONFLICT DO UPDATE WHERE expires_at <= now`:
//   - first acquire on a key inserts a row → returns it
//   - acquire when the existing lease has expired → updates the row to the
//     new lease (steal) → returns it
//   - acquire when the existing lease is live → ON CONFLICT path's WHERE
//     fails → no row updated → RETURNING yields no rows → loser does a
//     follow-up SELECT to surface the current owner
//
// Doesn't pin a PG connection per held lease (unlike pg_try_advisory_lock),
// so concurrent active turns are bounded by table contention, not pool
// size. The cost is a TTL-vs-realtime failover gap: a crashed worker's
// lease releases at expiresAt, not at TCP-disconnect time.
// ---------------------------------------------------------------------------

import { randomUUID } from "node:crypto";
import { and, eq, sql } from "drizzle-orm";
import type {
  AcquireResult,
  ExtendResult,
  LeaseStore,
  ThreadLease,
  ThreadLeaseKey,
} from "@promin/agent";
import type { DrizzleDb } from "./drizzle-db.ts";
import { agentThreadLease } from "./schema.ts";

export interface PostgresLeaseStoreConfig {
  readonly db: DrizzleDb;
  /** Optional clock override for tests. Defaults to `Date.now()`. */
  readonly now?: () => number;
}

export class PostgresLeaseStore implements LeaseStore {
  private readonly db: DrizzleDb;
  private readonly clock: () => number;

  constructor(config: PostgresLeaseStoreConfig) {
    this.db = config.db;
    this.clock = config.now ?? (() => Date.now());
  }

  async acquire(params: {
    key: ThreadLeaseKey;
    ownerId: string;
    ttlMs: number;
  }): Promise<AcquireResult> {
    const now = this.clock();
    const newLeaseId = randomUUID();
    const expiresAt = now + params.ttlMs;

    // Atomic insert-or-steal-if-expired. The CONFLICT WHERE filter only
    // updates when the existing row is past expiresAt; otherwise the
    // upsert no-ops and RETURNING yields zero rows.
    const inserted = await this.db
      .insert(agentThreadLease)
      .values({
        namespaceId: params.key.namespaceId,
        threadId: params.key.threadId,
        leaseId: newLeaseId,
        ownerId: params.ownerId,
        acquiredAt: now,
        expiresAt,
      })
      .onConflictDoUpdate({
        target: [agentThreadLease.namespaceId, agentThreadLease.threadId],
        set: {
          leaseId: newLeaseId,
          ownerId: params.ownerId,
          acquiredAt: now,
          expiresAt,
        },
        setWhere: sql`${agentThreadLease.expiresAt} <= ${now}`,
      })
      .returning();

    const row = inserted[0];
    if (row && row.leaseId === newLeaseId) {
      return { acquired: true, lease: rowToLease(row) };
    }

    // Lost contention — fetch the live owner to surface in the response.
    const liveRows = await this.db
      .select()
      .from(agentThreadLease)
      .where(
        and(
          eq(agentThreadLease.namespaceId, params.key.namespaceId),
          eq(agentThreadLease.threadId, params.key.threadId),
        ),
      )
      .limit(1);
    const live = liveRows[0];
    if (!live) {
      // Race between our INSERT and another worker's release/expire.
      // Retry once — the second attempt is on a guaranteed-empty key.
      return this.acquire(params);
    }
    return { acquired: false, currentLease: rowToLease(live) };
  }

  async extend(params: { leaseId: string; additionalMs: number }): Promise<ExtendResult> {
    const now = this.clock();
    // Update only if leaseId matches AND lease hasn't expired yet.
    // Drizzle's update returns affected rows via .returning().
    const updated = await this.db
      .update(agentThreadLease)
      .set({ expiresAt: sql`${agentThreadLease.expiresAt} + ${params.additionalMs}` })
      .where(
        and(
          eq(agentThreadLease.leaseId, params.leaseId),
          sql`${agentThreadLease.expiresAt} > ${now}`,
        ),
      )
      .returning();
    if (updated[0]) {
      return { extended: true, lease: rowToLease(updated[0]) };
    }
    // Either leaseId doesn't exist, or the lease has expired (and may
    // have been stolen). Fetch the current state by leaseId so the
    // caller can see whether it was expired-but-still-theirs or stolen.
    const liveByLease = await this.db
      .select()
      .from(agentThreadLease)
      .where(eq(agentThreadLease.leaseId, params.leaseId))
      .limit(1);
    return {
      extended: false,
      currentLease: liveByLease[0] ? rowToLease(liveByLease[0]) : null,
    };
  }

  async release(params: { leaseId: string }): Promise<void> {
    // Delete only if leaseId matches — protects against deleting
    // someone else's lease that came in after a steal.
    await this.db.delete(agentThreadLease).where(eq(agentThreadLease.leaseId, params.leaseId));
  }

  async get(key: ThreadLeaseKey): Promise<ThreadLease | null> {
    const rows = await this.db
      .select()
      .from(agentThreadLease)
      .where(
        and(
          eq(agentThreadLease.namespaceId, key.namespaceId),
          eq(agentThreadLease.threadId, key.threadId),
        ),
      )
      .limit(1);
    const row = rows[0];
    return row ? rowToLease(row) : null;
  }
}

interface DbLeaseRow {
  namespaceId: string;
  threadId: string;
  leaseId: string;
  ownerId: string;
  acquiredAt: number;
  expiresAt: number;
}

function rowToLease(r: DbLeaseRow): ThreadLease {
  return {
    key: { namespaceId: r.namespaceId, threadId: r.threadId },
    ownerId: r.ownerId,
    leaseId: r.leaseId,
    acquiredAt: r.acquiredAt,
    expiresAt: r.expiresAt,
  };
}
