// ---------------------------------------------------------------------------
// PgSchedulerStorage — Postgres adapter for SchedulerStorage.
//
// Heavy lifting (cron/rrule/catch-up/jitter/leader loop) lives in the generic
// DurableScheduler shell in @promin/workflow. This file is the storage-only
// adapter — schedule CRUD, due-row lookup, and leader leases
// (`PgLeaderLeaseStore`, one `wf_leader_leases` row per lease key) with the
// lease fence checked inside `commitPoll`'s transaction.
// ---------------------------------------------------------------------------

import { and, asc, eq, inArray, isNotNull, lte, sql, type SQL } from "drizzle-orm";
import type {
  CommitPollResult,
  DurableScheduleConfig,
  LeaderLease,
  ScheduleCommit,
  SchedulerStorage,
} from "@promin/workflow";
import { durableSchedules, durableScheduleTicks, leaderLeases } from "./scheduler-schema.ts";
import type { DrizzleDb } from "@spilne/perfect-postgres";
import { SystemWallClock, type WallClock } from "@promin/workflow";
import { assertPgLeaseCurrent, PgLeaderLeaseStore } from "./pg-leader-lease-store.ts";
import { execRaw } from "./exec-raw.ts";

export interface PgSchedulerStorageConfig {
  db: DrizzleDb;
  /**
   * Time source for client-side `updatedAt` timestamps on schedule CRUD.
   * Lease expiry always uses the database clock. Default: `SystemWallClock`.
   */
  clock?: WallClock;
}

export class PgSchedulerStorage implements SchedulerStorage {
  /** Drizzle schema export — include in your migration pipeline. */
  static readonly schema = {
    schedules: durableSchedules,
    ticks: durableScheduleTicks,
    leaderLeases,
  };

  private readonly db: DrizzleDb;
  private readonly clock: WallClock;
  private readonly leases: PgLeaderLeaseStore;

  constructor(config: PgSchedulerStorageConfig) {
    this.db = config.db;
    this.clock = config.clock ?? SystemWallClock;
    this.leases = new PgLeaderLeaseStore({ db: config.db });
  }

  // -------------------------------------------------------------------------
  // Hot path
  // -------------------------------------------------------------------------

  async findDue(params: { now: Date; limit: number; namespace?: string }): Promise<string[]> {
    const filters: SQL[] = [
      eq(durableSchedules.enabled, true),
      isNotNull(durableSchedules.nextRun),
      lte(durableSchedules.nextRun, params.now),
    ];
    filters.push(
      params.namespace !== undefined
        ? eq(durableSchedules.namespace, params.namespace)
        : sql`${durableSchedules.namespace} IS NULL`,
    );
    const rows = await this.db
      .select({ id: durableSchedules.id })
      .from(durableSchedules)
      .where(and(...filters))
      .orderBy(asc(durableSchedules.nextRun))
      .limit(params.limit);
    return rows.map((r) => r.id);
  }

  async loadSchedule(id: string): Promise<DurableScheduleConfig | null> {
    const [row] = await this.db.select().from(durableSchedules).where(eq(durableSchedules.id, id));
    return row ? rowToConfig(row) : null;
  }

  async loadSchedules(ids: string[]): Promise<Map<string, DurableScheduleConfig>> {
    if (ids.length === 0) return new Map();
    const rows = await this.db
      .select()
      .from(durableSchedules)
      .where(inArray(durableSchedules.id, ids));
    const out = new Map<string, DurableScheduleConfig>();
    for (const row of rows) out.set(row.id, rowToConfig(row));
    return out;
  }

  async loadScheduleState(
    id: string,
  ): Promise<{ lastFired: Date | null; tickCount: number } | null> {
    const [row] = await this.db
      .select({
        lastFired: durableSchedules.lastFiredAt,
        tickCount: durableSchedules.tickCount,
      })
      .from(durableSchedules)
      .where(eq(durableSchedules.id, id));
    if (!row) return null;
    return { lastFired: row.lastFired ?? null, tickCount: Number(row.tickCount) };
  }

  async loadScheduleStates(
    ids: string[],
  ): Promise<Map<string, { lastFired: Date | null; tickCount: number }>> {
    if (ids.length === 0) return new Map();
    const rows = await this.db
      .select({
        id: durableSchedules.id,
        lastFired: durableSchedules.lastFiredAt,
        tickCount: durableSchedules.tickCount,
      })
      .from(durableSchedules)
      .where(inArray(durableSchedules.id, ids));
    const out = new Map<string, { lastFired: Date | null; tickCount: number }>();
    for (const row of rows) {
      out.set(row.id, {
        lastFired: row.lastFired ?? null,
        tickCount: Number(row.tickCount),
      });
    }
    return out;
  }

  async recordFire(id: string, firedAt: Date, count: number = 1): Promise<void> {
    await this.db
      .update(durableSchedules)
      .set({
        lastFiredAt: firedAt,
        tickCount: sql`${durableSchedules.tickCount} + ${count}`,
        updatedAt: this.clock.now(),
      })
      .where(eq(durableSchedules.id, id));
  }

  async setNextRun(id: string, nextRun: Date | null): Promise<void> {
    await this.db
      .update(durableSchedules)
      .set({ nextRun, updatedAt: this.clock.now() })
      .where(eq(durableSchedules.id, id));
  }

  async commitPoll(params: {
    updates: readonly ScheduleCommit[];
    lease?: LeaderLease;
  }): Promise<CommitPollResult> {
    const { updates, lease } = params;
    if (updates.length === 0 && !lease) return { conflicts: [] };
    if (!lease) return { conflicts: await this.applyCommits({ db: this.db, updates }) };
    // Fence and writes in one transaction: the share lock on the lease row
    // holds off a takeover until the writes are committed.
    const conflicts = await this.db.transaction(async (tx) => {
      const db = tx as unknown as DrizzleDb;
      await assertPgLeaseCurrent({ db, lease });
      return updates.length === 0 ? [] : await this.applyCommits({ db, updates });
    });
    return { conflicts };
  }

  /**
   * One `UPDATE … FROM (VALUES …)` for the whole batch. Rows whose
   * `expected` tick count no longer matches are left alone; the ids of
   * guarded entries that weren't updated are returned as conflicts.
   * Dates are bound as ISO strings with a `::timestamptz` cast — the
   * postgres-js bind path inside `sql.join` doesn't coerce Date.
   */
  private async applyCommits(params: {
    db: DrizzleDb;
    updates: readonly ScheduleCommit[];
  }): Promise<string[]> {
    const { updates } = params;
    const valuesSql = sql.join(
      updates.map(
        (u) =>
          sql`(${u.id}::text, ${u.firedAt?.toISOString() ?? null}::timestamptz, ${u.tickIncrement ?? 0}::bigint, ${u.nextRun !== undefined}::boolean, ${u.nextRun?.toISOString() ?? null}::timestamptz, ${u.expectedTickCount ?? null}::bigint)`,
      ),
      sql`, `,
    );
    const rows = await execRaw(
      params.db,
      sql`
      UPDATE wf_schedules AS s SET
        last_fired_at = COALESCE(v.fired_at, s.last_fired_at),
        tick_count = s.tick_count + v.tick_inc,
        -- A schedule paused since the poll loaded it stays out of due-tracking.
        next_run = CASE
          WHEN NOT s.enabled THEN NULL
          WHEN v.set_next THEN v.next_run
          ELSE s.next_run
        END,
        updated_at = ${this.clock.now().toISOString()}::timestamptz
      FROM (VALUES ${valuesSql}) AS v(id, fired_at, tick_inc, set_next, next_run, expected)
      WHERE s.id = v.id AND (v.expected IS NULL OR s.tick_count = v.expected)
      RETURNING s.id
    `,
    );
    const applied = new Set(rows.map((r) => String(r.id)));
    return updates
      .filter((u) => u.expectedTickCount !== undefined && !applied.has(u.id))
      .map((u) => u.id);
  }

  // -------------------------------------------------------------------------
  // Admin / CRUD
  // -------------------------------------------------------------------------

  async upsertSchedule(config: DurableScheduleConfig): Promise<void> {
    const enabled = config.enabled !== false;
    const values = {
      id: config.id,
      namespace: config.namespace ?? null,
      name: config.name,
      cron: config.cron,
      rrule: config.rrule,
      intervalMs: config.intervalMs,
      timezone: config.timezone ?? "UTC",
      maxCatchUp: config.maxCatchUp ?? 0,
      jitterMs: config.jitterMs ?? 0,
      enabled,
      startAt: config.startAt,
      endAt: config.endAt,
      metadata: config.metadata,
    };
    // Due-tracking follows the enabled flag. A disabled schedule gets
    // `nextRun = NULL`. An enabled one keeps an existing `nextRun` (the
    // caller recomputes it when the trigger changes) or is seeded at now so
    // `findDue` picks it up without a separate `setNextRun` call; a future
    // `startAt` is honored so a deferred schedule isn't reported as due.
    const now = this.clock.now();
    const seededNextRun = enabled
      ? config.startAt && config.startAt > now
        ? config.startAt
        : now
      : null;
    await this.db
      .insert(durableSchedules)
      .values({ ...values, nextRun: seededNextRun })
      .onConflictDoUpdate({
        target: durableSchedules.id,
        set: {
          ...values,
          nextRun: sql`CASE WHEN excluded.enabled THEN COALESCE(wf_schedules.next_run, excluded.next_run) ELSE NULL END`,
          updatedAt: now,
        },
      });
  }

  async deleteSchedule(id: string): Promise<void> {
    await this.db.delete(durableSchedules).where(eq(durableSchedules.id, id));
  }

  async setEnabled(id: string, enabled: boolean): Promise<void> {
    // Disabling drops the schedule from due-tracking; enabling one with no
    // `nextRun` seeds it at now (or a later `startAt`), like an insert.
    const now = this.clock.now();
    const nextRun = enabled
      ? sql`COALESCE(${durableSchedules.nextRun}, GREATEST(${now.toISOString()}::timestamptz, ${durableSchedules.startAt}))`
      : null;
    await this.db
      .update(durableSchedules)
      .set({ enabled, nextRun, updatedAt: now })
      .where(eq(durableSchedules.id, id));
  }

  async listSchedules(params?: {
    enabled?: boolean;
    namespace?: string;
    metadata?: Record<string, unknown>;
    limit?: number;
    offset?: number;
  }): Promise<DurableScheduleConfig[]> {
    const filters = this.buildScheduleFilters(params);
    const limit = params?.limit ?? 100;
    const offset = params?.offset ?? 0;
    const rows = await this.db
      .select()
      .from(durableSchedules)
      .where(filters.length > 0 ? and(...filters) : undefined)
      .limit(limit)
      .offset(offset);
    return rows.map(rowToConfig);
  }

  async countSchedules(params?: {
    enabled?: boolean;
    namespace?: string;
    metadata?: Record<string, unknown>;
  }): Promise<number> {
    const filters = this.buildScheduleFilters(params);
    const [row] = await this.db
      .select({ count: sql<number>`count(*)::int` })
      .from(durableSchedules)
      .where(filters.length > 0 ? and(...filters) : undefined);
    return Number(row?.count ?? 0);
  }

  /**
   * Shared filter assembly so list / count don't drift on predicate
   * semantics. Mirrors the same containment-`@>` shape that
   * `PostgresWorkflowStorage.listWorkflows` uses for its metadata filter,
   * and the in-memory / sqlite scheduler storages' equivalents.
   */
  private buildScheduleFilters(params?: {
    enabled?: boolean;
    namespace?: string;
    metadata?: Record<string, unknown>;
  }): SQL[] {
    const filters: SQL[] = [];
    if (params?.enabled !== undefined) {
      filters.push(eq(durableSchedules.enabled, params.enabled));
    }
    if (params?.namespace !== undefined) {
      filters.push(eq(durableSchedules.namespace, params.namespace));
    }
    if (params?.metadata && Object.keys(params.metadata).length > 0) {
      filters.push(sql`${durableSchedules.metadata} @> ${JSON.stringify(params.metadata)}::jsonb`);
    }
    return filters;
  }

  // -------------------------------------------------------------------------
  // Leader election — lease rows, expiry on the server clock
  // -------------------------------------------------------------------------

  async tryAcquireLeader(params: {
    key: string;
    instanceId: string;
    ttlMs: number;
  }): Promise<LeaderLease | null> {
    return await this.leases.tryAcquireLeader(params);
  }

  async releaseLeader(params: { lease: LeaderLease }): Promise<void> {
    await this.leases.releaseLeader(params);
  }

  async findDueAcross(params: {
    now: Date;
    limit: number;
    namespaces?: readonly (string | undefined)[];
  }): Promise<readonly { id: string; namespace?: string }[]> {
    // Single index scan over (namespace, next_run). The namespace filter,
    // when supplied, becomes an IN list (with optional NULL for global).
    const filters: SQL[] = [
      eq(durableSchedules.enabled, true),
      isNotNull(durableSchedules.nextRun),
      lte(durableSchedules.nextRun, params.now),
    ];
    if (params.namespaces) {
      const named = params.namespaces.filter((n): n is string => n !== undefined);
      const includeGlobal = params.namespaces.some((n) => n === undefined);
      const branches: SQL[] = [];
      if (named.length > 0) branches.push(inArray(durableSchedules.namespace, named));
      if (includeGlobal) branches.push(sql`${durableSchedules.namespace} IS NULL`);
      // Empty list (no global, no named) — match nothing.
      if (branches.length === 0) return [];
      filters.push(branches.length === 1 ? branches[0]! : sql`(${branches[0]} OR ${branches[1]})`);
    }
    const rows = await this.db
      .select({ id: durableSchedules.id, namespace: durableSchedules.namespace })
      .from(durableSchedules)
      .where(and(...filters))
      .orderBy(asc(durableSchedules.nextRun))
      .limit(params.limit);
    return rows.map((r) => ({ id: r.id, namespace: r.namespace ?? undefined }));
  }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function rowToConfig(row: any): DurableScheduleConfig {
  return {
    id: row.id,
    namespace: row.namespace ?? undefined,
    name: row.name ?? undefined,
    cron: row.cron ?? undefined,
    rrule: row.rrule ?? undefined,
    intervalMs: row.intervalMs ? Number(row.intervalMs) : undefined,
    timezone: row.timezone,
    maxCatchUp: row.maxCatchUp,
    jitterMs: row.jitterMs,
    enabled: row.enabled,
    startAt: row.startAt ?? undefined,
    endAt: row.endAt ?? undefined,
    metadata: row.metadata as Record<string, unknown> | undefined,
  };
}
