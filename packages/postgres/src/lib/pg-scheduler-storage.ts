// ---------------------------------------------------------------------------
// PgSchedulerStorage — Postgres adapter for SchedulerStorage.
//
// Heavy lifting (cron/rrule/catch-up/jitter/leader loop) lives in the generic
// DurableScheduler shell in @promin/workflow. This file is the storage-only
// adapter — schedule CRUD, due-row lookup, and pg_advisory_lock-based leader
// election.
// ---------------------------------------------------------------------------

import { and, asc, eq, inArray, isNotNull, lte, sql, type SQL } from "drizzle-orm";
import type { DurableScheduleConfig, SchedulerStorage } from "@promin/workflow";
import { durableSchedules, durableScheduleTicks } from "./scheduler-schema.ts";
import { type DrizzleDb, execRaw } from "./drizzle-db.ts";
import { SystemClock, type Clock } from "@promin/core";

export interface PgSchedulerStorageConfig {
  db: DrizzleDb;
  /**
   * Namespace → leader-lock advisory ID. If unset, all namespaces share one
   * lock derived from the literal "wf-scheduler-leader". Callers running
   * multiple per-namespace leaders should provide unique IDs per namespace.
   */
  leaderLockId?: number;
  /**
   * Time source for client-side `updatedAt` timestamps on schedule CRUD.
   * Default: `SystemClock`. Pass a `FakeClock` for deterministic tests.
   */
  clock?: Clock;
}

export class PgSchedulerStorage implements SchedulerStorage {
  /** Drizzle schema export — include in your migration pipeline. */
  static readonly schema = {
    schedules: durableSchedules,
    ticks: durableScheduleTicks,
  };

  private readonly db: DrizzleDb;
  private readonly leaderLockId: number;
  private readonly clock: Clock;

  constructor(config: PgSchedulerStorageConfig) {
    this.db = config.db;
    this.leaderLockId = config.leaderLockId ?? hashToInt32("wf-scheduler-leader");
    this.clock = config.clock ?? SystemClock;
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

  async commitPoll(
    updates: Array<{
      id: string;
      firedAt?: Date;
      tickIncrement?: number;
      nextRun: Date | null;
    }>,
  ): Promise<void> {
    if (updates.length === 0) return;

    // One UPDATE … FROM (VALUES …) statement covers every id in the batch.
    // Drizzle's .update().from() expects a typed Table, not a (VALUES …)
    // literal — so the VALUES list and join predicate use `sql`, while the
    // column SETs reference the typed `durableSchedules` table for safety.
    // Dates are serialized to ISO strings explicitly — the postgres-js bind
    // path inside `sql.raw`/`sql.join` doesn't auto-coerce Date in this path,
    // so we rely on the `::timestamptz` cast to parse the string server-side.
    const valuesSql = sql.join(
      updates.map(
        (u) =>
          sql`(${u.id}::text, ${u.firedAt?.toISOString() ?? null}::timestamptz, ${u.tickIncrement ?? 0}::bigint, ${u.nextRun?.toISOString() ?? null}::timestamptz)`,
      ),
      sql`, `,
    );

    await this.db
      .update(durableSchedules)
      .set({
        lastFiredAt: sql`COALESCE(v.fired_at, ${durableSchedules.lastFiredAt})`,
        tickCount: sql`${durableSchedules.tickCount} + v.tick_inc`,
        nextRun: sql`v.next_run::timestamptz`,
        updatedAt: this.clock.now(),
      })
      .from(sql`(VALUES ${valuesSql}) AS v(id, fired_at, tick_inc, next_run)` as any)
      .where(sql`${durableSchedules.id} = v.id`);
  }

  // -------------------------------------------------------------------------
  // Admin / CRUD
  // -------------------------------------------------------------------------

  async upsertSchedule(config: DurableScheduleConfig): Promise<void> {
    const values = {
      id: config.id,
      namespace: config.namespace ?? null,
      name: config.name,
      cron: config.cron,
      rrule: config.rrule,
      intervalMs: config.intervalMs,
      timezone: config.timezone ?? "UTC",
      overlapPolicy: config.overlapPolicy ?? "allow",
      maxCatchUp: config.maxCatchUp ?? 0,
      jitterMs: config.jitterMs ?? 0,
      enabled: config.enabled !== false,
      startAt: config.startAt,
      endAt: config.endAt,
      metadata: config.metadata,
    };
    await this.db
      .insert(durableSchedules)
      .values(values)
      .onConflictDoUpdate({
        target: durableSchedules.id,
        set: { ...values, updatedAt: this.clock.now() },
      });
  }

  async deleteSchedule(id: string): Promise<void> {
    await this.db.delete(durableSchedules).where(eq(durableSchedules.id, id));
  }

  async setEnabled(id: string, enabled: boolean): Promise<void> {
    await this.db
      .update(durableSchedules)
      .set({ enabled, updatedAt: this.clock.now() })
      .where(eq(durableSchedules.id, id));
  }

  async listSchedules(params?: {
    enabled?: boolean;
    namespace?: string;
    limit?: number;
    offset?: number;
  }): Promise<DurableScheduleConfig[]> {
    const filters: SQL[] = [];
    if (params?.enabled !== undefined) {
      filters.push(eq(durableSchedules.enabled, params.enabled));
    }
    if (params?.namespace !== undefined) {
      filters.push(eq(durableSchedules.namespace, params.namespace));
    }
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

  async countSchedules(params?: { enabled?: boolean; namespace?: string }): Promise<number> {
    const filters: SQL[] = [];
    if (params?.enabled !== undefined) {
      filters.push(eq(durableSchedules.enabled, params.enabled));
    }
    if (params?.namespace !== undefined) {
      filters.push(eq(durableSchedules.namespace, params.namespace));
    }
    const [row] = await this.db
      .select({ count: sql<number>`count(*)::int` })
      .from(durableSchedules)
      .where(filters.length > 0 ? and(...filters) : undefined);
    return Number(row?.count ?? 0);
  }

  // -------------------------------------------------------------------------
  // Leader election — pg_advisory_lock per namespace
  // -------------------------------------------------------------------------

  async tryAcquireLeader(params: {
    instanceId: string;
    namespace?: string;
    ttlMs: number;
  }): Promise<boolean> {
    // Per-namespace lock: derive the advisory ID from base + namespace hash.
    const lockId = params.namespace
      ? this.leaderLockId ^ hashToInt32(params.namespace)
      : this.leaderLockId;
    const [result] = await execRaw(
      this.db,
      sql`SELECT pg_try_advisory_lock(${lockId}) as acquired`,
    );
    return result?.acquired === true;
  }

  async findDueAcross(params: {
    now: Date;
    limit: number;
    namespaces?: readonly (string | undefined)[];
  }): Promise<readonly { id: string; namespace?: string }[]> {
    // Single index scan over (namespace, next_run). The namespace filter,
    // when supplied, becomes an IN list (with optional NULL for global).
    const filters: SQL[] = [
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
    overlapPolicy: row.overlapPolicy,
    maxCatchUp: row.maxCatchUp,
    jitterMs: row.jitterMs,
    enabled: row.enabled,
    startAt: row.startAt ?? undefined,
    endAt: row.endAt ?? undefined,
    metadata: row.metadata as Record<string, unknown> | undefined,
  };
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
