// ---------------------------------------------------------------------------
// PgSchedulerStorage — Postgres adapter for SchedulerStorage.
//
// Heavy lifting (cron/rrule/catch-up/jitter/leader loop) lives in the generic
// DurableScheduler shell in @promin/workflow. This file is the storage-only
// adapter — schedule CRUD, due-row lookup, and pg_advisory_lock-based leader
// election.
// ---------------------------------------------------------------------------

import { and, asc, eq, isNotNull, lte, sql, type SQL } from "drizzle-orm";
import type { DurableScheduleConfig, SchedulerStorage } from "@promin/workflow";
import { durableSchedules, durableScheduleTicks } from "./scheduler-schema.ts";
import { type DrizzleDb, execRaw } from "./drizzle-db.ts";

export interface PgSchedulerStorageConfig {
  db: DrizzleDb;
  /**
   * Namespace → leader-lock advisory ID. If unset, all namespaces share one
   * lock derived from the literal "wf-scheduler-leader". Callers running
   * multiple per-namespace leaders should provide unique IDs per namespace.
   */
  leaderLockId?: number;
}

export class PgSchedulerStorage implements SchedulerStorage {
  /** Drizzle schema export — include in your migration pipeline. */
  static readonly schema = {
    schedules: durableSchedules,
    ticks: durableScheduleTicks,
  };

  private readonly db: DrizzleDb;
  private readonly leaderLockId: number;

  constructor(config: PgSchedulerStorageConfig) {
    this.db = config.db;
    this.leaderLockId = config.leaderLockId ?? hashToInt32("wf-scheduler-leader");
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

  async recordFire(id: string, firedAt: Date): Promise<void> {
    await this.db
      .update(durableSchedules)
      .set({
        lastFiredAt: firedAt,
        tickCount: sql`${durableSchedules.tickCount} + 1`,
        updatedAt: new Date(),
      })
      .where(eq(durableSchedules.id, id));
  }

  async setNextRun(id: string, nextRun: Date | null): Promise<void> {
    await this.db
      .update(durableSchedules)
      .set({ nextRun, updatedAt: new Date() })
      .where(eq(durableSchedules.id, id));
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
        set: { ...values, updatedAt: new Date() },
      });
  }

  async deleteSchedule(id: string): Promise<void> {
    await this.db.delete(durableSchedules).where(eq(durableSchedules.id, id));
  }

  async setEnabled(id: string, enabled: boolean): Promise<void> {
    await this.db
      .update(durableSchedules)
      .set({ enabled, updatedAt: new Date() })
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
