// ---------------------------------------------------------------------------
// InMemorySchedulerStorage — reference implementation of SchedulerStorage.
//
// Used for unit tests, single-process production deployments that don't need
// durability, and as the executable spec the conformance suite runs against.
// All state lives in a few Maps.
// ---------------------------------------------------------------------------

import type { DurableScheduleConfig, ScheduleTick } from "./types.ts";
import type { ScheduleCommit, SchedulerStorage } from "./scheduler-storage.ts";
import { scheduleMetadataContains } from "./metadata-filter.ts";
import { SystemWallClock, type WallClock } from "../shared/wall-clock.ts";

export interface InMemorySchedulerStorageConfig {
  /**
   * Time source for the initial next-run of new schedules and leader-lock
   * expiry. Default: `SystemWallClock`.
   */
  clock?: WallClock;
}

interface ScheduleState {
  lastFired: Date | null;
  tickCount: number;
}

interface LeaderLock {
  instanceId: string;
  expiresAt: number;
}

export class InMemorySchedulerStorage implements SchedulerStorage {
  private schedules = new Map<string, DurableScheduleConfig>();
  private state = new Map<string, ScheduleState>();
  /** Per-id nextRun timestamp; missing = not in due-tracking. */
  private nextRun = new Map<string, number>();
  /** Per-namespace leader locks. Key = namespace ?? "__global__". */
  private leaders = new Map<string, LeaderLock>();
  /** Per-schedule tick log. Inner array is append-order; queries reverse it
   *  for newest-first. */
  private ticks = new Map<string, ScheduleTick[]>();
  private readonly clock: WallClock;

  constructor(config?: InMemorySchedulerStorageConfig) {
    this.clock = config?.clock ?? SystemWallClock;
  }

  // -------------------------------------------------------------------------
  // Hot path
  // -------------------------------------------------------------------------

  async findDue(params: { now: Date; limit: number; namespace?: string }): Promise<string[]> {
    const nowMs = params.now.getTime();
    const due: { id: string; nextRun: number }[] = [];
    for (const [id, ts] of this.nextRun) {
      if (ts > nowMs) continue;
      const cfg = this.schedules.get(id);
      if (!cfg || cfg.enabled === false) continue;
      if ((cfg.namespace ?? undefined) !== (params.namespace ?? undefined)) continue;
      due.push({ id, nextRun: ts });
    }
    due.sort((a, b) => a.nextRun - b.nextRun);
    return due.slice(0, params.limit).map((d) => d.id);
  }

  async loadSchedule(id: string): Promise<DurableScheduleConfig | null> {
    return this.schedules.get(id) ?? null;
  }

  async loadSchedules(ids: string[]): Promise<Map<string, DurableScheduleConfig>> {
    const out = new Map<string, DurableScheduleConfig>();
    for (const id of ids) {
      const cfg = this.schedules.get(id);
      if (cfg) out.set(id, cfg);
    }
    return out;
  }

  async loadScheduleState(
    id: string,
  ): Promise<{ lastFired: Date | null; tickCount: number } | null> {
    if (!this.schedules.has(id)) return null;
    return this.state.get(id) ?? { lastFired: null, tickCount: 0 };
  }

  async loadScheduleStates(
    ids: string[],
  ): Promise<Map<string, { lastFired: Date | null; tickCount: number }>> {
    const out = new Map<string, { lastFired: Date | null; tickCount: number }>();
    for (const id of ids) {
      if (!this.schedules.has(id)) continue;
      out.set(id, this.state.get(id) ?? { lastFired: null, tickCount: 0 });
    }
    return out;
  }

  async recordFire(id: string, firedAt: Date, count: number = 1): Promise<void> {
    const prev = this.state.get(id) ?? { lastFired: null, tickCount: 0 };
    this.state.set(id, { lastFired: firedAt, tickCount: prev.tickCount + count });
  }

  async setNextRun(id: string, nextRun: Date | null): Promise<void> {
    if (nextRun === null) this.nextRun.delete(id);
    else this.nextRun.set(id, nextRun.getTime());
  }

  async commitPoll(updates: ScheduleCommit[]): Promise<void> {
    // Single-process JS — no transaction primitive needed; all mutations
    // happen inside this synchronous block, which IS the atomic boundary.
    for (const u of updates) {
      if (u.firedAt !== undefined && u.tickIncrement && u.tickIncrement > 0) {
        const prev = this.state.get(u.id) ?? { lastFired: null, tickCount: 0 };
        this.state.set(u.id, {
          lastFired: u.firedAt,
          tickCount: prev.tickCount + u.tickIncrement,
        });
      }
      // A schedule paused (or deleted) since the poll loaded it stays out of due-tracking.
      const cfg = this.schedules.get(u.id);
      if (u.nextRun === null || !cfg || cfg.enabled === false) this.nextRun.delete(u.id);
      else this.nextRun.set(u.id, u.nextRun.getTime());
      if (u.ticks?.length) {
        const log = this.ticks.get(u.id) ?? [];
        // Dedupe on (scheduleId, tickNumber) so a retried commitPoll doesn't
        // double-log — same idempotency the SQLite PK enforces.
        const existing = new Set(log.map((t) => t.tickNumber));
        for (const t of u.ticks) {
          if (!existing.has(t.tickNumber)) log.push(t);
        }
        this.ticks.set(u.id, log);
      }
    }
  }

  async listTicks(params: {
    scheduleId: string;
    limit?: number;
    offset?: number;
  }): Promise<readonly ScheduleTick[]> {
    const log = this.ticks.get(params.scheduleId) ?? [];
    // Newest-first by firedAt, mirroring the SQLite ordering. Stable sort
    // keeps insertion order as the tiebreaker on identical firedAt values
    // (catchup ticks fired in one poll all share the same wall-clock).
    const sorted = [...log].sort((a, b) => b.firedAt.getTime() - a.firedAt.getTime());
    const offset = params.offset ?? 0;
    const limit = params.limit ?? sorted.length;
    return sorted.slice(offset, offset + limit);
  }

  async countTicks(params: { scheduleId: string }): Promise<number> {
    return this.ticks.get(params.scheduleId)?.length ?? 0;
  }

  // -------------------------------------------------------------------------
  // Admin / CRUD
  // -------------------------------------------------------------------------

  async upsertSchedule(config: DurableScheduleConfig): Promise<void> {
    // Normalize: enabled defaults to true.
    const enabled = config.enabled !== false;
    this.schedules.set(config.id, { ...config, enabled });
    if (!this.state.has(config.id)) {
      this.state.set(config.id, { lastFired: null, tickCount: 0 });
    }
    // Due-tracking follows the enabled flag. Disabled: out of due-tracking.
    // Enabled: keep an existing nextRun (the caller recomputes it when the
    // trigger changes), or seed one so findDue picks the schedule up —
    // honoring a future `startAt` so a deferred schedule isn't reported as
    // due immediately.
    if (!enabled) this.nextRun.delete(config.id);
    else this.seedNextRun(config);
  }

  async deleteSchedule(id: string): Promise<void> {
    this.schedules.delete(id);
    this.state.delete(id);
    this.nextRun.delete(id);
    this.ticks.delete(id);
  }

  async setEnabled(id: string, enabled: boolean): Promise<void> {
    const cfg = this.schedules.get(id);
    if (!cfg) return;
    this.schedules.set(id, { ...cfg, enabled });
    if (!enabled) this.nextRun.delete(id);
    else this.seedNextRun(cfg);
  }

  /** Put an enabled schedule with no nextRun into due-tracking at now (or a later startAt). */
  private seedNextRun(config: DurableScheduleConfig): void {
    if (this.nextRun.has(config.id)) return;
    const now = this.clock.currentTimeMs();
    const startAtMs = config.startAt ? config.startAt.getTime() : 0;
    this.nextRun.set(config.id, Math.max(now, startAtMs));
  }

  async listSchedules(params?: {
    enabled?: boolean;
    namespace?: string;
    metadata?: Record<string, unknown>;
    limit?: number;
    offset?: number;
  }): Promise<DurableScheduleConfig[]> {
    const filtered = this.applyFilters(params);
    const offset = params?.offset ?? 0;
    const limit = params?.limit ?? 100;
    return filtered.slice(offset, offset + limit);
  }

  async countSchedules(params?: {
    enabled?: boolean;
    namespace?: string;
    metadata?: Record<string, unknown>;
  }): Promise<number> {
    return this.applyFilters(params).length;
  }

  /**
   * Single filter pass shared by list / count so they can't drift on
   * which predicates apply. Mirrors the workflow-storage convention:
   * metadata containment is `actual @> filter` deep-equal per key.
   */
  private applyFilters(params?: {
    enabled?: boolean;
    namespace?: string;
    metadata?: Record<string, unknown>;
  }): DurableScheduleConfig[] {
    let all = [...this.schedules.values()];
    if (params?.enabled !== undefined) {
      all = all.filter((s) => (s.enabled ?? true) === params.enabled);
    }
    if (params?.namespace !== undefined) {
      all = all.filter((s) => s.namespace === params.namespace);
    }
    if (params?.metadata) {
      const filter = params.metadata;
      all = all.filter((s) => scheduleMetadataContains(s.metadata, filter));
    }
    return all;
  }

  // -------------------------------------------------------------------------
  // Leader election — per-namespace lock with TTL.
  // -------------------------------------------------------------------------

  async tryAcquireLeader(params: {
    instanceId: string;
    namespace?: string;
    ttlMs: number;
  }): Promise<boolean> {
    const key = params.namespace ?? "__global__";
    const now = this.clock.currentTimeMs();
    const existing = this.leaders.get(key);
    if (existing && existing.expiresAt > now && existing.instanceId !== params.instanceId) {
      return false;
    }
    this.leaders.set(key, {
      instanceId: params.instanceId,
      expiresAt: now + params.ttlMs,
    });
    return true;
  }

  async findDueAcross(params: {
    now: Date;
    limit: number;
    namespaces?: readonly (string | undefined)[];
  }): Promise<readonly { id: string; namespace?: string }[]> {
    const nowMs = params.now.getTime();
    const filter = params.namespaces ? new Set(params.namespaces) : undefined;
    const due: { id: string; namespace?: string; nextRun: number }[] = [];
    for (const [id, ts] of this.nextRun) {
      if (ts > nowMs) continue;
      const cfg = this.schedules.get(id);
      if (!cfg || cfg.enabled === false) continue;
      if (filter && !filter.has(cfg.namespace)) continue;
      due.push({ id, namespace: cfg.namespace, nextRun: ts });
    }
    due.sort((a, b) => a.nextRun - b.nextRun);
    return due.slice(0, params.limit).map(({ id, namespace }) => ({ id, namespace }));
  }
}
