// ---------------------------------------------------------------------------
// InMemorySchedulerStorage — reference implementation of SchedulerStorage.
//
// Used for unit tests, single-process production deployments that don't need
// durability, and as the executable spec the conformance suite runs against.
// All state lives in a few Maps.
// ---------------------------------------------------------------------------

import type { DurableScheduleConfig } from "./types.ts";
import type { SchedulerStorage } from "./scheduler-storage.ts";

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

  // -------------------------------------------------------------------------
  // Hot path
  // -------------------------------------------------------------------------

  async findDue(params: { now: Date; limit: number; namespace?: string }): Promise<string[]> {
    const nowMs = params.now.getTime();
    const due: { id: string; nextRun: number }[] = [];
    for (const [id, ts] of this.nextRun) {
      if (ts > nowMs) continue;
      const cfg = this.schedules.get(id);
      if (!cfg) continue;
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

  async commitPoll(
    updates: Array<{
      id: string;
      firedAt?: Date;
      tickIncrement?: number;
      nextRun: Date | null;
    }>,
  ): Promise<void> {
    for (const u of updates) {
      if (u.firedAt !== undefined && u.tickIncrement && u.tickIncrement > 0) {
        const prev = this.state.get(u.id) ?? { lastFired: null, tickCount: 0 };
        this.state.set(u.id, {
          lastFired: u.firedAt,
          tickCount: prev.tickCount + u.tickIncrement,
        });
      }
      if (u.nextRun === null) this.nextRun.delete(u.id);
      else this.nextRun.set(u.id, u.nextRun.getTime());
    }
  }

  // -------------------------------------------------------------------------
  // Admin / CRUD
  // -------------------------------------------------------------------------

  async upsertSchedule(config: DurableScheduleConfig): Promise<void> {
    // Normalize: enabled defaults to true.
    this.schedules.set(config.id, { ...config, enabled: config.enabled !== false });
    if (!this.state.has(config.id)) {
      this.state.set(config.id, { lastFired: null, tickCount: 0 });
    }
  }

  async deleteSchedule(id: string): Promise<void> {
    this.schedules.delete(id);
    this.state.delete(id);
    this.nextRun.delete(id);
  }

  async setEnabled(id: string, enabled: boolean): Promise<void> {
    const cfg = this.schedules.get(id);
    if (!cfg) return;
    this.schedules.set(id, { ...cfg, enabled });
  }

  async listSchedules(params?: {
    enabled?: boolean;
    namespace?: string;
    limit?: number;
    offset?: number;
  }): Promise<DurableScheduleConfig[]> {
    let all = [...this.schedules.values()];
    if (params?.enabled !== undefined) {
      all = all.filter((s) => (s.enabled ?? true) === params.enabled);
    }
    if (params?.namespace !== undefined) {
      all = all.filter((s) => s.namespace === params.namespace);
    }
    const offset = params?.offset ?? 0;
    const limit = params?.limit ?? 100;
    return all.slice(offset, offset + limit);
  }

  async countSchedules(params?: { enabled?: boolean; namespace?: string }): Promise<number> {
    let all = [...this.schedules.values()];
    if (params?.enabled !== undefined) {
      all = all.filter((s) => (s.enabled ?? true) === params.enabled);
    }
    if (params?.namespace !== undefined) {
      all = all.filter((s) => s.namespace === params.namespace);
    }
    return all.length;
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
    const now = Date.now();
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
}
