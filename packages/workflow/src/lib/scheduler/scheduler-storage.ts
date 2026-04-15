// ---------------------------------------------------------------------------
// SchedulerStorage — pluggable persistence interface for DurableScheduler
//
// Backends (Postgres, Redis, in-memory) implement this thin storage contract;
// all cron/rrule/interval/catch-up/jitter/leader-loop logic lives once in
// `DurableScheduler`. Adding a new backend means writing one storage adapter,
// not a whole scheduler.
// ---------------------------------------------------------------------------

import type { DurableScheduleConfig } from "./types.ts";

export interface SchedulerStorage {
  // -------------------------------------------------------------------------
  // Hot path — called every poll cycle. Implementations must be fast.
  // -------------------------------------------------------------------------

  /**
   * Return up to `limit` schedule IDs whose next run is at or before `now`.
   * Filtered by namespace if provided. Implementations should use an index on
   * `(namespace, nextRun)` (Postgres) or a per-namespace sorted set (Redis).
   */
  findDue(params: { now: Date; limit: number; namespace?: string }): Promise<string[]>;

  /** Load a schedule's full config. Returns null if not found. */
  loadSchedule(id: string): Promise<DurableScheduleConfig | null>;

  /** Read fire-state — `lastFired` is the last firedAt, `tickCount` is the running counter. */
  loadScheduleState(id: string): Promise<{ lastFired: Date | null; tickCount: number } | null>;

  /** Record that a schedule fired. Increments `tickCount` and stores `firedAt` as `lastFired`. */
  recordFire(id: string, firedAt: Date): Promise<void>;

  /**
   * Update the next run time used by `findDue`. `null` removes the schedule
   * from due-tracking (e.g. endAt has passed and there's no future run).
   */
  setNextRun(id: string, nextRun: Date | null): Promise<void>;

  // -------------------------------------------------------------------------
  // Admin / CRUD path — used by registerAsync, listAsync, pause, etc.
  // -------------------------------------------------------------------------

  /** Insert or replace a schedule. */
  upsertSchedule(config: DurableScheduleConfig): Promise<void>;

  /** Delete a schedule and any due-tracking state for it. */
  deleteSchedule(id: string): Promise<void>;

  /** Toggle a schedule's enabled flag. Disabled schedules don't fire. */
  setEnabled(id: string, enabled: boolean): Promise<void>;

  /** Paginated list of schedules, optionally filtered by enabled flag and namespace. */
  listSchedules(params?: {
    enabled?: boolean;
    namespace?: string;
    /** Default: 100. */
    limit?: number;
    /** Default: 0. */
    offset?: number;
  }): Promise<DurableScheduleConfig[]>;

  /** Total count of schedules matching the filters. */
  countSchedules(params?: { enabled?: boolean; namespace?: string }): Promise<number>;

  // -------------------------------------------------------------------------
  // Leader election — only the leader emits ticks.
  // -------------------------------------------------------------------------

  /**
   * Acquire (or refresh) the leader lock for a namespace. Returns true if this
   * `instanceId` is the leader. Implementations use `pg_advisory_lock` /
   * `SET NX PX` / etc. Per-namespace locks let tenants run independent workers.
   */
  tryAcquireLeader(params: {
    instanceId: string;
    namespace?: string;
    ttlMs: number;
  }): Promise<boolean>;
}
