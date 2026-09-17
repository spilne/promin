// ---------------------------------------------------------------------------
// SchedulerStorage — pluggable persistence interface for DurableScheduler
//
// Backends (Postgres, Redis, in-memory) implement this thin storage contract;
// all cron/rrule/interval/catch-up/jitter/leader-loop logic lives once in
// `DurableScheduler`. Adding a new backend means writing one storage adapter,
// not a whole scheduler.
// ---------------------------------------------------------------------------

import type { DurableScheduleConfig, ScheduleTick } from "./types.ts";

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

  /**
   * Bulk variant of `loadSchedule`. Used by the poll loop to fetch all due
   * configs in one round-trip (Postgres: `WHERE id IN (...)`; Redis: pipeline).
   * Returned map MUST omit ids that don't exist in storage.
   */
  loadSchedules(ids: string[]): Promise<Map<string, DurableScheduleConfig>>;

  /** Read fire-state — `lastFired` is the last firedAt, `tickCount` is the running counter. */
  loadScheduleState(id: string): Promise<{ lastFired: Date | null; tickCount: number } | null>;

  /**
   * Bulk variant of `loadScheduleState` — same single-round-trip rationale.
   * Returned map omits ids that don't exist; ids that exist but have no fire
   * history return `{ lastFired: null, tickCount: 0 }`.
   */
  loadScheduleStates(
    ids: string[],
  ): Promise<Map<string, { lastFired: Date | null; tickCount: number }>>;

  /**
   * Record that a schedule fired. Increments `tickCount` by `count` (default 1)
   * and stores `firedAt` as `lastFired`. The `count` parameter lets callers
   * collapse N catch-up ticks into one storage write.
   */
  recordFire(id: string, firedAt: Date, count?: number): Promise<void>;

  /**
   * Update the next run time used by `findDue`. `null` removes the schedule
   * from due-tracking (e.g. endAt has passed and there's no future run).
   */
  setNextRun(id: string, nextRun: Date | null): Promise<void>;

  /**
   * Atomic single-round-trip commit for an entire poll cycle. For each entry:
   * - if `firedAt`/`tickIncrement` are set, update `lastFired`/`tickCount`;
   * - always update `nextRun` (use `null` to remove from due-tracking).
   *
   * Implementations should collapse this to one network round-trip per poll
   * (Postgres: `UPDATE … FROM (VALUES …)`; Redis: MULTI/pipeline). Sequential
   * fallback (loop over single-record methods) is acceptable for in-memory.
   */
  commitPoll(
    updates: Array<{
      id: string;
      firedAt?: Date;
      tickIncrement?: number;
      nextRun: Date | null;
      /**
       * Individual ticks fired in this poll cycle. Backends that maintain
       * a tick log persist these IN THE SAME TRANSACTION as the state
       * advance — so `tickCount` and the count of logged rows never
       * diverge. Backends without a tick log silently ignore this field.
       */
      ticks?: readonly ScheduleTick[];
    }>,
  ): Promise<void>;

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
    /**
     * Filter by metadata key/value pairs. A row matches when its metadata
     * contains every supplied key with a deep-equal value (Postgres jsonb
     * `@>` containment semantics — same as `WorkflowStorage.listWorkflows`).
     *
     * Used by the dashboard to filter agent schedules by `target.type`,
     * `target.threadId`, etc. without scanning the full namespace
     * client-side. Backends with native JSON support push the predicate
     * to the database; others apply it after loading.
     *
     * Index strategy is per-backend — SQLite ships with no JSON index by
     * default, so common filter paths (e.g. `metadata.target.type`) want
     * a functional index for hot deployments.
     */
    metadata?: Record<string, unknown>;
    /** Default: 100. */
    limit?: number;
    /** Default: 0. */
    offset?: number;
  }): Promise<DurableScheduleConfig[]>;

  /** Total count of schedules matching the filters. */
  countSchedules(params?: {
    enabled?: boolean;
    namespace?: string;
    /** Same containment semantics as `listSchedules({ metadata })`. */
    metadata?: Record<string, unknown>;
  }): Promise<number>;

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

  /**
   * Cross-namespace `findDue`: returns due schedules along with their
   * namespace, with no per-namespace filter. Used by the embedded
   * scheduler tick loop's multi-namespace mode so a single Zorya
   * instance can cover N tenants without paying O(N) idle RPCs per
   * poll — empty namespaces never appear here, so they cost nothing.
   *
   * Postgres collapses to one `SELECT id, namespace ... WHERE next_run
   * <= $1 LIMIT $2` against the existing `(namespace, next_run)` index.
   * Redis SCANs the per-namespace due ZSETs and unions the results.
   * In-memory iterates the nextRun map.
   *
   * Pass `namespaces` to restrict to a specific subset (filter happens
   * server-side / in-storage so the limit is honored after the filter).
   */
  findDueAcross(params: {
    now: Date;
    limit: number;
    namespaces?: readonly (string | undefined)[];
  }): Promise<readonly { id: string; namespace?: string }[]>;

  // -------------------------------------------------------------------------
  // Tick log — past-fire history. Optional capability: backends may declare
  // it by implementing both `listTicks` and `countTicks`. Consumers should
  // use `isTickLogStorage(storage)` to gate features that need it.
  // -------------------------------------------------------------------------

  /**
   * Paginated past-fire log for a schedule, ordered most-recent-first.
   * Implementations populate this from `commitPoll`'s `ticks` field — the
   * same transaction that advances `tickCount`, so the two never disagree.
   */
  listTicks?(params: {
    scheduleId: string;
    limit?: number;
    offset?: number;
  }): Promise<readonly ScheduleTick[]>;

  /** Total ticks logged for a schedule. Bounded by `tickCount`. */
  countTicks?(params: { scheduleId: string }): Promise<number>;
}

/** Type guard: backend supports the optional tick-log capability. */
export function isTickLogStorage(
  storage: SchedulerStorage,
): storage is SchedulerStorage & Required<Pick<SchedulerStorage, "listTicks" | "countTicks">> {
  return typeof storage.listTicks === "function" && typeof storage.countTicks === "function";
}
