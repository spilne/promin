// ---------------------------------------------------------------------------
// SchedulerStorage — pluggable persistence interface for DurableScheduler
//
// Backends (Postgres, Redis, in-memory) implement this thin storage contract;
// all cron/rrule/interval/catch-up/jitter/leader-loop logic lives once in
// `DurableScheduler`. Adding a new backend means writing one storage adapter,
// not a whole scheduler.
// ---------------------------------------------------------------------------

import type { DurableScheduleConfig, ScheduleTick } from "./types.ts";
import type { LeaderLease, LeaderLeaseStore } from "./leader-lease.ts";

/** One schedule's entry in a `commitPoll` batch. */
export interface ScheduleCommit {
  readonly id: string;
  /** Last fire time to record as `lastFired`. Set together with `tickIncrement`. */
  readonly firedAt?: Date;
  /** How much to add to `tickCount`. */
  readonly tickIncrement?: number;
  /**
   * New `nextRun`; `null` removes the schedule from due-tracking. Omit it to
   * leave `nextRun` as it is (manual fires do).
   */
  readonly nextRun?: Date | null;
  /**
   * Compare-and-set guard: apply this entry only while the stored `tickCount`
   * still equals this value. A skipped entry is reported in
   * `CommitPollResult.conflicts` and changes nothing. Planned polls set it to
   * the `tickCount` their ticks were numbered from, so a manual fire (or any
   * other writer) that advanced the count in between is never overwritten
   * and two different fires never get the same `tickNumber` in storage.
   */
  readonly expectedTickCount?: number;
  /**
   * Individual ticks fired in this poll cycle. Backends that maintain
   * a tick log persist these IN THE SAME TRANSACTION as the state
   * advance — so `tickCount` and the count of logged rows never
   * diverge. Backends without a tick log silently ignore this field.
   */
  readonly ticks?: readonly ScheduleTick[];
}

/** What `commitPoll` did. */
export interface CommitPollResult {
  /**
   * Ids of entries skipped because their `expectedTickCount` no longer
   * matched or the schedule no longer exists. Entries without
   * `expectedTickCount` are never reported.
   */
  readonly conflicts: readonly string[];
}

/**
 * Persistence for `DurableScheduler`. Also a `LeaderLeaseStore`: only the
 * holder of a key's lease polls and commits, and a commit carrying a stale
 * lease is rejected.
 */
export interface SchedulerStorage extends LeaderLeaseStore {
  // -------------------------------------------------------------------------
  // Hot path — called every poll cycle. Implementations must be fast.
  // -------------------------------------------------------------------------

  /**
   * Return up to `limit` schedule IDs whose next run is at or before `now`,
   * oldest `nextRun` first. Filtered by namespace if provided. Never returns
   * disabled schedules, even if a stale `nextRun` is still recorded for one,
   * so paused schedules can't fill the `limit` and starve active ones.
   * Implementations should use an index on `(namespace, nextRun)` (Postgres)
   * or a per-namespace sorted set (Redis).
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
   * Atomic commit for an entire poll cycle. For each entry:
   * - skip it (reporting it in `conflicts`) when `expectedTickCount` is set
   *   and differs from the stored `tickCount`, or the schedule is gone;
   * - if `firedAt`/`tickIncrement` are set, update `lastFired`/`tickCount`
   *   and log `ticks` (backends with a tick log);
   * - if `nextRun` is present, update it (`null` removes the schedule from
   *   due-tracking). A schedule disabled since it was loaded stays out of
   *   due-tracking.
   *
   * Fencing: with `lease`, the whole commit is rejected with
   * `StaleLeaseError` and nothing is written unless `lease.epoch` is still
   * the current epoch of `lease.key`. The check runs in the same
   * transaction (or script) as the writes, so a leader that lost its lease
   * mid-poll can't commit.
   *
   * Implementations should collapse this to one network round-trip
   * (Postgres: `UPDATE … FROM (VALUES …)` in a transaction; Redis: one Lua
   * script). In-memory backends apply it synchronously.
   */
  commitPoll(params: {
    updates: readonly ScheduleCommit[];
    lease?: LeaderLease;
  }): Promise<CommitPollResult>;

  // -------------------------------------------------------------------------
  // Admin / CRUD path — used by register, list, pause, etc.
  // -------------------------------------------------------------------------

  /**
   * Insert or replace a schedule. Fire state (`lastFired`, `tickCount`)
   * survives a replace. Due-tracking follows the enabled flag: a disabled
   * schedule gets `nextRun = null`; an enabled one keeps its existing
   * `nextRun`, or is seeded at now (or a later `startAt`) when it has none.
   */
  upsertSchedule(config: DurableScheduleConfig): Promise<void>;

  /** Delete a schedule and any due-tracking state for it. */
  deleteSchedule(id: string): Promise<void>;

  /**
   * Toggle a schedule's enabled flag. Disabled schedules don't fire:
   * disabling also removes the schedule from due-tracking (`nextRun = null`),
   * and enabling a schedule that has no `nextRun` seeds it at now (or
   * `startAt` when that is later), the same as inserting an enabled schedule.
   * Unknown ids are a no-op.
   */
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
  // Leader election: `tryAcquireLeader` / `releaseLeader` come from
  // `LeaderLeaseStore`. Poll loops lease one key per namespace and partition
  // (`schedulerLeaderKey`) and pass the lease to `commitPoll`.
  // -------------------------------------------------------------------------

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
   * Like `findDue`, never returns disabled schedules.
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
