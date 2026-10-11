// ---------------------------------------------------------------------------
// Scheduler types
// ---------------------------------------------------------------------------

/** A schedule definition — supports cron expressions, RRULE, or fixed intervals. */
export interface ScheduleConfig {
  readonly id: string;
  readonly name?: string;
  /**
   * Logical grouping for multi-tenant deployments. `findDue`, `listSchedules`,
   * and leader election all scope by namespace so tenants don't interfere with
   * each other. Default: schedule belongs to the global (undefined) namespace.
   */
  readonly namespace?: string;
  /** Cron expression (5 or 6 field). Mutually exclusive with rrule and intervalMs. */
  readonly cron?: string;
  /** iCalendar RRULE string (RFC 5545). Mutually exclusive with cron and intervalMs. */
  readonly rrule?: string;
  /** Fixed interval in ms. Mutually exclusive with cron and rrule. */
  readonly intervalMs?: number;
  /** IANA timezone for cron/rrule evaluation. Default: "UTC". */
  readonly timezone?: string;
  /** Whether this schedule is active. Default: true. */
  readonly enabled?: boolean;
  /** Don't fire before this time. Ticks with scheduledAt < startAt are skipped. */
  readonly startAt?: Date;
  /** Stop firing after this time. Schedule auto-disables when endAt is reached. */
  readonly endAt?: Date;
  /**
   * Random delay in ms added before each tick is emitted. Spreads load when
   * many schedules fire on the same boundary (e.g. midnight crons): each
   * tick is emitted a uniformly-random `[0, jitterMs)` after its nominal
   * time. `scheduledAt` stays the nominal time; `firedAt` is when the tick
   * was actually emitted. The first tick after registration is not delayed.
   * Default: 0 (no jitter).
   */
  readonly jitterMs?: number;
  /** Arbitrary metadata passed through to ScheduleTick. */
  readonly metadata?: Record<string, unknown>;
}

/**
 * Extended config for durable schedulers (poll-based, with persistent state).
 * Strict superset of `ScheduleConfig` — switching backends doesn't require
 * changing the schedule definition.
 */
export interface DurableScheduleConfig extends ScheduleConfig {
  /**
   * How many missed occurrences to fire when a poll finds more than one due
   * (the scheduler was down, or the schedule fires faster than the poll
   * interval). The newest `max(1, maxCatchUp)` missed occurrences fire, in
   * chronological order; older ones are skipped. Applies the same way to
   * cron, RRULE and interval schedules. Default: 0 (only the most recent
   * missed occurrence fires).
   */
  readonly maxCatchUp?: number;
}

/** Emitted when a schedule fires. */
export interface ScheduleTick {
  /** Which schedule fired. */
  readonly scheduleId: string;
  /** Human-readable schedule name (if provided). */
  readonly scheduleName?: string;
  /** Nominal fire time — when this should have fired (cron-computed). */
  readonly scheduledAt: Date;
  /** Actual fire time — when it was emitted (later than `scheduledAt` under jitter/load). */
  readonly firedAt: Date;
  /**
   * Token of the schedule's incarnation: chosen when a schedule id is first
   * stored (or registered), kept by every later update, and new once the
   * schedule is deleted and created again. `tickNumber` restarts at 0 for a
   * new incarnation, so run ids include this token (`scheduleTickRunId`)
   * and a recreated schedule's ticks never name a run of the deleted one.
   */
  readonly incarnation: string;
  /**
   * Monotonic counter per schedule incarnation (0, 1, 2, ...). Durable
   * schedulers deliver ticks at least once: a tick that was emitted but not
   * acknowledged is emitted again with the same `tickNumber`, so derive run
   * ids from it (`scheduleTickRunId`) to make redelivery a no-op.
   */
  readonly tickNumber: number;
  /** Metadata from the ScheduleConfig. */
  readonly metadata?: Record<string, unknown>;
}

/**
 * Deterministic id for the run a schedule tick produces — workflow id for
 * workflow-targeted ticks, agent run id for agent-targeted ticks:
 * `<scheduleId>.<incarnation>.<tickNumber>`. Keeps a duplicate tick
 * (TTL-window leader race, redelivery) from creating two rows: the second
 * dispatch lands on the same id and `createWorkflow` (or the agent
 * runtime's equivalent) is idempotent. The incarnation keeps a schedule
 * deleted and created again under the same id from landing on the old
 * schedule's runs.
 *
 * Both the scheduler loop's workflow-trigger path and
 * `dispatchAgentSchedule`'s agent path use this — single source of truth.
 * A `ScheduleTick` can be passed as is.
 */
export function scheduleTickRunId(params: {
  scheduleId: string;
  incarnation: string;
  tickNumber: number;
}): string {
  return `${params.scheduleId}.${params.incarnation}.${params.tickNumber}`;
}

/**
 * A fresh incarnation token for a schedule being stored for the first time
 * (12 hex characters). Storages call it on insert and keep the token on
 * every replace.
 */
export function newScheduleIncarnation(): string {
  return crypto.randomUUID().replaceAll("-", "").slice(0, 12);
}
