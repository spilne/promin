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
   * Random jitter in ms added to each fire time. Spreads load when many
   * schedules fire on the same boundary (e.g. midnight crons). Each tick
   * fires at a uniformly-random offset in `[0, jitterMs)` past its nominal
   * time. Default: 0 (no jitter). Same approach as Temporal/Quartz.
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
  /** What to do if the previous run hasn't finished. Default: "allow". */
  readonly overlapPolicy?: "skip" | "queue" | "cancel_previous" | "allow";
  /** Max catch-up runs when scheduler was down. Default: 0 (no catch-up). */
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
  /** Actual fire time — when it actually fired (may differ due to jitter/load). */
  readonly firedAt: Date;
  /** Monotonic counter per schedule (0, 1, 2, ...). Useful for idempotent workflowIds. */
  readonly tickNumber: number;
  /** Metadata from the ScheduleConfig. */
  readonly metadata?: Record<string, unknown>;
}

/**
 * Deterministic id for the run a schedule tick produces — workflow id for
 * workflow-targeted ticks, agent run id for agent-targeted ticks. Keeps a
 * duplicate tick (TTL-window leader race) from creating two rows: the
 * second dispatch lands on the same id and `createWorkflow` (or the
 * agent runtime's equivalent) is idempotent.
 *
 * Both the scheduler loop's workflow-trigger path and
 * `dispatchAgentSchedule`'s agent path use this — single source of truth.
 */
export function scheduleTickRunId(scheduleId: string, tickNumber: number): string {
  return `${scheduleId}.${tickNumber}`;
}
