// ---------------------------------------------------------------------------
// Scheduler types
// ---------------------------------------------------------------------------

/** A schedule definition — supports cron expressions, RRULE, or fixed intervals. */
export interface ScheduleConfig {
  readonly id: string;
  readonly name?: string;
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
  /** Arbitrary metadata passed through to ScheduleTick. */
  readonly metadata?: Record<string, unknown>;
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
