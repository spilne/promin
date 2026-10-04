// ---------------------------------------------------------------------------
// Scheduler interface — core contract for schedule sources
// ---------------------------------------------------------------------------

import type { Stream } from "@spilne/perfect-core";
import type { Codec } from "@spilne/perfect-core/connect";
import type { Streamable } from "../shared/streamable.ts";
import type { ScheduleConfig, ScheduleTick } from "./types.ts";

/**
 * A scheduler manages named schedules and emits ScheduleTicks.
 * Implements Streamable<ScheduleTick>: `stream()` and `subscribe()` return
 * perfect `Stream`s, built fresh on every call.
 *
 * Management methods are async: they resolve once the change is applied
 * (persisted, for durable implementations) and reject on invalid configs or
 * storage errors, so callers can await or handle every failure.
 *
 * Two implementations:
 * - `InMemoryScheduler` — non-blocking, in-process, no persistence
 * - `DurableScheduler` — persistent (pluggable storage), catch-up, leader
 *   election, at-least-once tick delivery
 */
export interface Scheduler extends Streamable<ScheduleTick> {
  /**
   * Register (or replace) a schedule. Rejects if the config is invalid: no
   * trigger or more than one, an unparseable cron/RRULE, and so on.
   */
  register(config: ScheduleConfig): Promise<void>;

  /** Remove a schedule. Optional reason for audit/logging. */
  unregister(params: { scheduleId: string; reason?: string }): Promise<void>;

  /** Pause a schedule (stops firing, keeps config). */
  pause(scheduleId: string): Promise<void>;

  /** Resume a paused schedule. */
  resume(scheduleId: string): Promise<void>;

  /** List registered schedules, with `enabled` reflecting paused state. */
  list(): Promise<ScheduleConfig[]>;

  /**
   * Stream ticks from a specific schedule (or all if no id given).
   * Non-blocking — sleeps until next fire time, emits, repeats. Stopping the
   * consumer (`take(n)`, leaving a `for await` loop) releases the stream's
   * timers and listeners.
   */
  stream(scheduleId?: string): Stream<ScheduleTick>;

  /** Streamable implementation — same as stream() with no filter. */
  subscribe(params?: { group?: string }): Stream<ScheduleTick>;

  /** Codec for ScheduleTick serialization. */
  codec: Codec<ScheduleTick>;
}
