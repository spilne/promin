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
 * Two implementations:
 * - `InMemoryScheduler` (core) — non-blocking, in-process, no persistence
 * - `DurableScheduler` (postgres) — persistent, catch-up, overlap policies, leader election
 */
export interface Scheduler extends Streamable<ScheduleTick> {
  /** Register a new schedule. */
  register(config: ScheduleConfig): void;

  /** Remove a schedule. Optional reason for audit/logging. */
  unregister(scheduleId: string, options?: { reason?: string }): void;

  /** Pause a schedule (stops firing, keeps config). */
  pause(scheduleId: string): void;

  /** Resume a paused schedule. */
  resume(scheduleId: string): void;

  /** List all registered schedules. */
  list(): ScheduleConfig[];

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
