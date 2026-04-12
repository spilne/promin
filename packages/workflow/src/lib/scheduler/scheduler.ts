// ---------------------------------------------------------------------------
// Scheduler interface — core contract for schedule sources
// ---------------------------------------------------------------------------

import type { StreamPipeline } from "@promin/core";
import type { Codec } from "@promin/core";
import type { Streamable } from "@promin/core";
import type { ScheduleConfig, ScheduleTick } from "./types.ts";

/**
 * A scheduler manages named schedules and emits ScheduleTicks.
 * Implements Streamable<ScheduleTick> — use with StreamPipeline.fromSource().
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
   * Non-blocking — sleeps until next fire time, emits, repeats.
   */
  stream(scheduleId?: string): StreamPipeline<ScheduleTick, never>;

  /** Streamable implementation — same as stream() with no filter. */
  subscribe(params?: { group?: string }): StreamPipeline<ScheduleTick, never>;

  /** Codec for ScheduleTick serialization. */
  codec: Codec<ScheduleTick>;
}
