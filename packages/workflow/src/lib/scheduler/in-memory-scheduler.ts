// ---------------------------------------------------------------------------
// InMemoryScheduler — non-blocking, in-process cron/interval scheduler
//
// Per schedule: compute next fire time → wait(delta) → emit → repeat
// No polling. No setInterval. No busy-wait. Event loop stays free.
// Every time read and wait goes through the injected `WallClock`.
// ---------------------------------------------------------------------------

import { Cron } from "croner";
import { RRule } from "./rrule.ts";
import { Stream, succeed, suspend, sync, type Eff } from "@spilne/perfect-core";
import { JsonCodec } from "@spilne/perfect-core/connect";
import { SystemWallClock } from "../shared/wall-clock.ts";
import { wallClockSleep } from "./wall-clock-sleep.ts";
import { jitterDelayMs, validateScheduleConfig } from "./schedule-config.ts";
import type { WallClock } from "../shared/wall-clock.ts";
import type { Codec } from "@spilne/perfect-core/connect";
import type { Scheduler } from "./scheduler.ts";
import type { ScheduleConfig, ScheduleTick } from "./types.ts";

export interface InMemorySchedulerConfig {
  /**
   * Time source. Drives "now" for next-fire computation, each tick's
   * `firedAt`, the wait until the next fire time, and the paused-schedule
   * recheck. Default: `SystemWallClock`. Tests pass a `FakeWallClock`.
   */
  clock?: WallClock;
  /** Randomness for `jitterMs`, returning `[0, 1)`. Default: `Math.random`. */
  random?: () => number;
}

/**
 * Non-blocking, in-process scheduler for cron expressions and fixed intervals.
 *
 * Waits on a one-shot timer from the configured `WallClock` until the next fire
 * time — no polling, no `setInterval`, no busy-wait. The event loop stays
 * completely free between ticks.
 *
 * Implements `Streamable<ScheduleTick>`: `stream()` and `subscribe()` return a
 * fresh perfect `Stream` on every call.
 *
 * Ticks are in-process only: there is no persistence and no catch-up, so a
 * tick whose fire time passes while nothing is consuming the stream is not
 * replayed. A schedule paused, replaced or removed while a stream waits for
 * its next fire time emits nothing for that wait.
 *
 * For production multi-instance deployments, use `DurableScheduler`, which
 * adds persistence, catch-up, at-least-once delivery and leader election.
 *
 * @example
 * ```ts
 * import { trigger } from "@promin/workflow";
 * import { InMemoryScheduler } from "@promin/workflow/scheduler";
 *
 * const scheduler = new InMemoryScheduler();
 *
 * // Cron — every weekday at 9am EST
 * await scheduler.register({
 *   id: "morning-report",
 *   cron: "0 9 * * MON-FRI",
 *   timezone: "America/New_York",
 *   metadata: { team: "analytics" },
 * });
 *
 * // Fixed interval — every 30 seconds
 * await scheduler.register({ id: "health-check", intervalMs: 30_000 });
 *
 * // Stream a single schedule → workflow trigger
 * scheduler.stream("morning-report")
 *   .through(trigger({
 *     workflow: reportWorkflow,
 *     runner,
 *     storage,
 *     toInput: (tick) => ({ date: tick.scheduledAt.toISOString().split("T")[0] }),
 *     toWorkflowId: (tick) => `report-${tick.scheduledAt.toISOString().split("T")[0]}`,
 *   }))
 *   .drain()
 *   .run();
 *
 * // Stream all schedules merged — picks up schedules registered after subscribe() is called
 * for await (const tick of scheduler.subscribe().toAsyncIterable()) {
 *   console.log(`${tick.scheduleId} fired at ${tick.firedAt}`);
 * }
 *
 * // Pause / resume at runtime
 * await scheduler.pause("health-check");
 * await scheduler.resume("health-check");
 *
 * // Unregister ends the stream
 * await scheduler.unregister({ scheduleId: "health-check" });
 * ```
 */
export class InMemoryScheduler implements Scheduler {
  private schedules = new Map<string, ScheduleEntry>();
  private readonly _registerCallbacks = new Set<(id: string) => void>();
  readonly codec: Codec<ScheduleTick> = JsonCodec as Codec<ScheduleTick>;
  private readonly clock: WallClock;
  private readonly random: () => number;

  constructor(config: InMemorySchedulerConfig = {}) {
    this.clock = config.clock ?? SystemWallClock;
    this.random = config.random ?? Math.random;
  }

  /**
   * Register (or replace) a schedule. Rejects on an invalid config (see
   * `validateScheduleConfig`). Replacing a schedule restarts its wait with
   * the new config in every active stream.
   */
  async register(config: ScheduleConfig): Promise<void> {
    validateScheduleConfig(config);
    this.schedules.set(config.id, { ...config, paused: config.enabled === false });
    // Notify any active subscribe() streams so they pick up the new schedule immediately.
    for (const cb of this._registerCallbacks) cb(config.id);
  }

  /** Remove a schedule. Its stream will end. */
  async unregister(params: { scheduleId: string; reason?: string }): Promise<void> {
    const { scheduleId } = params;
    this.schedules.delete(scheduleId);
  }

  /** Pause a schedule — stops emitting ticks but keeps the config. */
  async pause(scheduleId: string): Promise<void> {
    const s = this.schedules.get(scheduleId);
    if (s) s.paused = true;
  }

  /** Resume a paused schedule. */
  async resume(scheduleId: string): Promise<void> {
    const s = this.schedules.get(scheduleId);
    if (s) s.paused = false;
  }

  /** List all registered schedules with their current enabled state. */
  async list(): Promise<ScheduleConfig[]> {
    return [...this.schedules.values()].map(({ paused, ...config }) => ({
      ...config,
      enabled: !paused,
    }));
  }

  /**
   * Stream ticks from a specific schedule, or all schedules merged.
   *
   * Single-schedule: waits until the next fire time — no polling, no busy-wait.
   * All-schedules: same per-schedule sleep approach, with schedules registered
   * after the stream starts picked up automatically via a register callback.
   *
   * Every call builds a fresh stream. Stopping the consumer interrupts the
   * pending waits (clearing their clock timers) and removes the register
   * callback.
   *
   * @param scheduleId - If provided, stream only this schedule. Otherwise merge all.
   */
  stream(scheduleId?: string): Stream<ScheduleTick> {
    if (scheduleId) {
      return this.createScheduleStream(scheduleId);
    }
    return this.createAllSchedulesStream();
  }

  subscribe(_params?: { group?: string }): Stream<ScheduleTick> {
    return this.createAllSchedulesStream();
  }

  // ---------------------------------------------------------------------------
  // All-schedules stream — dynamic merge on register
  //
  // The outer stream emits schedule ids: those registered when the stream is
  // first pulled, then every later registration via a register callback. Each
  // id becomes its own per-schedule stream and parJoinUnbounded runs them all
  // concurrently. An id that already has a running per-schedule stream is not
  // started twice (re-registering replaces the config that stream follows).
  // The callback is removed when the stream terminates.
  // ---------------------------------------------------------------------------

  private createAllSchedulesStream(): Stream<ScheduleTick> {
    return Stream.suspend(() => {
      const running = new Set<string>();
      const scheduleIds = Stream.async<string, never>(
        (emit) =>
          sync(() => {
            const start = (id: string) => {
              if (running.has(id)) return;
              running.add(id);
              emit(id);
            };
            for (const id of this.schedules.keys()) start(id);
            this._registerCallbacks.add(start);
            return () => {
              this._registerCallbacks.delete(start);
            };
          }),
        Infinity,
      );

      return scheduleIds
        .map((id) =>
          this.createScheduleStream(id).onFinalize(
            sync(() => {
              running.delete(id);
            }),
          ),
        )
        .parJoinUnbounded();
    });
  }

  // ---------------------------------------------------------------------------
  // Non-blocking stream per schedule
  //
  // unfoldEffect step results:
  //   [tick, next]  → emit tick, continue
  //   [null, next]  → nothing to emit yet (paused / before startAt), continue
  //   null          → end stream (schedule unregistered or past endAt)
  // ---------------------------------------------------------------------------

  private createScheduleStream(scheduleId: string): Stream<ScheduleTick> {
    // `suspend` defers each step to its pull, so "now" is read when the
    // consumer asks for the next tick rather than when the stream is built.
    const clock = this.clock;
    const random = this.random;
    const current = () => this.schedules.get(scheduleId);
    const step = (tickNumber: number): Eff<ScheduleStep> =>
      suspend(() => {
        const config = current();
        if (!config) return succeed(null);

        if (config.paused) {
          // Check again after a while; the next step picks up a resume.
          return wallClockSleep({ clock, ms: PAUSED_RECHECK_MS }).map(
            (): ScheduleStep => (current() ? [null, tickNumber] : null),
          );
        }

        return computeAndSleep({ config, tickNumber, clock, random, current });
      });

    return Stream.unfoldEffect(0, step).unNone();
  }
}

/** How often a paused schedule re-checks whether it has been resumed. */
const PAUSED_RECHECK_MS = 1000;

/** A registered schedule plus its paused flag (mutated in place by pause/resume). */
type ScheduleEntry = ScheduleConfig & { paused: boolean };

/** One unfold step: a tick (or nothing yet) plus the next tick number, or end. */
type ScheduleStep = [ScheduleTick | null, number] | null;

function computeAndSleep(params: {
  config: ScheduleEntry;
  tickNumber: number;
  clock: WallClock;
  random: () => number;
  /** Reads the schedule's live entry, to re-check it after the wait. */
  current: () => ScheduleEntry | undefined;
}): Eff<ScheduleStep> {
  const { config, tickNumber, clock, random, current } = params;
  const now = clock.now();

  if (config.startAt && now < config.startAt) {
    const waitMs = config.startAt.getTime() - now.getTime();
    return wallClockSleep({ clock, ms: waitMs }).map((): ScheduleStep => [null, tickNumber]);
  }

  if (config.endAt && now >= config.endAt) {
    return succeed(null);
  }

  const nextFireTime = config.cron
    ? getNextCronTime(config.cron, config.timezone ?? "UTC", now)
    : config.rrule
      ? getNextRruleTime(config.rrule, now)
      : new Date(now.getTime() + config.intervalMs!);

  if (config.endAt && nextFireTime >= config.endAt) {
    return succeed(null);
  }

  // Jitter delays the emission past the nominal fire time.
  const sleepMs =
    Math.max(0, nextFireTime.getTime() - now.getTime()) + jitterDelayMs({ config, random });

  return wallClockSleep({ clock, ms: sleepMs }).map((): ScheduleStep => {
    // Re-check after the wait: a schedule removed, replaced or paused while
    // waiting must not emit the tick computed from its old state.
    const live = current();
    if (!live) return null;
    if (live !== config || live.paused) return [null, tickNumber];
    const tick: ScheduleTick = {
      scheduleId: config.id,
      scheduleName: config.name,
      scheduledAt: nextFireTime,
      firedAt: clock.now(),
      tickNumber,
      metadata: config.metadata,
    };
    return [tick, tickNumber + 1];
  });
}

function getNextCronTime(expression: string, timezone: string, after: Date): Date {
  const cron = new Cron(expression, { timezone });
  const next = cron.nextRun(after);
  if (!next) {
    throw new Error(
      `Cron expression "${expression}" has no next fire time after ${after.toISOString()}`,
    );
  }
  return next;
}

function getNextRruleTime(rruleStr: string, after: Date): Date {
  const rule = RRule.fromString(rruleStr);
  const next = rule.after(after, false);
  if (!next) {
    throw new Error(`RRULE "${rruleStr}" has no next occurrence after ${after.toISOString()}`);
  }
  return next;
}
