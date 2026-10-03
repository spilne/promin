// ---------------------------------------------------------------------------
// InMemoryScheduler — non-blocking, in-process cron/interval scheduler
//
// Per schedule: compute next fire time → sleep(delta) → emit → repeat
// No polling. No setInterval. No busy-wait. Event loop stays free.
// ---------------------------------------------------------------------------

import { Cron } from "croner";
import { RRule } from "rrule";
import { Stream, sleep, succeed, suspend, sync, type Eff } from "@spilne/perfect-core";
import { JsonCodec } from "@spilne/perfect-core/connect";
import type { Codec } from "@spilne/perfect-core/connect";
import type { Scheduler } from "./scheduler.ts";
import type { ScheduleConfig, ScheduleTick } from "./types.ts";

/**
 * Non-blocking, in-process scheduler for cron expressions and fixed intervals.
 *
 * Uses perfect's `sleep()` to yield the fiber until the next fire time — no polling,
 * no `setInterval`, no busy-wait. The event loop stays completely free between ticks.
 *
 * Implements `Streamable<ScheduleTick>`: `stream()` and `subscribe()` return a
 * fresh perfect `Stream` on every call.
 *
 * For production multi-instance deployments, use `DurableScheduler` from `@promin/postgres`
 * which adds persistence, catch-up, overlap policies, and leader election.
 *
 * @example
 * ```ts
 * import { createScheduler, trigger } from "@promin/workflow";
 *
 * const scheduler = createScheduler();
 *
 * // Cron — every weekday at 9am EST
 * scheduler.register({
 *   id: "morning-report",
 *   cron: "0 9 * * MON-FRI",
 *   timezone: "America/New_York",
 *   metadata: { team: "analytics" },
 * });
 *
 * // Fixed interval — every 30 seconds
 * scheduler.register({ id: "health-check", intervalMs: 30_000 });
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
 * scheduler.pause("health-check");
 * scheduler.resume("health-check");
 *
 * // Unregister ends the stream
 * scheduler.unregister("health-check");
 * ```
 */
export class InMemoryScheduler implements Scheduler {
  private schedules = new Map<string, ScheduleConfig & { paused: boolean }>();
  private readonly _registerCallbacks = new Set<(id: string) => void>();
  readonly codec: Codec<ScheduleTick> = JsonCodec as Codec<ScheduleTick>;

  /**
   * Register a schedule. Validates cron expression eagerly.
   * @throws If neither `cron` nor `intervalMs` is provided, or if cron is invalid.
   */
  register(config: ScheduleConfig): void {
    const triggers = [config.cron, config.rrule, config.intervalMs].filter(Boolean).length;
    if (triggers === 0) {
      throw new Error(`Schedule "${config.id}" must have one of: cron, rrule, or intervalMs`);
    }
    if (triggers > 1) {
      throw new Error(
        `Schedule "${config.id}" must have exactly one of: cron, rrule, or intervalMs`,
      );
    }
    if (config.cron) {
      try {
        new Cron(config.cron, { timezone: config.timezone ?? "UTC" });
      } catch (e) {
        throw new Error(
          `Invalid cron expression "${config.cron}" for schedule "${config.id}": ${e}`,
        );
      }
    }
    if (config.rrule) {
      try {
        RRule.fromString(config.rrule);
      } catch (e) {
        throw new Error(`Invalid RRULE "${config.rrule}" for schedule "${config.id}": ${e}`);
      }
    }
    this.schedules.set(config.id, { ...config, paused: config.enabled === false });
    // Notify any active subscribe() streams so they pick up the new schedule immediately.
    for (const cb of this._registerCallbacks) cb(config.id);
  }

  /** Remove a schedule. Its stream will end. */
  unregister(scheduleId: string, _options?: { reason?: string }): void {
    this.schedules.delete(scheduleId);
  }

  /** Pause a schedule — stops emitting ticks but keeps the config. */
  pause(scheduleId: string): void {
    const s = this.schedules.get(scheduleId);
    if (s) s.paused = true;
  }

  /** Resume a paused schedule. */
  resume(scheduleId: string): void {
    const s = this.schedules.get(scheduleId);
    if (s) s.paused = false;
  }

  /** List all registered schedules with their current enabled state. */
  list(): ScheduleConfig[] {
    return [...this.schedules.values()].map(({ paused, ...config }) => ({
      ...config,
      enabled: !paused,
    }));
  }

  /**
   * Stream ticks from a specific schedule, or all schedules merged.
   *
   * Single-schedule: sleeps until the next fire time — no polling, no busy-wait.
   * All-schedules: same per-schedule sleep approach, with schedules registered
   * after the stream starts picked up automatically via a register callback.
   *
   * Every call builds a fresh stream. Stopping the consumer interrupts the
   * pending sleeps and removes the register callback.
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
  // concurrently. The callback is removed when the stream terminates.
  // ---------------------------------------------------------------------------

  private createAllSchedulesStream(): Stream<ScheduleTick> {
    const scheduleIds = Stream.async<string, never>(
      (emit) =>
        sync(() => {
          for (const id of this.schedules.keys()) emit(id);
          const onRegister = (id: string) => emit(id);
          this._registerCallbacks.add(onRegister);
          return () => {
            this._registerCallbacks.delete(onRegister);
          };
        }),
      Infinity,
    );

    return scheduleIds.map((id) => this.createScheduleStream(id)).parJoinUnbounded();
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
    const step = (tickNumber: number): Eff<ScheduleStep> =>
      suspend(() => {
        const config = this.schedules.get(scheduleId);
        if (!config) return succeed(null);

        if (config.paused) {
          return sleep(PAUSED_RECHECK_MS).flatMap((): Eff<ScheduleStep> => {
            const rechecked = this.schedules.get(scheduleId);
            if (!rechecked) return succeed(null);
            if (rechecked.paused) return succeed([null, tickNumber]);
            return computeAndSleep(rechecked, tickNumber);
          });
        }

        return computeAndSleep(config, tickNumber);
      });

    return Stream.unfoldEffect(0, step).unNone();
  }
}

/** How often a paused schedule re-checks whether it has been resumed. */
const PAUSED_RECHECK_MS = 1000;

/** One unfold step: a tick (or nothing yet) plus the next tick number, or end. */
type ScheduleStep = [ScheduleTick | null, number] | null;

function computeAndSleep(
  config: ScheduleConfig & { paused: boolean },
  tickNumber: number,
): Eff<ScheduleStep> {
  const now = new Date();

  if (config.startAt && now < config.startAt) {
    const waitMs = config.startAt.getTime() - now.getTime();
    return sleep(waitMs).map((): ScheduleStep => [null, tickNumber]);
  }

  if (config.endAt && now >= config.endAt) {
    return succeed(null);
  }

  const nextFireTime = config.cron
    ? getNextCronTime(config.cron, config.timezone ?? "UTC", now)
    : config.rrule
      ? getNextRruleTime(config.rrule, now)
      : new Date(now.getTime() + (config.intervalMs ?? 1000));

  if (config.endAt && nextFireTime >= config.endAt) {
    return succeed(null);
  }

  const sleepMs = Math.max(0, nextFireTime.getTime() - now.getTime());

  return sleep(sleepMs).map((): ScheduleStep => {
    const jitterMs = config.jitterMs ?? 0;
    const jitter = jitterMs > 0 ? Math.random() * jitterMs : 0;
    const tick: ScheduleTick = {
      scheduleId: config.id,
      scheduleName: config.name,
      scheduledAt: nextFireTime,
      firedAt: new Date(Date.now() + jitter),
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

// ---------------------------------------------------------------------------
// Factory
// ---------------------------------------------------------------------------

/**
 * Create an in-memory scheduler. Non-blocking, no persistence.
 *
 * @example
 * ```ts
 * const scheduler = createScheduler();
 * scheduler.register({ id: "daily", cron: "0 2 * * *", timezone: "America/New_York" });
 * scheduler.register({ id: "heartbeat", intervalMs: 30_000 });
 *
 * await scheduler.stream("daily")
 *   .through(trigger({ workflow: etlWorkflow, ... }))
 *   .drain()
 *   .run();
 * ```
 */
export function createScheduler(): InMemoryScheduler {
  return new InMemoryScheduler();
}
