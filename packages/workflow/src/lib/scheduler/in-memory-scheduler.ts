// ---------------------------------------------------------------------------
// InMemoryScheduler — non-blocking, in-process cron/interval scheduler
//
// Per schedule: compute next fire time → Effect.sleep(delta) → emit → repeat
// No polling. No setInterval. No busy-wait. Event loop stays free.
// ---------------------------------------------------------------------------

import { Effect, Stream, Duration, Option, Queue } from "effect";
import { Cron } from "croner";
import { RRule } from "rrule";
import { StreamPipeline } from "@promin/core";
import { JsonCodec } from "@promin/core";
import type { Codec } from "@promin/core";
import type { Scheduler } from "./scheduler.ts";
import type { ScheduleConfig, ScheduleTick } from "./types.ts";

/**
 * Non-blocking, in-process scheduler for cron expressions and fixed intervals.
 *
 * Uses `Effect.sleep()` to yield the fiber until the next fire time — no polling,
 * no `setInterval`, no busy-wait. The event loop stays completely free between ticks.
 *
 * Implements `Streamable<ScheduleTick>` so it works with `StreamPipeline.fromSource()`.
 *
 * For production multi-instance deployments, use `DurableScheduler` from `@promin/postgres`
 * which adds persistence, catch-up, overlap policies, and leader election.
 *
 * @example
 * ```ts
 * import { createScheduler, StreamPipeline } from "@promin/core";
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
 *     toInput: (tick) => ({ date: tick.scheduledAt.toISOString().split("T")[0] }),
 *     toWorkflowId: (tick) => `report-${tick.scheduledAt.toISOString().split("T")[0]}`,
 *   }))
 *   .drain();
 *
 * // Stream all schedules merged — picks up schedules registered after subscribe() is called
 * StreamPipeline.fromSource(scheduler)
 *   .forEach((tick) => console.log(`${tick.scheduleId} fired at ${tick.firedAt}`));
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
   * @param scheduleId - If provided, stream only this schedule. Otherwise merge all.
   */
  stream(scheduleId?: string): StreamPipeline<ScheduleTick, never> {
    if (scheduleId) {
      return this.createScheduleStream(scheduleId);
    }
    return this.createAllSchedulesStream();
  }

  subscribe(_params?: { group?: string }): StreamPipeline<ScheduleTick, never> {
    return this.createAllSchedulesStream();
  }

  // ---------------------------------------------------------------------------
  // All-schedules stream — dynamic merge on register
  //
  // Uses Effect's Queue<Stream> as the outer channel so take()/interrupt()
  // propagates cleanly. The async-generator approach used a never-resolving
  // Promise for backpressure, which caused generator.return() to hang when
  // Effect tried to interrupt the stream after take(N) completed.
  // ---------------------------------------------------------------------------

  private createAllSchedulesStream(): StreamPipeline<ScheduleTick, never> {
    const self = this;

    const s = Stream.unwrapScoped(
      Effect.gen(function* () {
        const q = yield* Queue.unbounded<Stream.Stream<ScheduleTick, never>>();

        // Seed with schedules already registered at call time.
        for (const id of self.schedules.keys()) {
          yield* Queue.offer(q, self.createScheduleStream(id).stream);
        }

        // Forward future registrations into the queue.
        const onRegister = (id: string) => {
          Effect.runFork(Queue.offer(q, self.createScheduleStream(id).stream));
        };
        self._registerCallbacks.add(onRegister);

        // Remove the callback when the stream scope is released (interrupted or done).
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => self._registerCallbacks.delete(onRegister)),
        );

        // Stream.fromQueue with shutdown:false keeps the stream open indefinitely;
        // take() / interrupt() will terminate it via Effect's interrupt mechanism.
        return Stream.flatMap(Stream.fromQueue(q, { shutdown: false }), (inner) => inner, {
          concurrency: "unbounded",
        });
      }),
    );

    return StreamPipeline.from(s) as StreamPipeline<ScheduleTick, never>;
  }

  // ---------------------------------------------------------------------------
  // Non-blocking stream per schedule
  //
  // Uses unfoldEffect with Option:
  //   Some([tick, nextState]) → emit tick, continue
  //   None → end stream (schedule unregistered)
  //
  // Paused schedules sleep briefly and re-check (no emission).
  // ---------------------------------------------------------------------------

  private createScheduleStream(scheduleId: string): StreamPipeline<ScheduleTick, never> {
    const self = this;

    const s = Stream.unfoldEffect(0, (tickNumber: number) =>
      Effect.suspend((): Effect.Effect<Option.Option<readonly [ScheduleTick, number]>> => {
        const config = self.schedules.get(scheduleId);
        if (!config) {
          return Effect.succeed(Option.none());
        }

        if (config.paused) {
          return Effect.sleep(Duration.seconds(1)).pipe(
            Effect.flatMap(() => {
              const rechecked = self.schedules.get(scheduleId);
              if (!rechecked) return Effect.succeed(Option.none());
              if (rechecked.paused) {
                return Effect.succeed(Option.some([SKIP_MARKER, tickNumber] as const));
              }
              return computeAndSleep(rechecked, tickNumber);
            }),
          );
        }

        return computeAndSleep(config, tickNumber);
      }),
    );

    const filtered = Stream.filter(s, (tick) => tick !== SKIP_MARKER);
    return StreamPipeline.from(filtered);
  }
}

// Sentinel for paused ticks that should be filtered out
const SKIP_MARKER: ScheduleTick = {
  scheduleId: "__skip__",
  scheduledAt: new Date(0),
  firedAt: new Date(0),
  tickNumber: -1,
};

function computeAndSleep(
  config: ScheduleConfig & { paused: boolean },
  tickNumber: number,
): Effect.Effect<Option.Option<readonly [ScheduleTick, number]>> {
  const now = new Date();

  if (config.startAt && now < config.startAt) {
    const waitMs = config.startAt.getTime() - now.getTime();
    return Effect.sleep(Duration.millis(waitMs)).pipe(
      Effect.map(() => Option.some([SKIP_MARKER, tickNumber] as const)),
    );
  }

  if (config.endAt && now >= config.endAt) {
    return Effect.succeed(Option.none());
  }

  const nextFireTime = config.cron
    ? getNextCronTime(config.cron, config.timezone ?? "UTC", now)
    : config.rrule
      ? getNextRruleTime(config.rrule, now)
      : new Date(now.getTime() + (config.intervalMs ?? 1000));

  if (config.endAt && nextFireTime >= config.endAt) {
    return Effect.succeed(Option.none());
  }

  const sleepMs = Math.max(0, nextFireTime.getTime() - now.getTime());

  return Effect.sleep(Duration.millis(sleepMs)).pipe(
    Effect.map(() => {
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
      return Option.some([tick, tickNumber + 1] as const);
    }),
  );
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
 * scheduler.stream("daily")
 *   .through(trigger({ workflow: etlWorkflow, ... }))
 *   .drain();
 * ```
 */
export function createScheduler(): InMemoryScheduler {
  return new InMemoryScheduler();
}
