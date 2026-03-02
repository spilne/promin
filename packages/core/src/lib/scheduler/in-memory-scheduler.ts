// ---------------------------------------------------------------------------
// InMemoryScheduler — non-blocking, in-process cron/interval scheduler
//
// Per schedule: compute next fire time → Effect.sleep(delta) → emit → repeat
// No polling. No setInterval. No busy-wait. Event loop stays free.
// ---------------------------------------------------------------------------

import { Effect, Stream, Duration, Option } from "effect";
import { Cron } from "croner";
import { StreamPipeline } from "../stream-pipeline.ts";
import { JsonCodec } from "../typeclasses/codec.ts";
import type { Codec } from "../typeclasses/codec.ts";
import type { Scheduler } from "./scheduler.ts";
import type { ScheduleConfig, ScheduleTick } from "./types.ts";

export class InMemoryScheduler implements Scheduler {
  private schedules = new Map<string, ScheduleConfig & { paused: boolean }>();
  readonly codec: Codec<ScheduleTick> = JsonCodec as Codec<ScheduleTick>;

  register(config: ScheduleConfig): void {
    if (!config.cron && !config.intervalMs) {
      throw new Error(`Schedule "${config.id}" must have either cron or intervalMs`);
    }
    if (config.cron && config.intervalMs) {
      throw new Error(`Schedule "${config.id}" cannot have both cron and intervalMs`);
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
    this.schedules.set(config.id, { ...config, paused: config.enabled === false });
  }

  unregister(scheduleId: string): void {
    this.schedules.delete(scheduleId);
  }

  pause(scheduleId: string): void {
    const s = this.schedules.get(scheduleId);
    if (s) s.paused = true;
  }

  resume(scheduleId: string): void {
    const s = this.schedules.get(scheduleId);
    if (s) s.paused = false;
  }

  list(): ScheduleConfig[] {
    return [...this.schedules.values()].map(({ paused, ...config }) => ({
      ...config,
      enabled: !paused,
    }));
  }

  stream(scheduleId?: string): StreamPipeline<ScheduleTick, never> {
    if (scheduleId) {
      return this.createScheduleStream(scheduleId);
    }
    const ids = [...this.schedules.keys()];
    if (ids.length === 0) {
      return StreamPipeline.from(Stream.empty);
    }
    const streams = ids.map((id) => this.createScheduleStream(id).stream);
    const merged = streams.reduce((acc, s) => Stream.merge(acc, s));
    return StreamPipeline.from(merged);
  }

  subscribe(params?: { group?: string }): StreamPipeline<ScheduleTick, never> {
    return this.stream();
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
          // Sleep 1s, then re-check — emit nothing, keep same tickNumber
          return Effect.sleep(Duration.seconds(1)).pipe(
            Effect.flatMap(() => {
              // Re-check after sleep
              const rechecked = self.schedules.get(scheduleId);
              if (!rechecked) return Effect.succeed(Option.none());
              if (rechecked.paused) {
                // Still paused — use recursion via a sentinel: return a "skip" tick
                // Actually, unfoldEffect can't skip. Use a wrapper stream with filter.
                // Simplest: just return a tick with a negative tickNumber as skip marker.
                return Effect.succeed(Option.some([SKIP_MARKER, tickNumber] as const));
              }
              // Resumed — fall through to normal scheduling
              return computeAndSleep(rechecked, tickNumber);
            }),
          );
        }

        return computeAndSleep(config, tickNumber);
      }),
    );

    // Filter out skip markers
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
  const nextFireTime = config.cron
    ? getNextCronTime(config.cron, config.timezone ?? "UTC", now)
    : new Date(now.getTime() + (config.intervalMs ?? 1000));

  const sleepMs = Math.max(0, nextFireTime.getTime() - now.getTime());

  return Effect.sleep(Duration.millis(sleepMs)).pipe(
    Effect.map(() => {
      const tick: ScheduleTick = {
        scheduleId: config.id,
        scheduleName: config.name,
        scheduledAt: nextFireTime,
        firedAt: new Date(),
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
