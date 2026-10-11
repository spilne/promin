// ---------------------------------------------------------------------------
// Schedule config validation and jitter — shared by every scheduler.
// ---------------------------------------------------------------------------

import { Cron } from "croner";
import { RRule } from "./rrule.ts";
import type { DurableScheduleConfig, ScheduleConfig } from "./types.ts";

/**
 * Validate a schedule config. Throws an `Error` describing the first problem:
 * no trigger or more than one of `cron` / `rrule` / `intervalMs`, an
 * unparseable cron expression or RRULE, a non-positive interval, or a
 * negative `jitterMs` / `maxCatchUp`.
 */
export function validateScheduleConfig(config: ScheduleConfig | DurableScheduleConfig): void {
  const triggers = [config.cron, config.rrule, config.intervalMs].filter(
    (t) => t !== undefined && t !== null && t !== "",
  ).length;
  if (triggers === 0) {
    throw new Error(`Schedule "${config.id}" must have one of: cron, rrule, or intervalMs`);
  }
  if (triggers > 1) {
    throw new Error(`Schedule "${config.id}" must have exactly one of: cron, rrule, or intervalMs`);
  }
  if (config.cron) {
    try {
      new Cron(config.cron, { timezone: config.timezone ?? "UTC" });
    } catch (e) {
      throw new Error(`Invalid cron expression "${config.cron}" for schedule "${config.id}": ${e}`);
    }
  }
  if (config.rrule) {
    try {
      RRule.fromString(config.rrule);
    } catch (e) {
      throw new Error(`Invalid RRULE "${config.rrule}" for schedule "${config.id}": ${e}`);
    }
  }
  if (config.intervalMs !== undefined) {
    if (!Number.isFinite(config.intervalMs) || config.intervalMs <= 0) {
      throw new Error(
        `Invalid intervalMs ${config.intervalMs} for schedule "${config.id}": must be a positive number`,
      );
    }
  }
  if (config.jitterMs !== undefined) {
    if (!Number.isFinite(config.jitterMs) || config.jitterMs < 0) {
      throw new Error(
        `Invalid jitterMs ${config.jitterMs} for schedule "${config.id}": must be >= 0`,
      );
    }
  }
  const maxCatchUp = (config as DurableScheduleConfig).maxCatchUp;
  if (maxCatchUp !== undefined) {
    if (!Number.isInteger(maxCatchUp) || maxCatchUp < 0) {
      throw new Error(
        `Invalid maxCatchUp ${maxCatchUp} for schedule "${config.id}": must be an integer >= 0`,
      );
    }
  }
}

/**
 * Shortest gap, in ms, between consecutive fires of a schedule's trigger,
 * sampled over its next `samples` fires (default 64) after `from`. Used to
 * enforce a minimum cadence on schedules written by untrusted callers: a
 * cron with a seconds field or an RRULE with `FREQ=SECONDLY` shows up as a
 * short gap. `null` when the trigger fires fewer than twice from `from` on.
 * Assumes the config passed `validateScheduleConfig`.
 */
export function shortestFireGapMs(params: {
  config: Pick<ScheduleConfig, "cron" | "rrule" | "intervalMs" | "timezone">;
  from: Date;
  samples?: number;
}): number | null {
  const { config, from } = params;
  const samples = Math.max(2, params.samples ?? 64);
  if (config.intervalMs !== undefined) return config.intervalMs;
  let fires: Date[] = [];
  if (config.cron) {
    fires = new Cron(config.cron, { timezone: config.timezone ?? "UTC" }).nextRuns(samples, from);
  } else if (config.rrule) {
    const horizon = new Date(from.getTime() + 400 * 24 * 60 * 60 * 1_000);
    fires = RRule.fromString(config.rrule).between(from, horizon, true, (_d, i) => i < samples);
  }
  let shortest: number | null = null;
  for (let i = 1; i < fires.length; i++) {
    const gap = fires[i]!.getTime() - fires[i - 1]!.getTime();
    if (shortest === null || gap < shortest) shortest = gap;
  }
  return shortest;
}

/**
 * Random emission delay for one tick: uniform in `[0, jitterMs)`, or 0 when
 * the schedule has no jitter. `random` returns a number in `[0, 1)`.
 */
export function jitterDelayMs(params: {
  config: Pick<ScheduleConfig, "jitterMs">;
  random: () => number;
}): number {
  const jitterMs = params.config.jitterMs ?? 0;
  if (jitterMs <= 0) return 0;
  return Math.floor(params.random() * jitterMs);
}
