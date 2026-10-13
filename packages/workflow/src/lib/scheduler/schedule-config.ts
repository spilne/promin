// ---------------------------------------------------------------------------
// Schedule config validation and jitter — shared by every scheduler.
// ---------------------------------------------------------------------------

import { Cron } from "croner";
import { RRule } from "rrule";
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
