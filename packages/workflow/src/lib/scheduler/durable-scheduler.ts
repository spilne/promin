// ---------------------------------------------------------------------------
// DurableScheduler — backend-agnostic poll-based scheduler.
//
// All cron/rrule/interval/catch-up/jitter/leader-loop logic lives here.
// Backends implement `SchedulerStorage` and plug in via the constructor.
// ---------------------------------------------------------------------------

import { Effect, Stream, Duration, Schedule } from "effect";
import { Cron } from "croner";
import { RRule } from "rrule";
import { StreamPipeline, JsonCodec } from "@promin/core";
import type { Codec } from "@promin/core";
import type { Scheduler } from "./scheduler.ts";
import type { DurableScheduleConfig, ScheduleConfig, ScheduleTick } from "./types.ts";
import type { SchedulerStorage } from "./scheduler-storage.ts";

export interface DurableSchedulerConfig {
  /** Storage backend (Postgres, Redis, in-memory, ...). */
  storage: SchedulerStorage;
  /** Instance ID for leader election. Default: random UUID. */
  instanceId?: string;
  /** Poll interval in ms. Default: 1000. */
  pollIntervalMs?: number;
  /** Leader-lock TTL. Default: 3 × pollIntervalMs. */
  leaderLockTtlMs?: number;
  /**
   * Scope this scheduler instance to a single namespace. `findDue`,
   * `listAsync`, and the leader lock are all filtered by this value.
   * Default: undefined (global namespace).
   */
  namespace?: string;
  /**
   * Max schedules to claim per poll cycle. Default: 100. Tune up for very
   * large schedule counts; the storage `findDue` enforces this via LIMIT.
   */
  batchSize?: number;
}

/**
 * Generic poll-based scheduler. Backend-agnostic — give it a `SchedulerStorage`
 * (Postgres, Redis, in-memory) and it handles cron/rrule/interval, catch-up,
 * jitter, leader election, and the streaming surface.
 *
 * Convenience factories live in backend packages (`createPgScheduler`,
 * `createRedisScheduler`) — they wrap this with a pre-built storage adapter.
 */
export class DurableScheduler implements Scheduler {
  readonly codec: Codec<ScheduleTick> = JsonCodec as Codec<ScheduleTick>;

  private readonly storage: SchedulerStorage;
  readonly instanceId: string;
  private readonly pollIntervalMs: number;
  private readonly leaderLockTtlMs: number;
  private readonly namespace?: string;
  private readonly batchSize: number;

  constructor(config: DurableSchedulerConfig) {
    this.storage = config.storage;
    this.instanceId = config.instanceId ?? crypto.randomUUID();
    this.pollIntervalMs = config.pollIntervalMs ?? 1000;
    this.leaderLockTtlMs = config.leaderLockTtlMs ?? this.pollIntervalMs * 3;
    this.namespace = config.namespace;
    this.batchSize = config.batchSize ?? 100;
  }

  // -------------------------------------------------------------------------
  // Schedule management
  // -------------------------------------------------------------------------

  /** Fire-and-forget register. Use `registerAsync` to await persistence. */
  register(config: DurableScheduleConfig | ScheduleConfig): void {
    void this.registerAsync(config);
  }

  async registerAsync(config: DurableScheduleConfig | ScheduleConfig): Promise<void> {
    validateScheduleConfig(config);
    const durable = config as DurableScheduleConfig;
    // If this scheduler instance is namespaced, force the registered schedule
    // into the same namespace so it's visible to this instance's findDue.
    const namespace = config.namespace ?? this.namespace;
    const stored: DurableScheduleConfig = { ...durable, namespace };
    await this.storage.upsertSchedule(stored);

    // Seed nextRun = now so the first poll picks it up immediately.
    await this.storage.setNextRun(config.id, new Date());
  }

  unregister(scheduleId: string, options?: { reason?: string }): void {
    void this.unregisterAsync(scheduleId, options);
  }

  async unregisterAsync(scheduleId: string, _options?: { reason?: string }): Promise<void> {
    await this.storage.deleteSchedule(scheduleId);
  }

  pause(scheduleId: string): void {
    void this.pauseAsync(scheduleId);
  }

  async pauseAsync(scheduleId: string): Promise<void> {
    await this.storage.setEnabled(scheduleId, false);
  }

  resume(scheduleId: string): void {
    void this.resumeAsync(scheduleId);
  }

  async resumeAsync(scheduleId: string): Promise<void> {
    await this.storage.setEnabled(scheduleId, true);
  }

  list(): ScheduleConfig[] {
    // Sync per Scheduler interface — returns empty. Use listAsync for real data.
    return [];
  }

  async listAsync(params?: {
    enabled?: boolean;
    namespace?: string;
    limit?: number;
    offset?: number;
  }): Promise<DurableScheduleConfig[]> {
    return await this.storage.listSchedules({
      enabled: params?.enabled,
      namespace: params?.namespace ?? this.namespace,
      limit: params?.limit,
      offset: params?.offset,
    });
  }

  async countAsync(params?: { enabled?: boolean; namespace?: string }): Promise<number> {
    return await this.storage.countSchedules({
      enabled: params?.enabled,
      namespace: params?.namespace ?? this.namespace,
    });
  }

  /** Preview next N fire times for a schedule. Pure cron/rrule math, no I/O. */
  async nextFireTimes(scheduleId: string, count: number): Promise<Date[]> {
    const config = await this.storage.loadSchedule(scheduleId);
    if (!config) return [];

    if (config.cron) {
      const cron = new Cron(config.cron, { timezone: config.timezone ?? "UTC" });
      const times: Date[] = [];
      let cursor = new Date();
      for (let i = 0; i < count; i++) {
        const next = cron.nextRun(cursor);
        if (!next) break;
        times.push(next);
        cursor = new Date(next.getTime() + 1);
      }
      return times;
    }
    if (config.rrule) {
      const rule = RRule.fromString(config.rrule);
      const now = new Date();
      const occurrences = rule.between(
        now,
        new Date(now.getTime() + 365 * 24 * 60 * 60 * 1000),
        true,
      );
      return occurrences.slice(0, count);
    }
    return [];
  }

  /** Manually fire a schedule now, regardless of cron. */
  async triggerNow(scheduleId: string): Promise<ScheduleTick | null> {
    const config = await this.storage.loadSchedule(scheduleId);
    if (!config) return null;
    const state = await this.storage.loadScheduleState(scheduleId);
    const now = new Date();
    const tickNumber = state?.tickCount ?? 0;
    const tick: ScheduleTick = {
      scheduleId,
      scheduleName: config.name,
      scheduledAt: now,
      firedAt: now,
      tickNumber,
      metadata: config.metadata,
    };
    await this.storage.recordFire(scheduleId, now);
    return tick;
  }

  /** Backfill ticks across a historical date range. */
  async backfill(scheduleId: string, params: { from: Date; to: Date }): Promise<ScheduleTick[]> {
    const config = await this.storage.loadSchedule(scheduleId);
    if (!config) return [];
    const state = await this.storage.loadScheduleState(scheduleId);
    let tickNumber = state?.tickCount ?? 0;

    const occurrences = config.cron
      ? backfillCron(config.cron, config.timezone ?? "UTC", params.from, params.to)
      : config.rrule
        ? RRule.fromString(config.rrule).between(params.from, params.to, false)
        : [];

    const ticks: ScheduleTick[] = [];
    for (const scheduledAt of occurrences) {
      const now = new Date();
      ticks.push({
        scheduleId,
        scheduleName: config.name,
        scheduledAt,
        firedAt: now,
        tickNumber,
        metadata: config.metadata,
      });
      await this.storage.recordFire(scheduleId, now);
      tickNumber++;
    }
    return ticks;
  }

  // -------------------------------------------------------------------------
  // Streamable — leader-elected polling loop
  // -------------------------------------------------------------------------

  stream(scheduleId?: string): StreamPipeline<ScheduleTick, never> {
    const self = this;

    const s = Stream.repeatEffect(
      Effect.promise(async () => {
        const isLeader = await self.storage.tryAcquireLeader({
          instanceId: self.instanceId,
          namespace: self.namespace,
          ttlMs: self.leaderLockTtlMs,
        });
        if (!isLeader) return [] as ScheduleTick[];

        const dueIds = await self.storage.findDue({
          now: new Date(),
          limit: self.batchSize,
          namespace: self.namespace,
        });
        const targetIds = scheduleId ? dueIds.filter((id) => id === scheduleId) : dueIds;

        const ticks: ScheduleTick[] = [];
        for (const id of targetIds) {
          const config = await self.storage.loadSchedule(id);
          if (!config) {
            await self.storage.setNextRun(id, null);
            continue;
          }
          if (config.enabled === false) continue;

          const state = await self.storage.loadScheduleState(id);
          const due = computeDueTicks(config, state?.lastFired ?? null, state?.tickCount ?? 0);
          for (const tick of due) {
            await self.storage.recordFire(id, tick.firedAt);
            ticks.push(tick);
          }

          // Re-schedule (or remove from due-tracking if exhausted).
          const next = computeNextRun(config);
          await self.storage.setNextRun(id, next);
        }
        return ticks;
      }),
    ).pipe(
      Stream.schedule(Schedule.spaced(Duration.millis(this.pollIntervalMs))),
      Stream.flatMap((ticks) => Stream.fromIterable(ticks)),
    );

    return StreamPipeline.from(s) as StreamPipeline<ScheduleTick, never>;
  }

  subscribe(_params?: { group?: string }): StreamPipeline<ScheduleTick, never> {
    return this.stream();
  }
}

// ---------------------------------------------------------------------------
// Pure helpers — no storage access, easy to unit-test.
// ---------------------------------------------------------------------------

export function validateScheduleConfig(config: ScheduleConfig | DurableScheduleConfig): void {
  const triggers = [config.cron, config.rrule, config.intervalMs].filter(Boolean).length;
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
}

/**
 * Compute the ticks that should fire NOW for a given schedule and its
 * lastFired state. Pure function — no I/O. Caller persists the results.
 */
export function computeDueTicks(
  config: DurableScheduleConfig,
  lastFired: Date | null,
  tickCount: number,
): ScheduleTick[] {
  const now = new Date();
  if (config.startAt && now < config.startAt) return [];
  if (config.endAt && now > config.endAt) return [];

  if (config.cron) return computeCronDue(config, now, lastFired, tickCount);
  if (config.rrule) return computeRruleDue(config, now, lastFired, tickCount);
  if (config.intervalMs !== undefined) return computeIntervalDue(config, now, lastFired, tickCount);
  return [];
}

function computeCronDue(
  config: DurableScheduleConfig,
  now: Date,
  lastFired: Date | null,
  startTickNumber: number,
): ScheduleTick[] {
  const cron = new Cron(config.cron!, { timezone: config.timezone ?? "UTC" });
  const ticks: ScheduleTick[] = [];
  const maxCatchUp = config.maxCatchUp ?? 0;
  const jitterMs = config.jitterMs ?? 0;

  // First-fire bootstrap: with no lastFired, the catch-up loop below would
  // never produce a tick (cron.nextRun(now-1) returns the next FUTURE
  // occurrence, which fails the `next > now` guard). Fire one immediate tick.
  if (!lastFired) {
    return [makeTick(config, now, jitterMs, startTickNumber)];
  }

  let cursor = new Date(lastFired.getTime() + 1);
  let tickNumber = startTickNumber;
  let catchUpCount = 0;

  while (true) {
    const next = cron.nextRun(cursor);
    if (!next || next > now) break;
    if (catchUpCount >= maxCatchUp) {
      cursor = new Date(next.getTime() + 1);
      continue;
    }
    ticks.push(makeTick(config, next, jitterMs, tickNumber));
    cursor = new Date(next.getTime() + 1);
    tickNumber++;
    catchUpCount++;
  }
  return ticks;
}

function computeRruleDue(
  config: DurableScheduleConfig,
  now: Date,
  lastFired: Date | null,
  startTickNumber: number,
): ScheduleTick[] {
  const rule = RRule.fromString(config.rrule!);
  const ticks: ScheduleTick[] = [];
  const maxCatchUp = config.maxCatchUp ?? 0;
  const jitterMs = config.jitterMs ?? 0;

  if (!lastFired) {
    return [makeTick(config, now, jitterMs, startTickNumber)];
  }

  const after = new Date(lastFired.getTime() + 1);
  const occurrences = rule.between(after, now, true);
  const limited = occurrences.length > maxCatchUp ? occurrences.slice(-maxCatchUp) : occurrences;

  let tickNumber = startTickNumber;
  for (const scheduledAt of limited) {
    ticks.push(makeTick(config, scheduledAt, jitterMs, tickNumber));
    tickNumber++;
  }
  return ticks;
}

function computeIntervalDue(
  config: DurableScheduleConfig,
  now: Date,
  lastFired: Date | null,
  startTickNumber: number,
): ScheduleTick[] {
  const intervalMs = config.intervalMs!;
  const nextFireTime = lastFired ? new Date(lastFired.getTime() + intervalMs) : now;
  if (nextFireTime > now) return [];
  const jitterMs = config.jitterMs ?? 0;
  return [makeTick(config, nextFireTime, jitterMs, startTickNumber)];
}

function makeTick(
  config: DurableScheduleConfig,
  scheduledAt: Date,
  jitterMs: number,
  tickNumber: number,
): ScheduleTick {
  const jitter = jitterMs > 0 ? Math.random() * jitterMs : 0;
  return {
    scheduleId: config.id,
    scheduleName: config.name,
    scheduledAt,
    firedAt: new Date(Date.now() + jitter),
    tickNumber,
    metadata: config.metadata,
  };
}

/** Compute the next time a schedule will fire — used to update the due index. */
export function computeNextRun(config: DurableScheduleConfig): Date | null {
  const now = new Date();
  if (config.endAt && now >= config.endAt) return null;

  const candidate = (() => {
    if (config.cron) {
      return new Cron(config.cron, { timezone: config.timezone ?? "UTC" }).nextRun(now);
    }
    if (config.rrule) {
      return RRule.fromString(config.rrule).after(now, false);
    }
    if (config.intervalMs !== undefined) {
      return new Date(now.getTime() + config.intervalMs);
    }
    return null;
  })();

  if (!candidate) return null;
  if (config.endAt && candidate >= config.endAt) return null;
  if (config.startAt && candidate < config.startAt) return config.startAt;
  return candidate;
}

function backfillCron(cron: string, timezone: string, from: Date, to: Date): Date[] {
  const c = new Cron(cron, { timezone });
  const dates: Date[] = [];
  let cursor = from;
  while (cursor < to) {
    const next = c.nextRun(cursor);
    if (!next || next >= to) break;
    dates.push(next);
    cursor = new Date(next.getTime() + 1);
  }
  return dates;
}

// ---------------------------------------------------------------------------
// Factory
// ---------------------------------------------------------------------------

export function createDurableScheduler(config: DurableSchedulerConfig): DurableScheduler {
  return new DurableScheduler(config);
}
