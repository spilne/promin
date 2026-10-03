// ---------------------------------------------------------------------------
// DurableScheduler — backend-agnostic poll-based scheduler.
//
// All cron/rrule/interval/catch-up/jitter/leader-loop logic lives here.
// Backends implement `SchedulerStorage` and plug in via the constructor.
// ---------------------------------------------------------------------------

import { Cron } from "croner";
import { RRule } from "rrule";
import { Stream, succeed, suspend, tryPromise, type Eff } from "@spilne/perfect-core";
import { SystemWallClock } from "../shared/wall-clock.ts";
import { wallClockSleep } from "./wall-clock-sleep.ts";
import { JsonCodec } from "@spilne/perfect-core/connect";
import { jitterDelayMs, validateScheduleConfig } from "./schedule-config.ts";
import type { WallClock } from "../shared/wall-clock.ts";
import type { Codec } from "@spilne/perfect-core/connect";
import type { Scheduler } from "./scheduler.ts";
import type { DurableScheduleConfig, ScheduleConfig, ScheduleTick } from "./types.ts";
import type { CommitPollResult, ScheduleCommit, SchedulerStorage } from "./scheduler-storage.ts";
import { schedulerLeaderKey, type LeaderLease } from "./leader-lease.ts";

/** Where a `DurableScheduler` error happened. */
export type SchedulerErrorPhase =
  /** Leader election, `findDue` or the bulk loads failed; the poll is retried with backoff. */
  | "poll"
  /**
   * `commitPoll` (or disabling an invalid schedule) failed; uncommitted ticks
   * are redelivered. A `StaleLeaseError` here means leadership moved to
   * another instance mid-poll and the new leader redelivers them.
   */
  | "commit"
  /** One stored schedule could not be evaluated; it is disabled and skipped. */
  | "schedule"
  /** Releasing the leader lease on stop failed; it then expires after its TTL. */
  | "release";

/** Passed to `DurableSchedulerConfig.onError`. */
export interface SchedulerErrorEvent {
  readonly phase: SchedulerErrorPhase;
  readonly error: unknown;
  /** Set for `"schedule"` errors. */
  readonly scheduleId?: string;
}

export interface DurableSchedulerConfig {
  /** Storage backend (Postgres, Redis, in-memory, ...). */
  storage: SchedulerStorage;
  /** Instance ID for leader election. Default: random UUID. */
  instanceId?: string;
  /** Poll interval in ms. Default: 1000. */
  pollIntervalMs?: number;
  /**
   * Leader-lease TTL. The lease is refreshed at every poll, so it must
   * cover one poll interval plus the time the consumer takes to pull one
   * poll's ticks; if it lapses mid-batch another instance can take over and
   * this instance's commit is then rejected (the ticks are redelivered by
   * the new leader). Default: 3 × pollIntervalMs.
   */
  leaderLockTtlMs?: number;
  /**
   * Scope this scheduler instance to a single namespace. `findDue`,
   * `list`, and the leader lease are all scoped by this value.
   * Default: undefined (global namespace).
   */
  namespace?: string;
  /**
   * Max schedules to claim per poll cycle. Default: 100. Tune up for very
   * large schedule counts; the storage `findDue` enforces this via LIMIT.
   */
  batchSize?: number;
  /**
   * Hash-based partitioning for horizontal scaling. When set, this instance
   * only fires schedules with `schedulePartition({ id, count }) == index`, and it
   * elects a leader among the instances of its own partition (one lease per
   * namespace and partition), so the N partitions fire in parallel. Run N
   * scheduler processes (or N per partition, for failover) with
   * `{ index: 0, count: N } ... { index: N-1, count: N }`.
   *
   * Filtering happens in the scheduler shell, so storage `findDue` is asked
   * for `batchSize × count` ids to leave room for the other partitions' due
   * schedules. Run every partition: a partition with no live instance leaves
   * its due schedules at the head of the due index, and once that backlog
   * exceeds `batchSize × count` it crowds the other partitions out.
   */
  partition?: { index: number; count: number };
  /**
   * Time source. Drives nextRun seeding, which ticks are due, each tick's
   * `firedAt`, the next-run computation, the poll-loop tick cadence and
   * the error backoff. Default: `SystemWallClock`.
   */
  clock?: WallClock;
  /**
   * Called when a poll or commit fails, or a stored schedule can't be
   * evaluated. The tick stream keeps running in every case. Default: logs
   * with `console.error`.
   */
  onError?: (event: SchedulerErrorEvent) => void;
  /**
   * Upper bound for the wait between failed polls. Consecutive failures
   * back off exponentially from `2 × pollIntervalMs` up to this value.
   * Default: 30 000 (or `pollIntervalMs`, if larger).
   */
  maxErrorBackoffMs?: number;
  /** Randomness for `jitterMs`, returning `[0, 1)`. Default: `Math.random`. */
  random?: () => number;
}

/**
 * Generic poll-based scheduler. Backend-agnostic — give it a `SchedulerStorage`
 * (Postgres, Redis, in-memory) and it handles cron/rrule/interval, catch-up,
 * jitter, leader election, and the streaming surface.
 *
 * ## Delivery guarantee: at least once
 *
 * Each poll computes the due ticks without writing anything, emits them, and
 * commits the fire state (`tickCount`, `lastFired`, `nextRun`) only once the
 * consumer has pulled past them. A tick counts as acknowledged when the
 * consumer pulls the next element, so a sequential consumer has finished
 * with it. If the consumer stops early (`take(n)`, interruption, a crash),
 * schedules whose ticks were all acknowledged are committed and the rest are
 * left due: the next poll emits them again with the **same `tickNumber`**.
 * Use `scheduleTickRunId(tick.scheduleId, tick.tickNumber)` as the run id so
 * a redelivered tick is a no-op. Operators that pull ahead (buffers,
 * `parMapAsync`) acknowledge a tick when they pull it.
 *
 * Storage errors never end the stream: a failed poll is reported through
 * `onError` and retried with backoff, a failed commit is reported and its
 * ticks are redelivered, and a stored schedule that can't be evaluated (for
 * example an invalid cron written straight to storage) is reported, disabled
 * and skipped without affecting the others.
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
  private readonly partition?: { index: number; count: number };
  private readonly clock: WallClock;
  private readonly onError: (event: SchedulerErrorEvent) => void;
  private readonly maxErrorBackoffMs: number;
  private readonly random: () => number;
  /** Lease key: one leader per namespace and partition. */
  private readonly leaderKey: string;
  /** The lease from the latest successful acquire, released when the last stream stops. */
  private heldLease: LeaderLease | null = null;
  /** Streams of this instance currently running; they share the lease. */
  private activeStreams = 0;

  constructor(config: DurableSchedulerConfig) {
    this.storage = config.storage;
    this.instanceId = config.instanceId ?? crypto.randomUUID();
    this.pollIntervalMs = config.pollIntervalMs ?? 1000;
    this.leaderLockTtlMs = config.leaderLockTtlMs ?? this.pollIntervalMs * 3;
    this.namespace = config.namespace;
    this.batchSize = config.batchSize ?? 100;
    this.clock = config.clock ?? SystemWallClock;
    this.onError = config.onError ?? defaultOnError;
    this.maxErrorBackoffMs = Math.max(config.maxErrorBackoffMs ?? 30_000, this.pollIntervalMs);
    this.random = config.random ?? Math.random;
    if (config.partition) {
      if (
        config.partition.count < 1 ||
        config.partition.index < 0 ||
        config.partition.index >= config.partition.count
      ) {
        throw new Error(
          `Invalid partition: index=${config.partition.index}, count=${config.partition.count}`,
        );
      }
      this.partition = config.partition;
    }
    this.leaderKey = schedulerLeaderKey({ namespace: this.namespace, partition: this.partition });
  }

  // -------------------------------------------------------------------------
  // Schedule management
  // -------------------------------------------------------------------------

  /**
   * Register (or replace) a schedule and persist it. An enabled schedule is
   * due immediately, so the next poll picks it up. Rejects on an invalid
   * config or a storage error.
   */
  async register(config: DurableScheduleConfig | ScheduleConfig): Promise<void> {
    validateScheduleConfig(config);
    const durable = config as DurableScheduleConfig;
    // If this scheduler instance is namespaced, force the registered schedule
    // into the same namespace so it's visible to this instance's findDue.
    const namespace = config.namespace ?? this.namespace;
    const stored: DurableScheduleConfig = { ...durable, namespace };
    await this.storage.upsertSchedule(stored);

    // Seed nextRun = now so the first poll picks it up immediately. A
    // disabled schedule stays out of due-tracking (the upsert cleared it).
    if (config.enabled !== false) {
      await this.storage.setNextRun(config.id, this.clock.now());
    }
  }

  /**
   * Update an existing schedule, merging changes with its current config.
   * Unlike `register` (which requires a full config and upserts), this
   * takes a partial and preserves unspecified fields — ergonomic for runtime
   * management UIs that only touch one field at a time ("change the cron",
   * "bump the timezone", etc.).
   *
   * Recomputes `nextRun` from the merged config and writes it back so the
   * change is picked up on the next poll without a round-trip lag.
   *
   * Throws if the schedule doesn't exist. Re-validates the merged result.
   */
  async update(
    scheduleId: string,
    patch: Partial<Omit<DurableScheduleConfig, "id">>,
  ): Promise<void> {
    const current = await this.storage.loadSchedule(scheduleId);
    if (!current) {
      throw new Error(`Cannot update schedule "${scheduleId}" — does not exist`);
    }
    const merged: DurableScheduleConfig = { ...current, ...patch, id: scheduleId };
    validateScheduleConfig(merged);
    await this.storage.upsertSchedule(merged);

    // Any change to trigger/timezone/startAt/endAt can alter when the next fire
    // should be. Recompute and push into due-tracking so the next poll sees it.
    if (merged.enabled !== false) {
      await this.storage.setNextRun(scheduleId, computeNextRun(merged, this.clock));
    }
  }

  async unregister(scheduleId: string, _options?: { reason?: string }): Promise<void> {
    await this.storage.deleteSchedule(scheduleId);
  }

  /** Pause a schedule. It leaves due-tracking until resumed. */
  async pause(scheduleId: string): Promise<void> {
    await this.storage.setEnabled(scheduleId, false);
  }

  /**
   * Resume a paused schedule. It is due immediately; the next poll fires
   * the most recent missed occurrence (more with `maxCatchUp`), the same as
   * after a scheduler restart.
   */
  async resume(scheduleId: string): Promise<void> {
    await this.storage.setEnabled(scheduleId, true);
  }

  /** List schedules in this scheduler's namespace (or `params.namespace`). */
  async list(params?: {
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

  async count(params?: { enabled?: boolean; namespace?: string }): Promise<number> {
    return await this.storage.countSchedules({
      enabled: params?.enabled,
      namespace: params?.namespace ?? this.namespace,
    });
  }

  /**
   * Preview the next `count` fire times of a schedule, within its
   * `startAt`/`endAt` window. Cron and RRULE schedules list their trigger's
   * occurrences; interval schedules continue the cadence from the last fire
   * (or start now / at `startAt` when they have never fired).
   */
  async nextFireTimes(scheduleId: string, count: number): Promise<Date[]> {
    const config = await this.storage.loadSchedule(scheduleId);
    if (!config) return [];
    const state =
      config.intervalMs !== undefined ? await this.storage.loadScheduleState(scheduleId) : null;
    return previewFireTimes({
      config,
      lastFired: state?.lastFired ?? null,
      count,
      now: this.clock.now(),
    });
  }

  /**
   * Manually fire a schedule now, regardless of its trigger. The tick takes
   * the next `tickNumber` atomically (a compare-and-set on `tickCount`), so
   * it never shares a number with a committed poll tick or another manual
   * fire. Returns `null` for an unknown schedule.
   */
  async triggerNow(scheduleId: string): Promise<ScheduleTick | null> {
    const ticks = await this.fireManually({
      scheduleId,
      occurrences: () => [this.clock.now()],
    });
    return ticks?.[0] ?? null;
  }

  /**
   * Emit ticks for the occurrences in `[from, to)` and record them as fired.
   * Like `triggerNow`, the tick numbers are taken atomically.
   */
  async backfill(scheduleId: string, params: { from: Date; to: Date }): Promise<ScheduleTick[]> {
    const ticks = await this.fireManually({
      scheduleId,
      occurrences: (config) =>
        config.cron
          ? backfillCron(config.cron, config.timezone ?? "UTC", params.from, params.to)
          : config.rrule
            ? RRule.fromString(config.rrule).between(params.from, params.to, false)
            : [],
    });
    return ticks ?? [];
  }

  /**
   * Record manual fires for `occurrences`, numbered from the current
   * `tickCount`, with a compare-and-set commit that is retried when a poll
   * or another manual fire advanced the count in between. `null` when the
   * schedule doesn't exist.
   */
  private async fireManually(params: {
    scheduleId: string;
    occurrences: (config: DurableScheduleConfig) => Date[];
  }): Promise<ScheduleTick[] | null> {
    const { scheduleId } = params;
    for (let attempt = 0; attempt < MANUAL_FIRE_ATTEMPTS; attempt++) {
      const [config, state] = await Promise.all([
        this.storage.loadSchedule(scheduleId),
        this.storage.loadScheduleState(scheduleId),
      ]);
      if (!config || !state) return null;
      const occurrences = params.occurrences(config);
      if (occurrences.length === 0) return [];
      const now = this.clock.now();
      const ticks = occurrences.map(
        (scheduledAt, i): ScheduleTick => ({
          scheduleId,
          scheduleName: config.name,
          scheduledAt,
          firedAt: now,
          tickNumber: state.tickCount + i,
          metadata: config.metadata,
        }),
      );
      const { conflicts } = await this.storage.commitPoll({
        updates: [
          {
            id: scheduleId,
            firedAt: now,
            tickIncrement: ticks.length,
            expectedTickCount: state.tickCount,
            ticks,
          },
        ],
      });
      if (conflicts.length === 0) return ticks;
    }
    throw new Error(
      `Schedule "${scheduleId}": tickCount kept changing; manual fire gave up after ${MANUAL_FIRE_ATTEMPTS} attempts`,
    );
  }

  // -------------------------------------------------------------------------
  // Streamable — leader-elected polling loop
  // -------------------------------------------------------------------------

  /**
   * Poll loop: poll storage, emit that poll's ticks as the consumer pulls
   * them, commit once the consumer has pulled past the last one, then wait
   * `pollIntervalMs` on the configured `WallClock` and poll again. The first
   * poll runs as soon as the stream is pulled and its ticks are delivered
   * right away. See the class docs for the at-least-once guarantee.
   *
   * Every call builds a fresh stream. Stopping the consumer cancels the
   * pending interval timer and commits the acknowledged part of the current
   * batch. When the last running stream of this instance stops, the leader
   * lease is released so another instance takes over at its next poll
   * instead of waiting out the TTL.
   */
  stream(scheduleId?: string): Stream<ScheduleTick> {
    return Stream.suspend(() => {
      this.activeStreams++;
      // The batch that has been emitted (in part) but not committed yet.
      let open: PollBatch | undefined;

      const afterBatch = (batch: PollBatch): Eff<LoopStep> =>
        this.commitEff({ batch, acked: batch.ticks.length }).flatMap(() =>
          this.sleep(this.pollIntervalMs).map(
            (): LoopStep => [null, { kind: "poll", failures: 0 }],
          ),
        );

      const step = (state: LoopState): Eff<LoopStep> =>
        suspend((): Eff<LoopStep> => {
          if (state.kind === "emit") {
            // Being pulled again means the consumer is done with tick `index - 1`.
            state.batch.acked = state.index;
            if (state.index < state.batch.ticks.length) {
              const tick = state.batch.ticks[state.index]!;
              return succeed<LoopStep>([
                tick,
                { kind: "emit", batch: state.batch, index: state.index + 1 },
              ]);
            }
            open = undefined;
            return afterBatch(state.batch);
          }

          return this.pollEff(scheduleId).flatMap((result): Eff<LoopStep> => {
            if (!result.ok) {
              const failures = state.failures + 1;
              return this.sleep(this.errorBackoffMs(failures)).map(
                (): LoopStep => [null, { kind: "poll", failures }],
              );
            }
            const batch = result.batch;
            if (batch.ticks.length === 0) return afterBatch(batch);
            open = batch;
            return succeed<LoopStep>([batch.ticks[0]!, { kind: "emit", batch, index: 1 }]);
          });
        });

      // On stop, commit what the consumer acknowledged (the rest stays due),
      // then hand leadership over if no other stream of this instance runs.
      const finalize = suspend((): Eff<void> => {
        const batch = open;
        open = undefined;
        const commit = batch ? this.commitEff({ batch, acked: batch.acked }) : succeed(undefined);
        return commit.flatMap(() => {
          this.activeStreams--;
          return this.activeStreams === 0 ? this.releaseEff() : succeed(undefined);
        });
      });

      const initial: LoopState = { kind: "poll", failures: 0 };
      return Stream.unfoldEffect(initial, step).unNone().onFinalize(finalize);
    });
  }

  /** Release the held lease. Never fails: errors are reported. */
  private releaseEff(): Eff<void> {
    const lease = this.heldLease;
    if (!lease) return succeed(undefined);
    this.heldLease = null;
    return tryPromise(
      () => this.storage.releaseLeader({ lease }),
      (e) => e,
    ).catch((error) => {
      this.report({ phase: "release", error });
      return succeed(undefined);
    });
  }

  subscribe(_params?: { group?: string }): Stream<ScheduleTick> {
    return this.stream();
  }

  private sleep(ms: number): Eff<void> {
    return wallClockSleep({ clock: this.clock, ms });
  }

  private errorBackoffMs(failures: number): number {
    return Math.min(this.maxErrorBackoffMs, this.pollIntervalMs * 2 ** failures);
  }

  private report(event: SchedulerErrorEvent): void {
    try {
      this.onError(event);
    } catch {
      // A throwing error hook must not take the poll loop down with it.
    }
  }

  /** One poll as an Eff that never fails: errors are reported and returned. */
  private pollEff(
    scheduleId: string | undefined,
  ): Eff<{ ok: true; batch: PollBatch } | { ok: false }> {
    return tryPromise(
      async () => ({ ok: true as const, batch: await this.pollOnce(scheduleId) }),
      (e) => e,
    ).catch((error) => {
      this.report({ phase: "poll", error });
      return succeed({ ok: false as const });
    });
  }

  /**
   * Commit the schedules of `batch` whose ticks are all acknowledged
   * (`acked` = number of acknowledged ticks, in emission order), plus the
   * entries that carry no ticks. Never fails: errors are reported.
   */
  private commitEff(params: { batch: PollBatch; acked: number }): Eff<void> {
    const { batch, acked } = params;
    const ready = batch.plans.filter((p, i) => p.ticks.length === 0 || batch.tickEnds[i]! <= acked);
    if (ready.length === 0) return succeed(undefined);
    return tryPromise(
      () => commitPlannedSchedules({ storage: this.storage, plans: ready, lease: batch.lease }),
      (e) => e,
    )
      .map(() => undefined)
      .catch((error) => {
        this.report({ phase: "commit", error });
        return succeed(undefined);
      });
  }

  /** One leader-gated poll: claim due schedules and compute their ticks. Writes nothing. */
  private async pollOnce(scheduleId: string | undefined): Promise<PollBatch> {
    const lease = await this.storage.tryAcquireLeader({
      key: this.leaderKey,
      instanceId: this.instanceId,
      ttlMs: this.leaderLockTtlMs,
    });
    this.heldLease = lease;
    if (!lease) return emptyBatch();

    // Partitions filter in the shell, so leave room for the other
    // partitions' due ids.
    const fetchLimit = this.batchSize * (this.partition?.count ?? 1);
    const dueIds = await this.storage.findDue({
      now: this.clock.now(),
      limit: fetchLimit,
      namespace: this.namespace,
    });

    // Apply partitioning + scheduleId filter in the shell so storage
    // backends don't need partition awareness.
    const targetIds = dueIds
      .filter((id) => {
        if (scheduleId && id !== scheduleId) return false;
        return (
          !this.partition ||
          schedulePartition({ id, count: this.partition.count }) === this.partition.index
        );
      })
      .slice(0, this.batchSize);

    if (targetIds.length === 0) return emptyBatch();

    // Bulk load — TWO storage round-trips for ALL due schedules instead
    // of 2N. Storage backends collapse this into one IN/pipeline call.
    const [configs, states] = await Promise.all([
      this.storage.loadSchedules(targetIds),
      this.storage.loadScheduleStates(targetIds),
    ]);

    const plans = planDueTicks({
      ids: targetIds,
      configs,
      states,
      clock: this.clock,
      random: this.random,
    });
    for (const plan of plans) {
      if (plan.error !== undefined) {
        this.report({ phase: "schedule", scheduleId: plan.id, error: plan.error });
      }
    }

    const ticks: ScheduleTick[] = [];
    const tickEnds: number[] = [];
    for (const plan of plans) {
      ticks.push(...plan.ticks);
      tickEnds.push(ticks.length);
    }
    return { plans, ticks, tickEnds, acked: 0, lease };
  }
}

/** Upper bound on compare-and-set retries for `triggerNow` / `backfill`. */
const MANUAL_FIRE_ATTEMPTS = 10;

/** Ticks of one poll, in emission order, plus what to commit for them. */
interface PollBatch {
  /** The lease the poll ran under; fences the commit. */
  readonly lease?: LeaderLease;
  readonly plans: readonly PlannedSchedule[];
  readonly ticks: readonly ScheduleTick[];
  /** For each plan, the index just past its last tick in `ticks`. */
  readonly tickEnds: readonly number[];
  /** How many ticks the consumer has acknowledged. */
  acked: number;
}

type LoopState =
  | { kind: "poll"; failures: number }
  | { kind: "emit"; batch: PollBatch; index: number };

/** One unfold step: a tick (or nothing) and the next state. The loop never ends by itself. */
type LoopStep = [ScheduleTick | null, LoopState];

function emptyBatch(): PollBatch {
  return { plans: [], ticks: [], tickEnds: [], acked: 0 };
}

function defaultOnError(event: SchedulerErrorEvent): void {
  const where = event.scheduleId ? ` (schedule "${event.scheduleId}")` : "";
  console.error(`[durable-scheduler] ${event.phase} failed${where}:`, event.error);
}

// ---------------------------------------------------------------------------
// Poll planning — shared with other poll loops built on SchedulerStorage.
// ---------------------------------------------------------------------------

/** What one poll decided for one due schedule. */
export interface PlannedSchedule {
  readonly id: string;
  /** Ticks to deliver, oldest first. Empty when nothing is due. */
  readonly ticks: readonly ScheduleTick[];
  /** Fire-state update to commit once the ticks are delivered. */
  readonly commit: ScheduleCommit;
  /**
   * Set when the stored config could not be evaluated (e.g. an invalid cron
   * written straight to storage). The plan then removes the schedule from
   * due-tracking, and `commitPlannedSchedules` also disables it.
   */
  readonly error?: unknown;
}

/**
 * Decide what each due schedule fires, without any I/O. Deleted and disabled
 * schedules are dropped from due-tracking. A schedule whose config throws
 * during evaluation is isolated: it gets an `error` and `nextRun = null`
 * instead of failing the whole poll. Enabled schedules get their due ticks
 * and their next run (delayed by a random `[0, jitterMs)` when set).
 */
export function planDueTicks(params: {
  ids: readonly string[];
  configs: ReadonlyMap<string, DurableScheduleConfig>;
  states: ReadonlyMap<string, { lastFired: Date | null; tickCount: number }>;
  clock: WallClock;
  /** Randomness for `jitterMs`, returning `[0, 1)`. Default: `Math.random`. */
  random?: () => number;
}): PlannedSchedule[] {
  const random = params.random ?? Math.random;
  const plans: PlannedSchedule[] = [];
  for (const id of params.ids) {
    const config = params.configs.get(id);
    if (!config || config.enabled === false) {
      // Deleted since findDue, or disabled with a stale nextRun: drop it from due-tracking.
      plans.push({ id, ticks: [], commit: { id, nextRun: null } });
      continue;
    }
    try {
      const state = params.states.get(id) ?? { lastFired: null, tickCount: 0 };
      const due = computeDueTicks(config, state.lastFired, state.tickCount, params.clock);
      const nextRun = jitterNextRun({
        config,
        nextRun: computeNextRun(config, params.clock),
        random,
      });
      const last = due[due.length - 1];
      plans.push({
        id,
        ticks: due,
        commit: {
          id,
          firedAt: last?.firedAt,
          tickIncrement: due.length > 0 ? due.length : undefined,
          // The ticks are numbered from this count; don't commit them over
          // a fire that took those numbers in the meantime.
          expectedTickCount: due.length > 0 ? state.tickCount : undefined,
          nextRun,
          ticks: due.length > 0 ? due : undefined,
        },
      });
    } catch (error) {
      plans.push({ id, ticks: [], commit: { id, nextRun: null }, error });
    }
  }
  return plans;
}

/**
 * Commit planned schedules in one `commitPoll`, then disable the ones whose
 * config could not be evaluated so they show up as paused. Pass the lease
 * the poll ran under: the commit is then rejected with `StaleLeaseError`
 * (writing nothing) when leadership has moved on since. Schedules whose
 * `tickCount` changed since planning come back in `conflicts`, uncommitted;
 * they stay due and the next poll plans them afresh.
 */
export async function commitPlannedSchedules(params: {
  storage: SchedulerStorage;
  plans: readonly PlannedSchedule[];
  lease?: LeaderLease;
}): Promise<CommitPollResult> {
  if (params.plans.length === 0) return { conflicts: [] };
  const result = await params.storage.commitPoll({
    updates: params.plans.map((p) => p.commit),
    lease: params.lease,
  });
  for (const plan of params.plans) {
    if (plan.error !== undefined) await params.storage.setEnabled(plan.id, false);
  }
  return result;
}

function jitterNextRun(params: {
  config: DurableScheduleConfig;
  nextRun: Date | null;
  random: () => number;
}): Date | null {
  const { config, nextRun } = params;
  if (!nextRun) return null;
  const delay = jitterDelayMs({ config, random: params.random });
  if (delay === 0) return nextRun;
  const jittered = nextRun.getTime() + delay;
  // Never push the last occurrence past endAt, where it would no longer fire.
  const capped = config.endAt ? Math.min(jittered, config.endAt.getTime()) : jittered;
  return new Date(Math.max(capped, nextRun.getTime()));
}

// ---------------------------------------------------------------------------
// Pure helpers — no storage access, easy to unit-test.
// ---------------------------------------------------------------------------

/**
 * Compute the ticks that should fire NOW for a given schedule and its
 * lastFired state. No I/O — "now" and each tick's `firedAt` come from
 * `clock` (default: `SystemWallClock`). Caller persists the results.
 *
 * With no `lastFired` (never fired), one tick fires at "now". Otherwise the
 * missed occurrences after `lastFired` up to "now" are due, of which the
 * newest `max(1, maxCatchUp)` fire, oldest first.
 */
export function computeDueTicks(
  config: DurableScheduleConfig,
  lastFired: Date | null,
  tickCount: number,
  clock: WallClock = SystemWallClock,
): ScheduleTick[] {
  const now = clock.now();
  if (config.startAt && now < config.startAt) return [];
  if (config.endAt && now > config.endAt) return [];

  if (!lastFired) {
    // First-fire bootstrap: a never-fired schedule fires once immediately.
    return [makeTick({ config, scheduledAt: now, tickNumber: tickCount, clock })];
  }

  const limit = Math.max(1, config.maxCatchUp ?? 0);
  const params = { config, now, lastFired, limit };
  const occurrences = config.cron
    ? missedCronOccurrences(params)
    : config.rrule
      ? missedRruleOccurrences(params)
      : config.intervalMs !== undefined
        ? missedIntervalOccurrences(params)
        : [];
  return occurrences.map((scheduledAt, i) =>
    makeTick({ config, scheduledAt, tickNumber: tickCount + i, clock }),
  );
}

interface MissedParams {
  config: DurableScheduleConfig;
  now: Date;
  lastFired: Date;
  /** How many of the newest missed occurrences to return. */
  limit: number;
}

/** Newest `limit` cron occurrences in `(lastFired, now]`, oldest first. */
function missedCronOccurrences(params: MissedParams): Date[] {
  const { config, now, lastFired, limit } = params;
  const cron = new Cron(config.cron!, { timezone: config.timezone ?? "UTC" });
  // `previousRuns` looks strictly before its reference at second precision,
  // so look from a second past "now" and drop anything after "now".
  return cron
    .previousRuns(limit + 2, new Date(now.getTime() + 1000))
    .filter((d) => d.getTime() > lastFired.getTime() && d.getTime() <= now.getTime())
    .slice(0, limit)
    .reverse();
}

/** Newest `limit` RRULE occurrences in `(lastFired, now]`, oldest first. */
function missedRruleOccurrences(params: MissedParams): Date[] {
  const { config, now, lastFired, limit } = params;
  const rule = RRule.fromString(config.rrule!);
  if (limit === 1) {
    const latest = rule.before(now, true);
    return latest && latest.getTime() > lastFired.getTime() ? [latest] : [];
  }
  return rule
    .between(lastFired, now, true)
    .filter((d) => d.getTime() > lastFired.getTime())
    .slice(-limit);
}

/** Newest `limit` interval occurrences in `(lastFired, now]`, oldest first. */
function missedIntervalOccurrences(params: MissedParams): Date[] {
  const { config, now, lastFired, limit } = params;
  const intervalMs = config.intervalMs!;
  const missed = Math.floor((now.getTime() - lastFired.getTime()) / intervalMs);
  if (missed < 1) return [];
  const count = Math.min(limit, missed);
  const out: Date[] = [];
  for (let k = missed - count + 1; k <= missed; k++) {
    out.push(new Date(lastFired.getTime() + k * intervalMs));
  }
  return out;
}

function makeTick(params: {
  config: DurableScheduleConfig;
  scheduledAt: Date;
  tickNumber: number;
  clock: WallClock;
}): ScheduleTick {
  const { config, scheduledAt, tickNumber, clock } = params;
  return {
    scheduleId: config.id,
    scheduleName: config.name,
    scheduledAt,
    firedAt: clock.now(),
    tickNumber,
    metadata: config.metadata,
  };
}

/**
 * Compute the next time a schedule will fire — used to update the due index.
 * "Now" comes from `clock` (default: `SystemWallClock`).
 */
export function computeNextRun(
  config: DurableScheduleConfig,
  clock: WallClock = SystemWallClock,
): Date | null {
  const now = clock.now();
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

/**
 * The partition (`0 … count-1`) a schedule id belongs to: a stable 32-bit
 * string hash mod `count`. Every poll loop that partitions schedules must
 * use this so their partitions line up.
 */
export function schedulePartition(params: { id: string; count: number }): number {
  const { id, count } = params;
  let h = 0;
  for (let i = 0; i < id.length; i++) {
    h = (h << 5) - h + id.charCodeAt(i);
    h |= 0;
  }
  return Math.abs(h) % count;
}

/**
 * Next `count` fire times of `config` after `now`, inside `[startAt, endAt)`.
 * Interval schedules continue from `lastFired`, or start at the later of
 * `now` and `startAt` when they have never fired.
 */
function previewFireTimes(params: {
  config: DurableScheduleConfig;
  lastFired: Date | null;
  count: number;
  now: Date;
}): Date[] {
  const { config, count } = params;
  const nowMs = params.now.getTime();
  const startMs = config.startAt?.getTime() ?? Number.NEGATIVE_INFINITY;
  const endMs = config.endAt?.getTime() ?? Number.POSITIVE_INFINITY;
  // Occurrences strictly after `fromMs` (so one exactly at `startAt` counts).
  const fromMs = Math.max(nowMs, startMs - 1);
  const times: Date[] = [];
  const push = (t: Date | null | undefined): boolean => {
    if (!t || t.getTime() >= endMs || times.length >= count) return false;
    times.push(t);
    return times.length < count;
  };
  if (count <= 0 || fromMs >= endMs) return times;

  if (config.cron) {
    const cron = new Cron(config.cron, { timezone: config.timezone ?? "UTC" });
    let cursor = new Date(fromMs);
    for (;;) {
      const next = cron.nextRun(cursor);
      if (!push(next)) break;
      cursor = new Date(next!.getTime() + 1);
    }
  } else if (config.rrule) {
    const rule = RRule.fromString(config.rrule);
    let cursor = new Date(fromMs);
    for (;;) {
      const next = rule.after(cursor, false);
      if (!push(next)) break;
      cursor = next!;
    }
  } else if (config.intervalMs !== undefined && config.intervalMs > 0) {
    const interval = config.intervalMs;
    let next: number;
    if (params.lastFired) {
      const last = params.lastFired.getTime();
      next = last + interval * (Math.floor(Math.max(0, fromMs - last) / interval) + 1);
    } else {
      next = Math.max(nowMs, startMs);
    }
    while (push(new Date(next))) next += interval;
  }
  return times;
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
