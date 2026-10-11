// ---------------------------------------------------------------------------
// SchedulerLoop — embedded poll loop that runs the DurableScheduler tick
// cycle inside ZoryaServer.
//
// Why embedded vs `scheduler.stream().forEach(...)`:
//   - Delivery. DurableScheduler acknowledges a tick when the consumer pulls
//     the next one, whatever the consumer did with it. This loop commits a
//     schedule only after every one of its ticks was dispatched, so a tick
//     whose dispatch failed stays due and is delivered again.
//   - Tenancy. `namespaces: "all"` polls every namespace with one
//     `findDueAcross` and a lease per active namespace; DurableScheduler
//     polls one namespace.
//   - Lifecycle. A while-loop with a flag stops cleanly on `server.stop()`;
//     draining the stream only stops via fiber interruption.
//
// The leader lease + tick computation use the same primitives the standalone
// DurableScheduler does — `tryAcquireLeader`, `findDue`, `loadSchedules`,
// `loadScheduleStates`, `planDueTicks`, `commitPlannedSchedules`.
// Wire-compatible with every SchedulerStorage backend (Postgres, Redis,
// in-memory, SQLite).
//
// Delivery is at least once: each poll dispatches its ticks first and
// commits the fire state of the schedules whose ticks all dispatched. A
// failed dispatch is reported (`phase: "dispatch"`), its schedule stays due
// and is retried with backoff; a crash or a failed commit re-fires the same
// ticks (same tickNumber) on the next poll rather than losing them. A stored
// schedule that can't be evaluated is reported, disabled and skipped without
// failing the poll for the others.
//
// Dispatch STARTS a run and returns: the trigger creates the run row and
// hands it to a driver; an agent tick starts the turn without awaiting it.
// The deterministic `${scheduleId}.${tickNumber}` run id makes a redelivered
// tick a no-op — the workflow layer answers an existing id from storage
// instead of running it again. Each dispatch has a deadline, and the lease
// is renewed while a poll dispatches, so a slow poll neither stalls the
// loop forever nor lets a second instance fire the same ticks meanwhile.
// ---------------------------------------------------------------------------

import type {
  LeaderLease,
  SchedulerStorage,
  ScheduleTick,
  DurableScheduleConfig,
  SchedulerErrorPhase,
  PlannedSchedule,
} from "@promin/workflow/scheduler";
import type { TimerHandle, WallClock } from "@promin/workflow";
import { SystemWallClock } from "@promin/workflow";
import {
  commitPlannedSchedules,
  computeNextRun,
  planDueTicks,
  schedulePartition,
  scheduleTickRunId,
  schedulerLeaderKey,
} from "@promin/workflow/scheduler";
import type { RunTrigger } from "../routes/runs.ts";

/** Where a `SchedulerLoop` error happened. */
export type SchedulerLoopErrorPhase =
  | SchedulerErrorPhase
  /**
   * A tick's dispatch threw or missed its deadline. Its schedule is left
   * uncommitted (still due) and retried with backoff.
   */
  | "dispatch"
  /** Renewing the leader lease mid-poll failed. */
  | "lease"
  /** A started agent turn failed after its dispatch returned. */
  | "turn";

/** Passed to `SchedulerLoopConfig.onError`. */
export interface SchedulerLoopErrorEvent {
  readonly phase: SchedulerLoopErrorPhase;
  readonly error: unknown;
  readonly scheduleId?: string;
  /** Set for `"dispatch"` and `"turn"` errors. */
  readonly tickNumber?: number;
}

/** A dispatch did not return within `dispatchTimeoutMs`. */
export class ScheduleDispatchTimeoutError extends Error {
  override readonly name = "ScheduleDispatchTimeoutError";
  constructor(params: { scheduleId: string; tickNumber: number; timeoutMs: number }) {
    super(
      `Dispatch of tick ${params.tickNumber} of schedule "${params.scheduleId}" ` +
        `did not return within ${params.timeoutMs} ms`,
    );
  }
}

/**
 * `fireOnce` recorded the tick but could not start its run. The tick keeps
 * its number; emit again to fire a new one.
 */
export class ScheduleDispatchError extends Error {
  override readonly name = "ScheduleDispatchError";
  readonly tick: ScheduleTick;
  constructor(params: { tick: ScheduleTick; cause: unknown }) {
    const reason = params.cause instanceof Error ? params.cause.message : String(params.cause);
    super(
      `Tick ${params.tick.tickNumber} of schedule "${params.tick.scheduleId}" was recorded but its dispatch failed: ${reason}`,
      { cause: params.cause },
    );
    this.tick = params.tick;
  }
}

export interface SchedulerLoopConfig {
  storage: SchedulerStorage;
  /**
   * Server-side trigger fn — the same one wired into
   * `/api/runs/trigger/:name`. Defaults to using `metadata.workflowName` +
   * `metadata.input` from the schedule config; override `fire` for full
   * control. It must start the run and return, not wait for it to finish.
   */
  trigger?: RunTrigger;
  /**
   * Optional dispatch hook. Inspect the tick / schedule and either
   * handle it (return `{ handled: true }`, or nothing) or pass — return
   * `{ handled: false }` and the loop runs its default `trigger`-based
   * dispatch using `metadata.workflowName`.
   *
   * Resolve once the tick's work has STARTED (a run created, a turn
   * kicked off); throw when it could not start, and the tick is delivered
   * again. The hook may see a tick more than once and must dedupe by
   * `scheduleTickRunId(tick)`.
   */
  fire?: (
    tick: ScheduleTick,
    schedule: DurableScheduleConfig,
  ) => Promise<{ handled: boolean } | void>;
  /** Stable id used for leader election. Default: random UUID. */
  instanceId?: string;
  /** Poll interval in ms. Default: 1000. */
  pollIntervalMs?: number;
  /**
   * Leader-lease TTL. Default: 3 × pollIntervalMs. A poll renews the lease
   * every third of it while it dispatches.
   */
  leaderLockTtlMs?: number;
  /**
   * Deadline for one tick's dispatch, in ms. A dispatch that misses it is
   * reported and its schedule stays due. Default: 30 000.
   */
  dispatchTimeoutMs?: number;
  /**
   * Cap on the backoff before a schedule whose dispatch failed is tried
   * again. Consecutive failures back off from `2 × pollIntervalMs`,
   * doubling. Default: 60 000.
   */
  maxDispatchBackoffMs?: number;
  /**
   * Scope this loop to a single namespace. Default: undefined (poll the
   * GLOBAL namespace only — schedules with no `namespace` field set).
   *
   * Mutually exclusive with `namespaces`. To poll across more than one
   * namespace from the same Zorya instance, set `namespaces` instead.
   */
  namespace?: string;
  /**
   * Multi-namespace mode. Each namespace gets its own leader lock, and
   * the loop runs per-namespace ticks in parallel within a single poll
   * cycle. Lets one Zorya cover every tenant without creating a single
   * global contention point.
   *
   * - `"all"` — call `storage.listNamespaces()` on every poll to discover
   *   tenants dynamically. New namespaces appear without a config change.
   * - `string[]` — explicit list (use `""` or `undefined` element to
   *   include the global namespace). Avoids the `listNamespaces` round
   *   trip when you already know the set.
   *
   * Mutually exclusive with `namespace`.
   */
  namespaces?: "all" | readonly (string | undefined)[];
  /** Max schedules per poll. Default: 100. */
  batchSize?: number;
  /**
   * Max concurrent dispatches per tick. Equivalent to
   * `scheduler.stream().pipe(parMapAsync(N))` from the standalone
   * scheduler — caps fan-out so a 100-schedule wakeup doesn't slam the
   * trigger / coordinator with 100 simultaneous calls. Default: 10.
   */
  dispatchConcurrency?: number;
  /**
   * Hash partitioning so multiple Zorya instances can share work across
   * different schedule subsets. Each (namespace, partition) pair has its own
   * leader lease, so the partitions fire in parallel. Run every partition:
   * see `DurableSchedulerConfig.partition`. Default: undefined (single
   * partition).
   */
  partition?: { index: number; count: number };
  /**
   * Time source. Drives `findDue`'s "now", which ticks are due and their
   * `firedAt`, the next-run computation, `fireOnce` stamps, the wait
   * between polls, dispatch deadlines, backoff and lease renewal. Default:
   * `SystemWallClock`. Tests pass a `FakeWallClock`.
   */
  clock?: WallClock;
  /**
   * Called when a poll, dispatch or commit fails, or a stored schedule
   * can't be evaluated (it is then disabled). The loop keeps running
   * either way. Default: logs with `console.error`.
   */
  onError?: (event: SchedulerLoopErrorEvent) => void;
}

export class SchedulerLoop {
  readonly instanceId: string;
  private readonly storage: SchedulerStorage;
  private readonly trigger?: RunTrigger;
  private readonly fireOverride?: SchedulerLoopConfig["fire"];
  private readonly pollIntervalMs: number;
  private readonly leaderLockTtlMs: number;
  private readonly dispatchTimeoutMs: number;
  private readonly maxDispatchBackoffMs: number;
  private readonly namespace?: string;
  private readonly namespacesMode?: "all" | readonly (string | undefined)[];
  private readonly batchSize: number;
  private readonly dispatchConcurrency: number;
  private readonly partition?: { index: number; count: number };
  private readonly clock: WallClock;
  private readonly onError: (event: SchedulerLoopErrorEvent) => void;
  private running = false;
  private loopPromise?: Promise<void>;
  /** Leases this loop currently holds, by lease key; released on `stop()`. */
  private readonly leases = new Map<string, LeaderLease>();
  /** Schedules whose last dispatch failed: not retried before `retryAtMs`. */
  private readonly dispatchBackoff = new Map<string, { failures: number; retryAtMs: number }>();
  /** Cuts the current between-poll wait short; set only while waiting. */
  private wakeUp?: () => void;

  constructor(config: SchedulerLoopConfig) {
    if (config.namespace !== undefined && config.namespaces !== undefined) {
      throw new Error("SchedulerLoop: pass either `namespace` or `namespaces`, not both");
    }
    this.storage = config.storage;
    this.trigger = config.trigger;
    this.fireOverride = config.fire;
    this.instanceId = config.instanceId ?? crypto.randomUUID();
    this.pollIntervalMs = config.pollIntervalMs ?? 1000;
    this.leaderLockTtlMs = config.leaderLockTtlMs ?? this.pollIntervalMs * 3;
    this.dispatchTimeoutMs = config.dispatchTimeoutMs ?? 30_000;
    this.maxDispatchBackoffMs = config.maxDispatchBackoffMs ?? 60_000;
    this.namespace = config.namespace;
    this.namespacesMode = config.namespaces;
    this.batchSize = config.batchSize ?? 100;
    this.dispatchConcurrency = config.dispatchConcurrency ?? 10;
    this.clock = config.clock ?? SystemWallClock;
    this.onError = config.onError ?? logSchedulerLoopError;
    if (config.partition) {
      const { index, count } = config.partition;
      if (count < 1 || index < 0 || index >= count) {
        throw new Error(`SchedulerLoop: invalid partition index=${index} count=${count}`);
      }
      this.partition = config.partition;
    }
  }

  start(): void {
    if (this.running) return;
    this.running = true;
    this.loopPromise = this.run();
  }

  async stop(): Promise<void> {
    this.running = false;
    // Cancel the pending poll wait so stop() doesn't sit out the interval
    // and no timer is left behind on the clock.
    this.wakeUp?.();
    if (this.loopPromise) await this.loopPromise.catch(() => {});
    this.loopPromise = undefined;
    // Hand leadership over now rather than after the TTL.
    const held = [...this.leases.values()];
    this.leases.clear();
    await Promise.all(
      held.map((lease) =>
        this.storage.releaseLeader({ lease }).catch((error: unknown) => {
          this.report({ phase: "release", error });
        }),
      ),
    );
  }

  /**
   * Drive one tick of the loop and return the ticks it dispatched (across
   * every namespace handled this cycle). Exposed for tests so they don't
   * have to wait on real timers.
   *
   * Single-namespace mode (`namespace?: string`, default global) keeps
   * the original two-step contract: acquireLeader → findDue → process.
   *
   * Multi-namespace mode (`namespaces: "all" | string[]`) collapses idle
   * tenants to zero RPCs: one cross-namespace `findDueAcross` returns
   * only the namespaces with work, and the per-namespace leader lock +
   * commit only runs for those. 1000 tenants with 5 active ticks per
   * cycle costs O(active_namespaces) RPCs, not O(total_namespaces).
   */
  async tickOnce(): Promise<ScheduleTick[]> {
    if (this.namespacesMode !== undefined) {
      return await this.tickAcrossNamespaces();
    }
    return await this.tickSingleNamespace(this.namespace);
  }

  /**
   * Fire a schedule out-of-band, right now. Used by the dashboard's
   * "emit now" button to bypass `computeDueTicks`'s interval guard
   * (which would otherwise refuse to fire while `lastFired + intervalMs`
   * is still in the future).
   *
   * Same atomic commit shape as the poll loop — `tickCount` advances,
   * `lastFired` becomes `now`, the tick is logged, and `nextRun` slides
   * to `now + interval` so the natural cadence resumes from the manual
   * fire. Dispatch goes through the configured trigger / fire override
   * exactly like a normal poll-driven fire.
   *
   * Returns the synthesized tick on success, or `null` if the schedule
   * doesn't exist or is disabled. Throws `ScheduleDispatchError` when the
   * tick was recorded but its run could not be started.
   */
  async fireOnce(scheduleId: string): Promise<ScheduleTick | null> {
    // The tick takes the next number with a compare-and-set on tickCount,
    // retried if a poll or another manual fire got there first, so it never
    // shares a tickNumber (and so a run id) with another fire.
    let fired: { tick: ScheduleTick; config: DurableScheduleConfig } | undefined;
    for (let attempt = 0; attempt < FIRE_ONCE_ATTEMPTS && !fired; attempt++) {
      const [config, state] = await Promise.all([
        this.storage.loadSchedule(scheduleId),
        this.storage.loadScheduleState(scheduleId),
      ]);
      if (!config || !state || config.enabled === false) return null;
      const now = this.clock.now();
      const tick: ScheduleTick = {
        scheduleId,
        scheduleName: config.name,
        scheduledAt: now,
        firedAt: now,
        tickNumber: state.tickCount,
        metadata: config.metadata,
      };
      const { conflicts } = await this.storage.commitPoll({
        updates: [
          {
            id: scheduleId,
            firedAt: now,
            tickIncrement: 1,
            expectedTickCount: state.tickCount,
            nextRun: computeNextRun({ config, clock: this.clock }),
            ticks: [tick],
          },
        ],
      });
      if (conflicts.length === 0) fired = { tick, config };
    }
    if (!fired) {
      throw new Error(
        `SchedulerLoop: schedule "${scheduleId}" kept changing; fireOnce gave up after ${FIRE_ONCE_ATTEMPTS} attempts`,
      );
    }
    const { tick, config } = fired;

    // The number is taken before dispatch so it can't collide with a poll's
    // tick; a failed dispatch is the caller's to see.
    try {
      await this.dispatchWithDeadline({ tick, schedule: config });
    } catch (error) {
      this.report({ phase: "dispatch", scheduleId, tickNumber: tick.tickNumber, error });
      throw new ScheduleDispatchError({ tick, cause: error });
    }
    return tick;
  }

  /** Acquire or refresh the lease for one namespace (and this loop's partition). */
  private async acquireLease(namespace: string | undefined): Promise<LeaderLease | null> {
    const key = schedulerLeaderKey({ namespace, partition: this.partition });
    const lease = await this.storage.tryAcquireLeader({
      key,
      instanceId: this.instanceId,
      ttlMs: this.leaderLockTtlMs,
    });
    if (lease) this.leases.set(key, lease);
    else this.leases.delete(key);
    return lease;
  }

  /** Partitions filter after `findDue`, so leave room for the other partitions' due ids. */
  private get fetchLimit(): number {
    return this.batchSize * (this.partition?.count ?? 1);
  }

  private inPartition(id: string): boolean {
    return (
      !this.partition ||
      schedulePartition({ id, count: this.partition.count }) === this.partition.index
    );
  }

  private async tickSingleNamespace(namespace: string | undefined): Promise<ScheduleTick[]> {
    const lease = await this.acquireLease(namespace);
    if (!lease) return [];

    const dueIds = await this.storage.findDue({
      now: this.clock.now(),
      limit: this.fetchLimit,
      namespace,
    });
    const targetIds = dueIds.filter((id) => this.inPartition(id)).slice(0, this.batchSize);
    return await this.processDueIds({ dueIds: targetIds, lease });
  }

  private async tickAcrossNamespaces(): Promise<ScheduleTick[]> {
    const filter = this.namespacesMode === "all" ? undefined : this.namespacesMode;
    // ONE call returns every due row + its namespace, regardless of
    // whether 0 or 10000 tenants are configured. Empty namespaces never
    // appear here so they cost nothing.
    const due = await this.storage.findDueAcross({
      now: this.clock.now(),
      limit: this.fetchLimit,
      namespaces: filter,
    });
    if (due.length === 0) return [];

    // Group by namespace so each tenant's leader lock + commit happens
    // independently. A slow / contested namespace can't block the others.
    const byNamespace = new Map<string | undefined, string[]>();
    for (const row of due.filter((r) => this.inPartition(r.id)).slice(0, this.batchSize)) {
      const list = byNamespace.get(row.namespace) ?? [];
      list.push(row.id);
      byNamespace.set(row.namespace, list);
    }
    if (byNamespace.size === 0) return [];

    // Run per-namespace processing in parallel. tryAcquireLeader is
    // per-namespace, so one Zorya can be leader for many namespaces at
    // once without coupling them.
    const perNamespace = await Promise.all(
      [...byNamespace.entries()].map(async ([namespace, ids]) => {
        const lease = await this.acquireLease(namespace);
        if (!lease) return [] as ScheduleTick[];
        return await this.processDueIds({ dueIds: ids, lease });
      }),
    );
    return perNamespace.flat();
  }

  /**
   * Shared post-findDue path (ids already partition-filtered): load
   * configs/states, compute ticks, dispatch, then commit under `lease` the
   * schedules whose ticks all dispatched.
   */
  private async processDueIds(params: {
    dueIds: readonly string[];
    lease: LeaderLease;
  }): Promise<ScheduleTick[]> {
    const targetIds = [...params.dueIds];
    if (targetIds.length === 0) return [];

    const [configs, states] = await Promise.all([
      this.storage.loadSchedules(targetIds),
      this.storage.loadScheduleStates(targetIds),
    ]);

    // Pure planning: due ticks + the fire-state update for each schedule.
    // A schedule whose config throws is isolated (error + nextRun null).
    // `commitPoll` gets the individual ticks so backends with a tick log
    // persist them in the SAME transaction as the state advance.
    const plans = planDueTicks({ ids: targetIds, configs, states, clock: this.clock });
    const nowMs = this.clock.currentTimeMs();
    const deliver: PlannedSchedule[] = [];
    for (const plan of plans) {
      if (plan.error !== undefined) {
        this.report({ phase: "schedule", scheduleId: plan.id, error: plan.error });
      }
      if (plan.ticks.length === 0) {
        this.dispatchBackoff.delete(plan.id);
        deliver.push(plan);
        continue;
      }
      // A schedule backing off after a failed dispatch stays due, untouched.
      const backoff = this.dispatchBackoff.get(plan.id);
      if (backoff && backoff.retryAtMs > nowMs) continue;
      deliver.push(plan);
    }
    const ticks = deliver.flatMap((p) => p.ticks);

    // Bounded-parallel dispatch — same shape as
    // `scheduler.stream().parMapAsync(dispatchConcurrency)`. A pool of N
    // workers each pulls the next tick off a shared cursor until the
    // batch is drained. Failures are isolated per schedule: a failed
    // schedule's remaining ticks are skipped and it is not committed.
    // The lease is renewed meanwhile; once it is lost, nothing more is
    // dispatched and the new leader redelivers the rest.
    const keeper = ticks.length > 0 ? this.keepLease(params.lease) : undefined;
    const failed = new Map<string, { tickNumber: number; error: unknown }>();
    const skipped = new Set<string>();
    const dispatched: ScheduleTick[] = [];
    let cursor = 0;
    const workerCount = Math.max(1, Math.min(this.dispatchConcurrency, ticks.length));
    try {
      await Promise.all(
        Array.from({ length: workerCount }, async () => {
          while (cursor < ticks.length) {
            const tick = ticks[cursor++]!;
            const config = configs.get(tick.scheduleId);
            if (!config || failed.has(tick.scheduleId)) continue;
            if (keeper?.lost()) {
              skipped.add(tick.scheduleId);
              continue;
            }
            try {
              await this.dispatchWithDeadline({ tick, schedule: config });
              dispatched.push(tick);
            } catch (error) {
              if (!failed.has(tick.scheduleId)) {
                failed.set(tick.scheduleId, { tickNumber: tick.tickNumber, error });
              }
            }
          }
        }),
      );
    } finally {
      keeper?.stop();
    }

    for (const [scheduleId, { tickNumber, error }] of failed) {
      this.report({ phase: "dispatch", scheduleId, tickNumber, error });
      this.backOff(scheduleId);
    }

    // Commit after dispatch, and only schedules whose ticks all started:
    // a failed or skipped one stays due and is redelivered under the same
    // tickNumber, and the deterministic run ids make the repeat of the
    // ticks that did start a no-op. The lease fences the commit: if
    // leadership moved on during dispatch it is rejected and the new
    // leader redelivers.
    const commit = deliver.filter((p) => !failed.has(p.id) && !skipped.has(p.id));
    for (const plan of commit) {
      if (plan.ticks.length > 0) this.dispatchBackoff.delete(plan.id);
    }
    try {
      await commitPlannedSchedules({ storage: this.storage, plans: commit, lease: params.lease });
    } catch (error) {
      this.report({ phase: "commit", error });
    }

    return dispatched;
  }

  /**
   * Renew `lease` every third of its TTL until `stop()`. `lost()` turns
   * true once a renewal finds the lease gone or re-issued (another epoch).
   */
  private keepLease(lease: LeaderLease): { lost(): boolean; stop(): void } {
    let lost = false;
    let renewing = false;
    const handle: TimerHandle = this.clock.setInterval(
      () => {
        if (lost || renewing) return;
        renewing = true;
        this.storage
          .tryAcquireLeader({
            key: lease.key,
            instanceId: this.instanceId,
            ttlMs: this.leaderLockTtlMs,
          })
          .then(
            (renewed) => {
              if (renewed) this.leases.set(lease.key, renewed);
              else this.leases.delete(lease.key);
              if (!renewed || renewed.epoch !== lease.epoch) {
                lost = true;
                handle.clear();
              }
            },
            (error: unknown) => this.report({ phase: "lease", error }),
          )
          .finally(() => {
            renewing = false;
          });
      },
      Math.max(1, Math.floor(this.leaderLockTtlMs / 3)),
    );
    return { lost: () => lost, stop: () => handle.clear() };
  }

  private backOff(scheduleId: string): void {
    const failures = (this.dispatchBackoff.get(scheduleId)?.failures ?? 0) + 1;
    const delayMs = Math.min(this.maxDispatchBackoffMs, this.pollIntervalMs * 2 ** failures);
    this.dispatchBackoff.set(scheduleId, {
      failures,
      retryAtMs: this.clock.currentTimeMs() + delayMs,
    });
  }

  private report(event: SchedulerLoopErrorEvent): void {
    try {
      this.onError(event);
    } catch {
      // A throwing error hook must not take the loop down with it.
    }
  }

  /** `dispatch`, rejected with `ScheduleDispatchTimeoutError` past the deadline. */
  private async dispatchWithDeadline(params: {
    tick: ScheduleTick;
    schedule: DurableScheduleConfig;
  }): Promise<void> {
    const { tick } = params;
    const work = this.dispatch(params);
    // A dispatch that loses the race still settles later; never unhandled.
    work.catch(() => {});
    let timer: TimerHandle | undefined;
    const deadline = new Promise<never>((_, reject) => {
      timer = this.clock.setTimeout(
        () =>
          reject(
            new ScheduleDispatchTimeoutError({
              scheduleId: tick.scheduleId,
              tickNumber: tick.tickNumber,
              timeoutMs: this.dispatchTimeoutMs,
            }),
          ),
        this.dispatchTimeoutMs,
      );
    });
    try {
      await Promise.race([work, deadline]);
    } finally {
      timer?.clear();
    }
  }

  private async dispatch(params: {
    tick: ScheduleTick;
    schedule: DurableScheduleConfig;
  }): Promise<void> {
    const { tick, schedule } = params;
    if (this.fireOverride) {
      const result = await this.fireOverride(tick, schedule);
      // `void` keeps the "fire fully replaces dispatch" contract.
      // `{ handled: true }` is the explicit form. `{ handled: false }`
      // means the override didn't claim this tick — fall through to
      // the default trigger path.
      const handled = result === undefined ? true : result.handled;
      if (handled) return;
    }
    if (!this.trigger) {
      throw new Error(
        `SchedulerLoop: no \`trigger\` or \`fire\` configured — tick for "${tick.scheduleId}" can't be dispatched`,
      );
    }
    const meta = (schedule.metadata ?? {}) as {
      workflowName?: string;
      input?: unknown;
      workflowType?: string;
      version?: string;
      namespace?: string;
    };
    const workflowName = meta.workflowName;
    if (!workflowName) {
      throw new Error(
        `SchedulerLoop: schedule "${tick.scheduleId}" has no metadata.workflowName — set one or supply a custom \`fire\` callback`,
      );
    }
    // Deterministic workflowId: a redelivered tick (failed commit, lease
    // handover) names the run it already created, and the workflow layer
    // answers an existing id from storage instead of running it again.
    // Same id shape used by `dispatchAgentSchedule` for agent-targeted ticks.
    const workflowId = scheduleTickRunId({
      scheduleId: tick.scheduleId,
      tickNumber: tick.tickNumber,
    });
    await this.trigger(workflowName, meta.input, {
      workflowId,
      namespace: meta.namespace ?? schedule.namespace,
      workflowType: meta.workflowType,
      version: meta.version,
      // First-class link back to the schedule. Survives across replays /
      // retries / runner-internal createWorkflow paths because it's a
      // typed column, not a metadata key. Metadata stays for the timestamp
      // breakdown — `scheduledAt` vs `firedAt` is still useful info.
      runSource: "schedule",
      runSourceId: tick.scheduleId,
      metadata: {
        ...schedule.metadata,
        scheduleId: tick.scheduleId,
        scheduleTick: tick.tickNumber,
        scheduledAt: tick.scheduledAt.toISOString(),
        firedAt: tick.firedAt.toISOString(),
      },
    });
  }

  private async run(): Promise<void> {
    while (this.running) {
      try {
        await this.tickOnce();
      } catch (error) {
        // Loop-level errors (RPC blip, transient lock failure) should not
        // kill the loop; report them and poll again next interval.
        this.report({ phase: "poll", error });
      }
      if (!this.running) break;
      await this.waitForNextPoll();
    }
  }

  /** Wait `pollIntervalMs` on the clock; `stop()` cuts the wait short. */
  private waitForNextPoll(): Promise<void> {
    return new Promise<void>((resolve) => {
      const handle = this.clock.setTimeout(() => {
        this.wakeUp = undefined;
        resolve();
      }, this.pollIntervalMs);
      this.wakeUp = () => {
        this.wakeUp = undefined;
        handle.clear();
        resolve();
      };
    });
  }
}

/** The default `onError`: log the event with `console.error`. */
export function logSchedulerLoopError(event: SchedulerLoopErrorEvent): void {
  const where =
    event.scheduleId !== undefined
      ? ` (schedule "${event.scheduleId}"${event.tickNumber !== undefined ? ` tick ${event.tickNumber}` : ""})`
      : "";
  console.error(`[scheduler-loop] ${event.phase} failed${where}:`, event.error);
}

/** Upper bound on compare-and-set retries for `fireOnce`. */
const FIRE_ONCE_ATTEMPTS = 10;
