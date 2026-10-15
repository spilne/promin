// ---------------------------------------------------------------------------
// SchedulerLoop — embedded poll loop that runs the DurableScheduler tick
// cycle inside ZoryaServer.
//
// Why embedded vs `scheduler.stream().forEach(...)`:
//   - Lifecycle. A while-loop with a flag stops cleanly on `server.stop()`;
//     draining the stream only stops via fiber interruption.
//   - Tests. Driving one tick at a time via `tickOnce()` lets specs avoid
//     real timers.
//
// The leader-lock + tick computation use the same primitives the standalone
// DurableScheduler does — `tryAcquireLeader`, `findDue`, `loadSchedules`,
// `loadScheduleStates`, `planDueTicks`, `commitPlannedSchedules`.
// Wire-compatible with every SchedulerStorage backend (Postgres, Redis,
// in-memory).
//
// Delivery is at least once, like DurableScheduler: each poll dispatches
// its ticks first and commits the fire state after, so a crash or a failed
// commit re-fires the same ticks (same tickNumber) on the next poll rather
// than losing them. A stored schedule that can't be evaluated is reported,
// disabled and skipped without failing the poll for the others.
//
// Dispatch routes through `RunTrigger`, which is whichever trigger the
// server is configured with — `CoordinatedTriggerService` under
// `coordination: { enabled: true }`, the workflow-start `TriggerService`
// otherwise. The deterministic `${scheduleId}.${tickNumber}` workflowId
// gives belt-and-suspenders idempotency: if a brief leader-transition
// causes two instances to fire the same tick, the second `createWorkflow`
// is a no-op.
// ---------------------------------------------------------------------------

import type {
  LeaderLease,
  SchedulerStorage,
  ScheduleTick,
  DurableScheduleConfig,
  SchedulerErrorEvent,
} from "@promin/workflow";
import type { WallClock } from "@promin/workflow";
import {
  SystemWallClock,
  commitPlannedSchedules,
  computeNextRun,
  planDueTicks,
  schedulePartition,
  scheduleTickRunId,
  schedulerLeaderKey,
} from "@promin/workflow";
import type { RunTrigger } from "../routes/runs.ts";

export interface SchedulerLoopConfig {
  storage: SchedulerStorage;
  /**
   * Server-side trigger fn — the same one wired into
   * `/api/runs/trigger/:name`. Defaults to using `metadata.workflowName` +
   * `metadata.input` from the schedule config; override `fire` for full
   * control.
   */
  trigger?: RunTrigger;
  /**
   * Optional dispatch hook. Inspect the tick / schedule and either
   * handle it (return `{ handled: true }` or `void` for backward
   * compat) or pass — return `{ handled: false }` and the loop runs
   * its default `trigger`-based dispatch using `metadata.workflowName`.
   *
   * Use this to layer additional dispatch kinds (agent-targeted ticks
   * via `dispatchAgentSchedule`, webhook ticks, etc.) without losing
   * the workflow-name path for ordinary schedule rows.
   */
  fire?: (
    tick: ScheduleTick,
    schedule: DurableScheduleConfig,
  ) => Promise<{ handled: boolean } | void>;
  /** Stable id used for leader election. Default: random UUID. */
  instanceId?: string;
  /** Poll interval in ms. Default: 1000. */
  pollIntervalMs?: number;
  /** Leader-lock TTL. Default: 3 × pollIntervalMs. */
  leaderLockTtlMs?: number;
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
   * `firedAt`, the next-run computation, `fireOnce` stamps, and the wait
   * between polls. Default: `SystemWallClock`. Tests pass a `FakeWallClock`.
   */
  clock?: WallClock;
  /**
   * Called when a poll or commit fails, or a stored schedule can't be
   * evaluated (it is then disabled). The loop keeps running either way.
   * Default: logs with `console.error`.
   */
  onError?: (event: SchedulerErrorEvent) => void;
}

export class SchedulerLoop {
  readonly instanceId: string;
  private readonly storage: SchedulerStorage;
  private readonly trigger?: RunTrigger;
  private readonly fireOverride?: SchedulerLoopConfig["fire"];
  private readonly pollIntervalMs: number;
  private readonly leaderLockTtlMs: number;
  private readonly namespace?: string;
  private readonly namespacesMode?: "all" | readonly (string | undefined)[];
  private readonly batchSize: number;
  private readonly dispatchConcurrency: number;
  private readonly partition?: { index: number; count: number };
  private readonly clock: WallClock;
  private readonly onError: (event: SchedulerErrorEvent) => void;
  private running = false;
  private loopPromise?: Promise<void>;
  /** Leases this loop currently holds, by lease key; released on `stop()`. */
  private readonly leases = new Map<string, LeaderLease>();
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
    this.namespace = config.namespace;
    this.namespacesMode = config.namespaces;
    this.batchSize = config.batchSize ?? 100;
    this.dispatchConcurrency = config.dispatchConcurrency ?? 10;
    this.clock = config.clock ?? SystemWallClock;
    this.onError = config.onError ?? defaultOnError;
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
   * Drive one tick of the loop synchronously and return the ticks that
   * fired (across every namespace handled this cycle). Exposed for tests
   * so they don't have to wait on real timers.
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
   * doesn't exist or is disabled.
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
            nextRun: computeNextRun(config, this.clock),
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

    try {
      await this.dispatch(tick, config);
    } catch {
      // Same swallow policy as the loop's dispatch — caller (route handler)
      // already returned 200 once commitPoll succeeded; surfaced errors
      // would only confuse the caller about the durability boundary.
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
   * configs/states, compute ticks, dispatch, then commit under `lease`.
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
    for (const plan of plans) {
      if (plan.error !== undefined) {
        this.report({ phase: "schedule", scheduleId: plan.id, error: plan.error });
      }
    }
    const ticks = plans.flatMap((p) => p.ticks);

    // Bounded-parallel dispatch — same shape as
    // `scheduler.stream().parMapAsync(dispatchConcurrency)`. A pool of N
    // workers each pulls the next tick off a shared cursor until the
    // batch is drained. Caps fan-out so a wakeup of 100 schedules
    // doesn't slam the trigger / coordinator with 100 simultaneous
    // calls. Failures are isolated per-tick so one bad schedule doesn't
    // stall its peers.
    let cursor = 0;
    const workerCount = Math.max(1, Math.min(this.dispatchConcurrency, ticks.length));
    await Promise.all(
      Array.from({ length: workerCount }, async () => {
        while (cursor < ticks.length) {
          const i = cursor++;
          const tick = ticks[i]!;
          const config = configs.get(tick.scheduleId);
          if (!config) continue;
          try {
            await this.dispatch(tick, config);
          } catch {
            // Caller-supplied trigger / fire writes its own logs.
          }
        }
      }),
    );

    // Commit after dispatch: if this fails, the next poll re-fires the same
    // ticks and the deterministic run ids make the repeat a no-op. The lease
    // fences it: if leadership moved on during dispatch the commit is
    // rejected and the new leader redelivers.
    try {
      await commitPlannedSchedules({ storage: this.storage, plans, lease: params.lease });
    } catch (error) {
      this.report({ phase: "commit", error });
    }

    return ticks;
  }

  private report(event: SchedulerErrorEvent): void {
    try {
      this.onError(event);
    } catch {
      // A throwing error hook must not take the loop down with it.
    }
  }

  private async dispatch(tick: ScheduleTick, schedule: DurableScheduleConfig): Promise<void> {
    if (this.fireOverride) {
      const result = await this.fireOverride(tick, schedule);
      // `void` keeps the old "fire fully replaces dispatch" contract.
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
    // Deterministic workflowId — keeps a duplicate tick (TTL-window leader
    // race) from creating two workflow rows. createWorkflow is idempotent
    // on workflowId so the second submission is a safe no-op. Same id
    // shape used by `dispatchAgentSchedule` for agent-targeted ticks.
    const workflowId = scheduleTickRunId(tick.scheduleId, tick.tickNumber);
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
        ...(schedule.metadata ?? {}),
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

function defaultOnError(event: SchedulerErrorEvent): void {
  const where = event.scheduleId ? ` (schedule "${event.scheduleId}")` : "";
  console.error(`[scheduler-loop] ${event.phase} failed${where}:`, event.error);
}

/** Upper bound on compare-and-set retries for `fireOnce`. */
const FIRE_ONCE_ATTEMPTS = 10;
