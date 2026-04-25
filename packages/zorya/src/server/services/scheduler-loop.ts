// ---------------------------------------------------------------------------
// SchedulerLoop — embedded poll loop that runs the DurableScheduler tick
// cycle inside ZoryaServer.
//
// Why embedded vs `scheduler.stream().forEach(...)`:
//   - Lifecycle. A while-loop with a flag stops cleanly on `server.stop()`;
//     `Effect.runPromise(stream...)` doesn't expose interruption.
//   - Tests. Driving one tick at a time via `tickOnce()` lets specs avoid
//     real timers.
//
// The leader-lock + tick computation use the same primitives the standalone
// DurableScheduler does — `tryAcquireLeader`, `findDue`, `loadSchedules`,
// `loadScheduleStates`, `computeDueTicks`, `computeNextRun`, `commitPoll`.
// Wire-compatible with every SchedulerStorage backend (Postgres, Redis,
// in-memory).
//
// Dispatch routes through `RunTrigger`, which is whichever trigger the
// server is configured with — `CoordinatedTriggerService` under
// `coordination: { enabled: true }`, the workflow-start `TriggerService`
// otherwise. The deterministic `${scheduleId}.${tickNumber}` workflowId
// gives belt-and-suspenders idempotency: if a brief leader-transition
// causes two instances to fire the same tick, the second `createWorkflow`
// is a no-op.
// ---------------------------------------------------------------------------

import type { SchedulerStorage, ScheduleTick, DurableScheduleConfig } from "@promin/workflow";
import { computeDueTicks, computeNextRun } from "@promin/workflow";
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
  /** Optional override for tick dispatch. Wins over `trigger`. */
  fire?: (tick: ScheduleTick, schedule: DurableScheduleConfig) => Promise<void>;
  /** Stable id used for leader election. Default: random UUID. */
  instanceId?: string;
  /** Poll interval in ms. Default: 1000. */
  pollIntervalMs?: number;
  /** Leader-lock TTL. Default: 3 × pollIntervalMs. */
  leaderLockTtlMs?: number;
  /** Scope this loop to a single namespace. Default: undefined (global). */
  namespace?: string;
  /** Max schedules per poll. Default: 100. */
  batchSize?: number;
  /**
   * Hash partitioning so multiple Zorya instances can share work across
   * different schedule subsets while leader election still gates each
   * partition's ticks. Default: undefined (single partition).
   */
  partition?: { index: number; count: number };
}

export class SchedulerLoop {
  readonly instanceId: string;
  private readonly storage: SchedulerStorage;
  private readonly trigger?: RunTrigger;
  private readonly fireOverride?: SchedulerLoopConfig["fire"];
  private readonly pollIntervalMs: number;
  private readonly leaderLockTtlMs: number;
  private readonly namespace?: string;
  private readonly batchSize: number;
  private readonly partition?: { index: number; count: number };
  private running = false;
  private loopPromise?: Promise<void>;

  constructor(config: SchedulerLoopConfig) {
    this.storage = config.storage;
    this.trigger = config.trigger;
    this.fireOverride = config.fire;
    this.instanceId = config.instanceId ?? crypto.randomUUID();
    this.pollIntervalMs = config.pollIntervalMs ?? 1000;
    this.leaderLockTtlMs = config.leaderLockTtlMs ?? this.pollIntervalMs * 3;
    this.namespace = config.namespace;
    this.batchSize = config.batchSize ?? 100;
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
    if (this.loopPromise) await this.loopPromise.catch(() => {});
    this.loopPromise = undefined;
  }

  /**
   * Drive one tick of the loop synchronously and return the ticks that
   * fired. Exposed for tests so they don't have to wait on real timers.
   */
  async tickOnce(): Promise<ScheduleTick[]> {
    const isLeader = await this.storage.tryAcquireLeader({
      instanceId: this.instanceId,
      namespace: this.namespace,
      ttlMs: this.leaderLockTtlMs,
    });
    if (!isLeader) return [];

    const dueIds = await this.storage.findDue({
      now: new Date(),
      limit: this.batchSize,
      namespace: this.namespace,
    });
    const targetIds = dueIds.filter((id) => {
      if (!this.partition) return true;
      return hashCode(id) % this.partition.count === this.partition.index;
    });
    if (targetIds.length === 0) return [];

    const [configs, states] = await Promise.all([
      this.storage.loadSchedules(targetIds),
      this.storage.loadScheduleStates(targetIds),
    ]);

    const ticks: ScheduleTick[] = [];
    const updates: Array<{
      id: string;
      firedAt?: Date;
      tickIncrement?: number;
      nextRun: Date | null;
    }> = [];

    for (const id of targetIds) {
      const config = configs.get(id);
      if (!config) {
        // Schedule deleted between findDue and now — clear it from due.
        updates.push({ id, nextRun: null });
        continue;
      }
      if (config.enabled === false) continue;
      const state = states.get(id) ?? { lastFired: null, tickCount: 0 };
      const due = computeDueTicks(config, state.lastFired, state.tickCount);
      ticks.push(...due);
      updates.push({
        id,
        firedAt: due.length > 0 ? due[due.length - 1]!.firedAt : undefined,
        tickIncrement: due.length > 0 ? due.length : undefined,
        nextRun: computeNextRun(config),
      });
    }

    if (updates.length > 0) await this.storage.commitPoll(updates);

    // Dispatch each tick. Failures are isolated per-tick so one bad
    // schedule doesn't stall the whole batch.
    for (const tick of ticks) {
      const config = configs.get(tick.scheduleId);
      if (!config) continue;
      try {
        await this.dispatch(tick, config);
      } catch {
        // Caller-supplied trigger / fire writes its own logs; swallowing
        // here keeps the loop alive for other schedules.
      }
    }

    return ticks;
  }

  private async dispatch(tick: ScheduleTick, schedule: DurableScheduleConfig): Promise<void> {
    if (this.fireOverride) {
      await this.fireOverride(tick, schedule);
      return;
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
    // on workflowId so the second submission is a safe no-op.
    const workflowId = `${tick.scheduleId}.${tick.tickNumber}`;
    await this.trigger(workflowName, meta.input, {
      workflowId,
      namespace: meta.namespace ?? schedule.namespace,
      workflowType: meta.workflowType,
      version: meta.version,
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
      } catch {
        // Loop-level errors (RPC blip, transient lock failure) should not
        // kill the loop; log surface lives with the storage / trigger.
      }
      if (!this.running) break;
      await new Promise<void>((r) => setTimeout(r, this.pollIntervalMs));
    }
  }
}

// 32-bit non-cryptographic string hash. Cheap + deterministic — same
// algorithm DurableScheduler uses for its partitioning so a SchedulerLoop
// and a standalone DurableScheduler can coexist on the same partition
// indices without colliding.
function hashCode(s: string): number {
  let h = 0;
  for (let i = 0; i < s.length; i++) {
    h = (h * 31 + s.charCodeAt(i)) | 0;
  }
  return Math.abs(h);
}
