// ---------------------------------------------------------------------------
// ZoryaScheduler — owns SchedulerStorage + the embedded tick loop. Wires
// ZoryaWorkflows.trigger as the default dispatch target and routes
// agent-targeted ticks through an AgentScheduleDispatcher (typically the
// ZoryaAgents class).
//
// Honest about coupling: the scheduler IS the dispatch hub for both
// workflow-targeted and agent-targeted schedules. Folding in
// buildAgentAwareFire (previously in server.ts) means the host doesn't
// need to special-case agent schedules in the server config.
//
// Every dispatch only STARTS work. A workflow tick creates its run and
// returns; an agent tick starts the turn and returns, and the turn's
// outcome is reported later (`phase: "turn"`). The poll never holds its
// lease across a run or an LLM turn.
// ---------------------------------------------------------------------------

import type { WallClock } from "@promin/workflow";
import type {
  DurableScheduleConfig,
  ScheduleTick,
  SchedulerStorage,
} from "@promin/workflow/scheduler";
import { scheduleTickRunId } from "@promin/workflow/scheduler";
import { isAgentSchedule } from "@promin/agent";
import {
  SchedulerLoop,
  logSchedulerLoopError,
  type SchedulerLoopConfig,
  type SchedulerLoopErrorEvent,
} from "../scheduler-loop.ts";
import type { ZoryaWorkflows } from "../workflows/index.ts";

/** A started agent turn; `done` settles when the turn ends and never rejects. */
export interface AgentScheduleStart {
  readonly done: Promise<{ readonly ok: boolean; readonly error?: string }>;
}

/**
 * Minimal contract ZoryaScheduler needs from an agent service to route
 * agent-targeted schedule ticks. ZoryaAgents implements this.
 */
export interface AgentScheduleDispatcher {
  /**
   * Start the agent turn for `tick` and resolve once it is under way —
   * never after the turn. Reject when it could not start; the tick is then
   * delivered again. The turn must run under
   * `scheduleTickRunId(tick)` so a redelivered tick finds it.
   */
  dispatchSchedule(
    tick: ScheduleTick,
    schedule: DurableScheduleConfig,
  ): Promise<AgentScheduleStart>;
}

export interface ZoryaSchedulerConfig {
  storage: SchedulerStorage;
  /** Workflow service — provides the default trigger. */
  workflows: ZoryaWorkflows;
  /**
   * Optional agent dispatcher — when set, ticks where isAgentSchedule()
   * returns true route through `agents.dispatchSchedule()` instead of the
   * default workflow trigger.
   */
  agents?: AgentScheduleDispatcher;
  /** Stable id for leader election. Default: random UUID. */
  instanceId?: string;
  /** Poll interval in ms. Default: 1000. */
  pollIntervalMs?: number;
  /** Leader-lock TTL in ms. Default: 3 × pollIntervalMs. */
  leaderLockTtlMs?: number;
  /** Deadline for one tick's dispatch in ms. Default: 30 000. */
  dispatchTimeoutMs?: number;
  /** Cap on the retry backoff of a schedule whose dispatch failed. Default: 60 000. */
  maxDispatchBackoffMs?: number;
  /** Single-namespace mode. Mutually exclusive with `namespaces`. */
  namespace?: string;
  /** Multi-namespace mode. */
  namespaces?: "all" | readonly (string | undefined)[];
  /** Hash partitioning across multiple instances. */
  partition?: { index: number; count: number };
  /** Max schedules per poll. Default: 100. */
  batchSize?: number;
  /** Max concurrent dispatches per tick. Default: 10. */
  dispatchConcurrency?: number;
  /**
   * Custom dispatch override. When set, bypasses the workflows / agents
   * routing entirely. Return `{ handled: false }` to fall through to the
   * default workflow path. Like every dispatch it must start the work and
   * return, not wait for it.
   */
  fire?: (
    tick: ScheduleTick,
    schedule: DurableScheduleConfig,
  ) => Promise<{ handled: boolean } | void>;
  /** Time source for the embedded tick loop. Default: `SystemWallClock`. */
  clock?: WallClock;
  /**
   * Poll, dispatch, commit and agent-turn failures. Default: logs with
   * `console.error`.
   */
  onError?: (event: SchedulerLoopErrorEvent) => void;
}

export class ZoryaScheduler {
  readonly storage: SchedulerStorage;
  /**
   * Namespace a schedule created without one lands in: the single
   * namespace this scheduler polls, or the global namespace.
   */
  readonly defaultNamespace?: string;
  private readonly namespaces: "all" | ReadonlySet<string | undefined>;
  private readonly loop: SchedulerLoop;
  private readonly onError: (event: SchedulerLoopErrorEvent) => void;
  /** Agent turns started by this instance and still running, by run id. */
  private readonly turns = new Set<string>();

  constructor(config: ZoryaSchedulerConfig) {
    this.storage = config.storage;
    this.onError = config.onError ?? logSchedulerLoopError;
    if (config.namespace !== undefined) this.defaultNamespace = config.namespace;
    this.namespaces =
      config.namespaces === "all" ? "all" : new Set(config.namespaces ?? [config.namespace]);

    const userFire = config.fire;
    const agents = config.agents;

    // Compose the fire handler:
    //   1. user override (if provided) — wins
    //   2. agent schedule? → start the turn through agents.dispatchSchedule
    //   3. otherwise fall through to workflows.trigger via the loop's default
    const fire: NonNullable<SchedulerLoopConfig["fire"]> = async (tick, schedule) => {
      if (userFire) {
        const result = await userFire(tick, schedule);
        const handled = result === undefined ? true : result.handled;
        if (handled) return { handled: true };
      }
      if (agents && isAgentSchedule(schedule)) {
        await this.startTurn({ agents, tick, schedule });
        return { handled: true };
      }
      return { handled: false }; // fall through to loop's default trigger path
    };

    const loopConfig: SchedulerLoopConfig = {
      storage: config.storage,
      trigger: (name, input, opts) => config.workflows.trigger(name, input, opts),
      fire,
      onError: (event) => this.report(event),
    };
    if (config.instanceId !== undefined) loopConfig.instanceId = config.instanceId;
    if (config.pollIntervalMs !== undefined) loopConfig.pollIntervalMs = config.pollIntervalMs;
    if (config.leaderLockTtlMs !== undefined) loopConfig.leaderLockTtlMs = config.leaderLockTtlMs;
    if (config.dispatchTimeoutMs !== undefined)
      loopConfig.dispatchTimeoutMs = config.dispatchTimeoutMs;
    if (config.maxDispatchBackoffMs !== undefined)
      loopConfig.maxDispatchBackoffMs = config.maxDispatchBackoffMs;
    if (config.namespace !== undefined) loopConfig.namespace = config.namespace;
    if (config.namespaces !== undefined) loopConfig.namespaces = config.namespaces;
    if (config.partition !== undefined) loopConfig.partition = config.partition;
    if (config.batchSize !== undefined) loopConfig.batchSize = config.batchSize;
    if (config.dispatchConcurrency !== undefined)
      loopConfig.dispatchConcurrency = config.dispatchConcurrency;
    if (config.clock !== undefined) loopConfig.clock = config.clock;

    this.loop = new SchedulerLoop(loopConfig);
  }

  /**
   * Whether this scheduler polls `namespace` (`undefined` is the global
   * namespace). A schedule in a namespace no scheduler polls never fires,
   * so the API refuses to create one.
   */
  servesNamespace(namespace: string | undefined): boolean {
    return this.namespaces === "all" || this.namespaces.has(namespace);
  }

  /** Powers POST /api/schedules/:id/emit. */
  async fireOnce(scheduleId: string): Promise<ScheduleTick | null> {
    return this.loop.fireOnce(scheduleId);
  }

  /** Drive one tick synchronously. Exposed for tests. */
  async tickOnce(): Promise<ScheduleTick[]> {
    return this.loop.tickOnce();
  }

  async start(): Promise<void> {
    this.loop.start();
  }

  /** Stop polling. Agent turns already started run on to their end. */
  async stop(): Promise<void> {
    await this.loop.stop();
  }

  /**
   * Start an agent turn and return once it is under way. A tick redelivered
   * while its turn still runs here is a no-op; one redelivered after the
   * turn ended lands on the same run id, which the runner answers from
   * storage.
   */
  private async startTurn(params: {
    agents: AgentScheduleDispatcher;
    tick: ScheduleTick;
    schedule: DurableScheduleConfig;
  }): Promise<void> {
    const { agents, tick, schedule } = params;
    const runId = scheduleTickRunId({ scheduleId: tick.scheduleId, tickNumber: tick.tickNumber });
    if (this.turns.has(runId)) return;
    this.turns.add(runId);
    let started: AgentScheduleStart;
    try {
      started = await agents.dispatchSchedule(tick, schedule);
    } catch (error) {
      this.turns.delete(runId);
      throw error;
    }
    void started.done
      .then(
        (result) => {
          if (!result.ok && result.error !== undefined) {
            this.report({
              phase: "turn",
              scheduleId: tick.scheduleId,
              tickNumber: tick.tickNumber,
              error: new Error(result.error),
            });
          }
        },
        (error: unknown) =>
          this.report({
            phase: "turn",
            scheduleId: tick.scheduleId,
            tickNumber: tick.tickNumber,
            error,
          }),
      )
      .finally(() => this.turns.delete(runId));
  }

  private report(event: SchedulerLoopErrorEvent): void {
    try {
      this.onError(event);
    } catch {
      // A throwing error hook must not take the loop down with it.
    }
  }
}
