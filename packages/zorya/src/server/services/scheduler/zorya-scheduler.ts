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
// ---------------------------------------------------------------------------

import type { DurableScheduleConfig, ScheduleTick, SchedulerStorage } from "@promin/workflow";
import { isAgentSchedule } from "@promin/agent";
import { SchedulerLoop } from "../scheduler-loop.ts";
import type { ZoryaWorkflows } from "../workflows/index.ts";

/**
 * Minimal contract ZoryaScheduler needs from an agent service to route
 * agent-targeted schedule ticks. ZoryaAgents implements this.
 */
export interface AgentScheduleDispatcher {
  dispatchSchedule(tick: ScheduleTick, schedule: DurableScheduleConfig): Promise<void>;
}

export interface ZoryaSchedulerConfig {
  storage: SchedulerStorage;
  /** Workflow service — provides the default trigger. */
  workflows: ZoryaWorkflows;
  /** Default input for workflow schedules that omit `metadata.input`. */
  sampleInput?: (workflowName: string) => unknown;
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
   * default workflow path.
   */
  fire?: (
    tick: ScheduleTick,
    schedule: DurableScheduleConfig,
  ) => Promise<{ handled: boolean } | void>;
}

export class ZoryaScheduler {
  readonly storage: SchedulerStorage;
  private readonly loop: SchedulerLoop;

  constructor(config: ZoryaSchedulerConfig) {
    this.storage = config.storage;

    const userFire = config.fire;
    const agents = config.agents;

    // Compose the fire handler:
    //   1. user override (if provided) — wins
    //   2. agent schedule? → agents.dispatchSchedule
    //   3. otherwise fall through to workflows.trigger via the loop's default
    const fire: SchedulerLoopConfigFire = async (tick, schedule) => {
      if (userFire) {
        const result = await userFire(tick, schedule);
        const handled = result === undefined ? true : result.handled;
        if (handled) return { handled: true };
      }
      if (agents && isAgentSchedule(schedule)) {
        await agents.dispatchSchedule(tick, schedule);
        return { handled: true };
      }
      return { handled: false }; // fall through to loop's default trigger path
    };

    const loopConfig: ConstructorParameters<typeof SchedulerLoop>[0] = {
      storage: config.storage,
      trigger: (name, input, opts) => config.workflows.trigger(name, input, opts),
      fire,
    };
    if (config.sampleInput !== undefined) loopConfig.sampleInput = config.sampleInput;
    if (config.instanceId !== undefined) loopConfig.instanceId = config.instanceId;
    if (config.pollIntervalMs !== undefined) loopConfig.pollIntervalMs = config.pollIntervalMs;
    if (config.leaderLockTtlMs !== undefined) loopConfig.leaderLockTtlMs = config.leaderLockTtlMs;
    if (config.namespace !== undefined) loopConfig.namespace = config.namespace;
    if (config.namespaces !== undefined) loopConfig.namespaces = config.namespaces;
    if (config.partition !== undefined) loopConfig.partition = config.partition;
    if (config.batchSize !== undefined) loopConfig.batchSize = config.batchSize;
    if (config.dispatchConcurrency !== undefined)
      loopConfig.dispatchConcurrency = config.dispatchConcurrency;

    this.loop = new SchedulerLoop(loopConfig);
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

  async stop(): Promise<void> {
    await this.loop.stop();
  }
}

type SchedulerLoopConfigFire = NonNullable<ConstructorParameters<typeof SchedulerLoop>[0]["fire"]>;
