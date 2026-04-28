// ---------------------------------------------------------------------------
// `dispatchAgentSchedule` — reference dispatch for tick events written by
// `createDurableSchedulerTool`. Wire this from the host's scheduler-loop
// `fireOverride` so agent-targeted ticks become fresh agent invocations.
//
// Contract
// --------
// Schedules created by the agent tool stamp metadata with:
//
//   metadata: {
//     agentTrigger: true,
//     agentId: string,
//     task: string,
//     namespaceId: string,
//     resourceId?: string,
//     threadId?: string,
//     createdByAgent: string,
//   }
//
// This helper reads those fields, resolves the recipe, and runs one
// turn — into the original thread when `threadId` is present, or as a
// one-shot `agent.invoke` otherwise.
//
// Task source UX (v1)
// -------------------
// We wrap the task with `[Scheduled trigger: <iso>] ${task}` so the
// model sees a clear signal that this isn't a live user typing — it
// can adapt its tone (notification voice vs conversational). When
// `AgentInput.source` lands as a first-class field this wrap goes
// away in favour of the loop prepending its own framing.
// ---------------------------------------------------------------------------

import type { ScheduleTick, DurableScheduleConfig } from "@promin/workflow";
import type { Agent } from "../agent/types.ts";
import type { AgentRegistry, RegisteredAgent } from "../registry/types.ts";

export interface AgentScheduleMetadata {
  readonly agentTrigger: true;
  readonly agentId: string;
  readonly task: string;
  readonly namespaceId: string;
  readonly resourceId?: string;
  readonly threadId?: string;
  readonly createdByAgent: string;
}

/**
 * Type guard — returns true when the schedule was written by the
 * agent scheduler tool. Hosts that wire the dispatch hook check this
 * before running the helper, so non-agent schedules go through their
 * normal `metadata.workflowName` path.
 */
export function isAgentSchedule(
  schedule: DurableScheduleConfig,
): schedule is DurableScheduleConfig & { metadata: AgentScheduleMetadata } {
  const m = schedule.metadata as Partial<AgentScheduleMetadata> | undefined;
  if (!m) return false;
  if (m.agentTrigger !== true) return false;
  if (typeof m.agentId !== "string") return false;
  if (typeof m.task !== "string") return false;
  if (typeof m.namespaceId !== "string") return false;
  return true;
}

export interface DispatchAgentScheduleDeps {
  readonly registry: AgentRegistry;
  readonly resolve: (recipe: RegisteredAgent) => Agent;
  /**
   * Optional hook fired BEFORE the agent invocation runs. Hosts can
   * use it to write an audit row, increment a metrics counter, or
   * short-circuit by returning false (skip dispatch).
   */
  readonly beforeFire?: (ctx: ScheduleFireContext) => Promise<boolean | void> | boolean | void;
  /**
   * Optional hook fired after the agent invocation completes (or fails).
   */
  readonly afterFire?: (
    ctx: ScheduleFireContext,
    result: { ok: true; text: string } | { ok: false; error: string },
  ) => Promise<void> | void;
}

export interface ScheduleFireContext {
  readonly tick: ScheduleTick;
  readonly metadata: AgentScheduleMetadata;
  readonly recipe: RegisteredAgent;
}

export interface DispatchAgentScheduleResult {
  readonly ok: boolean;
  readonly skipped?: "not_agent_schedule" | "recipe_not_found" | "before_hook";
  readonly text?: string;
  readonly error?: string;
}

export async function dispatchAgentSchedule(
  tick: ScheduleTick,
  schedule: DurableScheduleConfig,
  deps: DispatchAgentScheduleDeps,
): Promise<DispatchAgentScheduleResult> {
  if (!isAgentSchedule(schedule)) {
    return { ok: false, skipped: "not_agent_schedule" };
  }
  const meta = schedule.metadata;

  const recipe = await deps.registry.get(meta.agentId);
  if (!recipe) {
    return { ok: false, skipped: "recipe_not_found", error: `recipe "${meta.agentId}"` };
  }

  const ctx: ScheduleFireContext = { tick, metadata: meta, recipe };

  if (deps.beforeFire) {
    const proceed = await deps.beforeFire(ctx);
    if (proceed === false) {
      return { ok: false, skipped: "before_hook" };
    }
  }

  const wrappedTask = `[Scheduled trigger: ${tick.scheduledAt.toISOString()}] ${meta.task}`;
  const agent = deps.resolve(recipe).withScope({
    namespaceId: meta.namespaceId,
    ...(meta.resourceId !== undefined && { resourceId: meta.resourceId }),
  });

  try {
    let text: string;
    if (meta.threadId) {
      const thread = await agent.thread(meta.threadId);
      const out = await thread.send({ task: wrappedTask });
      text = await out.text;
    } else {
      const out = await agent.invoke({ task: wrappedTask });
      text = await out.text;
    }
    const result = { ok: true as const, text };
    await deps.afterFire?.(ctx, result);
    return result;
  } catch (err) {
    const error = err instanceof Error ? err.message : String(err);
    const result = { ok: false as const, error };
    await deps.afterFire?.(ctx, result);
    return result;
  }
}
