// ---------------------------------------------------------------------------
// `dispatchAgentSchedule` — reference dispatch for tick events written by
// `createDurableSchedulerTools`. Wire this from the host's scheduler-loop
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
// Task source UX
// --------------
// The dispatcher passes `source: { kind: "scheduled", firedAt }` on
// the AgentInput; the agent loop's framing helper prepends a
// `[Scheduled trigger at <iso>]` header to the task before sending it
// to the model. The header signals "this isn't a live user typing"
// so the model can adapt tone — and the wording lives in one place
// (frame-task.ts), not in every dispatch caller.
// ---------------------------------------------------------------------------

import type { ScheduleTick, DurableScheduleConfig } from "@promin/workflow";
import { scheduleTickRunId } from "@promin/workflow";
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
  /**
   * Resolve a recipe to a live Agent. Optional `scope` carries the
   * schedule's namespace + resource so hosts can resolve recipe-level
   * credentialRefs (BYOK) at this boundary; ignored by hosts that
   * don't care.
   */
  readonly resolve: (
    recipe: RegisteredAgent,
    scope?: { readonly namespaceId?: string; readonly resourceId?: string },
  ) => Agent | Promise<Agent>;
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

  const resolved = await deps.resolve(recipe, {
    namespaceId: meta.namespaceId,
    ...(meta.resourceId !== undefined && { resourceId: meta.resourceId }),
  });
  const agent = resolved.withScope({
    namespaceId: meta.namespaceId,
    ...(meta.resourceId !== undefined && { resourceId: meta.resourceId }),
  });
  const input = {
    task: meta.task,
    source: {
      kind: "scheduled" as const,
      firedAt: tick.scheduledAt,
      scheduleId: tick.scheduleId,
    },
  };

  // Deterministic run id — same `${scheduleId}.${tickNumber}` shape used
  // by the workflow-trigger path. Two effects: (1) the schedule history
  // route's join `wf_workflows.workflow_id = scheduleTickRunId(...)` finds
  // the agent's run row directly, no special-case needed; (2) a leader
  // race that fires the same tick twice lands on the same workflow row
  // (createWorkflow is idempotent) instead of double-billing the model.
  const runId = scheduleTickRunId(tick.scheduleId, tick.tickNumber);

  try {
    let text: string;
    if (meta.threadId) {
      const thread = await agent.thread(meta.threadId);
      const out = await thread.send(input, { runId });
      text = await out.text;
    } else {
      const out = await agent.invoke(input, { runId });
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
