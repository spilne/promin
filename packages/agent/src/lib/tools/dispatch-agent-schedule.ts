// ---------------------------------------------------------------------------
// `startAgentSchedule` / `dispatchAgentSchedule` — reference dispatch for
// tick events written by `createDurableSchedulerTools`. Wire
// `startAgentSchedule` from the host's scheduler-loop dispatch so
// agent-targeted ticks become fresh agent invocations.
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
// `startAgentSchedule` reads those fields, resolves the recipe, and
// starts one turn — into the original thread when `threadId` is present,
// or as a one-shot `agent.invoke` otherwise — returning once the turn is
// under way; a scheduler loop uses it so a poll never waits on the model.
// `dispatchAgentSchedule` does the same and awaits the turn.
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

import type { ScheduleTick, DurableScheduleConfig } from "@promin/workflow/scheduler";
import { scheduleTickRunId } from "@promin/workflow/scheduler";
import { THREAD_AGENT_METADATA_KEY, type Agent } from "../agent/types.ts";
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

/**
 * A started schedule turn. `done` settles when the turn ends and never
 * rejects: a failed turn resolves `{ ok: false, error }`.
 */
export interface StartedAgentSchedule {
  readonly ok: true;
  readonly runId: string;
  readonly done: Promise<DispatchAgentScheduleResult>;
}

/**
 * Start the agent turn for a schedule tick and return once it is under
 * way, without waiting for the model. Use it from a scheduler loop: the
 * poll must not hold its lease across an LLM turn.
 *
 * Resolves `{ ok: false, skipped }` when the schedule is not an agent
 * schedule, the recipe is gone or `beforeFire` declined; rejects when the
 * recipe can't be resolved to an agent or its thread can't be opened —
 * the caller can then deliver the tick again. The turn runs under the
 * deterministic `scheduleTickRunId` run id.
 */
export async function startAgentSchedule(
  tick: ScheduleTick,
  schedule: DurableScheduleConfig,
  deps: DispatchAgentScheduleDeps,
): Promise<StartedAgentSchedule | DispatchAgentScheduleResult> {
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

  // Deterministic run id — same `scheduleTickRunId(tick)` shape used
  // by the workflow-trigger path. Two effects: (1) the schedule history
  // route's join `wf_workflows.workflow_id = scheduleTickRunId(...)` finds
  // the agent's run row directly, no special-case needed; (2) a
  // redelivered tick lands on the same workflow row, and the runner's
  // terminal gate answers a finished turn from storage instead of
  // billing the model again.
  const runId = scheduleTickRunId(tick);

  // A thread this tick creates is stamped with the recipe it runs, so the
  // agent's gateway can list and open it like any other of its threads.
  const thread = meta.threadId
    ? await agent.thread(meta.threadId, {
        metadata: { [THREAD_AGENT_METADATA_KEY]: recipe.id },
      })
    : undefined;
  // Kick the turn off; only its outcome is awaited, in `done`.
  const turn = Promise.resolve(
    thread ? thread.send(input, { runId }) : agent.invoke(input, { runId }),
  );
  const done = (async (): Promise<DispatchAgentScheduleResult> => {
    let result: { ok: true; text: string } | { ok: false; error: string };
    try {
      const out = await turn;
      result = { ok: true, text: await out.text };
    } catch (err) {
      result = { ok: false, error: err instanceof Error ? err.message : String(err) };
    }
    try {
      await deps.afterFire?.(ctx, result);
    } catch {
      // An observer hook does not change the turn's outcome.
    }
    return result;
  })();
  return { ok: true, runId, done };
}

/** Run one schedule turn to the end: `startAgentSchedule`, then await it. */
export async function dispatchAgentSchedule(
  tick: ScheduleTick,
  schedule: DurableScheduleConfig,
  deps: DispatchAgentScheduleDeps,
): Promise<DispatchAgentScheduleResult> {
  const started = await startAgentSchedule(tick, schedule, deps);
  if (!("done" in started)) return started;
  return await started.done;
}
