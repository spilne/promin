// ---------------------------------------------------------------------------
// Step concurrency — resolves the `(scope, key, limit)` triple stamped on a
// dispatched step task from the workflow- and step-level queue config.
// ---------------------------------------------------------------------------

import type { StepDefinition, StepQueueContext, WorkflowQueueConfig } from "../durable-pipeline.ts";
import { assertValidGroupLimits } from "../../distributed/step-group-limits.ts";

/**
 * Resolve a step task's concurrency triple from the workflow + step queue
 * config. Step-level wins over workflow-level. A step-level key function
 * is evaluated against a `StepQueueContext` (the workflow input, `prev`,
 * `deps`, `workflowId`, `attempt`); a workflow-level one against the
 * workflow input. The resolved string is what `claim()` counts against.
 *
 * Returns `null` when neither level configures concurrency — the caller
 * stamps no concurrency fields on the task and `claim()` skips the count
 * check entirely.
 */
export function resolveStepConcurrency(params: {
  workflowName?: string;
  workflowQueue?: WorkflowQueueConfig<unknown>;
  stepDef: StepDefinition;
  workflowInput: unknown;
  stepInput: unknown;
  workflowId: string;
  attempt: number;
  results: Record<string, unknown>;
}): { readonly scope: string; readonly key: string; readonly limit: number } | null {
  const stepQueue = params.stepDef.queue;
  if (stepQueue?.concurrencyLimit !== undefined) {
    const ctx: StepQueueContext = {
      input: params.workflowInput,
      prev: params.stepInput,
      deps: params.results,
      workflowId: params.workflowId,
      attempt: params.attempt,
    };
    const key = stepQueue.concurrencyKey ? stepQueue.concurrencyKey(ctx) : "__all__";
    const scope = `${params.workflowName ?? ""}::${params.stepDef.name}`;
    return { scope, key, limit: stepQueue.concurrencyLimit };
  }
  if (params.workflowQueue?.concurrencyLimit !== undefined) {
    const key = params.workflowQueue.concurrencyKey
      ? params.workflowQueue.concurrencyKey(params.workflowInput)
      : "__all__";
    const scope = params.workflowName ?? "";
    return { scope, key, limit: params.workflowQueue.concurrencyLimit };
  }
  return null;
}

/** The group fields stamped on a dispatched step task. */
export interface ResolvedStepGroup {
  readonly groupName: string;
  readonly groupMaxConcurrent?: number;
  readonly groupMaxStarts?: number;
  readonly groupWindowMs?: number;
  readonly fairnessKey?: string;
}

/**
 * Resolve the step's rate-limit / concurrency group: the step's
 * `queue.group` wins over the workflow's. `fairnessKey` is evaluated here
 * (a step-level one against the `StepQueueContext`, a workflow-level one
 * against the workflow input). `null` when neither level names a group.
 * Throws on an unenforceable group (bad limits), which fails the step.
 */
export function resolveStepGroup(params: {
  workflowQueue?: WorkflowQueueConfig<unknown>;
  stepDef: StepDefinition;
  workflowInput: unknown;
  stepInput: unknown;
  workflowId: string;
  attempt: number;
  results: Record<string, unknown>;
}): ResolvedStepGroup | null {
  const stepGroup = params.stepDef.queue?.group;
  const group = stepGroup ?? params.workflowQueue?.group;
  if (!group) return null;
  assertValidGroupLimits(group);
  const fairnessKey = stepGroup
    ? stepGroup.fairnessKey?.({
        input: params.workflowInput,
        prev: params.stepInput,
        deps: params.results,
        workflowId: params.workflowId,
        attempt: params.attempt,
      })
    : params.workflowQueue!.group!.fairnessKey?.(params.workflowInput);
  return {
    groupName: group.name,
    ...(group.maxConcurrent !== undefined && { groupMaxConcurrent: group.maxConcurrent }),
    ...(group.maxStarts !== undefined && { groupMaxStarts: group.maxStarts }),
    ...(group.windowMs !== undefined && { groupWindowMs: group.windowMs }),
    ...(fairnessKey !== undefined && { fairnessKey }),
  };
}
