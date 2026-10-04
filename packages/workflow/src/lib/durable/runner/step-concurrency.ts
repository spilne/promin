// ---------------------------------------------------------------------------
// Step concurrency — resolves the `(scope, key, limit)` triple stamped on a
// dispatched step task from the workflow- and step-level queue config.
// ---------------------------------------------------------------------------

import type { StepDefinition, StepQueueContext, WorkflowQueueConfig } from "../durable-pipeline.ts";

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
  if (stepQueue) {
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
  if (params.workflowQueue) {
    const key = params.workflowQueue.concurrencyKey
      ? params.workflowQueue.concurrencyKey(params.workflowInput)
      : "__all__";
    const scope = params.workflowName ?? "";
    return { scope, key, limit: params.workflowQueue.concurrencyLimit };
  }
  return null;
}
