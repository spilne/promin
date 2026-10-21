// ---------------------------------------------------------------------------
// Remote dispatch — the `dispatch.remoteSteps` path: enqueues each listed
// ready step on the configured step queue and polls storage until a worker
// completes or fails it. One step at a time, before the local wave starts.
// ---------------------------------------------------------------------------

import type { WallClock } from "../../shared/wall-clock.ts";
import { StepError } from "../durable-pipeline-error.ts";
import type { DagExecutionContext, DagExecutionFailure } from "./dag-context.ts";
import type { StepDefinition } from "../durable-pipeline.ts";
import { fireHook } from "./hooks.ts";

/**
 * Dispatch `names` (all ready, all listed in `ctx.dispatch.remoteSteps`) and
 * wait for each. Completed steps land in `results` and are passed to
 * `markCompleted`. Returns the failure outcome for the first step a worker
 * fails, else `undefined`.
 */
export async function runDispatchedSteps(params: {
  ctx: DagExecutionContext;
  clock: WallClock;
  workflowId: string;
  input: unknown;
  names: string[];
  stepsByName: ReadonlyMap<string, StepDefinition>;
  results: Record<string, unknown>;
  markCompleted: (name: string) => void;
}): Promise<DagExecutionFailure | undefined> {
  const { ctx, clock, workflowId, input, results } = params;

  for (const name of params.names) {
    const stepDef = params.stepsByName.get(name);
    await ctx.dispatch!.stepQueue.enqueue({
      workflowId,
      stepName: name,
      needs: stepDef?.needs,
      priority: stepDef?.priority,
      input,
      prevResults: { ...results },
    });
    // Poll until the worker completes this step
    const pollMs = ctx.dispatch!.pollIntervalMs ?? 500;
    while (true) {
      await new Promise((r) => clock.setTimeout(() => r(undefined), pollMs));
      const currentState = await ctx.storage.loadWorkflow(workflowId);
      const stepState = currentState?.steps[name];
      if (stepState?.status === "completed") {
        results[name] = stepState.result;
        params.markCompleted(name);
        await fireHook({
          hooks: ctx.hooks,
          name: "onStepComplete",
          event: {
            workflowId,
            stepName: name,
            result: stepState.result,
            durationMs: stepState.durationMs ?? 0,
          },
        });
        break;
      }
      if (stepState?.status === "failed") {
        const errorMsg = stepState.error ?? "Remote step failed";
        await fireHook({
          hooks: ctx.hooks,
          name: "onStepFailure",
          event: {
            workflowId,
            stepName: name,
            error: errorMsg,
            durationMs: 0,
          },
        });
        return {
          success: false,
          error: new StepError({ workflowId, stepName: name, message: errorMsg }),
          suspension: false,
        };
      }
    }
  }
  return undefined;
}
