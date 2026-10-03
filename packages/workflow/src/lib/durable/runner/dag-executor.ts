// ---------------------------------------------------------------------------
// DAG executor — the wave loop. Replays completed steps from stored state,
// then repeatedly computes the ready set, runs it as one wave (remote
// dispatch, then the executor or inline wave), records a failure or folds
// the wave's results back in, and checks the workflow deadline and tripwire.
// ---------------------------------------------------------------------------

import type { TaggedError } from "../../shared/tagged-error.ts";
import { SystemWallClock } from "../../shared/wall-clock.ts";
import {
  StepError,
  WorkflowDeadlineError,
  WorkflowError,
  type StepTimeoutError,
  type WorkflowTimeoutError,
} from "../durable-pipeline-error.ts";
import { computeReadySet, type DagNode } from "../workflow-dag.ts";
import type { WorkflowState } from "../workflow-state.ts";
import type { DagExecutionContext, DagExecutionResult, WaveOutcome } from "./dag-context.ts";
import { runExecutorWave } from "./executor-wave.ts";
import { fireHook } from "./hooks.ts";
import { runInlineWave } from "./inline-wave.ts";
import { runDispatchedSteps } from "./remote-dispatch.ts";
import { persistStepFailure, persistStepResult } from "./step-checkpoint.ts";

/**
 * Execute the workflow DAG against its current state. Computes the ready
 * set per iteration, dispatches remote steps via the step queue, runs local
 * ready steps in parallel with per-step retry + timeout + onFailure, and
 * checkpoints every completed step via `saveStepResult`. Suspension
 * errors propagate through as `{ suspension: true }` so the caller can
 * distinguish "workflow is sleeping / waiting for signal" from real
 * failures.
 */
export async function executeWorkflowDag(
  ctx: DagExecutionContext,
  params: {
    workflowId: string;
    input: unknown;
    dagNodes: DagNode[];
    state: WorkflowState | null;
    workflowStartTime: number;
    /** Tracks attempt numbers per step — shared across workflow retries so counters keep incrementing. */
    stepAttempts: Map<string, number>;
    /** Workflow-level deadline (absolute timestamp). Steps completing after this fail the workflow. */
    deadlineMs?: number;
  },
): Promise<DagExecutionResult> {
  const { workflowId, input, dagNodes, state } = params;
  const clock = ctx.clock ?? SystemWallClock;
  const results: Record<string, unknown> = {};

  // Load previously completed step results. The stored shape is always the
  // codec's encoded form (written by saveStepResult), so we decode
  // through the step's codec here so downstream steps see the same shape
  // they would on a fresh run. Skip step rows that aren't declared in the
  // DAG — e.g., synthetic `<loop>.iter.<n>` rows written by `.dowhile()`,
  // or orphans from a prior version's topology. Those rows stay in
  // storage for observability but must not count as DAG progress, or the
  // `completed.size < ctx.steps.length` gate would skip the real step.
  if (state) {
    for (const [stepName, stepState] of Object.entries(state.steps)) {
      if (stepState.status !== "completed") continue;
      const stepDef = ctx.steps.find((s) => s.name === stepName);
      if (!stepDef) continue;
      results[stepName] = stepDef.codec.decode(stepState.result);
    }
  }

  const completed = new Set(Object.keys(results));
  // Every wave settles before the next ready-set computation, so no step
  // is ever in flight when the ready set is computed.
  const running = new Set<string>();

  while (completed.size < ctx.steps.length) {
    // Check workflow-level deadline before each batch
    if (params.deadlineMs != null && clock.currentTimeMs() > params.deadlineMs) {
      return {
        success: false,
        error: new WorkflowDeadlineError({
          workflowId,
          timeoutMs: ctx.timeoutMs!,
          message: `Workflow "${workflowId}" exceeded global deadline of ${ctx.timeoutMs}ms`,
        }),
        suspension: false,
      };
    }

    const ready = computeReadySet({ nodes: dagNodes, completed, running });

    if (ready.length === 0) {
      return {
        success: false,
        error: new WorkflowError({
          workflowId,
          message: "Deadlock: no steps are ready and none are running",
        }),
        suspension: false,
      };
    }

    // Split into local and dispatched steps
    const remoteSet = new Set(ctx.dispatch?.remoteSteps ?? []);
    const localReady: string[] = [];
    const dispatchReady: string[] = [];

    for (const name of ready) {
      if (remoteSet.has(name) && ctx.dispatch) {
        dispatchReady.push(name);
      } else {
        localReady.push(name);
      }
    }

    // Dispatch remote steps — enqueue (with the step's declared needs) and
    // poll until completed.
    if (dispatchReady.length > 0) {
      const dispatchFailure = await runDispatchedSteps({
        ctx,
        clock,
        workflowId,
        input,
        names: dispatchReady,
        results,
        completed,
      });
      if (dispatchFailure) return dispatchFailure;
    }

    // If all ready steps were dispatched, skip local execution
    if (localReady.length === 0) continue;

    // Execute local ready steps in parallel, with per-step retry and failure handling
    const readySteps = localReady.map((name) => ctx.steps.find((s) => s.name === name)!);

    // Emit `step-started` events to any subscribers before kicking the
    // batch off. Storages without the optional hook are silently skipped —
    // polling-only callers don't see step-started (no reliable signal
    // from snapshot diffs).
    if (typeof ctx.storage.notifyStepStarted === "function") {
      for (const stepDef of readySteps) {
        // Swallow errors from the notify path — subscription is advisory,
        // not load-bearing. A broken event bus must not fail a workflow.
        try {
          await ctx.storage.notifyStepStarted(workflowId, stepDef.name);
        } catch {
          // ignore
        }
      }
    }

    // Per-parallel-batch audit metadata map. `.match()` writes its chosen
    // case here via metadataRef; the failure path reads it back by step
    // name to persist metadata even when a match branch throws.
    const stepMetadata = new Map<string, Record<string, unknown>>();

    const waveParams = {
      ctx,
      workflowId,
      input,
      readySteps,
      results,
      stepAttempts: params.stepAttempts,
      clock,
    };
    const { batchResults, batchError }: WaveOutcome = ctx.stepExecutor
      ? await runExecutorWave(waveParams)
      : await runInlineWave({ ...waveParams, stepMetadata });

    if (batchError) {
      const tag = (batchError as TaggedError)._tag;

      // Suspension errors propagate without failing the workflow
      if (tag === "WorkflowSuspendedError") {
        return { success: false, error: batchError, suspension: true };
      }

      // Continue-as-new requests also unwind cleanly — no compensation,
      // no failure recording. The orchestration wrapper catches the
      // thrown error and chains a fresh run.
      if (tag === "WorkflowContinueAsNewError") {
        return { success: false, error: batchError, suspension: false, continueAsNew: true };
      }

      // Record step failure
      const stepName =
        tag === "StepError"
          ? (batchError as StepError).stepName
          : tag === "WorkflowTimeoutError"
            ? (batchError as WorkflowTimeoutError).stepName
            : tag === "StepTimeoutError"
              ? (batchError as StepTimeoutError).stepName
              : (ready[0] ?? "unknown");
      const errorMsg =
        batchError instanceof globalThis.Error ? batchError.message : String(batchError);
      await persistStepFailure({
        ctx,
        clock,
        workflowId,
        stepName,
        errorMsg,
        metadata: stepMetadata.get(stepName),
        attempt: params.stepAttempts.get(stepName) ?? 1,
      });
      await fireHook({
        hooks: ctx.hooks,
        name: "onStepFailure",
        event: {
          workflowId,
          stepName,
          error: errorMsg,
          durationMs: 0,
        },
      });

      return { success: false, error: batchError, suspension: false };
    }

    // Fold each step's result back in. `result` here is the codec-encoded
    // form; storage keeps that shape. Downstream steps and the
    // onStepComplete hook see the round-tripped decoded form so fresh-run
    // and replay paths are identical. Both waves save completed steps
    // themselves; only skipped steps still need saving here.
    let tripwireFire: { stepName: string; reason: unknown } | null = null;
    for (const stepResult of batchResults!) {
      const { name, result, metadata, durationMs, startedAt } = stepResult;
      const wasSkipped = stepResult.skipped === true;
      const stepDef = ctx.steps.find((s) => s.name === name);
      const decoded = stepDef ? stepDef.codec.decode(result) : result;
      if (stepResult.storageAlreadyCheckpointed !== true) {
        await persistStepResult({
          ctx,
          clock,
          workflowId,
          name,
          result,
          metadata,
          durationMs,
          startedAt,
          attempt: params.stepAttempts.get(name) ?? 1,
        });
      }
      if (!wasSkipped) {
        await fireHook({
          hooks: ctx.hooks,
          name: "onStepComplete",
          event: {
            workflowId,
            stepName: name,
            result: decoded,
            durationMs,
          },
        });
      }
      results[name] = decoded;
      completed.add(name);

      // Tripwire detection: a `.tripwire()` step signals termination by
      // writing `{ tripwireFired: true, reason }` to its metadata. Captured
      // here after save so the step row shows `status: completed` with the
      // reason as its result — ops can still query the step history.
      // Breaks out of DAG execution after the batch settles.
      if (
        stepDef?.kind === "tripwire" &&
        metadata &&
        (metadata as { tripwireFired?: boolean }).tripwireFired === true
      ) {
        tripwireFire = {
          stepName: name,
          reason: (metadata as { reason: unknown }).reason,
        };
      }
    }

    if (tripwireFire) {
      return { success: false, tripwire: true, ...tripwireFire };
    }

    // Check workflow-level deadline after steps complete
    if (
      params.deadlineMs != null &&
      clock.currentTimeMs() > params.deadlineMs &&
      completed.size < ctx.steps.length
    ) {
      return {
        success: false,
        error: new WorkflowDeadlineError({
          workflowId,
          timeoutMs: ctx.timeoutMs!,
          message: `Workflow "${workflowId}" exceeded global deadline of ${ctx.timeoutMs}ms`,
        }),
        suspension: false,
      };
    }
  }

  const lastStepName = ctx.steps[ctx.steps.length - 1]!.name;
  return { success: true, result: results[lastStepName] };
}
