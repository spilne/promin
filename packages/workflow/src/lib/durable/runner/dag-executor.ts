// ---------------------------------------------------------------------------
// DAG executor — the wave loop. Replays completed steps from stored state,
// then repeatedly computes the ready set, runs it as one wave (remote
// dispatch, then the executor or inline wave), folds the wave's completed
// steps back in, reports its failure or control flow, and checks the
// workflow deadline and tripwire.
// ---------------------------------------------------------------------------

import { SystemWallClock } from "../../shared/wall-clock.ts";
import { WorkflowDeadlineError, WorkflowError } from "../durable-pipeline-error.ts";
import type { StepDefinition } from "../durable-pipeline.ts";
import { createReadyTracker, type DagNode } from "../workflow-dag.ts";
import type { WorkflowState } from "../workflow-state.ts";
import type {
  DagExecutionContext,
  DagExecutionResult,
  WaveOutcome,
  WaveParams,
} from "./dag-context.ts";
import { runExecutorWave } from "./executor-wave.ts";
import { fireHook } from "./hooks.ts";
import { runInlineWave } from "./inline-wave.ts";
import { runDispatchedSteps } from "./remote-dispatch.ts";
import { assertRunActive } from "./run-status.ts";
import { errorMessage } from "./step-body.ts";

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
  const stepsByName = new Map<string, StepDefinition>();
  for (const step of ctx.steps) stepsByName.set(step.name, step);
  const remoteSet = ctx.dispatch ? new Set(ctx.dispatch.remoteSteps ?? []) : undefined;

  // Load previously completed step results. The stored shape is always the
  // codec's encoded form (written by saveStepResult), so we decode
  // through the step's codec here so downstream steps see the same shape
  // they would on a fresh run. Skip step rows that aren't declared in the
  // DAG — e.g., synthetic `<loop>.iter.<n>` rows written by `.dowhile()`,
  // or orphans from a prior version's topology. Those rows stay in
  // storage for observability but must not count as DAG progress, or the
  // completed-count gate would skip the real step.
  if (state) {
    for (const [stepName, stepState] of Object.entries(state.steps)) {
      if (stepState.status !== "completed") continue;
      const stepDef = stepsByName.get(stepName);
      if (!stepDef) continue;
      results[stepName] = stepDef.codec.decode(stepState.result);
    }
  }

  // Every wave settles before the next ready set is read, so no step is
  // ever in flight then: the ready set is every not-yet-completed step
  // whose dependencies have completed, in definition order.
  const tracker = createReadyTracker({ nodes: dagNodes, completed: Object.keys(results) });

  for (let wave = 0; tracker.completedCount < ctx.steps.length; wave++) {
    // Between waves: stop on a lost lock or a cancel that landed during the
    // last wave. The caller checked the run before the first one.
    if (wave > 0) await assertRunActive({ storage: ctx.storage, workflowId, signal: ctx.signal });

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

    const ready = tracker.ready();

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
    const localReady: string[] = [];
    const dispatchReady: string[] = [];

    for (const name of ready) {
      if (remoteSet?.has(name)) {
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
        stepsByName,
        results,
        markCompleted: tracker.markCompleted,
      });
      if (dispatchFailure) return dispatchFailure;
    }

    // If all ready steps were dispatched, skip local execution
    if (localReady.length === 0) continue;

    // Execute local ready steps in parallel, with per-step retry and failure handling
    const readySteps = localReady.map((name) => stepsByName.get(name)!);

    const waveParams: WaveParams = {
      ctx,
      workflowId,
      input,
      readySteps,
      results,
      stepAttempts: params.stepAttempts,
      clock,
      stepStates: state?.steps ?? {},
      stepStarted: notifyStepsStarted({ ctx, workflowId, readySteps }),
    };
    // Every step of the wave has settled and been checkpointed by here.
    const { outcomes }: WaveOutcome = ctx.stepExecutor
      ? await runExecutorWave(waveParams)
      : await runInlineWave(waveParams);

    // Fold completed steps back in. `result` is the codec-encoded form
    // storage keeps; downstream steps and the onStepComplete hook see the
    // round-tripped decoded form so fresh-run and replay paths are
    // identical. Siblings of a failed step count too: their rows are
    // `completed`, so compensation reverses them and a workflow retry does
    // not run them again.
    let tripwireFire: { stepName: string; reason: unknown } | null = null;
    for (const outcome of outcomes) {
      if (outcome.kind !== "completed") continue;
      const { name, result, metadata, durationMs } = outcome;
      const stepDef = stepsByName.get(name);
      const decoded = stepDef ? stepDef.codec.decode(result) : result;
      if (outcome.skipped !== true) {
        await fireHook({
          hooks: ctx.hooks,
          name: "onStepComplete",
          event: { workflowId, stepName: name, result: decoded, durationMs },
        });
      }
      results[name] = decoded;
      tracker.markCompleted(name);

      // Tripwire detection: a `.tripwire()` step signals termination by
      // writing `{ tripwireFired: true, reason }` to its metadata. The step
      // row shows `status: completed` with the reason as its result, so
      // ops can still query the step history. Ends the run after the wave.
      if (
        tripwireFire === null &&
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

    // A failure outranks control flow: each failed step has its own
    // failure row, and the run fails with the first one in ready order.
    let firstFailure: unknown = undefined;
    let failed = false;
    for (const outcome of outcomes) {
      if (outcome.kind !== "failed") continue;
      await fireHook({
        hooks: ctx.hooks,
        name: "onStepFailure",
        event: {
          workflowId,
          stepName: outcome.name,
          error: errorMessage(outcome.error),
          durationMs: outcome.durationMs,
        },
      });
      if (!failed) firstFailure = outcome.error;
      failed = true;
    }
    if (failed) return { success: false, error: firstFailure, suspension: false };

    // Continue-as-new unwinds cleanly — no compensation, no failure
    // recording. The orchestration wrapper catches the error and chains a
    // fresh run.
    const continued = outcomes.find((o) => o.kind === "continue-as-new");
    if (continued) {
      return { success: false, error: continued.error, suspension: false, continueAsNew: true };
    }

    // Suspension propagates without failing the workflow.
    const suspended = outcomes.find((o) => o.kind === "suspended");
    if (suspended) return { success: false, error: suspended.error, suspension: true };

    if (tripwireFire) {
      return { success: false, tripwire: true, ...tripwireFire };
    }

    // Check workflow-level deadline after steps complete
    if (
      params.deadlineMs != null &&
      clock.currentTimeMs() > params.deadlineMs &&
      tracker.completedCount < ctx.steps.length
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

/**
 * Emit `step-started` for every step of a wave, all at once, and return
 * each step's pending notice. The wave does not wait for the notices to
 * start its steps; it waits for a step's own notice before that step's
 * outcome is written (inline) or before handing the step to an executor
 * that may persist it itself, so a subscriber still sees a step's
 * `step-started` before its `step-completed` / `step-failed`.
 *
 * Notification is advisory: a notice that fails is reported and resolves,
 * and never fails the run. `undefined` when the storage has no
 * `notifyStepStarted` (polling-only callers see no `step-started`).
 */
function notifyStepsStarted(params: {
  ctx: DagExecutionContext;
  workflowId: string;
  readySteps: readonly StepDefinition[];
}): ReadonlyMap<string, Promise<void>> | undefined {
  const { ctx, workflowId } = params;
  const storage = ctx.storage;
  if (typeof storage.notifyStepStarted !== "function") return undefined;
  const report = (stepName: string, error: unknown): void => {
    console.warn(
      `[workflow] notifyStepStarted failed for step "${stepName}" of "${workflowId}":`,
      error,
    );
  };
  const notices = new Map<string, Promise<void>>();
  for (const { name } of params.readySteps) {
    let notice: Promise<void>;
    try {
      notice = Promise.resolve(storage.notifyStepStarted(workflowId, name)).then(
        () => undefined,
        (error: unknown) => report(name, error),
      );
    } catch (error) {
      report(name, error);
      notice = Promise.resolve();
    }
    notices.set(name, notice);
  }
  return notices;
}
