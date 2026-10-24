// ---------------------------------------------------------------------------
// Wave helpers shared by the inline and executor waves: settle every step
// of a wave, evaluate `skipWhen`, and turn a settled step body into its
// `StepOutcome`.
// ---------------------------------------------------------------------------

import type { WallClock } from "../../shared/wall-clock.ts";
import type { StepDefinition } from "../durable-pipeline.ts";
import { isCancelledRun, type WorkflowStatusSnapshot } from "../workflow-state.ts";
import type { WaveOutcome } from "./dag-context.ts";
import type { StepBodyOutcome } from "./step-body.ts";
import type { CheckpointedStep, StepOutcome } from "./step-checkpoint.ts";

/**
 * Run every ready step and wait for all of them, so no sibling is left in
 * flight (or half-checkpointed) when the wave reports a failure. `runStep`
 * settles body errors into outcomes itself; a rejection means a checkpoint
 * write failed, and the first one is rethrown once every step has settled.
 *
 * The wave's `runStatus` is known only when every step's checkpoint read
 * the run's status; it is a cancelled one if any of them saw a cancel.
 */
export async function settleWave(params: {
  readonly readySteps: readonly StepDefinition[];
  readonly runStep: (stepDef: StepDefinition) => Promise<CheckpointedStep>;
}): Promise<WaveOutcome> {
  const settled = await Promise.allSettled(params.readySteps.map((s) => params.runStep(s)));
  const outcomes: StepOutcome[] = [];
  let runStatus: WorkflowStatusSnapshot | null = null;
  let observed = true;
  for (const entry of settled) {
    if (entry.status === "rejected") throw entry.reason;
    outcomes.push(entry.value.outcome);
    const status = entry.value.runStatus;
    if (status === undefined) observed = false;
    else if (status !== null && (runStatus === null || !isCancelledRun(runStatus))) {
      runStatus = status;
    }
  }
  return observed ? { outcomes, runStatus } : { outcomes };
}

/**
 * The completed outcome of a step whose `skipWhen` matches its input, or
 * `undefined` when the step should run.
 */
export function skippedOutcome(params: {
  readonly stepDef: StepDefinition;
  readonly input: unknown;
  readonly results: Record<string, unknown>;
  readonly clock: WallClock;
  readonly attempt: number;
}): StepOutcome | undefined {
  const { stepDef } = params;
  if (!stepDef.skipWhen) return undefined;
  const prevStepName = stepDef.dependsOn[0];
  const prev = prevStepName != null ? params.results[prevStepName] : params.input;
  if (!stepDef.skipWhen(prev)) return undefined;
  const skipResult = stepDef.skipValue ? stepDef.skipValue(prev) : prev;
  return {
    kind: "completed",
    name: stepDef.name,
    result: stepDef.codec.encode(skipResult),
    startedAt: params.clock.now(),
    durationMs: 0,
    attempt: params.attempt,
    skipped: true,
  };
}

/** The `StepOutcome` of a settled step body that started at `startedAt`. */
export function outcomeOfBody(params: {
  readonly name: string;
  readonly body: StepBodyOutcome;
  readonly startedAt: Date;
  readonly clock: WallClock;
}): StepOutcome {
  const { name, body, startedAt } = params;
  const durationMs = params.clock.currentTimeMs() - startedAt.getTime();
  switch (body.kind) {
    case "completed":
      return {
        kind: "completed",
        name,
        result: body.result,
        metadata: body.metadata,
        startedAt,
        durationMs,
        attempt: body.attempt,
      };
    case "failed":
      return {
        kind: "failed",
        name,
        error: body.error,
        metadata: body.metadata,
        startedAt,
        durationMs,
        attempt: body.attempt,
      };
    case "suspended":
    case "continue-as-new":
      return { kind: body.kind, name, error: body.error };
  }
}
