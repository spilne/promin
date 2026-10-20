// ---------------------------------------------------------------------------
// Stub workflows — a runnable `Workflow` rebuilt from a persisted DAG when the
// original definition object isn't at hand (crash recovery without a
// registry, trigger handlers that only hold an advertised DAG).
//
// Ordinary steps run on workers, so their stub bodies are unreachable. Sleep
// and signal-wait steps run in-process on the coordinator; their stub bodies
// can resume a wait the real definition already started (the wake time,
// signal name and deadline are on the stored step row) but can't start one,
// because the duration / signal name only exist in the definition.
// Subworkflow (`child`) steps also run on the coordinator, but the child
// definition, its input and its id only exist in the parent's definition,
// so their stub bodies fail the step with an error saying so.
// ---------------------------------------------------------------------------

import { die, fail, succeed, type Eff, type Throws } from "@spilne/perfect-core";
import { LosslessJsonCodec } from "@spilne/perfect-core/connect";
import type {
  ExecuteParams,
  StepDefinition,
  Workflow,
  WorkflowDAG,
} from "../durable/durable-pipeline.ts";
import { WorkflowSuspendedError, WorkflowTimeoutError } from "../durable/durable-pipeline-error.ts";
import type { StepState } from "../durable/workflow-state.ts";
import { promiseOrDie } from "../shared/eff.ts";
import type { TaggedError } from "../shared/tagged-error.ts";
import { SystemWallClock } from "../shared/wall-clock.ts";

type StubOutcome = { readonly value: unknown } | { readonly error: TaggedError };

/**
 * Build a Workflow from a persisted DAG. Ordinary step bodies throw (the
 * distributed runner sends them to workers); `sleep` and `signal` steps get
 * resume-only bodies and `child` steps fail (see the file header).
 */
export function buildStubWorkflow(
  dag: WorkflowDAG,
  name: string,
  version?: string,
): Workflow<unknown, unknown> {
  const steps: StepDefinition[] = dag.steps.map((node) => ({
    name: node.name,
    dependsOn: [...node.dependsOn],
    kind: node.kind as StepDefinition["kind"],
    execute: stubBody({ stepName: node.name, kind: node.kind, dependsOn: node.dependsOn }),
    codec: LosslessJsonCodec,
    needs: node.needs,
    priority: node.priority,
  }));

  return {
    name,
    version,
    dag,
    _definition: {
      steps,
      onVersionMismatch: "strict",
    },
  };
}

function stubBody(params: {
  readonly stepName: string;
  readonly kind: string;
  readonly dependsOn: readonly string[];
}): StepDefinition["execute"] {
  const { stepName, kind, dependsOn } = params;
  if (kind === "sleep" || kind === "signal") {
    return (exec) => {
      const decide = kind === "sleep" ? resumeSleep : resumeSignal;
      return promiseOrDie(() => decide({ exec, stepName, dependsOn })).flatMap(
        (outcome): Eff<unknown, Throws<TaggedError>> =>
          "error" in outcome ? fail(outcome.error) : succeed(outcome.value),
      );
    };
  }
  if (kind === "child") {
    return () =>
      die(
        new Error(
          `stub workflow step "${stepName}" runs a child workflow, which only the real ` +
            `definition knows; register the workflow definition so the run can start it`,
        ),
      );
  }
  return () => {
    throw new Error(
      `unreachable: stub workflow step "${stepName}" should never be executed in-process`,
    );
  };
}

async function loadStepState(
  exec: ExecuteParams,
  stepName: string,
): Promise<StepState | undefined> {
  if (exec.stepState !== undefined) return exec.stepState ?? undefined;
  const state = await exec.storage.loadWorkflow(exec.workflowId);
  return state?.steps[stepName];
}

function cannotStart(stepName: string, what: string): never {
  throw new Error(
    `stub workflow step "${stepName}" can only resume a ${what} the real definition started; ` +
      `register the workflow definition so the run can start it`,
  );
}

async function resumeSleep(params: {
  readonly exec: ExecuteParams;
  readonly stepName: string;
  readonly dependsOn: readonly string[];
}): Promise<StubOutcome> {
  const { exec, stepName, dependsOn } = params;
  const clock = exec.clock ?? SystemWallClock;
  const step = await loadStepState(exec, stepName);
  if (step?.status !== "sleeping" || !step.wakeAt) return cannotStart(stepName, "sleep");
  if (clock.now() >= step.wakeAt) {
    const prevStepName = dependsOn[0];
    return { value: prevStepName != null ? exec.results[prevStepName] : exec.input };
  }
  return {
    error: new WorkflowSuspendedError({
      workflowId: exec.workflowId,
      stepName,
      reason: "sleep",
      message: `Sleeping until ${step.wakeAt.toISOString()}`,
    }),
  };
}

async function resumeSignal(params: {
  readonly exec: ExecuteParams;
  readonly stepName: string;
}): Promise<StubOutcome> {
  const { exec, stepName } = params;
  const clock = exec.clock ?? SystemWallClock;
  const step = await loadStepState(exec, stepName);
  if (step?.status !== "waiting_for_signal" || step.signalName === undefined) {
    return cannotStart(stepName, "signal wait");
  }
  const signalName = step.signalName;
  const signals = await exec.storage.loadSignals(exec.workflowId);
  const signal = signals.find((s) => s.signalName === signalName);
  if (signal) return { value: LosslessJsonCodec.decode(signal.payload) };

  if (step.signalTimeoutAt && clock.now() >= step.signalTimeoutAt) {
    return {
      error: new WorkflowTimeoutError({
        workflowId: exec.workflowId,
        stepName,
        message: `Signal "${signalName}" timed out`,
      }),
    };
  }
  await exec.storage.suspendWorkflow(
    exec.workflowId,
    stepName,
    {
      status: "waiting_for_signal",
      stepType: "signal",
      signalName,
      signalTimeoutAt: step.signalTimeoutAt,
    },
    exec.guard,
  );
  return {
    error: new WorkflowSuspendedError({
      workflowId: exec.workflowId,
      stepName,
      reason: "signal",
      message: `Waiting for signal "${signalName}"`,
    }),
  };
}
