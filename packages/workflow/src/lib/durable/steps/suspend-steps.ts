// ---------------------------------------------------------------------------
// `.sleep()` and `.waitForSignal()` — steps that park the run (suspend) and
// complete on a later resume: at the stored wake time, or once the signal
// has been delivered (or its stored deadline has passed).
// ---------------------------------------------------------------------------

import { eff, fail } from "@spilne/perfect-core";
import type { Codec } from "@spilne/perfect-core/connect";
import { promiseOrDie } from "../../shared/eff.ts";
import { SystemWallClock } from "../../shared/wall-clock.ts";
import { WorkflowSuspendedError, WorkflowTimeoutError } from "../durable-pipeline-error.ts";
import { currentStepState, readPrev, type StepDefinition } from "../step-definition.ts";

export function createSleepStep(params: {
  readonly name: string;
  readonly dependsOn: string[];
  readonly ms: number;
  /** The predecessor's codec: the checkpointed result is its value. */
  readonly codec: Codec<unknown>;
}): StepDefinition {
  const { name, dependsOn, ms } = params;
  return {
    name,
    dependsOn,
    kind: "sleep",
    codec: params.codec,
    execute: (exec) => {
      const clock = exec.clock ?? SystemWallClock;
      return eff(function* () {
        const stepState = yield* currentStepState({ exec, stepName: name });

        if (stepState?.status === "sleeping" && stepState.wakeAt) {
          if (clock.now() >= stepState.wakeAt) {
            // Pass the predecessor value through so the next linear step's
            // `prev` is what the types promise (`Current`), not `undefined`.
            return readPrev({ dependsOn, results: exec.results, input: exec.input });
          }
          return yield* fail(
            new WorkflowSuspendedError({
              workflowId: exec.workflowId,
              stepName: name,
              reason: "sleep",
              message: `Sleeping until ${stepState.wakeAt.toISOString()}`,
            }),
          );
        }

        const wakeAt = new Date(clock.currentTimeMs() + ms);
        yield* promiseOrDie(() =>
          exec.storage.suspendWorkflow({
            workflowId: exec.workflowId,
            stepName: name,
            stepUpdate: { status: "sleeping", stepType: "sleep", wakeAt },
            guard: exec.guard,
          }),
        );
        return yield* fail(
          new WorkflowSuspendedError({
            workflowId: exec.workflowId,
            stepName: name,
            reason: "sleep",
            message: `Sleeping until ${wakeAt.toISOString()}`,
          }),
        );
      });
    },
  };
}

export function createWaitForSignalStep(params: {
  readonly name: string;
  readonly dependsOn: string[];
  readonly signalName: string;
  readonly timeoutMs: number | undefined;
  readonly codec: Codec<unknown>;
}): StepDefinition {
  const { name, dependsOn, signalName, timeoutMs, codec } = params;
  return {
    name,
    dependsOn,
    kind: "signal",
    codec,
    execute: (execParams) => {
      const clock = execParams.clock ?? SystemWallClock;
      return eff(function* () {
        // Check if signal has been delivered
        const signals = yield* promiseOrDie(() =>
          execParams.storage.loadSignals(execParams.workflowId),
        );
        const signal = signals.find((s) => s.signalName === signalName);

        if (signal) {
          return codec.decode(signal.payload);
        }

        // Check if this is a re-entry while already waiting
        const stepState = yield* currentStepState({ exec: execParams, stepName: name });
        const alreadyWaiting = stepState?.status === "waiting_for_signal";

        if (alreadyWaiting && stepState.signalTimeoutAt) {
          if (clock.now() >= stepState.signalTimeoutAt) {
            return yield* fail(
              new WorkflowTimeoutError({
                workflowId: execParams.workflowId,
                stepName: name,
                message: `Signal "${signalName}" timed out after ${timeoutMs}ms`,
              }),
            );
          }
        }

        // First execution: compute the deadline once. Re-entry while still
        // waiting: keep the stored deadline. Recomputing it here would push
        // it forward on every resume (result polls, signal scans), so a
        // frequently resumed workflow would never time out.
        const signalTimeoutAt = alreadyWaiting
          ? stepState.signalTimeoutAt
          : timeoutMs != null
            ? new Date(clock.currentTimeMs() + timeoutMs)
            : undefined;
        yield* promiseOrDie(() =>
          execParams.storage.suspendWorkflow({
            workflowId: execParams.workflowId,
            stepName: name,
            stepUpdate: {
              status: "waiting_for_signal",
              stepType: "signal",
              signalName,
              signalTimeoutAt,
            },
            guard: execParams.guard,
          }),
        );
        return yield* fail(
          new WorkflowSuspendedError({
            workflowId: execParams.workflowId,
            stepName: name,
            reason: "signal",
            message: `Waiting for signal "${signalName}"`,
          }),
        );
      });
    },
  };
}
