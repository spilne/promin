// ---------------------------------------------------------------------------
// `.subworkflow()` — run a child workflow as one step of the parent.
// ---------------------------------------------------------------------------

import { die, tryPromise } from "@spilne/perfect-core";
import type { Codec } from "@spilne/perfect-core/connect";
import type { TaggedError } from "../../shared/tagged-error.ts";
import { suspendOnChild } from "../child-wake.ts";
import { StepError } from "../durable-pipeline-error.ts";
import { readPrev, type StepDefinition, type SubworkflowOptions } from "../step-definition.ts";
import { isControlFlowExit, toStepPolicy } from "../step-policy.ts";
import type { Workflow } from "../workflow-types.ts";

export function createSubworkflowStep(params: {
  readonly name: string;
  readonly dependsOn: string[];
  readonly child: Workflow<unknown, unknown>;
  readonly input: (prev: never) => unknown;
  readonly workflowId: (prev: never) => string;
  readonly options: SubworkflowOptions<unknown> | undefined;
  readonly codec: Codec<unknown>;
}): StepDefinition {
  const { name, dependsOn, child } = params;
  const inputOf = params.input as (prev: unknown) => unknown;
  const workflowIdOf = params.workflowId as (prev: unknown) => string;
  return {
    name,
    dependsOn,
    kind: "child",
    codec: params.codec,
    ...toStepPolicy(params.options),
    execute: (execParams) => {
      const prev = readPrev({ dependsOn, results: execParams.results, input: execParams.input });
      const childWorkflowId = workflowIdOf(prev);
      const childInput = inputOf(prev);
      const runChild = execParams.runChild;
      if (!runChild) {
        return die(
          new Error(
            `subworkflow "${name}": no \`runChild\` in ExecuteParams. Run the parent ` +
              `through a WorkflowRunner (or pass \`runChild\` when executing the step by hand).`,
          ),
        );
      }

      // The runner creates the child row with the parent pointer and the
      // child's version before running it, so `listWorkflows({ parentId })`
      // and recovery see the relationship and the version check compares
      // against the version the child was started with. Creation is
      // create-if-absent: an existing child row (with its original
      // version) is resumed.
      //
      // A child that fails is a typed `StepError` on this step, so step
      // retry (which re-drives the same child run) and `onFailure` apply.
      // A child that suspends parks this step as a wait on the child (see
      // `suspendOnChild`), so the parent is re-driven at the child's wake
      // time or once the child ends. Other engine control flow from the
      // child (a lost lock) propagates unchanged.
      return tryPromise(
        () =>
          runChild({
            workflow: child,
            workflowId: childWorkflowId,
            input: childInput,
          }).catch(async (err: unknown) => {
            if ((err as { _tag?: unknown } | null)?._tag !== "WorkflowSuspendedError") throw err;
            throw await suspendOnChild({
              storage: execParams.storage,
              workflowId: execParams.workflowId,
              stepName: name,
              childWorkflowId,
              childError: err,
              guard: execParams.guard,
            });
          }),
        (err): TaggedError =>
          isControlFlowExit(err)
            ? (err as TaggedError)
            : new StepError({
                workflowId: execParams.workflowId,
                stepName: name,
                message: `subworkflow "${name}" (child "${childWorkflowId}") failed: ${
                  err instanceof globalThis.Error ? err.message : String(err)
                }`,
                cause: err,
              }),
      );
    },
  };
}
