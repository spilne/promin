// ---------------------------------------------------------------------------
// ctx.child — run another workflow inline as a journaled outcome. The
// child's result (or failure) is recorded like an activity's; a child that
// suspends parks this step until the child wakes or ends (`child-wake.ts`).
// ---------------------------------------------------------------------------

import type { JournalExit } from "./activity-journal.ts";
import { suspendOnChild } from "./child-wake.ts";
import type { Workflow } from "./durable-pipeline.ts";
import { WorkflowSuspendedError } from "./durable-pipeline-error.ts";
import { errorTag, rehydrateFailure } from "./journal-exit.ts";
import { journaledBodyScope } from "./journaled-body-scope.ts";
import type { ActivityYield, JournaledContext, JournaledCtxEnv } from "./journaled-context.ts";

/** Build `ctx.child` for one body run. */
export function makeChild(env: JournaledCtxEnv): JournaledContext<unknown, unknown>["child"] {
  const { workflowId, stepName, cursor, workflowStorage, guard, stepCodec, runChild } = env;

  /**
   * The child workflow suspended. Suspend this step too, as a wait that
   * ends with the child (see `suspendOnChild`): the sleep scanner re-drives
   * the parent at the child's earliest wake time, and the child's runner
   * wakes it when the child run ends. Either way the parent's `ctx.child`
   * then resumes the child or reads its outcome.
   */
  async function suspendForChild(params: {
    childWorkflowId: string;
    childError: unknown;
  }): Promise<WorkflowSuspendedError> {
    const { childWorkflowId, childError } = params;
    if (workflowStorage) {
      return suspendOnChild({
        storage: workflowStorage,
        workflowId,
        stepName,
        childWorkflowId,
        childError,
        guard,
      });
    }
    const reason = (childError as { reason?: unknown }).reason === "sleep" ? "sleep" : "signal";
    return new WorkflowSuspendedError({
      workflowId,
      stepName,
      reason,
      message: `waiting for child workflow "${childWorkflowId}"`,
    });
  }

  return function* child<Output>(
    workflow: Workflow<unknown, Output>,
    options?: { readonly input?: unknown; readonly workflowId?: string },
  ): Generator<ActivityYield, Output, Output> {
    const slot = cursor.allocateSlot({ suspendOrChild: true });
    const { activityIndex, branchPath } = slot;
    // Inside a parallel branch the default id also carries the branch path
    // ("/" mapped to "~" so the id stays URL-safe); top-level ids keep the
    // `${workflowId}.${stepName}.${activityIndex}` shape.
    const childWorkflowId =
      options?.workflowId ??
      `${workflowId}.${stepName}.${activityIndex}${branchPath.replaceAll("/", "~")}`;
    const childInput = options?.input;
    const activityName = workflow.name;

    /** Value of a completed exit: a failure rethrows, a success decodes. */
    const settle = (exit: JournalExit): Output => {
      if (exit.tag === "Failure") throw rehydrateFailure(exit);
      return stepCodec.decode(exit.value) as Output;
    };

    const promise = (async (): Promise<Output> => {
      const recorded = cursor.expectRecorded({ slot, kind: "child", name: activityName });
      if (recorded) {
        // Replay — return cached result without re-running the child.
        const phase = recorded.phase ?? "completed";
        if (phase === "completed") {
          if (!recorded.exit) {
            throw new Error(
              `journal entry ${activityIndex} for step "${stepName}" (child: ${activityName}) is completed but has no exit`,
            );
          }
          return settle(recorded.exit);
        }
        // Pending row — previous worker started the child but didn't record
        // the result. Re-running is safe: the child workflow has its own
        // storage row and idempotency, so calling runChild again just resumes
        // it from where it left off.
      }

      if (!runChild) {
        throw new Error(
          `ctx.child("${activityName}"): requires a \`runChild\` callback to be provided ` +
            `to runJournaledStep. When using WorkflowRunner, this is wired automatically. ` +
            `If you are calling runJournaledStep directly from tests, pass a stub runChild.`,
        );
      }

      const outcome = await cursor.recordOutcome({
        slot,
        kind: "child",
        name: activityName,
        run: async () =>
          (await journaledBodyScope.exit(async () =>
            runChild({
              workflow: workflow as Workflow<unknown, unknown>,
              workflowId: childWorkflowId,
              input: childInput,
            }),
          )) as Output,
        encode: (result) => stepCodec.encode(result),
        passThrough: async (err) => {
          const tag = errorTag(err);
          if (tag === "WorkflowSuspendedError") {
            // The child is parked on a sleep or signal: not an outcome. The
            // entry stays pending, so the parent's next run calls runChild
            // again, which resumes the child.
            throw await suspendForChild({ childWorkflowId, childError: err });
          }
          // Another worker holds the child: nothing happened that the journal
          // should remember.
          if (tag === "WorkflowLockError" || tag === "FenceTokenMismatchError") throw err;
        },
      });
      if (outcome.kind === "failed") throw outcome.error;
      return settle(outcome.exit);
    })();

    return yield { _tag: "Activity", name: activityName, promise };
  };
}
