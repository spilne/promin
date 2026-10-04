// ---------------------------------------------------------------------------
// `.journaled()` — a generator body whose activities are journaled, so a
// retry or replay of the step re-runs only the activities not yet recorded.
// The journal itself lives in `journaled-step.ts`; this is the step shell.
// ---------------------------------------------------------------------------

import { tryPromise } from "@spilne/perfect-core";
import type { Codec } from "@spilne/perfect-core/connect";
import type { TaggedError } from "../../shared/tagged-error.ts";
import { isActivityJournalStorage, type ActivityJournalStorage } from "../activity-journal.ts";
import {
  JournalStorageMissingError,
  runJournaledStep,
  type JournaledStepBody,
} from "../journaled-step.ts";
import { readPrev, type JournaledStepOptions, type StepDefinition } from "../step-definition.ts";
import { toStepPolicy } from "../step-policy.ts";
import type { WorkflowStorage } from "../workflow-storage.ts";

export function createJournaledStep(params: {
  readonly name: string;
  readonly dependsOn: string[];
  readonly body: JournaledStepBody<unknown, unknown, unknown>;
  readonly options: JournaledStepOptions<unknown> | undefined;
  readonly codec: Codec<unknown>;
  /** Workflow-level default for `ActivityOptions.payloadHash`. */
  readonly payloadHash: boolean | undefined;
}): StepDefinition {
  const { name, dependsOn, body, codec, payloadHash } = params;
  // Storage capability check is deferred to execute time — the builder has
  // no storage of its own; validation runs against the runner's storage
  // via `execParams.storage`.
  const getJournalStorage = (
    runtimeStorage: WorkflowStorage,
  ): WorkflowStorage & ActivityJournalStorage => {
    if (!isActivityJournalStorage(runtimeStorage)) {
      throw new JournalStorageMissingError(name);
    }
    return runtimeStorage as WorkflowStorage & ActivityJournalStorage;
  };

  return {
    name,
    dependsOn,
    kind: "journaled",
    codec,
    ...toStepPolicy(params.options),
    execute: (execParams) => {
      const prev = readPrev({ dependsOn, results: execParams.results, input: execParams.input });
      // Version and patches come from the runner (the definition driving
      // the run), not from the builder: `.version()` returns a new builder
      // that shares this step object, so a closure over builder state would
      // miss any config set later in the chain, and a version-drained run
      // must see the older definition's values.
      const workflowVersion = execParams.workflowVersion;
      const patches = execParams.patches;
      const runtimeStorage = execParams.storage;
      // Use tryPromise (not a defect-raising promise lift) so any `throw`
      // from the generator body surfaces as a TYPED step failure.
      // Without this, thrown TaggedErrors like WorkflowSuspendedError or
      // LoopLimitExceededError land as defects, which the distributed
      // executor rethrows instead of recording a step failure. The
      // `async` thunk turns synchronous throws (a missing journal
      // storage) into rejections, and `err => err` preserves the
      // original error instance so downstream tag checks
      // (WorkflowSuspendedError handling, etc.) keep working.
      return tryPromise(
        async () =>
          runJournaledStep<unknown, unknown, unknown>({
            input: execParams.input,
            prev,
            workflowId: execParams.workflowId,
            stepName: name,
            storage: getJournalStorage(runtimeStorage),
            workflowStorage: runtimeStorage,
            workflowVersion,
            patches,
            codec,
            payloadHash,
            ...(execParams.clock !== undefined && { clock: execParams.clock }),
            ...(execParams.guard !== undefined && { guard: execParams.guard }),
            ...(execParams.runChild !== undefined && { runChild: execParams.runChild }),
            body,
          }),
        (err) => err as TaggedError,
      );
    },
  };
}
