// ---------------------------------------------------------------------------
// Compensation — the saga rollback run after a workflow exhausts its
// retries: reverses completed steps that carry a `compensate` function and
// records one attempt row per try.
// ---------------------------------------------------------------------------

import type { Eff, Throws } from "@spilne/perfect-core";
import { runHookResult } from "../../shared/eff.ts";
import { SystemWallClock, type WallClock } from "../../shared/wall-clock.ts";
import type { CompensateConfig } from "../durable-pipeline.ts";
import {
  isStepAttemptStorage,
  type FenceGuard,
  type WorkflowStorage,
} from "../workflow-storage.ts";
import type { DagNode } from "../workflow-dag.ts";

/**
 * A step definition's view as needed by compensation — just the name and
 * the rollback function. A full `StepDefinition` is a superset and passes
 * this check via structural typing; keeps compensation decoupled from the
 * rest of the step shape (execute / codec / retry / etc.).
 */
export interface CompensatableStep {
  readonly name: string;
  readonly compensate?: (params: {
    result: unknown;
    input: unknown;
    workflowId: string;
  }) => Eff<unknown, Throws<unknown>> | Promise<void>;
}

/**
 * Run the saga rollback for a workflow — reverses completed steps that
 * carry a `compensate` function, in reverse definition order, honoring
 * the compensate config's per-step retry policy. Records an attempt row
 * per try when the storage supports `StepAttemptStorage`.
 */
export async function compensateWorkflow(params: {
  storage: WorkflowStorage;
  steps: ReadonlyArray<CompensatableStep>;
  compensateConfig?: CompensateConfig;
  workflowId: string;
  input: unknown;
  dagNodes: DagNode[];
  /** Optional fence guard — threaded to `saveStepAttempt` writes so a stale holder's compensation rows are rejected. */
  guard?: FenceGuard;
  /** Time source. Drives compensation retry backoff + attempt timestamps. Default: SystemWallClock. */
  clock?: WallClock;
  /** Executor id stamped onto each compensation StepAttemptRecord. */
  executorId?: string;
}): Promise<{
  compensated: string[];
  failed: { stepName: string; error: unknown }[];
}> {
  const { storage, steps, compensateConfig, workflowId, input, guard, executorId } = params;
  const clock = params.clock ?? SystemWallClock;
  const compensated: string[] = [];
  const failed: { stepName: string; error: unknown }[] = [];

  const state = await storage.loadWorkflow(workflowId);
  if (!state) return { compensated, failed };

  // Reverse definition order — saga semantics. For a chain this is also
  // reverse completion order; for parallel branches it is not.
  const stepsToCompensate: { stepDef: CompensatableStep; result: unknown }[] = [];
  for (let i = steps.length - 1; i >= 0; i--) {
    const stepDef = steps[i]!;
    const stepState = state.steps[stepDef.name];
    if (stepState?.status === "completed" && stepDef.compensate) {
      stepsToCompensate.push({ stepDef, result: stepState.result });
    }
  }

  const retryConfig = compensateConfig?.retry;
  const maxCompRetries = retryConfig?.maxRetries ?? 0;
  const compRetryDelayMs = retryConfig?.baseDelayMs ?? 500;

  const attemptStorage = isStepAttemptStorage(storage) ? storage : undefined;

  for (const { stepDef, result } of stepsToCompensate) {
    for (let attempt = 0; attempt <= maxCompRetries; attempt++) {
      const compStartedAt = clock.now();
      try {
        if (attempt > 0) {
          await new Promise((r) =>
            clock.setTimeout(() => r(undefined), compRetryDelayMs * Math.pow(2, attempt - 1)),
          );
        }
        await runHookResult(stepDef.compensate!({ result, input, workflowId }));
        compensated.push(stepDef.name);
        if (attemptStorage) {
          await attemptStorage.saveStepAttempt(
            {
              workflowId,
              stepName: stepDef.name,
              attempt: attempt + 1,
              type: "compensation",
              status: "completed",
              durationMs: clock.currentTimeMs() - compStartedAt.getTime(),
              startedAt: compStartedAt,
              completedAt: clock.now(),
              ...(executorId !== undefined && { executorId }),
            },
            guard,
          );
        }
        break;
      } catch (err) {
        if (attemptStorage) {
          await attemptStorage.saveStepAttempt(
            {
              workflowId,
              stepName: stepDef.name,
              attempt: attempt + 1,
              type: "compensation",
              status: "failed",
              error: err instanceof Error ? err.message : String(err),
              durationMs: clock.currentTimeMs() - compStartedAt.getTime(),
              startedAt: compStartedAt,
              completedAt: clock.now(),
              ...(executorId !== undefined && { executorId }),
            },
            guard,
          );
        }
        if (attempt === maxCompRetries) {
          failed.push({ stepName: stepDef.name, error: err });
        }
      }
    }
  }

  return { compensated, failed };
}
