// ---------------------------------------------------------------------------
// Step progress reporting — the runner's side of `StepProgressStore`: tell
// storage when a step attempt starts running or goes into retry backoff, so
// `getStatus` can list it in `waitingOn` (`activity`, `retry-backoff`).
// ---------------------------------------------------------------------------

import type { TimerHandle, WallClock } from "../../shared/wall-clock.ts";
import type { StepProgress } from "../workflow-state.ts";
import { hasCapability, type FenceGuard, type WorkflowStorage } from "../workflow-storage.ts";

/**
 * How long an attempt has to run, on the run's clock, before its `running`
 * record is written. A step that settles sooner costs no extra write; one
 * that runs longer shows up in `waitingOn` from then on. A `backoff` record,
 * and a `running` one that replaces an earlier record, are written at once.
 */
export const STEP_PROGRESS_DELAY_MS = 1_000;

/**
 * Records one step's progress, in call order, without ever slowing or
 * failing the step: writes run behind each other in the background and a
 * failed one is dropped (progress is advisory; the step's own checkpoint is
 * what the run depends on).
 */
export interface StepProgressReporter {
  /** Queue `progress` for storage (a first `running` record after `STEP_PROGRESS_DELAY_MS`). Returns at once. */
  report(progress: StepProgress): void;
  /**
   * Drops a `running` record still waiting out its delay, then resolves once
   * every queued record is written (or dropped). Awaited before the step's
   * settled write, so a record never lands after it.
   */
  drain(): Promise<void>;
}

/**
 * A reporter for `stepName`, or `undefined` when `storage` does not record
 * progress.
 */
export function createStepProgressReporter(params: {
  readonly storage: WorkflowStorage;
  readonly workflowId: string;
  readonly stepName: string;
  readonly guard?: FenceGuard;
  readonly clock: WallClock;
  /** Default `STEP_PROGRESS_DELAY_MS`. */
  readonly delayMs?: number;
}): StepProgressReporter | undefined {
  const { storage, workflowId, stepName, guard, clock } = params;
  if (!hasCapability(storage, "stepProgress")) return undefined;
  const delayMs = params.delayMs ?? STEP_PROGRESS_DELAY_MS;
  let tail: Promise<void> = Promise.resolve();
  let wrote = false;
  let waiting: TimerHandle | undefined;
  const cancelWaiting = (): void => {
    waiting?.clear();
    waiting = undefined;
  };
  const write = (progress: StepProgress): void => {
    wrote = true;
    tail = tail.then(async () => {
      try {
        await storage.saveStepProgress({
          workflowId,
          stepName,
          progress,
          ...(guard !== undefined && { guard }),
        });
      } catch {
        // Advisory: the step's own checkpoint decides the run's fate.
      }
    });
  };
  return {
    report(progress) {
      // A newer fact replaces one still waiting.
      cancelWaiting();
      if (progress.phase !== "running" || wrote || delayMs <= 0) {
        write(progress);
        return;
      }
      waiting = clock.setTimeout(() => {
        waiting = undefined;
        write(progress);
      }, delayMs);
      waiting.unref?.();
    },
    drain() {
      cancelWaiting();
      return tail;
    },
  };
}
