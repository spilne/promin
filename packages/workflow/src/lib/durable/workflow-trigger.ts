// ---------------------------------------------------------------------------
// Stream → Workflow trigger
// ---------------------------------------------------------------------------

import { tryPromise, type Eff, type Pipe } from "@spilne/perfect-core";
import type { Workflow } from "./workflow-types.ts";
import type { WorkflowRunner } from "./workflow-runner.ts";
import type { WorkflowStorage } from "./workflow-storage.ts";
import { SystemWallClock, type WallClock } from "../shared/wall-clock.ts";

// ---------------------------------------------------------------------------
// WorkflowResult ADT
// ---------------------------------------------------------------------------

export type WorkflowResult<T> =
  | WorkflowResult.Completed<T>
  | WorkflowResult.Failed
  | WorkflowResult.Skipped;

export namespace WorkflowResult {
  export interface Completed<T> {
    readonly _tag: "completed";
    readonly workflowId: string;
    readonly result: T;
    readonly durationMs: number;
  }

  export interface Failed {
    readonly _tag: "failed";
    readonly workflowId: string;
    readonly error: unknown;
    readonly durationMs: number;
  }

  export interface Skipped {
    readonly _tag: "skipped";
    readonly workflowId: string;
    readonly reason: "duplicate";
  }

  export const completed = <T>(params: {
    workflowId: string;
    result: T;
    durationMs: number;
  }): Completed<T> => ({ _tag: "completed", ...params });

  export const failed = (params: {
    workflowId: string;
    error: unknown;
    durationMs: number;
  }): Failed => ({ _tag: "failed", ...params });

  export const skipped = (params: { workflowId: string; reason: "duplicate" }): Skipped => ({
    _tag: "skipped",
    ...params,
  });

  export const isCompleted = <T>(r: WorkflowResult<T>): r is Completed<T> => r._tag === "completed";
  export const isFailed = <T>(r: WorkflowResult<T>): r is Failed => r._tag === "failed";
  export const isSkipped = <T>(r: WorkflowResult<T>): r is Skipped => r._tag === "skipped";
}

// ---------------------------------------------------------------------------
// trigger() — stream transformer
// ---------------------------------------------------------------------------

/**
 * What `trigger` does with an item whose workflow id already has a run:
 * - `"run"` (default) hands it to the runner anyway, which resumes or
 *   returns the existing run under that id.
 * - `"skip"` emits `WorkflowResult.Skipped` without touching the run,
 *   whatever its status.
 */
export type TriggerDuplicatePolicy = "skip" | "run";

/** Parameters of `trigger`. */
export interface TriggerParams<T, Input, Output> {
  readonly workflow: Workflow<Input, Output>;
  /** Runner used to execute each workflow instance. */
  readonly runner: WorkflowRunner;
  /**
   * Storage for the duplicate lookup, typically the runner's storage.
   * Required with `onDuplicate: "skip"`.
   */
  readonly storage?: WorkflowStorage;
  readonly toInput: (item: T) => Input;
  readonly toWorkflowId: (item: T) => string;
  /** Items run concurrently; results keep input order. Default: 1. */
  readonly concurrency?: number;
  /** Default: `"run"`. */
  readonly onDuplicate?: TriggerDuplicatePolicy;
  /** Time source for each result's `durationMs`. Default: `SystemWallClock`. */
  readonly clock?: WallClock;
}

/**
 * Bridge a stream to workflow execution.
 * Each stream item triggers a workflow instance.
 * Returns a perfect `Pipe` for use with `Stream.through()`.
 *
 * A workflow failure, and any error from the duplicate lookup or the
 * runner, comes out as `WorkflowResult.Failed`; the stream keeps going.
 * The duplicate lookup is a read before the run, so two items with the same
 * id in flight at once can both pass it; the runner's run lock still keeps
 * them from executing the run twice.
 *
 * @example
 * ```ts
 * await eventStream
 *   .through(trigger({
 *     workflow: myWorkflow,
 *     runner,
 *     storage,
 *     toInput: (event) => ({ url: event.data }),
 *     toWorkflowId: (event) => `process-${event.id}`,
 *     concurrency: 5,
 *     onDuplicate: "skip",
 *   }))
 *   .tap((result) => log(result))
 *   .drain()
 *   .run();
 * ```
 */
export function trigger<T, Input, Output>(
  params: TriggerParams<T, Input, Output>,
): Pipe<T, WorkflowResult<Output>> {
  const { workflow, runner, storage, toInput, toWorkflowId, concurrency = 1 } = params;
  const onDuplicate = params.onDuplicate ?? "run";
  const clock = params.clock ?? SystemWallClock;
  if (onDuplicate === "skip" && storage === undefined) {
    throw new Error('trigger: onDuplicate "skip" needs `storage` for the duplicate lookup');
  }

  const runOne = async (item: T): Promise<WorkflowResult<Output>> => {
    const startTime = clock.currentTimeMs();
    let workflowId: string | undefined;
    try {
      workflowId = toWorkflowId(item);
      const input = toInput(item);

      if (onDuplicate === "skip" && (await storage!.loadWorkflow(workflowId)) !== null) {
        return WorkflowResult.skipped({ workflowId, reason: "duplicate" });
      }

      const { data, error } = await runner.runSafe({ workflow, workflowId, input });
      if (error) {
        return WorkflowResult.failed({
          workflowId,
          error,
          durationMs: clock.currentTimeMs() - startTime,
        });
      }
      return WorkflowResult.completed({
        workflowId,
        result: data as Output,
        durationMs: clock.currentTimeMs() - startTime,
      });
    } catch (error) {
      return WorkflowResult.failed({
        workflowId: workflowId ?? "",
        error,
        durationMs: clock.currentTimeMs() - startTime,
      });
    }
  };

  // `runOne` never rejects, so the stream only ends when its source does.
  const runOneEff = (item: T): Eff<WorkflowResult<Output>> =>
    tryPromise(
      () => runOne(item),
      (e) => e,
    ).orDie();

  // Ordered: results come out in input order, up to `concurrency` in flight.
  return (stream) => stream.parEvalMap(concurrency, runOneEff);
}
