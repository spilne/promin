// ---------------------------------------------------------------------------
// Stream → Workflow trigger
// ---------------------------------------------------------------------------

import { Effect, Stream } from "effect";
import type { TaggedError } from "@promin/core";
import { StreamPipeline } from "@promin/core";
import type { WorkflowDefinition } from "./durable-pipeline.ts";

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
 * Bridge a stream to workflow execution.
 * Each stream item triggers a workflow instance.
 * Returns a stream transformer for use with `.through()`.
 *
 * @example
 * ```ts
 * eventStream
 *   .through(trigger({
 *     workflow: myWorkflow,
 *     toInput: (event) => ({ url: event.data }),
 *     toWorkflowId: (event) => `process-${event.id}`,
 *     concurrency: 5,
 *     onDuplicate: "skip",
 *   }))
 *   .forEach((result) => log(result));
 * ```
 */
export function trigger<T, Input, Output>(params: {
  workflow: WorkflowDefinition<Input, Output>;
  toInput: (item: T) => Input;
  toWorkflowId: (item: T) => string;
  concurrency?: number;
  onDuplicate?: "skip" | "queue" | "fail";
}): <E extends TaggedError>(
  stream: StreamPipeline<T, E>,
) => StreamPipeline<WorkflowResult<Output>, E> {
  const { workflow, toInput, toWorkflowId, concurrency = 1, onDuplicate = "fail" } = params;

  return <E extends TaggedError>(
    stream: StreamPipeline<T, E>,
  ): StreamPipeline<WorkflowResult<Output>, E> => {
    const mapped = Stream.mapEffect(
      stream.stream,
      (item) =>
        Effect.promise(async (): Promise<WorkflowResult<Output>> => {
          const workflowId = toWorkflowId(item);
          const input = toInput(item);
          const startTime = Date.now();

          // Dedup check: see if workflow already exists
          if (onDuplicate === "skip") {
            const existing = await workflow.storage.loadWorkflow(workflowId);
            if (existing) {
              if (
                existing.status === "completed" ||
                existing.status === "pending" ||
                existing.status === "running"
              ) {
                return WorkflowResult.skipped({ workflowId, reason: "duplicate" });
              }
            }
          }

          const { data, error } = await workflow.runSafe({ workflowId, input });

          if (error) {
            return WorkflowResult.failed({
              workflowId,
              error,
              durationMs: Date.now() - startTime,
            });
          }

          return WorkflowResult.completed({
            workflowId,
            result: data as Output,
            durationMs: Date.now() - startTime,
          });
        }),
      { concurrency },
    );

    return new StreamPipeline(mapped);
  };
}
