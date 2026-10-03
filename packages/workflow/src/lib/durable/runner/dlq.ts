// ---------------------------------------------------------------------------
// Dead-letter queue — publishes a `FailedWorkflowRecord` once a workflow has
// exhausted its retries and compensation.
// ---------------------------------------------------------------------------

import type { Sinkable } from "../../shared/streamable.ts";
import { SystemWallClock, type WallClock } from "../../shared/wall-clock.ts";
import type { WorkflowStorage } from "../workflow-storage.ts";
import type { FailedWorkflowRecord } from "../workflow-state.ts";

/**
 * Publish a dead-letter record after a workflow exhausts retries +
 * compensation. Load the final state (so the DLQ record reflects the
 * last-checkpointed steps), build the record, and hand it to the sink.
 *
 * Failures are swallowed — a broken DLQ sink shouldn't mask the
 * original error.
 */
export async function publishDlqRecord(params: {
  dlq: Sinkable<FailedWorkflowRecord>;
  storage: WorkflowStorage;
  workflowId: string;
  workflowName: string;
  input: unknown;
  errorMsg: string;
  compensationReport: {
    compensated: string[];
    failed: { stepName: string; error: unknown }[];
  };
  metadata?: Record<string, unknown>;
  /** Time source for the `failedAt` timestamp. Default: SystemWallClock. */
  clock?: WallClock;
}): Promise<void> {
  const clock = params.clock ?? SystemWallClock;
  try {
    const failedState = await params.storage.loadWorkflow(params.workflowId);
    await params.dlq.publish({
      workflowId: params.workflowId,
      workflowName: params.workflowName,
      input: params.input,
      error: params.errorMsg,
      failedAt: clock.now(),
      steps: failedState?.steps ?? {},
      compensatedSteps: params.compensationReport.compensated,
      failedCompensations: params.compensationReport.failed.map((f) => ({
        stepName: f.stepName,
        error: f.error instanceof Error ? f.error.message : String(f.error),
      })),
      metadata: params.metadata,
    });
  } catch {
    // DLQ failure is swallowed — the original error is more important.
  }
}
