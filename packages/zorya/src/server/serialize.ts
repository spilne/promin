// ---------------------------------------------------------------------------
// Serialisers — WorkflowState / StepState → wire DTOs
// ---------------------------------------------------------------------------

import type { WorkflowState, StepState, StepTaskState } from "@promin/workflow";
import type { RunDto, RunSummaryDto, StepDto, StepTaskDto } from "./api-types.ts";

function iso(d?: Date): string | undefined {
  return d ? d.toISOString() : undefined;
}

function taskToDto(t: StepTaskState): StepTaskDto {
  return {
    taskIndex: t.taskIndex,
    status: t.status,
    input: t.input,
    result: t.result,
    error: t.error,
    startedAt: iso(t.startedAt),
    completedAt: iso(t.completedAt),
    attempt: t.attempt,
  };
}

export function stepToDto(s: StepState): StepDto {
  return {
    stepName: s.stepName,
    run: s.run,
    status: s.status,
    stepType: s.stepType,
    dependsOn: s.dependsOn,
    result: s.result,
    error: s.error,
    startedAt: iso(s.startedAt),
    completedAt: iso(s.completedAt),
    durationMs: s.durationMs,
    attempt: s.attempt,
    wakeAt: iso(s.wakeAt),
    signalName: s.signalName,
    signalTimeoutAt: iso(s.signalTimeoutAt),
    metadata: s.metadata,
    tasks: s.tasks ? s.tasks.map(taskToDto) : undefined,
    compensationStatus: s.compensationStatus,
    compensationError: s.compensationError,
    compensatedAt: iso(s.compensatedAt),
  };
}

export function runToDto(w: WorkflowState): RunDto {
  return {
    workflowId: w.workflowId,
    workflowName: w.workflowName,
    workflowType: w.workflowType,
    namespace: w.namespace,
    status: w.status,
    version: w.version,
    run: w.run,
    input: w.input,
    result: w.result,
    error: w.error,
    metadata: w.metadata,
    steps: Object.values(w.steps).map(stepToDto),
    parentWorkflowId: w.parentWorkflowId,
    createdAt: w.createdAt.toISOString(),
    startedAt: iso(w.startedAt),
    updatedAt: w.updatedAt.toISOString(),
    completedAt: iso(w.completedAt),
  };
}

export function runToSummaryDto(w: WorkflowState): RunSummaryDto {
  const totalMs = w.completedAt ? w.completedAt.getTime() - w.createdAt.getTime() : undefined;
  return {
    workflowId: w.workflowId,
    workflowName: w.workflowName,
    workflowType: w.workflowType,
    namespace: w.namespace,
    status: w.status,
    run: w.run,
    createdAt: w.createdAt.toISOString(),
    startedAt: iso(w.startedAt),
    completedAt: iso(w.completedAt),
    updatedAt: w.updatedAt.toISOString(),
    totalMs,
  };
}
