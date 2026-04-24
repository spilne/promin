// ---------------------------------------------------------------------------
// Serialisers — WorkflowState / StepState → wire DTOs
// ---------------------------------------------------------------------------

import type { WorkflowState, StepState, StepTaskState } from "@promin/workflow";
import type {
  ExtendedStepStatus,
  RunDto,
  RunSummaryDto,
  StepDto,
  StepTaskDto,
} from "./api-types.ts";

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
  const steps = Object.values(w.steps).map(stepToDto);
  // Compute "upstream_failed" — a pending step whose dependency already
  // failed won't run, and surfacing it as just "pending" is misleading.
  const byName = new Map(steps.map((s) => [s.stepName, s]));
  for (const s of steps) {
    if (s.status !== "pending") continue;
    const hasFailedDep = s.dependsOn.some((dep) => {
      const upstream = byName.get(dep);
      if (!upstream) return false;
      return (
        upstream.status === "failed" ||
        upstream.status === "compensation_failed" ||
        (upstream.effectiveStatus as ExtendedStepStatus | undefined) === "upstream_failed"
      );
    });
    if (hasFailedDep) s.effectiveStatus = "upstream_failed";
  }
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
    steps,
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
