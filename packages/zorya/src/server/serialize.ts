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
  const { runSource, runSourceId } = deriveRunSource(w);
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
    runSource,
    runSourceId,
    createdAt: w.createdAt.toISOString(),
    startedAt: iso(w.startedAt),
    updatedAt: w.updatedAt.toISOString(),
    completedAt: iso(w.completedAt),
  };
}

export function runToSummaryDto(w: WorkflowState): RunSummaryDto {
  // Use `startedAt` as the duration anchor when it's set — workflow rows
  // can be reused (e.g. scheduler-fired ids surviving across reboots), in
  // which case `createdAt` reflects the FIRST creation hours/days ago.
  // Computing `completedAt - createdAt` would then report the stale gap
  // ("11h 5m") instead of the run's actual execution time. Falling back to
  // `createdAt` only when `startedAt` is missing keeps the metric meaningful
  // for not-yet-started or single-run scenarios.
  const anchor = w.startedAt ?? w.createdAt;
  const totalMs = w.completedAt ? w.completedAt.getTime() - anchor.getTime() : undefined;
  const { runSource, runSourceId } = deriveRunSource(w);
  return {
    workflowId: w.workflowId,
    workflowName: w.workflowName,
    workflowType: w.workflowType,
    namespace: w.namespace,
    status: w.status,
    version: w.version,
    run: w.run,
    runSource,
    runSourceId,
    createdAt: w.createdAt.toISOString(),
    startedAt: iso(w.startedAt),
    completedAt: iso(w.completedAt),
    updatedAt: w.updatedAt.toISOString(),
    totalMs,
  };
}

/**
 * Resolve `runSource` / `runSourceId` for the wire DTO. Prefers the typed
 * columns (`w.runSource`, `w.runSourceId`); when they're absent — e.g. on
 * legacy rows from before the column existed — falls back to inferring
 * from `metadata.scheduleId`, which the scheduler dispatcher has been
 * stamping for longer. Keeps the dashboard's source badge usable on a
 * mixed-vintage history without a write-side migration.
 */
function deriveRunSource(w: WorkflowState): {
  runSource?: WorkflowState["runSource"];
  runSourceId?: string;
} {
  if (w.runSource) return { runSource: w.runSource, runSourceId: w.runSourceId };
  const meta = w.metadata as Record<string, unknown> | undefined;
  const scheduleId =
    typeof meta?.["scheduleId"] === "string" ? (meta["scheduleId"] as string) : undefined;
  if (scheduleId) return { runSource: "schedule", runSourceId: scheduleId };
  return {};
}
