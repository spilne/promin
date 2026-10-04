// ---------------------------------------------------------------------------
// The in-memory run row and the pure helpers over it.
// ---------------------------------------------------------------------------

import { workflowMetadataMatches } from "../storage/metadata.ts";
import type { WorkflowSortFields } from "../storage/ordering.ts";
import type { WorkflowListFilter } from "../storage/query-store.ts";
import type {
  RunSource,
  StepState,
  StepTaskState,
  WorkflowRunSummary,
  WorkflowState,
  WorkflowStatus,
  WorkflowStatusSnapshot,
} from "../workflow-state.ts";

/** Mutable internal workflow state — avoids spread-copy on every mutation. */
export interface MutableWorkflow {
  workflowId: string;
  workflowName: string;
  workflowType?: string;
  parentWorkflowId?: string;
  namespace?: string;
  status: WorkflowStatus;
  version?: string;
  run: number;
  input: unknown;
  result?: unknown;
  error?: string;
  errorTag?: string;
  tripwire?: unknown;
  runSource?: RunSource;
  runSourceId?: string;
  metadata?: Record<string, unknown>;
  idempotencyKey?: string;
  idempotencyExpiresAt?: Date;
  steps: Map<string, StepState>;
  /**
   * Task arrays this storage may still append to in place, by step name
   * (see `writeTaskRow`). An entry is writable only while it is the step
   * row's own `tasks` array and no row of the run was handed out since it
   * was built (`handedOut` still equals its `epoch`).
   */
  ownedTasks: Map<string, OwnedTasks>;
  /** Bumped every time the run's step rows are handed out to a caller. */
  handedOut: number;
  createdAt: Date;
  startedAt?: Date;
  updatedAt: Date;
  completedAt?: Date;
}

/**
 * A map step's task array, owned by the storage, with each task's position
 * by task index: a task write is a lookup plus an in-place set or push
 * instead of a copy and a linear search.
 */
interface OwnedTasks {
  readonly tasks: StepTaskState[];
  readonly position: Map<number, number>;
  readonly epoch: number;
}

/**
 * The run's step rows as a record for a caller. Their `tasks` arrays are
 * now shared with the caller, so the next task write copies them first.
 */
export function handOutSteps(wf: MutableWorkflow): Record<string, StepState> {
  wf.handedOut++;
  const steps: Record<string, StepState> = {};
  for (const [k, v] of wf.steps) steps[k] = v;
  return steps;
}

/** The run as `loadWorkflow` returns it. */
export function toWorkflowState(wf: MutableWorkflow): WorkflowState {
  const steps = handOutSteps(wf);
  return {
    workflowId: wf.workflowId,
    workflowName: wf.workflowName,
    workflowType: wf.workflowType,
    parentWorkflowId: wf.parentWorkflowId,
    namespace: wf.namespace,
    status: wf.status,
    version: wf.version,
    run: wf.run,
    input: wf.input,
    result: wf.result,
    error: wf.error,
    errorTag: wf.errorTag,
    tripwire: wf.tripwire,
    runSource: wf.runSource,
    runSourceId: wf.runSourceId,
    metadata: wf.metadata,
    steps,
    createdAt: wf.createdAt,
    startedAt: wf.startedAt,
    updatedAt: wf.updatedAt,
    completedAt: wf.completedAt,
  };
}

/** The current run as a run-history row. */
export function currentRunSummary(wf: MutableWorkflow): WorkflowRunSummary {
  return {
    run: wf.run,
    version: wf.version,
    status: wf.status,
    result: wf.result,
    error: wf.error,
    tripwire: wf.tripwire,
    steps: handOutSteps(wf),
    createdAt: wf.createdAt,
    startedAt: wf.startedAt,
    completedAt: wf.completedAt,
  };
}

export function statusSnapshot(wf: MutableWorkflow): WorkflowStatusSnapshot {
  return {
    status: wf.status,
    ...(wf.error !== undefined && { error: wf.error }),
    ...(wf.errorTag !== undefined && { errorTag: wf.errorTag }),
  };
}

/** The sort fields of `listWorkflows`. */
export function sortFieldsOf(wf: MutableWorkflow): WorkflowSortFields {
  return {
    workflowName: wf.workflowName,
    status: wf.status,
    createdAtMs: wf.createdAt.getTime(),
    startedAtMs: wf.startedAt?.getTime(),
    completedAtMs: wf.completedAt?.getTime(),
  };
}

/**
 * Whether `wf` passes a `listWorkflows` / `countWorkflows` filter, scoped to
 * `namespace` (the filter's own, else the storage's).
 */
export function matchesListFilter(params: {
  wf: MutableWorkflow;
  filter: WorkflowListFilter | undefined;
  namespace: string | null | undefined;
}): boolean {
  const { wf, filter: f, namespace: ns } = params;
  if (ns && wf.namespace !== ns) return false;
  if (!f) return true;
  if (f.status && wf.status !== f.status) return false;
  if (f.name && wf.workflowName !== f.name) return false;
  if (f.version !== undefined && wf.version !== f.version) return false;
  if (f.type && wf.workflowType !== f.type) return false;
  if (f.parentId && wf.parentWorkflowId !== f.parentId) return false;
  if (f.runSource !== undefined && wf.runSource !== f.runSource) return false;
  if (f.runSourceId !== undefined && wf.runSourceId !== f.runSourceId) return false;
  if (f.metadata && !workflowMetadataMatches(wf.metadata, f.metadata)) return false;
  return true;
}

/** Key of the `(namespace, workflowName, idempotencyKey)` index. */
export function idempotencyIndexKey(params: {
  namespace: string | undefined;
  workflowName: string;
  idempotencyKey: string;
}): string {
  return JSON.stringify([params.namespace ?? null, params.workflowName, params.idempotencyKey]);
}

/**
 * Upsert one task row of a map step (creating a `running` map step row
 * when the step has none). Amortised O(1): the step's task array is
 * appended to or updated in place while the storage owns it, and copied
 * once after the run's rows were handed out to a caller, so a caller's
 * snapshot never changes under it.
 */
export function writeTaskRow(params: {
  wf: MutableWorkflow;
  stepName: string;
  taskIndex: number;
  outcome:
    | { readonly status: "completed"; readonly result: unknown }
    | { readonly status: "failed"; readonly error: string };
  now: Date;
}): void {
  const { wf, stepName, taskIndex, outcome, now } = params;
  const existing = wf.steps.get(stepName);
  let owned = wf.ownedTasks.get(stepName);
  if (owned === undefined || owned.epoch !== wf.handedOut || existing?.tasks !== owned.tasks) {
    const tasks = existing?.tasks ? [...existing.tasks] : [];
    const position = new Map<number, number>();
    for (let i = 0; i < tasks.length; i++) position.set(tasks[i]!.taskIndex, i);
    owned = { tasks, position, epoch: wf.handedOut };
    wf.ownedTasks.set(stepName, owned);
  }

  const at = owned.position.get(taskIndex);
  const prev = at !== undefined ? owned.tasks[at] : undefined;
  const task: StepTaskState = {
    taskIndex,
    status: outcome.status,
    ...(outcome.status === "completed" ? { result: outcome.result } : { error: outcome.error }),
    startedAt: prev?.startedAt ?? now,
    completedAt: now,
    attempt: (prev?.attempt ?? 0) + 1,
  };
  if (at !== undefined) owned.tasks[at] = task;
  else {
    owned.position.set(taskIndex, owned.tasks.length);
    owned.tasks.push(task);
  }

  wf.steps.set(stepName, {
    ...(existing ?? {
      stepName,
      run: wf.run,
      status: "running" as const,
      dependsOn: [],
      stepType: "map" as const,
      attempt: 1,
    }),
    tasks: owned.tasks,
  });
  wf.updatedAt = now;
}

/** Steps of a run sorted by name — the scanners pick the smallest due one. */
export function stepsByName(wf: MutableWorkflow): StepState[] {
  return [...wf.steps.values()].sort((a, b) =>
    a.stepName < b.stepName ? -1 : a.stepName > b.stepName ? 1 : 0,
  );
}
