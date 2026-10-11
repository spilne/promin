export class UnknownWorkflowError extends Error {
  override readonly name = "UnknownWorkflowError";
  constructor(public readonly workflowName: string) {
    super(`Unknown workflow "${workflowName}" — no layer in the chain accepted it`);
  }
}

/**
 * A trigger asked for a version of a workflow that no live worker (or
 * in-process definition) can run. Nothing was created.
 */
export class UnknownWorkflowVersionError extends Error {
  override readonly name = "UnknownWorkflowVersionError";
  readonly workflowName: string;
  readonly version: string;
  readonly available: readonly string[];
  constructor(params: { workflowName: string; version: string; available: readonly string[] }) {
    super(
      `Unknown version "${params.version}" of workflow "${params.workflowName}"` +
        (params.available.length > 0
          ? ` — available: ${params.available.join(", ")}`
          : " — no versioned definition is available"),
    );
    this.workflowName = params.workflowName;
    this.version = params.version;
    this.available = params.available;
  }
}

/**
 * Resume refused: the run was cancelled. A cancelled run stays cancelled;
 * start a fresh run (`rerun`) to run it again.
 */
export class RunCancelledError extends Error {
  override readonly name = "RunCancelledError";
  readonly workflowId: string;
  constructor(workflowId: string) {
    super(`Run "${workflowId}" was cancelled; rerun it to start it again`);
    this.workflowId = workflowId;
  }
}

/**
 * A trigger named a `workflowId` that already belongs to a run of a
 * different workflow. The existing run is left untouched.
 */
export class WorkflowIdConflictError extends Error {
  override readonly name = "WorkflowIdConflictError";
  readonly workflowId: string;
  readonly existingWorkflowName: string;
  readonly requestedWorkflowName: string;
  constructor(params: {
    workflowId: string;
    existingWorkflowName: string;
    requestedWorkflowName: string;
  }) {
    super(
      `Workflow id "${params.workflowId}" already belongs to a run of "${params.existingWorkflowName}", ` +
        `not "${params.requestedWorkflowName}"`,
    );
    this.workflowId = params.workflowId;
    this.existingWorkflowName = params.existingWorkflowName;
    this.requestedWorkflowName = params.requestedWorkflowName;
  }
}
