export class UnknownWorkflowError extends Error {
  override readonly name = "UnknownWorkflowError";
  constructor(public readonly workflowName: string) {
    super(`Unknown workflow "${workflowName}" — no layer in the chain accepted it`);
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
