// ---------------------------------------------------------------------------
// Errors the workflow-builder service throws. The HTTP routes map each to a
// status (see routes/workflow-builder.ts); embedders catch them by class.
// ---------------------------------------------------------------------------

/** One problem with a submitted workflow: where (`path`, may be empty) and what. */
export interface WorkflowIssue {
  readonly path: string;
  readonly message: string;
}

/** No builder workflow has this name. */
export class BuilderWorkflowNotFoundError extends Error {
  readonly workflowName: string;
  constructor(name: string) {
    super(`Builder workflow "${name}" does not exist.`);
    this.name = "BuilderWorkflowNotFoundError";
    this.workflowName = name;
  }
}

/**
 * The name is taken: by another builder workflow (`reason: "exists"`) or by
 * a workflow the host defines in code or registers itself (`"reserved"`).
 */
export class BuilderWorkflowNameTakenError extends Error {
  readonly workflowName: string;
  readonly reason: "exists" | "reserved";
  constructor(params: { name: string; reason: "exists" | "reserved" }) {
    super(
      params.reason === "exists"
        ? `Builder workflow "${params.name}" already exists.`
        : `"${params.name}" is the name of a workflow this server runs from code; pick another name.`,
    );
    this.name = "BuilderWorkflowNameTakenError";
    this.workflowName = params.name;
    this.reason = params.reason;
  }
}

/** The submitted schema does not validate or compile. */
export class InvalidBuilderWorkflowError extends Error {
  readonly issues: ReadonlyArray<WorkflowIssue>;
  constructor(issues: ReadonlyArray<WorkflowIssue>) {
    super(
      `Workflow is invalid: ${issues
        .map((i) => (i.path ? `${i.path}: ${i.message}` : i.message))
        .join("; ")}`,
    );
    this.name = "InvalidBuilderWorkflowError";
    this.issues = issues;
  }
}

/**
 * The draft changed since the caller read it: another editor saved first.
 * `currentRevision` is the stored one; reload, merge, and save again.
 */
export class BuilderWorkflowConflictError extends Error {
  readonly workflowName: string;
  readonly currentRevision: number | null;
  constructor(params: { name: string; currentRevision: number | null; message?: string }) {
    super(
      params.message ??
        `Builder workflow "${params.name}" was changed by someone else (now at revision ${params.currentRevision}); reload it and apply your edit again.`,
    );
    this.name = "BuilderWorkflowConflictError";
    this.workflowName = params.name;
    this.currentRevision = params.currentRevision;
  }
}

/** The workflow still has runs that have not ended, so it cannot be deleted. */
export class BuilderWorkflowInUseError extends Error {
  readonly workflowName: string;
  readonly activeRuns: number;
  constructor(params: { name: string; activeRuns: number }) {
    super(
      `Builder workflow "${params.name}" has ${params.activeRuns} run(s) that have not ended; cancel or finish them before deleting it.`,
    );
    this.name = "BuilderWorkflowInUseError";
    this.workflowName = params.name;
    this.activeRuns = params.activeRuns;
  }
}
