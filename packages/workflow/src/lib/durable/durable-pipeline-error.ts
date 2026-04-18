import { Data } from "effect";

/** A workflow-level error (e.g., cycle detected, invalid DAG). */
export class WorkflowError extends Data.TaggedError("WorkflowError")<{
  readonly workflowId: string;
  readonly message: string;
}> {}

/** A step-level execution error. */
export class StepError extends Data.TaggedError("StepError")<{
  readonly workflowId: string;
  readonly stepName: string;
  readonly message: string;
  readonly cause?: unknown;
}> {}

/** A storage backend error. */
export class StorageError extends Data.TaggedError("StorageError")<{
  readonly message: string;
  readonly cause?: unknown;
}> {}

/** Could not acquire a lock on the workflow (already running elsewhere). */
export class WorkflowLockError extends Data.TaggedError("WorkflowLockError")<{
  readonly workflowId: string;
  readonly message: string;
}> {}

/** Workflow is suspended (sleeping or waiting for signal). Not a failure — expected state. */
export class WorkflowSuspendedError extends Data.TaggedError("WorkflowSuspendedError")<{
  readonly workflowId: string;
  readonly stepName: string;
  readonly reason: "sleep" | "signal";
  readonly message: string;
}> {}

/** Signal wait timed out. */
export class WorkflowTimeoutError extends Data.TaggedError("WorkflowTimeoutError")<{
  readonly workflowId: string;
  readonly stepName: string;
  readonly message: string;
}> {}

/** A step exceeded its configured activity timeout. */
export class StepTimeoutError extends Data.TaggedError("StepTimeoutError")<{
  readonly workflowId: string;
  readonly stepName: string;
  readonly timeoutMs: number;
  readonly message: string;
}> {}

/** The entire workflow exceeded its configured global deadline. */
export class WorkflowDeadlineError extends Data.TaggedError("WorkflowDeadlineError")<{
  readonly workflowId: string;
  readonly timeoutMs: number;
  readonly message: string;
}> {}

/** Workflow version mismatch — stored version differs from code version. */
export class WorkflowVersionMismatchError extends Data.TaggedError("WorkflowVersionMismatchError")<{
  readonly workflowId: string;
  readonly expected: string;
  readonly actual: string;
  readonly message: string;
}> {}

/** A guard precondition failed — the step should not execute. */
export class GuardError extends Data.TaggedError("GuardError")<{
  readonly workflowId: string;
  readonly stepName: string;
  readonly message: string;
}> {}
