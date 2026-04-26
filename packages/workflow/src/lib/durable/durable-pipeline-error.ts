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

/**
 * A mutating call carried a fence token that doesn't match the current
 * lock holder. Thrown when a stale worker (whose lock expired + was picked
 * up by someone else) tries to commit state after the fact.
 *
 * Expected during normal operation when clocks skew or a step runs long
 * enough to miss its heartbeat; the orchestration should abandon the
 * write and exit — the new holder re-drives the workflow from storage.
 */
export class FenceTokenMismatchError extends Data.TaggedError("FenceTokenMismatchError")<{
  readonly workflowId: string;
  readonly expected: string;
  readonly provided: string;
  readonly message: string;
}> {}

/** Workflow is suspended (sleeping or waiting for signal). Not a failure — expected state. */
export class WorkflowSuspendedError extends Data.TaggedError("WorkflowSuspendedError")<{
  readonly workflowId: string;
  readonly stepName: string;
  readonly reason: "sleep" | "signal";
  readonly message: string;
}> {}

/**
 * Signal that the workflow body is requesting a "continue-as-new" — terminate
 * the current execution and start a fresh run under the same workflowId with
 * new input. Thrown by `ctx.continueAsNew(input)` inside a journaled step
 * body. The runner catches it, calls `storage.startFreshRun(workflowId)`,
 * and re-runs the workflow from scratch with the carried input.
 *
 * Not a failure — the original execution terminates cleanly. Compensations
 * do NOT run (continue-as-new is a clean restart, not rollback).
 */
export class WorkflowContinueAsNewError extends Data.TaggedError("WorkflowContinueAsNewError")<{
  readonly workflowId: string;
  readonly nextInput: unknown;
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

/**
 * A journaled activity was replayed while its journal row is still in the
 * `pending` phase — the worker that started it crashed between the pending
 * write and the completion write, so we don't know whether the side effect
 * ran. Thrown for non-idempotent activities (the default) so the workflow
 * halts and the operator can inspect the external system before resuming.
 *
 * Opt in to automatic re-run by passing `idempotent: true` in
 * `ActivityOptions`, which suppresses this error and re-runs the activity.
 */
export class AmbiguousActivityOutcome extends Data.TaggedError("AmbiguousActivityOutcome")<{
  readonly workflowId: string;
  readonly stepName: string;
  readonly activityIndex: number;
  readonly activityName: string;
  readonly message: string;
}> {}

/**
 * Marker for errors that MUST NOT be retried by the activity retry loop.
 * Intended for business-logic failures (validation errors, "not found",
 * "already exists", policy violations) that won't succeed on retry. When a
 * step body throws a TerminalError inside `ctx.activity(..., { retry: ... })`,
 * the retry loop exits immediately and the activity is journaled as Failure.
 *
 * Throw from user code like `throw new TerminalError({ message: "..." })`.
 */
export class TerminalError extends Data.TaggedError("TerminalError")<{
  readonly message: string;
  readonly cause?: unknown;
}> {}

/**
 * Marker for errors that SHOULD be retried by the activity retry loop —
 * forces retry even when a user-supplied `retryable` predicate would reject.
 * Useful for transient infra failures (timeouts, 5xx responses, connection
 * drops) the retry loop should always take another shot at.
 */
export class RetryableError extends Data.TaggedError("RetryableError")<{
  readonly message: string;
  readonly cause?: unknown;
}> {}

/**
 * A `.tripwire()` step fired — the workflow ended early with a structured
 * reason. Not a failure; an intentional short-circuit. `reason` carries the
 * payload returned by the tripwire step's `reason(prev)` function.
 *
 * Thrown from `run()` so callers who want typed access to the reason can
 * check `error instanceof WorkflowTripwireError`. Callers that prefer a
 * non-throwing interface use `runSafe()` and inspect `.error`.
 */
export class WorkflowTripwireError extends Data.TaggedError("WorkflowTripwireError")<{
  readonly workflowId: string;
  readonly stepName: string;
  readonly reason: unknown;
  readonly message: string;
}> {}

/**
 * A `.tripwire()` step fired but the configured `WorkflowStorage` does not
 * implement `tripwireWorkflow`. Surfaces the capability gap at the fire
 * site instead of silently corrupting state or falling back to `failed`.
 */
export class TripwireStorageMissingError extends Data.TaggedError("TripwireStorageMissingError")<{
  readonly workflowId: string;
  readonly stepName: string;
  readonly message: string;
}> {}

/**
 * A `.dowhile()` / `.dountil()` loop exceeded its configured maximum
 * iteration count without the exit condition being satisfied. Default cap
 * is 100 iterations; override with the `maxIterations` option. Thrown as a
 * step failure so the surrounding workflow retry / compensation policy
 * applies.
 */
export class LoopLimitExceededError extends Data.TaggedError("LoopLimitExceededError")<{
  readonly workflowId: string;
  readonly stepName: string;
  readonly maxIterations: number;
  readonly message: string;
}> {}
