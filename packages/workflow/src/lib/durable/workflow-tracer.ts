// ---------------------------------------------------------------------------
// WorkflowTracer — span-shaped lifecycle callbacks for the engine.
//
// A tracer is an observer: every callback is optional, runs synchronously
// from the engine's point of view, and can never change a run's outcome (a
// callback that throws or returns a rejecting promise is reported to
// `onTracerError`). With no tracer configured the engine does no tracing
// work at all: every emit site checks for `undefined` before it builds an
// event.
//
// Events are correlated by stable ids (`workflowId`, `run`, `stepName`,
// `attempt`), not by handles, so a tracer keeps whatever state it needs
// keyed by them. A run end always closes everything the run opened.
// ---------------------------------------------------------------------------

import type { WorkflowFailure } from "./workflow-state.ts";

/** Identity shared by every event of a run. */
export interface WorkflowTraceRef {
  readonly workflowId: string;
  readonly workflowName: string;
  readonly namespace?: string;
  /** `WorkflowState.run`; absent only before the run row exists. */
  readonly run?: number;
}

/** A failure or error as a tracer sees it: the tag and message, never a payload. */
export interface WorkflowTraceError {
  /** `_tag` of the error; `UnknownError` for a defect. */
  readonly tag: string;
  readonly message: string;
}

/** How one orchestration of a run ended. */
export type WorkflowTraceRunOutcome =
  | "completed"
  | "failed"
  | "cancelled"
  | "tripwire"
  /** Parked on a sleep or signal wait; a later orchestration resumes it. */
  | "suspended"
  /** Ended to start a fresh run under the same id. */
  | "continued"
  /** This driver stopped (lock lost, abandoned, checkpoint lost); the run is still in flight. */
  | "abandoned";

export interface WorkflowTraceRunStart extends WorkflowTraceRef {
  readonly startedAt: Date;
  readonly version?: string;
  /** The parent run, when this run is a child workflow. */
  readonly parentWorkflowId?: string;
  /** W3C `traceparent` of the caller's request, or the one stored with the run. */
  readonly traceparent?: string;
  /** The run had already started in storage (a resume), not created or first driven by this call. */
  readonly resumed: boolean;
  readonly executorId?: string;
}

export interface WorkflowTraceRunEnd extends WorkflowTraceRef {
  readonly startedAt: Date;
  readonly endedAt: Date;
  readonly durationMs: number;
  readonly outcome: WorkflowTraceRunOutcome;
  /** Set when `outcome` is `failed` (the same shape as `onWorkflowFailure`). */
  readonly failure?: WorkflowFailure;
  readonly executorId?: string;
}

export interface WorkflowTraceStepAttemptStart extends WorkflowTraceRef {
  readonly stepName: string;
  readonly attempt: number;
  readonly startedAt: Date;
}

export type WorkflowTraceStepOutcome =
  | "completed"
  /** `skipWhen` matched: the body never ran. */
  | "skipped"
  | "failed"
  | "suspended"
  | "continued";

export interface WorkflowTraceStepAttemptEnd extends WorkflowTraceRef {
  readonly stepName: string;
  readonly attempt: number;
  readonly startedAt: Date;
  readonly endedAt: Date;
  readonly durationMs: number;
  readonly outcome: WorkflowTraceStepOutcome;
  readonly error?: WorkflowTraceError;
  /** The attempt failed and the step's retry policy ran another attempt. */
  readonly willRetry: boolean;
  /** Who ran the attempt, when not this runner (a worker id). */
  readonly executorId?: string;
}

/** A step parked on a sleep or a signal wait. */
export interface WorkflowTraceWaitStart extends WorkflowTraceRef {
  readonly stepName: string;
  readonly kind: "sleep" | "signal";
  readonly at: Date;
}

/**
 * A resuming run found a step that was parked: the wait is over. `since` is
 * when the run was last written before this resume (the best the stored run
 * says about when the wait began).
 */
export interface WorkflowTraceWaitEnd extends WorkflowTraceRef {
  readonly stepName: string;
  readonly kind: "sleep" | "signal";
  readonly since?: Date;
  readonly endedAt: Date;
  readonly waitedMs?: number;
  readonly wakeAt?: Date;
}

/**
 * A queued step is being held back by its rate-limit / concurrency group
 * (`StepGroupOption`): it stays queued, uses no retry attempt, and starts
 * when the group has room. Emitted once per throttle episode, when the
 * coordinator first sees the step held back.
 */
export interface WorkflowTraceStepThrottled extends WorkflowTraceRef {
  readonly stepName: string;
  readonly attempt: number;
  readonly groupName: string;
  readonly reason: "concurrency" | "rate";
  readonly at: Date;
  /** Earliest time the rate window frees a slot (`reason: "rate"`); a hint. */
  readonly retryAt?: Date;
}

/** The step a group held back was claimed (or its wait ended another way). */
export interface WorkflowTraceStepThrottleEnd extends WorkflowTraceRef {
  readonly stepName: string;
  readonly attempt: number;
  readonly groupName: string;
  readonly since: Date;
  readonly endedAt: Date;
  readonly waitedMs: number;
}

export interface WorkflowTraceLockAcquired {
  readonly workflowId: string;
  readonly namespace?: string;
  readonly at: Date;
  /** The storage issued a fence token. */
  readonly fenced: boolean;
}

export interface WorkflowTraceLockLost {
  readonly workflowId: string;
  readonly namespace?: string;
  readonly at: Date;
  /** `WorkflowLockLostError` (or its cause) as the lock reported it. */
  readonly error: WorkflowTraceError;
}

export interface WorkflowTraceCompensationStart extends WorkflowTraceRef {
  readonly at: Date;
  /** The rollback answers a cancel, not a failure. */
  readonly cancelled: boolean;
  /** The rollback finishes one a previous driver left `compensating`. */
  readonly resumed: boolean;
}

/** One `compensate` attempt of one step. */
export interface WorkflowTraceCompensationStep extends WorkflowTraceRef {
  readonly stepName: string;
  readonly attempt: number;
  readonly startedAt: Date;
  readonly endedAt: Date;
  readonly durationMs: number;
  readonly outcome: "completed" | "failed";
  readonly error?: WorkflowTraceError;
}

export interface WorkflowTraceCompensationEnd extends WorkflowTraceRef {
  readonly startedAt: Date;
  readonly endedAt: Date;
  readonly durationMs: number;
  readonly compensated: number;
  readonly failed: number;
}

/** `runner.recover()` handed a run nobody was driving back to a driver. */
export interface WorkflowTraceRecoveryReclaim {
  readonly workflowId: string;
  readonly workflowName: string;
  readonly version?: string;
  readonly at: Date;
}

/** A worker claimed a queued step task and starts its attempt. */
export interface WorkflowTraceWorkerTask {
  readonly taskId: string;
  readonly workflowId: string;
  readonly run: number;
  readonly stepName: string;
  readonly attempt: number;
  readonly workerId: string;
  /** Trace context the coordinator stored with the task (`metadata.traceparent`). */
  readonly traceparent?: string;
  readonly startedAt: Date;
}

export interface WorkflowTraceWorkerTaskEnd extends WorkflowTraceWorkerTask {
  readonly endedAt: Date;
  readonly durationMs: number;
  /** `lease-lost`: the claim was taken away and the outcome dropped. */
  readonly outcome: "completed" | "failed" | "lease-lost";
  readonly error?: WorkflowTraceError;
}

/** Where a tracer callback failed. */
export interface WorkflowTracerErrorEvent {
  /** The tracer callback that threw. */
  readonly callback: keyof WorkflowTracerCallbacks | "traceparent";
  readonly error: unknown;
}

/** The lifecycle callbacks; all optional. */
export interface WorkflowTracerCallbacks {
  runStart(event: WorkflowTraceRunStart): void | Promise<void>;
  runEnd(event: WorkflowTraceRunEnd): void | Promise<void>;
  stepAttemptStart(event: WorkflowTraceStepAttemptStart): void | Promise<void>;
  /**
   * Attempts are reported when the step settles, so an attempt that ran
   * elsewhere (a queue worker's retries) may have no `stepAttemptStart`:
   * the event carries `startedAt` for that reason.
   */
  stepAttemptEnd(event: WorkflowTraceStepAttemptEnd): void | Promise<void>;
  waitStart(event: WorkflowTraceWaitStart): void | Promise<void>;
  waitEnd(event: WorkflowTraceWaitEnd): void | Promise<void>;
  stepThrottled(event: WorkflowTraceStepThrottled): void | Promise<void>;
  stepThrottleEnd(event: WorkflowTraceStepThrottleEnd): void | Promise<void>;
  lockAcquired(event: WorkflowTraceLockAcquired): void | Promise<void>;
  lockLost(event: WorkflowTraceLockLost): void | Promise<void>;
  compensationStart(event: WorkflowTraceCompensationStart): void | Promise<void>;
  compensationStep(event: WorkflowTraceCompensationStep): void | Promise<void>;
  compensationEnd(event: WorkflowTraceCompensationEnd): void | Promise<void>;
  recoveryReclaim(event: WorkflowTraceRecoveryReclaim): void | Promise<void>;
  workerTaskStart(event: WorkflowTraceWorkerTask): void | Promise<void>;
  workerTaskEnd(event: WorkflowTraceWorkerTaskEnd): void | Promise<void>;
}

/**
 * Receives the engine's lifecycle as span-shaped events. Child workflows are
 * runs with a `parentWorkflowId` on their `runStart`.
 */
export interface WorkflowTracer extends Partial<WorkflowTracerCallbacks> {
  /**
   * The W3C `traceparent` to carry across a step queue for the step attempt
   * (`stepName` + `attempt`) the coordinator is about to dispatch, or for the
   * run when `stepName` is absent. The engine stores it with the queue task
   * (`StepTask.metadata.traceparent`); workers hand it back on
   * `workerTaskStart`. Called only when a step is dispatched to a queue.
   */
  traceparent?(ref: {
    readonly workflowId: string;
    readonly run?: number;
    readonly stepName?: string;
    readonly attempt?: number;
  }): string | undefined;
  /** A tracer callback failed. Default: `console.error`. Must not throw. */
  onTracerError?(event: WorkflowTracerErrorEvent): void;
}

type EventOf<K extends keyof WorkflowTracerCallbacks> = Parameters<WorkflowTracerCallbacks[K]>[0];

/**
 * Invoke one tracer callback. Never throws and never rejects: a failure is
 * reported to `onTracerError`. Callers check that a tracer is configured
 * before building `event`, so an untraced run allocates nothing here.
 */
export function emitTrace<K extends keyof WorkflowTracerCallbacks>(
  tracer: WorkflowTracer,
  callback: K,
  event: EventOf<K>,
): void {
  const fn = tracer[callback] as ((event: EventOf<K>) => void | Promise<void>) | undefined;
  if (fn === undefined) return;
  try {
    const out = fn.call(tracer, event);
    if (out !== undefined && typeof (out as Promise<void>).then === "function") {
      (out as Promise<void>).then(undefined, (error: unknown) =>
        reportTracerError({ tracer, callback, error }),
      );
    }
  } catch (error) {
    reportTracerError({ tracer, callback, error });
  }
}

/** The tracer's `traceparent` for a dispatch, or `undefined` (also when it throws). */
export function tracerTraceparent(
  tracer: WorkflowTracer,
  ref: Parameters<NonNullable<WorkflowTracer["traceparent"]>>[0],
): string | undefined {
  if (tracer.traceparent === undefined) return undefined;
  try {
    const value = tracer.traceparent(ref);
    return typeof value === "string" && value.length > 0 ? value : undefined;
  } catch (error) {
    reportTracerError({ tracer, callback: "traceparent", error });
    return undefined;
  }
}

function reportTracerError(params: {
  tracer: WorkflowTracer;
  callback: keyof WorkflowTracerCallbacks | "traceparent";
  error: unknown;
}): void {
  const { tracer, callback, error } = params;
  try {
    if (tracer.onTracerError !== undefined) {
      tracer.onTracerError({ callback, error });
      return;
    }
    console.error(`[workflow] tracer ${callback} failed:`, error);
  } catch {
    // A failing error reporter has nowhere left to report to.
  }
}

/** The tag and message of an error, for a trace event. */
export function traceErrorOf(error: unknown): WorkflowTraceError {
  const e = (typeof error === "object" && error !== null ? error : {}) as {
    _tag?: unknown;
    errorTag?: unknown;
  };
  const tag =
    e._tag === "StepError" && typeof e.errorTag === "string"
      ? e.errorTag
      : typeof e._tag === "string"
        ? e._tag
        : "UnknownError";
  return { tag, message: error instanceof Error ? error.message : String(error) };
}
