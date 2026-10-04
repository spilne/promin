// ---------------------------------------------------------------------------
// RunEventStore — live step / lifecycle events of one run.
// ---------------------------------------------------------------------------

import type { WorkflowRunEvent } from "../workflow-state.ts";

/** Params of `notifyStepStarted`. */
export interface NotifyStepStartedParams {
  readonly workflowId: string;
  readonly stepName: string;
}

/** Params of `subscribeToWorkflow`. */
export interface SubscribeToWorkflowParams {
  readonly workflowId: string;
  /** Ends the subscription when aborted. */
  readonly signal?: AbortSignal;
}

/**
 * Live run events. Both members are optional; storages without them don't
 * support live subscriptions, and callers poll `loadWorkflow` instead.
 */
export interface RunEventStore {
  /**
   * Notify subscribers that a step is about to execute. Fired by the
   * runner before each local step body runs so subscribers can observe
   * `step-started` events. Persists nothing; this is purely an event-bus
   * hook.
   *
   * Optional (capability `stepStartedEvents`) — the runner skips it
   * silently on a storage without it.
   */
  notifyStepStarted?(params: NotifyStepStartedParams): Promise<void> | void;

  /**
   * Subscribe to step/workflow-lifecycle events for a single workflow run.
   * Returns an async iterable; the stream closes on the first terminal
   * event (`workflow-completed`, `workflow-failed`, `workflow-tripwire`) or
   * when the caller aborts via `signal`.
   *
   * Backends typically implement this by tapping the same code paths that
   * write the state transitions (in-memory: an in-process event bus;
   * Postgres: `pg_notify` on relevant tables).
   *
   * Subscribers that attach before the workflow starts receive every event;
   * subscribers that attach mid-execution receive from-now onwards.
   *
   * Optional (capability `runEvents`).
   */
  subscribeToWorkflow?(params: SubscribeToWorkflowParams): AsyncIterable<WorkflowRunEvent>;
}
