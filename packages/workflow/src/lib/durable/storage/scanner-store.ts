// ---------------------------------------------------------------------------
// WorkflowScannerStore — the indexed queries behind the sleep / signal
// scanners and coordinator recovery.
// ---------------------------------------------------------------------------

/**
 * A suspended run that is due to be resumed, returned by the scanner
 * queries (`listDueTimers`, `listSignalWakeups`). One row per run.
 */
export interface WorkflowWakeup {
  readonly workflowId: string;
  readonly workflowName: string;
  /** Definition version the run was created with. */
  readonly version?: string;
  readonly input: unknown;
  /** The suspended step that is due. */
  readonly stepName: string;
  /**
   * Why the run is due:
   * - `sleep` — a sleeping step's `wakeAt` has passed.
   * - `signal-timeout` — a signal wait's `signalTimeoutAt` has passed.
   * - `signal` — the signal a waiting step waits on has been delivered.
   */
  readonly reason: "sleep" | "signal-timeout" | "signal";
  /** `signal` / `signal-timeout`: the awaited signal name. */
  readonly signalName?: string;
  /** `signal`: the delivered payload (the latest delivery under the name). */
  readonly signalPayload?: unknown;
}

/**
 * A `pending` / `running` / `compensating` run whose lock is free or
 * expired, returned by `listOrphanedRuns`: nobody is driving it, so a
 * coordinator may adopt it. Adopting a `compensating` run finishes its
 * rollback.
 */
export interface OrphanedRun {
  readonly workflowId: string;
  readonly workflowName: string;
  readonly version?: string;
  readonly status: "pending" | "running" | "compensating";
  readonly input: unknown;
  readonly metadata?: Record<string, unknown>;
}

/** Params of `listDueTimers`. */
export interface ListDueTimersParams {
  readonly now: Date;
  readonly limit: number;
  /** Keyset cursor: the last row's `workflowId` of the previous page. */
  readonly afterWorkflowId?: string;
}

/** Params of `listSignalWakeups`. */
export interface ListSignalWakeupsParams {
  readonly limit: number;
  /** Keyset cursor: the last row's `workflowId` of the previous page. */
  readonly afterWorkflowId?: string;
}

/** Params of `listOrphanedRuns`. */
export interface ListOrphanedRunsParams {
  readonly now: Date;
  readonly updatedBefore: Date;
  readonly limit: number;
  /** Keyset cursor: the last row's `workflowId` of the previous page. */
  readonly afterWorkflowId?: string;
}

/**
 * Scanner and recovery queries. Every member is optional (capabilities
 * `dueTimers`, `signalWakeups`, `orphanedRuns`): without one, its scanner
 * pages through `listWorkflows` instead. All three are scoped to the
 * storage's namespace, like `listWorkflows`, and keyset-paginated on
 * `workflowId`, so resuming rows between pages never makes a later page
 * skip one.
 */
export interface WorkflowScannerStore {
  /**
   * Suspended runs (current run only) with a due timer — a `sleeping` step
   * whose `wakeAt <= now` (`reason: "sleep"`), or a `waiting_for_signal`
   * step whose `signalTimeoutAt <= now` (`reason: "signal-timeout"`). One
   * row per run (its due step with the smallest name), ordered by
   * `workflowId` ascending, at most `limit`.
   *
   * Backends index `wakeAt` / `signalTimeoutAt` on suspended steps.
   */
  listDueTimers?(params: ListDueTimersParams): Promise<WorkflowWakeup[]>;

  /**
   * Suspended runs (current run only) with a `waiting_for_signal` step
   * whose `signalName` has a delivered signal (`reason: "signal"`,
   * `signalPayload` = the delivered payload). One row per run (its matching
   * step with the smallest name), ordered by `workflowId` ascending.
   */
  listSignalWakeups?(params: ListSignalWakeupsParams): Promise<WorkflowWakeup[]>;

  /**
   * `pending` / `running` / `compensating` runs that nobody is driving —
   * no lock, or a lock that expired at or before `now` — and that were last
   * updated before `updatedBefore` (so a run a live coordinator has just
   * created and not locked yet is left alone). Never returns `suspended`
   * runs: the scanners own those. Ordered by `workflowId` ascending.
   */
  listOrphanedRuns?(params: ListOrphanedRunsParams): Promise<OrphanedRun[]>;
}
