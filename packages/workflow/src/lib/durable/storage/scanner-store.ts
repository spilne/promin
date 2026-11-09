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
   * - `update` — an update request is waiting to be applied
   *   (`listUpdateWakeups`); `stepName` is one of the run's suspended steps.
   */
  readonly reason: "sleep" | "signal-timeout" | "signal" | "update";
  /** `signal` / `signal-timeout`: the awaited signal name. */
  readonly signalName?: string;
  /** `signal`: the delivered payload (the latest delivery under the name). */
  readonly signalPayload?: unknown;
}

/**
 * A `pending` / `running` / `compensating` run whose lock is free or
 * expired, returned by `listOrphanedRuns`: nobody is driving it, so a
 * coordinator may adopt it. Adopting a `compensating` run finishes its
 * rollback. A `failed` run is a cancelled one whose cancel is not settled
 * (`compensationOrigin` still `"cancel"`, see `owesCancelRollback`): adopting
 * it rolls it back when its definition asks for `onCancel: "rollback"`, and
 * settles it either way.
 */
export interface OrphanedRun {
  readonly workflowId: string;
  readonly workflowName: string;
  readonly version?: string;
  readonly status: "pending" | "running" | "compensating" | "failed";
  readonly input: unknown;
  readonly metadata?: Record<string, unknown>;
}

/**
 * A suspended run parked on one or more children, returned by
 * `listChildEndedWaits`: its `waiting_for_signal` steps whose signal is a
 * reserved child-ended wake-up (`workflow.child-ended:<childId>#<run>`)
 * that has not been delivered.
 */
export interface ChildEndedWaits {
  readonly workflowId: string;
  /** The undelivered child-ended waits, by step name. */
  readonly waits: ReadonlyArray<{ readonly stepName: string; readonly signalName: string }>;
}

/** Params of `listChildEndedWaits`. */
export interface ListChildEndedWaitsParams {
  /** At most this many runs. */
  readonly limit: number;
  /** Keyset cursor: the last row's `workflowId` of the previous page. */
  readonly afterWorkflowId?: string;
}

/** Params of `listUpdateWakeups`. */
export interface ListUpdateWakeupsParams {
  readonly limit: number;
  /** Keyset cursor: the last row's `workflowId` of the previous page. */
  readonly afterWorkflowId?: string;
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
 * `dueTimers`, `signalWakeups`, `orphanedRuns`, `childEndedWaits`): without one, its scanner
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
   * created and not locked yet is left alone). Also returns cancelled runs
   * whose cancel is not settled: `failed` runs whose `compensationOrigin` is
   * still `"cancel"` (`cancelWorkflow` stores it; the final `failWorkflow` of
   * a cancel's rollback settles it), under the same lock and `updatedBefore`
   * rules: the run was cancelled while no driver held it, or its driver died
   * before settling. Never returns `suspended` runs: the scanners own those.
   * Ordered by `workflowId` ascending.
   */
  listOrphanedRuns?(params: ListOrphanedRunsParams): Promise<OrphanedRun[]>;

  /**
   * Suspended runs (current run only) waiting on a child-ended wake-up
   * that has not been delivered — the candidates for re-delivering a
   * wake-up lost when the child's driver died. One row per run with every
   * such wait, ordered by `workflowId` ascending, at most `limit` runs.
   * Whether each child has actually ended is for the caller to check.
   */
  listChildEndedWaits?(params: ListChildEndedWaitsParams): Promise<ChildEndedWaits[]>;

  /**
   * Suspended runs (current run only) holding an update request that has no
   * result yet: a delivered signal named `workflow.update:<id>` with no
   * signal `workflow.update-result:<id>` (see `updates/update-protocol.ts`).
   * One row per run, `reason: "update"`, `stepName` the run's suspended
   * step with the smallest name, ordered by `workflowId` ascending, at most
   * `limit`. The signal scanner resumes these runs so the journaled body
   * applies the update at its next update point.
   */
  listUpdateWakeups?(params: ListUpdateWakeupsParams): Promise<WorkflowWakeup[]>;
}
