// ---------------------------------------------------------------------------
// WorkflowQueryStore — listing, counting, run history and retention.
// ---------------------------------------------------------------------------

import type {
  RunSource,
  WorkflowRunSummary,
  WorkflowState,
  WorkflowStatus,
  WorkflowSummary,
} from "../workflow-state.ts";

/**
 * Sortable columns on `WorkflowQueryStore.listWorkflows`. `duration` is
 * computed as `completedAt - createdAt` and sorts NULL-last for runs that
 * haven't finished yet.
 */
export type WorkflowOrderBy =
  | "createdAt"
  | "startedAt"
  | "completedAt"
  | "duration"
  | "status"
  | "name";

/**
 * Row filters shared by `listWorkflows`, `listWorkflowSummaries` and
 * `countWorkflows`. Every filter is optional; an empty filter matches every
 * run of every status in the storage's namespace.
 */
export interface WorkflowListFilter {
  readonly status?: WorkflowStatus;
  readonly name?: string;
  /** Workflow definition version the run was created with. */
  readonly version?: string;
  readonly type?: string;
  readonly parentId?: string;
  readonly namespace?: string;
  /**
   * Filter by what kicked the run off (`"schedule"`, `"manual"`, …).
   * Stored as an integer column, so this is a single index lookup.
   */
  readonly runSource?: RunSource;
  /** Optional producer id; only meaningful with `runSource`. */
  readonly runSourceId?: string;
  /**
   * Filter by metadata key/value pairs. A row matches when its metadata
   * contains every supplied key with a deep-equal value. Backends with
   * native JSON support (Postgres `@>`) push the filter to the database;
   * others apply it after loading. Index strategy is the user's call —
   * Postgres ships with no metadata index by default.
   */
  readonly metadata?: Record<string, unknown>;
}

/** Params of `listWorkflows` and `listWorkflowSummaries`: a filter plus paging and order. */
export interface ListWorkflowsParams extends WorkflowListFilter {
  readonly limit?: number;
  readonly offset?: number;
  readonly orderBy?: WorkflowOrderBy;
  readonly orderDir?: "asc" | "desc";
}

/** Params of `loadRunHistory`. */
export interface LoadRunHistoryParams {
  readonly workflowId: string;
  readonly limit?: number;
  readonly offset?: number;
}

/**
 * Params of `purgeCompleted`: a relative window (`olderThanMs` — completed
 * more than N ms ago) or an absolute one (`completedAt` in `[from, to)`).
 */
export type PurgeCompletedParams =
  | { readonly olderThanMs: number; readonly limit: number }
  | { readonly from: Date; readonly to: Date; readonly limit: number };

/** Params of `cancelStaleWorkflows`. */
export interface CancelStaleWorkflowsParams {
  readonly olderThanMs: number;
  readonly error?: string;
  readonly statuses?: ReadonlyArray<"pending" | "running" | "suspended">;
}

/** Read-side queries over many runs, plus bulk retention. */
export interface WorkflowQueryStore {
  /**
   * List workflows, optionally filtered (see `WorkflowListFilter`).
   *
   * `orderBy` defaults to `startedAt`, `orderDir` defaults to `desc` —
   * the most-recently-started run is what dashboards usually want, even
   * when some pending runs were created later but haven't picked up a
   * worker yet. Sort fields with NULL values (e.g. `startedAt` on a
   * still-pending row, `duration` on a still-running row) sort last
   * regardless of direction so the most-relevant rows surface first in
   * both views.
   *
   * `status` orders by the underlying enum/id ordering — not alphabetical —
   * to keep the cost a single column read across backends. Callers that
   * need alphabetical can sort the returned page client-side.
   */
  listWorkflows(params?: ListWorkflowsParams): Promise<WorkflowState[]>;

  /**
   * Lean variant of `listWorkflows` that skips the heavy blob columns
   * (`steps`, `input`, `result`, `error`). Returns `WorkflowSummary` —
   * everything dashboards need for list-view rows without deserialising
   * step JSON on every poll.
   *
   * Optional (capability `summaries`) — callers fall back to
   * `listWorkflows` without it.
   */
  listWorkflowSummaries?(params?: ListWorkflowsParams): Promise<WorkflowSummary[]>;

  /**
   * Count workflows matching the given filters without loading rows.
   *
   * Optional (capability `countWorkflows`) — callers fall back to counting
   * `listWorkflows` rows without it.
   */
  countWorkflows?(params?: WorkflowListFilter): Promise<number>;

  /**
   * Distinct workflow names ever observed in storage, optionally scoped to
   * a namespace. Returned alphabetically sorted. Used by dashboard filter
   * dropdowns so a workflow that hasn't run recently still appears.
   */
  distinctWorkflowNames(params?: { readonly namespace?: string }): Promise<string[]>;

  /**
   * Distinct workflow types ever observed in storage, optionally scoped to
   * a namespace. Returned alphabetically sorted; null/undefined types are
   * excluded.
   */
  distinctWorkflowTypes(params?: { readonly namespace?: string }): Promise<string[]>;

  /**
   * Distinct namespaces ever observed in storage. Returned alphabetically
   * sorted; null/undefined namespaces are excluded.
   */
  distinctNamespaces(): Promise<string[]>;

  /**
   * Load run history for a workflow — all runs with their step results.
   * Ordered by run number descending (newest first).
   */
  loadRunHistory(params: LoadRunHistoryParams): Promise<WorkflowRunSummary[]>;

  /**
   * Delete ended workflows whose `completedAt` falls in the window, at most
   * `limit` of them. Returns the number deleted. Never deletes a running or
   * suspended workflow.
   */
  purgeCompleted(params: PurgeCompletedParams): Promise<number>;

  /**
   * Bulk-fail every workflow in `statuses` (default: pending, running,
   * suspended) whose `createdAt` is older than `olderThanMs`, recording
   * `error` on each. Returns the number of rows updated.
   *
   * Optional (capability `cancelStale`): `WorkflowRunner.recover()` uses it
   * as a single-statement fast path for stale-run termination and otherwise
   * pages through `listWorkflows` and cancels rows one by one.
   */
  cancelStaleWorkflows?(params: CancelStaleWorkflowsParams): number | Promise<number>;
}
