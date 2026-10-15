// ---------------------------------------------------------------------------
// Recovery — the startup sweep behind `WorkflowRunner.recover()`: the
// `RecoveryStrategy` (+ builder) describing what to do, and
// `recoverWorkflows`, which terminates stale runs and resubmits the rest.
// ---------------------------------------------------------------------------

import type { WallClock } from "../../shared/wall-clock.ts";
import type { Workflow } from "../durable-pipeline.ts";
import type { WorkflowStorage } from "../workflow-storage.ts";
import type { WorkflowStatus } from "../workflow-state.ts";
import type {
  IWorkflowVersionRegistry,
  WorkflowVersionRegistry,
} from "../workflow-version-registry.ts";

export interface RecoveryResult {
  /** Workflows terminated as stale (cancelled or failed). */
  terminated: number;
  /** Workflows resumed (fire-and-forget runs kicked off). */
  resumed: number;
  /** Workflows that could not be resumed (missing registry, unknown definition, etc.). */
  skipped: Array<{ workflowId: string; name: string; reason: string }>;
}

export type StaleTerminationAction =
  | { readonly kind: "cancel" }
  | { readonly kind: "fail"; readonly error: string };

interface RecoveryStrategyOpts {
  readonly staleThresholdMs: number | undefined;
  readonly staleStatuses: Array<"pending" | "running" | "suspended">;
  readonly staleAction: StaleTerminationAction;
  readonly resumeRecent: boolean;
  readonly resumeConcurrency: number;
}

/**
 * Encapsulates the recovery actions `WorkflowRunner.recover()` should take.
 * Build one via `RecoveryStrategy.builder()`.
 *
 * ```ts
 * const strategy = RecoveryStrategy.builder()
 *   .cancelStale({ olderThanMs: 60 * 60 * 1000 })
 *   .resumeRecent()
 *   .build();
 *
 * await runner.recover(strategy);
 * ```
 */
export class RecoveryStrategy {
  /** @internal */
  readonly _opts: RecoveryStrategyOpts;

  constructor(opts: RecoveryStrategyOpts) {
    this._opts = opts;
  }

  static builder(): RecoveryStrategyBuilder {
    return new RecoveryStrategyBuilder();
  }
}

/**
 * Fluent builder for `RecoveryStrategy`. Call `.build()` when done.
 */
export class RecoveryStrategyBuilder {
  private _staleThresholdMs: number | undefined;
  private _staleStatuses: Array<"pending" | "running" | "suspended"> = ["pending", "running"];
  private _staleAction: StaleTerminationAction = { kind: "cancel" };
  private _resumeRecent = false;
  private _resumeConcurrency = 10;

  /**
   * Terminate pending/running workflows whose `createdAt` is older than
   * `olderThanMs`. Uses `cancelWorkflow` — the run ends up `failed` with
   * error `"Stale run cancelled on restart"`.
   *
   * When the configured storage exposes a bulk `cancelStaleWorkflows` method
   * (e.g. `SqliteWorkflowStorage`), a single `UPDATE` is issued; otherwise
   * the runner pages through matching rows and cancels them one-by-one.
   */
  cancelStale(params: {
    olderThanMs: number;
    statuses?: Array<"pending" | "running" | "suspended">;
  }): this {
    this._staleThresholdMs = params.olderThanMs;
    if (params.statuses) this._staleStatuses = params.statuses;
    this._staleAction = { kind: "cancel" };
    return this;
  }

  /**
   * Terminate stale runs via `failWorkflow` with a custom `error` message.
   * Same age-cutoff logic as `cancelStale` but lets you control the error
   * string recorded on the workflow row and visible in the dashboard.
   */
  failStale(params: {
    olderThanMs: number;
    error?: string;
    statuses?: Array<"pending" | "running" | "suspended">;
  }): this {
    this._staleThresholdMs = params.olderThanMs;
    if (params.statuses) this._staleStatuses = params.statuses;
    this._staleAction = {
      kind: "fail",
      error: params.error ?? "Stale run auto-failed on restart",
    };
    return this;
  }

  /**
   * Resume all pending/running workflows that survived stale termination.
   * Each is re-launched as a fire-and-forget run via the runner's registry.
   * The runner must have been configured with a `registry`; throws otherwise.
   *
   * `concurrent` controls how many runs are submitted per event-loop tick
   * (default: 10) to avoid thundering-herd on large backlogs.
   */
  resumeRecent(params?: { concurrent?: number }): this {
    this._resumeRecent = true;
    if (params?.concurrent != null) this._resumeConcurrency = params.concurrent;
    return this;
  }

  build(): RecoveryStrategy {
    if (this._staleThresholdMs === undefined && !this._resumeRecent) {
      throw new Error(
        "RecoveryStrategy.builder() must include at least one action: " +
          "cancelStale(), failStale(), or resumeRecent().",
      );
    }
    return new RecoveryStrategy({
      staleThresholdMs: this._staleThresholdMs,
      staleStatuses: this._staleStatuses,
      staleAction: this._staleAction,
      resumeRecent: this._resumeRecent,
      resumeConcurrency: this._resumeConcurrency,
    });
  }
}

/**
 * Apply `strategy` to `storage`: terminate stale runs, then hand every
 * remaining pending/running run whose definition the registry knows to
 * `resume` (fire-and-forget).
 */
export async function recoverWorkflows(params: {
  strategy: RecoveryStrategy;
  storage: WorkflowStorage;
  registry: WorkflowVersionRegistry | IWorkflowVersionRegistry | undefined;
  clock: WallClock;
  resume: (run: {
    workflow: Workflow<unknown, unknown>;
    workflowId: string;
    input: unknown;
  }) => void;
}): Promise<RecoveryResult> {
  const { storage, registry, clock } = params;
  const opts = params.strategy._opts;
  let terminated = 0;
  let resumed = 0;
  const skipped: RecoveryResult["skipped"] = [];

  // ---- Phase 1: terminate stale runs ----
  if (opts.staleThresholdMs !== undefined) {
    const errorMsg =
      opts.staleAction.kind === "fail" ? opts.staleAction.error : "Stale run cancelled on restart";

    // Fast path: storage exposes a bulk cancelStaleWorkflows (e.g. SQLite).
    if (typeof storage.cancelStaleWorkflows === "function") {
      terminated = await storage.cancelStaleWorkflows({
        olderThanMs: opts.staleThresholdMs,
        error: errorMsg,
        statuses: opts.staleStatuses,
      });
    } else {
      // Slow path: page through, cancel stale rows one-by-one.
      // Ordered by createdAt ASC so stale rows surface first; we break
      // once the first non-stale row appears in a page.
      const cutoff = clock.currentTimeMs() - opts.staleThresholdMs;
      const PAGE = 200;
      for (const status of opts.staleStatuses as WorkflowStatus[]) {
        while (true) {
          const page = await storage.listWorkflows({
            status,
            limit: PAGE,
            offset: 0,
            orderBy: "createdAt",
            orderDir: "asc",
          });
          if (page.length === 0) break;
          let anyStale = false;
          for (const wf of page) {
            if (wf.createdAt.getTime() >= cutoff) break;
            anyStale = true;
            if (opts.staleAction.kind === "cancel") {
              await storage.cancelWorkflow(wf.workflowId);
            } else {
              await storage.failWorkflow(wf.workflowId, opts.staleAction.error);
            }
            terminated++;
          }
          if (!anyStale) break;
        }
      }
    }
  }

  // ---- Phase 2: resume recent runs ----
  if (opts.resumeRecent) {
    if (!registry) {
      throw new Error(
        "WorkflowRunner.recover() with resumeRecent() requires a registry. " +
          "Pass createWorkflowRunner({ storage, registry }) to enable name-based resume.",
      );
    }
    const PAGE = 200;
    for (const status of ["pending", "running"] as const) {
      let offset = 0;
      while (true) {
        const page = await storage.listWorkflows({ status, limit: PAGE, offset });
        if (page.length === 0) break;

        // Fire in batches to avoid thundering-herd on large backlogs.
        for (let i = 0; i < page.length; i += opts.resumeConcurrency) {
          const batch = page.slice(i, i + opts.resumeConcurrency);
          for (const wf of batch) {
            const def = await registry.resolve(wf.workflowName, wf.version);
            if (!def) {
              skipped.push({
                workflowId: wf.workflowId,
                name: wf.workflowName,
                reason:
                  `No definition found for "${wf.workflowName}"` +
                  (wf.version ? ` v${wf.version}` : "") +
                  " in registry",
              });
              continue;
            }
            params.resume({ workflow: def, workflowId: wf.workflowId, input: wf.input });
            resumed++;
          }
          // Yield to the event loop between batches.
          if (i + opts.resumeConcurrency < page.length) {
            await new Promise<void>((r) => clock.setTimeout(() => r(), 0));
          }
        }

        if (page.length < PAGE) break;
        offset += PAGE;
      }
    }
  }

  return { terminated, resumed, skipped };
}
