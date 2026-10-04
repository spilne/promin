// ---------------------------------------------------------------------------
// Recovery — the startup sweep behind `WorkflowRunner.recover()`: the
// `RecoveryStrategy` (+ builder) describing what to do, and
// `recoverWorkflows`, which terminates stale runs and resubmits the rest.
// ---------------------------------------------------------------------------

import type { WallClock } from "../../shared/wall-clock.ts";
import type { Workflow } from "../durable-pipeline.ts";
import type { WorkflowStorage } from "../workflow-storage.ts";
import type { WorkflowStatus } from "../workflow-state.ts";
import type { WorkflowVersionRegistry } from "../workflow-version-registry.ts";
import { hasCapability } from "../storage/capabilities.ts";

export interface RecoveryResult {
  /** Workflows terminated as stale (cancelled or failed). */
  terminated: number;
  /** Workflows resumed (accepted for a run; at most `concurrent` run at once). */
  resumed: number;
  /** Workflows that could not be resumed (missing registry, unknown definition, etc.). */
  skipped: Array<{ workflowId: string; name: string; reason: string }>;
  /**
   * Resolves once every resumed run has settled (whatever its outcome).
   * `recover()` itself resolves as soon as every run is accepted.
   */
  settled: Promise<void>;
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
   * Resume all pending / running / compensating workflows that survived
   * stale termination and that nobody is driving (no live lock). Each is
   * re-launched via the runner's registry; a `compensating` run finishes
   * its rollback. The runner must have been configured with a `registry`;
   * throws otherwise.
   *
   * `concurrent` bounds how many resumed runs are in flight at once
   * (default: 10); the rest wait for a free slot.
   */
  resumeRecent(params?: { concurrent?: number }): this {
    this._resumeRecent = true;
    if (params?.concurrent != null) {
      if (!(params.concurrent >= 1)) {
        throw new Error(`resumeRecent: concurrent must be at least 1, got ${params.concurrent}`);
      }
      this._resumeConcurrency = Math.floor(params.concurrent);
    }
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

/** Page size of the recovery listings. */
const RECOVERY_PAGE_SIZE = 200;

/** Statuses `resumeRecent` resumes. */
const RESUMABLE_STATUSES = ["pending", "running", "compensating"] as const;

/** A run `resumeRecent` hands to `resume`. */
interface ResumeCandidate {
  readonly workflowId: string;
  readonly workflowName: string;
  readonly version?: string;
  readonly input: unknown;
}

/**
 * Apply `strategy` to `storage`: terminate stale runs, then hand every
 * remaining pending / running / compensating run nobody is driving, whose
 * definition the registry knows, to `resume`.
 *
 * - Stale termination uses the storage's bulk `cancelStaleWorkflows` when
 *   it has one; otherwise it pages through the oldest rows, and stops on a
 *   page where no row it has not already tried is stale (a backend whose
 *   terminate leaves a row listed cannot loop it forever).
 * - Resume candidates come from `listOrphanedRuns` (keyset-paginated on
 *   `workflowId`, so runs changing status meanwhile never shift a page),
 *   or, without it, from a snapshot of every listing page taken before the
 *   first run is resumed.
 * - At most `resumeRecent({ concurrent })` resumed runs are in flight at
 *   once. The result comes back once every run is accepted; its
 *   `settled` resolves when they have all finished.
 */
export async function recoverWorkflows(params: {
  strategy: RecoveryStrategy;
  storage: WorkflowStorage;
  registry: WorkflowVersionRegistry | undefined;
  clock: WallClock;
  resume: (run: {
    workflow: Workflow<unknown, unknown>;
    workflowId: string;
    input: unknown;
  }) => Promise<unknown> | void;
}): Promise<RecoveryResult> {
  const { storage, registry, clock } = params;
  const opts = params.strategy._opts;
  let terminated = 0;
  let resumed = 0;
  const skipped: RecoveryResult["skipped"] = [];

  // ---- Phase 1: terminate stale runs ----
  if (opts.staleThresholdMs !== undefined) {
    terminated = await terminateStaleRuns({ storage, clock, opts });
  }

  // ---- Phase 2: resume recent runs ----
  if (!opts.resumeRecent) return { terminated, resumed, skipped, settled: Promise.resolve() };
  if (!registry) {
    throw new Error(
      "WorkflowRunner.recover() with resumeRecent() requires a registry. " +
        "Pass createWorkflowRunner({ storage, registry }) to enable name-based resume.",
    );
  }

  const candidates = await listResumeCandidates({ storage, clock });
  const pool = createRunPool(Math.max(1, opts.resumeConcurrency));
  for (const run of candidates) {
    const def = await registry.resolve(run.workflowName, run.version);
    if (!def) {
      skipped.push({
        workflowId: run.workflowId,
        name: run.workflowName,
        reason:
          `No definition found for "${run.workflowName}"` +
          (run.version ? ` v${run.version}` : "") +
          " in registry",
      });
      continue;
    }
    resumed++;
    pool.submit(() =>
      params.resume({ workflow: def, workflowId: run.workflowId, input: run.input }),
    );
  }
  return { terminated, resumed, skipped, settled: pool.drained() };
}

/** Phase 1 of `recoverWorkflows`: returns how many runs it terminated. */
async function terminateStaleRuns(params: {
  storage: WorkflowStorage;
  clock: WallClock;
  opts: RecoveryStrategy["_opts"];
}): Promise<number> {
  const { storage, clock, opts } = params;
  const thresholdMs = opts.staleThresholdMs!;
  const errorMsg =
    opts.staleAction.kind === "fail" ? opts.staleAction.error : "Stale run cancelled on restart";

  // Fast path: storage exposes a bulk cancelStaleWorkflows (e.g. SQLite).
  if (hasCapability(storage, "cancelStale")) {
    return await storage.cancelStaleWorkflows({
      olderThanMs: thresholdMs,
      error: errorMsg,
      statuses: opts.staleStatuses,
    });
  }

  // Slow path: page through the oldest rows of each status and terminate
  // the stale ones. A terminated row leaves the listing; one that stays
  // (a terminate the backend ignored) is counted as stuck and skipped by
  // the offset, and a page with nothing new to terminate ends the status.
  const cutoff = clock.currentTimeMs() - thresholdMs;
  let terminated = 0;
  for (const status of opts.staleStatuses as WorkflowStatus[]) {
    const tried = new Set<string>();
    const stuck = new Set<string>();
    while (true) {
      const page = await storage.listWorkflows({
        status,
        limit: RECOVERY_PAGE_SIZE,
        offset: stuck.size,
        orderBy: "createdAt",
        orderDir: "asc",
      });
      let progressed = false;
      const stuckBefore = stuck.size;
      for (const wf of page) {
        if (wf.createdAt.getTime() >= cutoff) break;
        if (tried.has(wf.workflowId)) {
          stuck.add(wf.workflowId);
          continue;
        }
        tried.add(wf.workflowId);
        progressed = true;
        if (opts.staleAction.kind === "cancel") {
          await storage.cancelWorkflow({ workflowId: wf.workflowId });
        } else {
          await storage.failWorkflow({ workflowId: wf.workflowId, error: opts.staleAction.error });
        }
        terminated++;
      }
      // Newly stuck rows move the offset past them; a page that neither
      // terminated nor skipped anything new is the end.
      if (!progressed && stuck.size === stuckBefore) break;
      if (progressed && page.length < RECOVERY_PAGE_SIZE) break;
    }
  }
  return terminated;
}

/**
 * Every run `resumeRecent` should resume, listed in full before any is
 * resumed: `listOrphanedRuns` (runs without a live lock, keyset-paged),
 * else every pending / running / compensating row from `listWorkflows`.
 */
async function listResumeCandidates(params: {
  storage: WorkflowStorage;
  clock: WallClock;
}): Promise<ResumeCandidate[]> {
  const { storage, clock } = params;
  const out: ResumeCandidate[] = [];
  if (hasCapability(storage, "orphanedRuns")) {
    const now = clock.now();
    let afterWorkflowId: string | undefined;
    while (true) {
      const page = await storage.listOrphanedRuns({
        now,
        updatedBefore: now,
        limit: RECOVERY_PAGE_SIZE,
        ...(afterWorkflowId !== undefined && { afterWorkflowId }),
      });
      out.push(...page);
      if (page.length < RECOVERY_PAGE_SIZE) return out;
      afterWorkflowId = page[page.length - 1]!.workflowId;
    }
  }
  const seen = new Set<string>();
  for (const status of RESUMABLE_STATUSES) {
    for (let offset = 0; ; offset += RECOVERY_PAGE_SIZE) {
      const page = await storage.listWorkflows({
        status,
        limit: RECOVERY_PAGE_SIZE,
        offset,
        orderBy: "createdAt",
        orderDir: "asc",
      });
      for (const wf of page) {
        if (seen.has(wf.workflowId)) continue;
        seen.add(wf.workflowId);
        out.push(wf);
      }
      if (page.length < RECOVERY_PAGE_SIZE) break;
    }
  }
  return out;
}

/**
 * Runs submitted tasks with at most `concurrency` in flight; the rest wait
 * in submission order. A task's rejection is swallowed (the run records its
 * own outcome). `drained()` resolves once every submitted task has settled.
 */
function createRunPool(concurrency: number): {
  submit: (task: () => Promise<unknown> | void) => void;
  drained: () => Promise<void>;
} {
  const queue: Array<() => Promise<unknown> | void> = [];
  let active = 0;
  let onDrained: (() => void) | undefined;
  let drainedPromise: Promise<void> | undefined;

  const pump = (): void => {
    while (active < concurrency && queue.length > 0) {
      const task = queue.shift()!;
      active++;
      void Promise.resolve()
        .then(task)
        .catch(() => undefined)
        .finally(() => {
          active--;
          pump();
        });
    }
    if (active === 0 && queue.length === 0) onDrained?.();
  };

  return {
    submit: (task) => {
      queue.push(task);
      pump();
    },
    drained: () => {
      drainedPromise ??= new Promise<void>((resolve) => {
        onDrained = resolve;
        if (active === 0 && queue.length === 0) resolve();
      });
      return drainedPromise;
    },
  };
}
