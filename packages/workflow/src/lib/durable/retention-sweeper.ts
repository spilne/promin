// ---------------------------------------------------------------------------
// RetentionSweeper — deletes ended runs (and old step-queue tasks) once the
// retention policy says they are old enough, so storage does not grow without
// bound.
//
// What it does, per sweep:
//
//   1. Resolves the policy of every workflow: the definition's own
//      (`workflow({ retention })`), field by field over the sweeper's default.
//   2. Pages ended runs oldest first with a keyset cursor
//      (`listRetentionCandidates`), in batches of `batchSize`, and decides per
//      run: old enough for its class (completed / failed / cancelled)? Not one
//      of the newest `keepAtLeast`? Not a cancelled run still owing its
//      rollback? No parent that still needs it, and no child still running?
//   3. Deletes the survivors with one guarded `purgeRuns` call per batch. The
//      storage re-checks each run atomically (still ended, same `completedAt`,
//      no rollback owed), so a run resumed or ended again since it was listed
//      is left alone, and several sweepers over one storage never double
//      count.
//   4. Purges terminal step-queue tasks older than `stepQueue`, in batches.
//   5. Stops once `perSweepBudgetMs` is spent; the keyset cursor is kept, so
//      the next sweep resumes where this one stopped.
//
// `dryRun` runs the same decisions and deletes nothing.
//
// Run it standalone (`start()` / `stop()`, or `sweep()` from your own job), or
// hand `retention` to the distributed runner and its leader runs it.
// ---------------------------------------------------------------------------

import { PollLoop } from "../shared/poll-loop.ts";
import { SystemWallClock, type WallClock } from "../shared/wall-clock.ts";
import type { LeaderElection } from "../distributed/leader-election.ts";
import type { StepQueue } from "../distributed/step-queue.ts";
import { hasCapability } from "./storage/capabilities.ts";
import type {
  RetentionCandidate,
  RetentionCursor,
  RetentionStatus,
} from "./storage/retention-store.ts";
import {
  mergeRetentionPolicy,
  retentionAgeMs,
  validateRetentionPolicy,
  type RetentionPolicy,
  type RetentionRunClass,
} from "./retention-policy.ts";
import { emitTrace, type WorkflowTracer } from "./workflow-tracer.ts";
import type { WorkflowStorage } from "./workflow-storage.ts";
import {
  CANCELLED_ERROR_TAG,
  type WorkflowStatus,
  isTerminalWorkflowStatus,
  owesCancelRollback,
} from "./workflow-state.ts";
import type { Workflow } from "./workflow-types.ts";
import type { WorkflowVersionRegistry } from "./workflow-version-registry.ts";

export const DEFAULT_RETENTION_BATCH_SIZE = 500;
export const DEFAULT_RETENTION_BUDGET_MS = 30_000;
export const DEFAULT_RETENTION_INTERVAL_MS = 60 * 60 * 1000;

/** Relation lookups (parent, children) run this many at once. */
const RELATION_CONCURRENCY = 16;

/** Why a listed run was kept. */
export interface RetentionKept {
  /** The run's own class has no age, or the run is younger than it. */
  readonly young: number;
  /** One of the newest `keepAtLeast` runs of its workflow. */
  readonly keepAtLeast: number;
  /** A cancelled run still owing its rollback (or one mid-compensation). */
  readonly owedRollback: number;
  /** Its parent run is still running or owes a rollback. */
  readonly parentActive: number;
  /** One of its child runs is still running. */
  readonly childActive: number;
  /** Changed since it was listed (resumed, ended again, or deleted by another sweeper). */
  readonly changed: number;
}

/** What one sweep did. */
export interface RetentionSweepReport {
  readonly dryRun: boolean;
  readonly startedAt: Date;
  readonly durationMs: number;
  /** Runs deleted; with `dryRun`, runs a real sweep would delete now. */
  readonly deletedRuns: number;
  /** Of `deletedRuns`, by workflow name. */
  readonly deletedByWorkflow: Readonly<Record<string, number>>;
  /** Of `deletedRuns`, by class. */
  readonly deletedByClass: Readonly<Record<RetentionRunClass, number>>;
  /** Terminal step-queue tasks deleted. Not counted in a dry run (`undefined`). */
  readonly deletedStepTasks: number | undefined;
  readonly kept: RetentionKept;
  /** False when `perSweepBudgetMs` ended the sweep before it drained the backlog. */
  readonly complete: boolean;
  /** Set when purging the step queue failed; the run deletes above still stand. */
  readonly stepQueueError?: unknown;
}

export interface RetentionSweeperConfig {
  readonly storage: WorkflowStorage;
  /**
   * The default policy, and the only place `stepQueue` is read. Workflows
   * with their own `retention` override it field by field. Without any age
   * here or on a definition, a sweep deletes nothing.
   */
  readonly retention?: RetentionPolicy;
  /** Definitions whose `retention` applies to their runs. */
  readonly workflows?: readonly Workflow<any, any, any>[];
  /** Registry whose latest version of each name supplies `retention` (added to `workflows`). */
  readonly registry?: WorkflowVersionRegistry;
  /** The step queue `retention.stepQueue` purges. Without it step-queue retention is off. */
  readonly stepQueue?: StepQueue;
  /** Runs (or tasks) deleted per batch. Default: 500. */
  readonly batchSize?: number;
  /** A sweep stops starting new batches once it has run this long (ms). Default: 30 000. */
  readonly perSweepBudgetMs?: number;
  /** Wait between sweeps of `start()` (ms). Default: one hour. */
  readonly intervalMs?: number;
  /** Time source: the age cutoffs, the sweep budget and the `start()` cadence. Default: `SystemWallClock`. */
  readonly clock?: WallClock;
  readonly tracer?: WorkflowTracer;
  /** Called with every report (metrics). Must not throw. */
  readonly onSweep?: (report: RetentionSweepReport) => void;
  /** Called when a sweep started by `start()` fails. Default: `console.error`. */
  readonly onError?: (error: unknown) => void;
  /**
   * Only the leader sweeps in `start()`. Several sweepers over one storage
   * are safe without it (they would only duplicate work).
   */
  readonly leaderElection?: LeaderElection;
}

interface Pass {
  /** Workflow name the storage filters by; undefined = every workflow. */
  readonly name: string | undefined;
  readonly completedBefore: Date;
  readonly statuses: readonly RetentionStatus[];
}

interface Resolved {
  readonly defaults: RetentionPolicy | undefined;
  readonly byName: ReadonlyMap<string, RetentionPolicy>;
}

type Decision =
  | { readonly delete: true; readonly cls: RetentionRunClass }
  | { readonly delete: false; readonly why: keyof RetentionKept };

type RetentionStorage = WorkflowStorage &
  Required<Pick<WorkflowStorage, "listRetentionCandidates" | "purgeRuns">>;

export class RetentionSweeper {
  private readonly storage: RetentionStorage;
  private readonly config: RetentionSweeperConfig;
  private readonly clock: WallClock;
  private readonly batchSize: number;
  private readonly budgetMs: number;
  private readonly loop: PollLoop;
  /** Keyset position per pass, kept between sweeps so a budget-limited sweep resumes. */
  private readonly cursors = new Map<string, RetentionCursor>();
  /** Sweeps run one at a time; each waits for the one before. */
  private queue: Promise<unknown> = Promise.resolve();
  /** The last sweep's report (diagnostics, health endpoints). */
  lastReport?: RetentionSweepReport;

  constructor(config: RetentionSweeperConfig) {
    if (!hasCapability(config.storage, "retention")) {
      throw new Error(
        "RetentionSweeper needs a storage with the `retention` capability (listRetentionCandidates and purgeRuns)",
      );
    }
    this.storage = config.storage as RetentionStorage;
    this.config = config;
    this.clock = config.clock ?? SystemWallClock;
    this.batchSize = config.batchSize ?? DEFAULT_RETENTION_BATCH_SIZE;
    this.budgetMs = config.perSweepBudgetMs ?? DEFAULT_RETENTION_BUDGET_MS;
    if (!Number.isInteger(this.batchSize) || this.batchSize <= 0) {
      throw new Error("RetentionSweeper batchSize must be a positive integer");
    }
    if (!Number.isFinite(this.budgetMs) || this.budgetMs <= 0) {
      throw new Error("RetentionSweeper perSweepBudgetMs must be a positive finite number");
    }
    const intervalMs = config.intervalMs ?? DEFAULT_RETENTION_INTERVAL_MS;
    if (!Number.isFinite(intervalMs) || intervalMs <= 0) {
      throw new Error("RetentionSweeper intervalMs must be a positive finite number");
    }
    if (config.retention !== undefined) {
      validateRetentionPolicy({ policy: config.retention, where: "RetentionSweeper" });
    }
    const onError = config.onError ?? ((error: unknown) => console.error("[retention]", error));
    this.loop = new PollLoop({
      name: "retention-sweeper",
      intervalMs,
      clock: this.clock,
      tick: async () => {
        if (config.leaderElection !== undefined && !(await config.leaderElection.tryAcquire())) {
          return;
        }
        await this.sweep();
      },
      onError: (error) => onError(error),
    });
  }

  /** Sweep every `intervalMs` until `stop()`. Resolves once stopped. A failed sweep is reported through `onError` and retried with backoff. */
  async start(): Promise<void> {
    try {
      await this.loop.start();
    } finally {
      await this.config.leaderElection?.release();
    }
  }

  /** Stop `start()`; resolves once the in-flight sweep has finished. */
  stop(): Promise<void> {
    return this.loop.stop();
  }

  /**
   * One sweep now. With `dryRun`, nothing is deleted and the report says
   * what a real sweep would delete (the step queue is not counted).
   */
  sweep(options: { readonly dryRun?: boolean } = {}): Promise<RetentionSweepReport> {
    const run = this.queue.then(() => this.sweepNow({ dryRun: options.dryRun === true }));
    this.queue = run.catch(() => undefined);
    return run;
  }

  // -------------------------------------------------------------------------

  private async sweepNow(params: { readonly dryRun: boolean }): Promise<RetentionSweepReport> {
    const { dryRun } = params;
    const startedMs = this.clock.currentTimeMs();
    const startedAt = new Date(startedMs);
    const deadlineMs = startedMs + this.budgetMs;
    const spent = (): boolean => this.clock.currentTimeMs() >= deadlineMs;

    const resolved = await this.resolve();
    const tally = new Tally();
    const ctx: SweepContext = {
      dryRun,
      nowMs: startedMs,
      resolved,
      tally,
      spent,
      parents: new Map(),
      boundaries: new Map(),
    };

    let complete = true;
    for (const pass of this.plan({ resolved, nowMs: startedMs })) {
      if (spent()) {
        complete = false;
        break;
      }
      if (!(await this.runPass({ pass, ctx }))) complete = false;
    }

    let deletedStepTasks: number | undefined;
    let stepQueueError: unknown;
    const queueAge = retentionAgeMs(resolved.defaults?.stepQueue);
    if (this.config.stepQueue !== undefined && queueAge !== undefined && !dryRun) {
      deletedStepTasks = 0;
      try {
        for (;;) {
          if (spent()) {
            complete = false;
            break;
          }
          const n = await this.config.stepQueue.purge({
            completedBefore: new Date(startedMs - queueAge),
            limit: this.batchSize,
          });
          deletedStepTasks += n;
          if (n === 0) break;
        }
      } catch (error) {
        stepQueueError = error;
      }
    }

    const endedMs = this.clock.currentTimeMs();
    const report: RetentionSweepReport = {
      dryRun,
      startedAt,
      durationMs: endedMs - startedMs,
      deletedRuns: tally.deleted,
      deletedByWorkflow: Object.fromEntries(tally.byWorkflow),
      deletedByClass: { ...tally.byClass },
      deletedStepTasks,
      kept: { ...tally.kept },
      complete,
      ...(stepQueueError !== undefined && { stepQueueError }),
    };
    this.lastReport = report;
    if (this.config.tracer !== undefined) {
      emitTrace(this.config.tracer, "retentionSweep", {
        startedAt,
        endedAt: new Date(endedMs),
        durationMs: report.durationMs,
        dryRun,
        deletedRuns: report.deletedRuns,
        deletedStepTasks: deletedStepTasks ?? 0,
        keptRuns: Object.values(report.kept).reduce((a, b) => a + b, 0),
        complete,
      });
    }
    try {
      this.config.onSweep?.(report);
    } catch {
      // A metrics hook never changes a sweep.
    }
    return report;
  }

  /** Each name's merged policy (definitions plus registry) and the default. */
  private async resolve(): Promise<Resolved> {
    const own = new Map<string, RetentionPolicy | undefined>();
    for (const wf of this.config.workflows ?? []) own.set(wf.name, wf._definition.retention);
    const registry = this.config.registry;
    if (registry !== undefined) {
      for (const name of await registry.names()) {
        if (own.has(name)) continue;
        const wf = await registry.resolve({ name });
        if (wf !== undefined) own.set(name, wf._definition.retention);
      }
    }
    const defaults = this.config.retention;
    const byName = new Map<string, RetentionPolicy>();
    for (const [name, policy] of own) {
      if (policy === undefined) continue;
      validateRetentionPolicy({ policy, where: `workflow "${name}"` });
      byName.set(name, mergeRetentionPolicy({ defaults, own: policy }));
    }
    return { defaults, byName };
  }

  /** The passes over storage this sweep makes. */
  private plan(params: { readonly resolved: Resolved; readonly nowMs: number }): Pass[] {
    const { resolved, nowMs } = params;
    if (ageClasses(resolved.defaults).length > 0) {
      // One pass over every workflow; each run is judged by its own workflow's policy.
      const policies = [resolved.defaults!, ...resolved.byName.values()];
      return [passFor({ name: undefined, policies, nowMs })];
    }
    const passes: Pass[] = [];
    for (const [name, policy] of resolved.byName) {
      if (ageClasses(policy).length > 0) passes.push(passFor({ name, policies: [policy], nowMs }));
    }
    return passes;
  }

  /** Drain one pass. False when the budget ended it early. */
  private async runPass(params: {
    readonly pass: Pass;
    readonly ctx: SweepContext;
  }): Promise<boolean> {
    const { pass, ctx } = params;
    const key = pass.name ?? "\u0000all";
    let cursor = ctx.dryRun ? undefined : this.cursors.get(key);
    for (;;) {
      if (ctx.spent()) {
        if (cursor !== undefined && !ctx.dryRun) this.cursors.set(key, cursor);
        return false;
      }
      const page = await this.storage.listRetentionCandidates({
        completedBefore: pass.completedBefore,
        limit: this.batchSize,
        statuses: pass.statuses,
        ...(pass.name !== undefined && { name: pass.name }),
        ...(cursor !== undefined && { after: cursor }),
      });
      if (page.length === 0) {
        this.cursors.delete(key);
        return true;
      }
      const doomed = await this.decide({ page, ctx });
      if (doomed.length > 0) {
        if (ctx.dryRun) {
          for (const d of doomed) ctx.tally.delete(d);
        } else {
          const deleted = new Set(
            await this.storage.purgeRuns({
              runs: doomed.map((d) => ({
                workflowId: d.candidate.workflowId,
                completedAt: d.candidate.completedAt,
              })),
            }),
          );
          for (const d of doomed) {
            if (deleted.has(d.candidate.workflowId)) ctx.tally.delete(d);
            else ctx.tally.kept.changed++;
          }
        }
      }
      const last = page[page.length - 1]!;
      cursor = { completedAt: last.completedAt, workflowId: last.workflowId };
      if (page.length < this.batchSize) {
        this.cursors.delete(key);
        return true;
      }
    }
  }

  /** The runs of a page that may be deleted, with their class. */
  private async decide(params: {
    readonly page: readonly RetentionCandidate[];
    readonly ctx: SweepContext;
  }): Promise<Array<{ candidate: RetentionCandidate; cls: RetentionRunClass }>> {
    const { page, ctx } = params;
    const decisions: Decision[] = [];
    for (let i = 0; i < page.length; i += RELATION_CONCURRENCY) {
      const chunk = page.slice(i, i + RELATION_CONCURRENCY);
      const out = await Promise.all(chunk.map((c) => this.decideOne({ candidate: c, ctx })));
      decisions.push(...out);
    }
    const doomed: Array<{ candidate: RetentionCandidate; cls: RetentionRunClass }> = [];
    page.forEach((candidate, i) => {
      const d = decisions[i]!;
      if (d.delete) doomed.push({ candidate, cls: d.cls });
      else ctx.tally.kept[d.why]++;
    });
    return doomed;
  }

  private async decideOne(params: {
    readonly candidate: RetentionCandidate;
    readonly ctx: SweepContext;
  }): Promise<Decision> {
    const { candidate: c, ctx } = params;
    const cls = classOf(c);
    if (cls === undefined) return { delete: false, why: "owedRollback" };

    const policy = ctx.resolved.byName.get(c.workflowName) ?? ctx.resolved.defaults;
    const ageMs = retentionAgeMs(policy?.[cls]);
    if (ageMs === undefined || c.completedAt.getTime() >= ctx.nowMs - ageMs) {
      return { delete: false, why: "young" };
    }

    const keep = policy?.keepAtLeast ?? 0;
    if (keep > 0) {
      const boundary = await this.boundaryOf({ name: c.workflowName, keep, ctx });
      if (boundary === undefined || c.completedAt.getTime() >= boundary) {
        return { delete: false, why: "keepAtLeast" };
      }
    }

    if (c.parentWorkflowId !== undefined && (await this.parentNeedsChild({ c, ctx }))) {
      return { delete: false, why: "parentActive" };
    }
    if (await this.hasActiveChild({ workflowId: c.workflowId })) {
      return { delete: false, why: "childActive" };
    }
    return { delete: true, cls };
  }

  /**
   * `completedAt` (ms) of the keep-th newest ended run of `name`: runs at or
   * after it are kept. Undefined when the workflow has fewer ended runs than
   * that, so all of them are kept.
   */
  private boundaryOf(params: {
    readonly name: string;
    readonly keep: number;
    readonly ctx: SweepContext;
  }): Promise<number | undefined> {
    const { name, keep, ctx } = params;
    let pending = ctx.boundaries.get(name);
    if (pending === undefined) {
      pending = (async () => {
        const rows = await this.listRuns({
          name,
          orderBy: "completedAt",
          orderDir: "desc",
          limit: keep,
        });
        const ended = rows.filter((r) => isTerminalWorkflowStatus(r.status) && r.completedAt);
        return ended.length < keep ? undefined : ended[keep - 1]!.completedAt!.getTime();
      })();
      ctx.boundaries.set(name, pending);
    }
    return pending;
  }

  /** The parent still exists and is running or owes a rollback. */
  private parentNeedsChild(params: {
    readonly c: RetentionCandidate;
    readonly ctx: SweepContext;
  }): Promise<boolean> {
    const { c, ctx } = params;
    const parentId = c.parentWorkflowId!;
    let pending = ctx.parents.get(parentId);
    if (pending === undefined) {
      pending = this.storage.loadWorkflow(parentId).then((parent) => {
        if (parent === null) return false;
        return !isTerminalWorkflowStatus(parent.status) || owesCancelRollback(parent);
      });
      ctx.parents.set(parentId, pending);
    }
    return pending;
  }

  /** Some child of the run has not ended. */
  private async hasActiveChild(params: { readonly workflowId: string }): Promise<boolean> {
    const pageSize = 100;
    for (let offset = 0; ; offset += pageSize) {
      const children = await this.listRuns({
        parentId: params.workflowId,
        limit: pageSize,
        offset,
      });
      if (children.some((child) => !isTerminalWorkflowStatus(child.status))) return true;
      if (children.length < pageSize) return false;
    }
  }

  private listRuns(
    params: Parameters<WorkflowStorage["listWorkflows"]>[0],
  ): Promise<ReadonlyArray<{ status: WorkflowStatus; completedAt?: Date }>> {
    return hasCapability(this.storage, "summaries")
      ? this.storage.listWorkflowSummaries(params)
      : this.storage.listWorkflows(params);
  }
}

// ---------------------------------------------------------------------------

interface SweepContext {
  readonly dryRun: boolean;
  readonly nowMs: number;
  readonly resolved: Resolved;
  readonly tally: Tally;
  readonly spent: () => boolean;
  /** Per-sweep caches, so a parent or workflow is read once however many runs ask. */
  readonly parents: Map<string, Promise<boolean>>;
  readonly boundaries: Map<string, Promise<number | undefined>>;
}

class Tally {
  deleted = 0;
  readonly byWorkflow = new Map<string, number>();
  readonly byClass: Record<RetentionRunClass, number> = { completed: 0, failed: 0, cancelled: 0 };
  readonly kept = {
    young: 0,
    keepAtLeast: 0,
    owedRollback: 0,
    parentActive: 0,
    childActive: 0,
    changed: 0,
  };

  delete(d: { candidate: RetentionCandidate; cls: RetentionRunClass }): void {
    this.deleted++;
    this.byClass[d.cls]++;
    const name = d.candidate.workflowName;
    this.byWorkflow.set(name, (this.byWorkflow.get(name) ?? 0) + 1);
  }
}

/** The class a run is judged by; undefined for a run that must not be deleted (rollback owed). */
function classOf(c: RetentionCandidate): RetentionRunClass | undefined {
  if (c.status !== "failed") return "completed";
  if (c.compensationOrigin === "cancel" || c.compensationStatus === "compensating") {
    return undefined;
  }
  return c.errorTag === CANCELLED_ERROR_TAG ? "cancelled" : "failed";
}

/** The classes of a policy that have an age. */
function ageClasses(policy: RetentionPolicy | undefined): RetentionRunClass[] {
  return (["completed", "failed", "cancelled"] as const).filter(
    (cls) => retentionAgeMs(policy?.[cls]) !== undefined,
  );
}

/** One pass: the cutoff of the shortest age among `policies`, and the statuses their classes cover. */
function passFor(params: {
  readonly name: string | undefined;
  readonly policies: readonly RetentionPolicy[];
  readonly nowMs: number;
}): Pass {
  let minAge = Infinity;
  const statuses = new Set<RetentionStatus>();
  for (const policy of params.policies) {
    for (const cls of ageClasses(policy)) {
      minAge = Math.min(minAge, retentionAgeMs(policy[cls])!);
      if (cls === "completed") {
        statuses.add("completed");
        statuses.add("tripwire");
      } else {
        statuses.add("failed");
      }
    }
  }
  return {
    name: params.name,
    completedBefore: new Date(params.nowMs - minAge),
    statuses: [...statuses],
  };
}
