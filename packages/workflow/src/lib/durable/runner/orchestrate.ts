// ---------------------------------------------------------------------------
// Orchestration loop — drives one workflow run end-to-end: version drain,
// idempotency cache, lock + heartbeat, then under the lock the
// continue-as-new chain, load-or-create, the entry gate for terminal runs,
// workflow-level retry around the DAG executor, the terminal transitions
// (complete / tripwire / compensate + fail + DLQ) and the lifecycle hooks
// at each of them.
// ---------------------------------------------------------------------------

import type { TaggedError } from "../../shared/tagged-error.ts";
import { runHookResult } from "../../shared/eff.ts";
import { nextRetryDelayMs, type WorkflowRetryPolicy } from "../../shared/retry-policy.ts";
import { SystemWallClock, type WallClock } from "../../shared/wall-clock.ts";
import {
  TripwireStorageMissingError,
  WorkflowError,
  WorkflowTripwireError,
  WorkflowVersionMismatchError,
  type WorkflowContinueAsNewError,
} from "../durable-pipeline-error.ts";
import { clearQueryHandlers } from "../query-registry.ts";
import { isAbandonRunExit, isControlFlowExit } from "../step-policy.ts";
import { withLock, type LockContext } from "../with-lock.ts";
import { topologicalSort, type DagNode } from "../workflow-dag.ts";
import { isCancelledRun, isTerminalWorkflowStatus, type WorkflowState } from "../workflow-state.ts";
import { isTripwireCapableStorage } from "../workflow-storage.ts";
import type { Workflow } from "../durable-pipeline.ts";
import { compensateWorkflow } from "./compensation.ts";
import type { DagExecutionContext } from "./dag-context.ts";
import { executeWorkflowDag } from "./dag-executor.ts";
import { resolveDrainContext } from "./definition-resolver.ts";
import { publishDlqRecord } from "./dlq.ts";
import { fireHook } from "./hooks.ts";
import { getIdempotencyTtl } from "./idempotency.ts";
import {
  orchestrationContextFor,
  runtimeOf,
  type OrchestrationRuntime,
  type WorkflowOrchestrationContext,
} from "./orchestration-context.ts";
import { assertRunActive, cancelledError, rejectEndedRun, runStartMs } from "./run-status.ts";
import { checkpointWrite } from "./step-checkpoint.ts";
import { errorMessage, errorTagOf } from "./step-body.ts";

/**
 * Default lock extension for orchestration runs. Matches `withLock`'s
 * `DEFAULT_LOCK_EXTENSION_MS` so a 30s heartbeat keeps the lock healthy
 * with a ~90s grace window for transient network hiccups.
 */
const DEFAULT_LOCK_DURATION_MS = 120_000;

/** Hard cap on chained continue-as-new restarts, to catch infinite loops in user code. */
const MAX_CONTINUE_AS_NEW_CHAIN = 1024;

interface RunParams {
  readonly workflowId: string;
  readonly input: unknown;
  readonly force?: boolean;
  readonly namespace?: string;
  readonly idempotencyKey?: string;
  readonly idempotencyExpiresAt?: Date;
}

/**
 * Run a workflow end-to-end: version-drain pre-check, idempotency cache,
 * then under the workflow lock (with heartbeat) the continue-as-new chain
 * of runs.
 *
 * A stored run that already ended is not executed again: `completed`
 * returns its stored result, `failed`, cancelled and `tripwire` reject
 * with `WorkflowFailedError`, `WorkflowCancelledError` and
 * `WorkflowTripwireError`. With `force`, an ended run is archived and a
 * fresh run starts instead (`startFreshRun`). To re-drive a failed run
 * from a step, use `WorkflowRunner.resume`.
 *
 * Query handlers registered by the run are cleared when it ends (not when
 * it suspends), and only by the invocation that holds the lock, so a
 * duplicate call rejected with `WorkflowLockError` leaves the live run's
 * handlers in place.
 */
export async function runWorkflowOrchestration(
  ctx: WorkflowOrchestrationContext,
  params: RunParams,
): Promise<unknown> {
  const { workflowId, input, force, namespace } = params;
  const clock = ctx.clock ?? SystemWallClock;
  const idempotency = force ? undefined : ctx.idempotency;

  // Version drain — a run stored under an older version is driven by the
  // matching previousVersion definition.
  const drainCtx = await resolveDrainContext({ ctx, workflowId });
  if (drainCtx) {
    return runWorkflowOrchestration(drainCtx, { workflowId, input, force, namespace });
  }

  // Idempotency check — return the cached outcome if within TTL.
  if (idempotency) {
    const existing = await ctx.storage.loadWorkflow(workflowId);
    const cached = existing ? cachedIdempotentOutcome({ ctx, state: existing, clock }) : undefined;
    if (cached) return cached.result;
  }

  return withLock({
    storage: ctx.storage,
    workflowId,
    options: { lockDurationMs: DEFAULT_LOCK_DURATION_MS, clock },
    fn: async (lock) => {
      try {
        const result = await runChain({ ctx, params, lock, clock });
        clearQueryHandlers(workflowId);
        return result;
      } catch (err) {
        // A suspended run is still hosted here: the resume re-registers
        // its handlers on replay, so they stay up across the wait.
        if (errorTag(err) !== "WorkflowSuspendedError") clearQueryHandlers(workflowId);
        throw err;
      }
    },
  });
}

/**
 * The continue-as-new chain, run while holding the lock: a run that asks
 * to continue is archived (`startFreshRun`) and the next run starts under
 * the same lock, so no other worker can resume or archive the old run in
 * between.
 */
async function runChain(params: {
  ctx: WorkflowOrchestrationContext;
  params: RunParams;
  lock: LockContext;
  clock: WallClock;
}): Promise<unknown> {
  const { ctx, lock, clock } = params;
  const { workflowId } = params.params;
  let input = params.params.input;
  for (let chain = 0; chain < MAX_CONTINUE_AS_NEW_CHAIN; chain++) {
    try {
      return await runOneOrchestrationCycle({
        ctx,
        lock,
        clock,
        // A continued run is an internal restart: no idempotency cache,
        // and the key stays on the archived row.
        params:
          chain === 0
            ? { ...params.params, input }
            : { workflowId, input, force: true, namespace: params.params.namespace },
      });
    } catch (err) {
      if (errorTag(err) !== "WorkflowContinueAsNewError") throw err;
      await ctx.storage.startFreshRun(workflowId);
      clearQueryHandlers(workflowId);
      input = (err as WorkflowContinueAsNewError).nextInput;
    }
  }
  throw new Error(
    `Workflow "${workflowId}" exceeded continue-as-new chain limit (${MAX_CONTINUE_AS_NEW_CHAIN}). ` +
      `Likely an infinite continue-as-new loop in the workflow body.`,
  );
}

/**
 * Run a child workflow on its parent's runtime (`.subworkflow()`, journaled
 * `ctx.child`). The child row is created first, with the parent pointer and
 * the child's version, so `listWorkflows({ parentId })` and recovery see
 * the relationship. Creation is create-if-absent: a row that already exists
 * (a re-run of the parent step) is resumed as-is. Any other create failure
 * propagates. The child then runs under its own lock through the same
 * orchestration loop, inheriting storage, clock, step executor, executor id
 * and runner-level hooks.
 */
export async function runChildWorkflow(params: {
  readonly runtime: OrchestrationRuntime;
  readonly parentWorkflowId: string;
  readonly workflow: Workflow<unknown, unknown>;
  readonly workflowId: string;
  readonly input: unknown;
}): Promise<unknown> {
  const { runtime, workflow, workflowId, input } = params;
  const def = workflow._definition;
  const created = await runtime.storage.createWorkflow({
    workflowId,
    workflowName: workflow.name,
    input,
    workflowType: def.type,
    parentWorkflowId: params.parentWorkflowId,
    metadata: def.metadata,
    version: workflow.version,
  });
  // A child that failed is run again from scratch when its parent step is
  // retried; a completed, cancelled or tripwired child answers as stored.
  const existing = created.created ? undefined : created.existing;
  const rerunFailed =
    existing !== undefined && existing.status === "failed" && !isCancelledRun(existing);
  return runWorkflowOrchestration(orchestrationContextFor({ workflow, runtime }), {
    workflowId,
    input,
    ...(rerunFailed && { force: true }),
  });
}

async function runOneOrchestrationCycle(cycle: {
  ctx: WorkflowOrchestrationContext;
  params: RunParams;
  lock: LockContext;
  clock: WallClock;
}): Promise<unknown> {
  const { ctx, lock, clock } = cycle;
  const { workflowId, input, force, namespace } = cycle.params;
  const workflowStartTime = clock.currentTimeMs();
  const idempotency = force ? undefined : ctx.idempotency;
  const guard = lock.fenceToken ? { fenceToken: lock.fenceToken } : undefined;

  // 1. Load the run.
  let state = await ctx.storage.loadWorkflow(workflowId);

  // Double-check idempotency under the lock — prevents a race with a
  // concurrent run that completed after the pre-check.
  if (idempotency && state?.completedAt) {
    const cached = cachedIdempotentOutcome({ ctx, state, clock });
    if (cached) return cached.result;
    const onExpiry = idempotency.onExpiry ?? "fresh-run";
    if (onExpiry === "fresh-run" && (state.status === "completed" || state.status === "failed")) {
      await ctx.storage.startFreshRun(workflowId);
      state = await ctx.storage.loadWorkflow(workflowId);
    }
  }

  // 2. Create the run, or check the stored version of an existing one.
  if (!state) {
    const createResult = await ctx.storage.createWorkflow({
      workflowId,
      workflowName: ctx.name,
      input,
      workflowType: ctx.type,
      ...(namespace !== undefined ? { namespace } : {}),
      metadata: ctx.metadata,
      version: ctx.version,
      ...(cycle.params.idempotencyKey && cycle.params.idempotencyExpiresAt
        ? {
            idempotencyKey: cycle.params.idempotencyKey,
            idempotencyExpiresAt: cycle.params.idempotencyExpiresAt,
          }
        : {}),
    });
    if (!createResult.created) {
      // Race: another caller created the workflow between load and
      // create — could be the same workflowId (PK collision) or a
      // concurrent create under the same idempotency key (partial
      // unique-index collision). Either way, attach to whoever won.
      state = createResult.existing;
    } else {
      state = await ctx.storage.loadWorkflow(workflowId);
    }
  } else if (ctx.version) {
    // Version mismatch check — only when builder explicitly sets a version
    const storedVersion = state.version;
    if (storedVersion !== ctx.version) {
      throw new WorkflowVersionMismatchError({
        workflowId,
        expected: ctx.version,
        actual: storedVersion ?? "(none)",
        message:
          `Workflow "${workflowId}" was created with version "${storedVersion ?? "(none)"}" ` +
          `but current code is version "${ctx.version}". ` +
          `To resume this workflow, either use \`onVersionMismatch: "drain"\` + ` +
          `\`previousVersions: [v${storedVersion ?? "N"}]\` on the workflow config, or ` +
          `register both versions in a WorkflowVersionRegistry.`,
      });
    }
  }

  // 3. Entry gate — an ended run is not executed again: a completed run
  // answers with its result, decoded as a replay would; any other ended run
  // rejects with its stored outcome. `force` archives it and starts a fresh
  // run instead.
  if (state && isTerminalWorkflowStatus(state.status)) {
    if (!force) {
      rejectEndedRun(state);
      return completedRunResult({ ctx, state });
    }
    await ctx.storage.startFreshRun(workflowId);
    state = await ctx.storage.loadWorkflow(workflowId);
  }

  // 4. Validate DAG
  const dagNodes: DagNode[] = ctx.steps.map((s) => ({
    name: s.name,
    dependsOn: s.dependsOn,
  }));
  topologicalSort({ nodes: dagNodes, workflowId });

  // The deadline runs from the run's persisted start, so a resume after a
  // sleep or signal wait does not restart it.
  const deadlineMs =
    ctx.timeoutMs != null && state
      ? runStartMs({ state, clock }) + ctx.timeoutMs
      : ctx.timeoutMs != null
        ? workflowStartTime + ctx.timeoutMs
        : undefined;

  // 5. Execute DAG with workflow-level retry
  let lastStepError: unknown = null;
  // Shared across workflow retries so attempt counters keep incrementing
  const stepAttempts = new Map<string, number>();

  const dagCtx: DagExecutionContext = {
    storage: ctx.storage,
    steps: ctx.steps,
    hooks: ctx.hooks,
    timeoutMs: ctx.timeoutMs,
    dispatch: ctx.dispatch,
    stepExecutor: ctx.stepExecutor,
    guard,
    clock,
    signal: lock.signal,
    workflowName: ctx.name,
    workflowQueue: ctx.queue,
    ...(ctx.executorId !== undefined && { executorId: ctx.executorId }),
    ...(ctx.version !== undefined && { workflowVersion: ctx.version }),
    ...(ctx.patches !== undefined && { patches: ctx.patches }),
    runChild: (child) =>
      runChildWorkflow({ runtime: runtimeOf(ctx), parentWorkflowId: workflowId, ...child }),
  };

  const retryPolicy = workflowRetryPolicy(ctx);
  let firstFailureMs: number | undefined;
  for (let workflowAttempt = 0; ; workflowAttempt++) {
    const dagResult = await executeWorkflowDag(dagCtx, {
      workflowId,
      input,
      dagNodes,
      state,
      workflowStartTime,
      stepAttempts,
      ...(deadlineMs !== undefined && { deadlineMs }),
    });

    if (dagResult.success) {
      // 6. Complete workflow. The write is conditional: a cancel that
      // landed first stays, and the run reports it.
      const finalResult = dagResult.result;
      await checkpointWrite({
        clock,
        workflowId,
        operation: "completeWorkflow",
        write: () => ctx.storage.completeWorkflow(workflowId, finalResult, guard),
      });
      await assertNotCancelled({ ctx, workflowId });
      await fireHook({
        hooks: ctx.hooks,
        name: "onWorkflowComplete",
        event: {
          workflowId,
          result: finalResult,
          durationMs: clock.currentTimeMs() - workflowStartTime,
        },
      });
      return finalResult;
    }

    // Tripwire — intentional early exit. Skip compensation + DLQ since
    // this is not a failure. Mark the workflow with `status: "tripwire"`
    // and throw a typed error carrying the reason so callers using
    // `run()` can `instanceof`-check it; `runSafe()` surfaces it as
    // `{ data: null, error }`.
    if ("tripwire" in dagResult) {
      const { stepName, reason } = dagResult;
      const storage = ctx.storage;
      if (!isTripwireCapableStorage(storage)) {
        throw new TripwireStorageMissingError({
          workflowId,
          stepName,
          message:
            `Tripwire step "${stepName}" fired but the configured ` +
            `WorkflowStorage does not implement tripwireWorkflow. Use a ` +
            `storage backend that supports tripwire (InMemory, Postgres) ` +
            `or remove the .tripwire() step.`,
        });
      }
      await checkpointWrite({
        clock,
        workflowId,
        operation: "tripwireWorkflow",
        write: () => storage.tripwireWorkflow(workflowId, reason, guard),
      });
      await assertNotCancelled({ ctx, workflowId });
      await fireHook({
        hooks: ctx.hooks,
        name: "onWorkflowTripwire",
        event: {
          workflowId,
          stepName,
          reason,
          durationMs: clock.currentTimeMs() - workflowStartTime,
        },
      });
      throw new WorkflowTripwireError({
        workflowId,
        stepName,
        reason,
        message: `Workflow "${workflowId}" ended via tripwire at step "${stepName}"`,
      });
    }

    // Suspension and continue-as-new unwind cleanly — no compensation, no
    // failure recording. Continue-as-new is caught by `runChain`, which
    // starts the next run under the same lock.
    if (dagResult.suspension || dagResult.continueAsNew) throw dagResult.error;

    // A lost lock or an unsaved checkpoint abandons the run as it stands:
    // the lock's next holder or recovery re-drives it from storage.
    if (isAbandonRunExit(dagResult.error)) throw dagResult.error;

    lastStepError = dagResult.error;

    // A cancel that landed during the failing wave wins over the failure.
    await assertNotCancelled({ ctx, workflowId });

    const nowMs = clock.currentTimeMs();
    firstFailureMs ??= nowMs;
    const delay =
      retryPolicy === undefined
        ? undefined
        : nextRetryDelayMs({
            policy: retryPolicy,
            retry: workflowAttempt,
            error: dagResult.error as TaggedError,
            firstFailureMs,
            nowMs,
          });
    if (delay === undefined) break;

    await new Promise<void>((r) => clock.setTimeout(() => r(), delay));
    await assertRunActive({ storage: ctx.storage, workflowId, signal: lock.signal });
    // Reload state to pick up checkpointed steps
    state = await ctx.storage.loadWorkflow(workflowId);
  }

  // Not retryable or retries exhausted — run the compensation cascade
  const compensationReport = await compensateWorkflow({
    storage: ctx.storage,
    steps: ctx.steps,
    compensateConfig: ctx.compensateConfig,
    workflowId,
    input,
    dagNodes,
    guard,
    clock,
    ...(ctx.executorId !== undefined && { executorId: ctx.executorId }),
  });

  // Fire workflow-level onComplete callback
  if (ctx.compensateConfig?.onComplete) {
    try {
      const result = ctx.compensateConfig.onComplete({
        input,
        error: lastStepError,
        compensatedSteps: compensationReport.compensated,
        failedCompensations: compensationReport.failed,
      });
      await runHookResult(result);
    } catch {
      // onComplete failure is swallowed — the original error is more important
    }
  }

  // Fail the workflow, keeping the error's tag with it.
  const errorMsg = errorMessage(lastStepError);
  const failedTag = errorTagOf(lastStepError);
  await checkpointWrite({
    clock,
    workflowId,
    operation: "failWorkflow",
    write: () =>
      ctx.storage.failWorkflow(workflowId, errorMsg, guard, {
        ...(failedTag !== undefined && { errorTag: failedTag }),
      }),
  });
  await fireHook({
    hooks: ctx.hooks,
    name: "onWorkflowFailure",
    event: {
      workflowId,
      error: errorMsg,
      durationMs: clock.currentTimeMs() - workflowStartTime,
    },
  });

  // Publish to DLQ
  if (ctx.dlq) {
    await publishDlqRecord({
      dlq: ctx.dlq,
      storage: ctx.storage,
      workflowId,
      workflowName: ctx.name,
      input,
      errorMsg,
      clock,
      compensationReport,
      metadata: ctx.metadata,
    });
  }

  throw lastStepError;
}

/**
 * The result a completed run answers with: its last step's stored result,
 * decoded through the step's codec (what replaying the run would return),
 * or the stored workflow result when that row is missing.
 */
function completedRunResult(params: {
  ctx: WorkflowOrchestrationContext;
  state: WorkflowState;
}): unknown {
  const { ctx, state } = params;
  const lastStep = ctx.steps[ctx.steps.length - 1];
  const row = lastStep ? state.steps[lastStep.name] : undefined;
  if (lastStep && row?.status === "completed") return lastStep.codec.decode(row.result);
  return state.result;
}

/**
 * The workflow-level retry policy, or `undefined` when the run must not
 * retry (no policy, or compensation triggers immediately). Its `when` only
 * admits failures worth another run (see `WorkflowRetryPolicy`).
 */
function workflowRetryPolicy(
  ctx: WorkflowOrchestrationContext,
): WorkflowRetryPolicy<TaggedError> | undefined {
  const policy = ctx.retry;
  if (policy === undefined) return undefined;
  if ((ctx.compensateConfig?.trigger ?? "after-retries") === "immediate") return undefined;
  const userWhen = policy.when;
  return {
    ...policy,
    when: (error) => {
      if (isControlFlowExit(error)) return false;
      const tag = errorTag(error);
      if (tag === "WorkflowDeadlineError" || tag === "WorkflowCancelledError") return false;
      if (tag === undefined && policy.retryDefects !== true) return false;
      return userWhen === undefined || userWhen(error);
    },
  };
}

/**
 * The idempotency cache's answer for a stored run inside its TTL: the
 * stored result of a completed run, a `WorkflowError` for a failed one.
 * `undefined` when no cached outcome applies.
 */
function cachedIdempotentOutcome(params: {
  ctx: WorkflowOrchestrationContext;
  state: WorkflowState;
  clock: WallClock;
}): { readonly result: unknown } | undefined {
  const { ctx, state, clock } = params;
  if (!state.completedAt) return undefined;
  const elapsed = clock.currentTimeMs() - state.completedAt.getTime();
  const ttl = getIdempotencyTtl(ctx.idempotency, state.status);
  if (ttl === undefined || elapsed >= ttl) return undefined;
  if (state.status === "completed") return { result: state.result };
  if (state.status === "failed") {
    throw new WorkflowError({
      workflowId: state.workflowId,
      message: state.error ?? `Workflow "${state.workflowId}" failed (cached, TTL ${ttl}ms)`,
    });
  }
  return undefined;
}

/**
 * After a terminal write (which is conditional), reject with
 * `WorkflowCancelledError` if a cancel won. A failed read leaves the
 * write's outcome standing.
 */
async function assertNotCancelled(params: {
  ctx: WorkflowOrchestrationContext;
  workflowId: string;
}): Promise<void> {
  const { ctx, workflowId } = params;
  let status;
  try {
    status = await ctx.storage.loadWorkflowStatus(workflowId);
  } catch {
    return;
  }
  if (status !== null && isCancelledRun(status)) throw cancelledError(workflowId);
}

/** `_tag` of a thrown value, when it has one. */
function errorTag(err: unknown): string | undefined {
  if (typeof err !== "object" || err === null) return undefined;
  const tag = (err as { readonly _tag?: unknown })._tag;
  return typeof tag === "string" ? tag : undefined;
}
