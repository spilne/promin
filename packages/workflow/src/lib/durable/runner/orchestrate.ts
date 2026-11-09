// ---------------------------------------------------------------------------
// Orchestration loop — drives one workflow run end-to-end: version drain,
// idempotency cache, lock + heartbeat, then under the lock the
// continue-as-new chain, load-or-create, the entry gate for terminal runs,
// workflow-level retry around the DAG executor, the terminal transitions
// (complete / tripwire / compensate + fail + DLQ) and the lifecycle hooks
// at each of them.
// ---------------------------------------------------------------------------

import { eventOfWakeSignal } from "../wait-for-event.ts";
import type { UsageTotals } from "../budget.ts";
import type { TaggedError } from "../../shared/tagged-error.ts";
import { LosslessJsonCodec } from "@spilne/perfect-core/connect";
import { runHookResult } from "../../shared/eff.ts";
import { nextRetryDelayMs, type WorkflowRetryPolicy } from "../../shared/retry-policy.ts";
import { SystemWallClock, type WallClock } from "../../shared/wall-clock.ts";
import {
  TripwireStorageMissingError,
  WorkflowError,
  WorkflowLockError,
  WorkflowTripwireError,
  WorkflowRunNotFoundError,
  WorkflowVersionMismatchError,
  type WorkflowContinueAsNewError,
} from "../durable-pipeline-error.ts";
import { openQueryScope, type QueryScope } from "../query-registry.ts";
import { isAbandonRunExit, isControlFlowExit } from "../step-policy.ts";
import { withLock, type LockContext } from "../with-lock.ts";
import { topologicalSort, type DagNode } from "../workflow-dag.ts";
import {
  CANCELLED_ERROR,
  CANCELLED_ERROR_TAG,
  isCancelledRun,
  isFinalFailedRun,
  isTerminalWorkflowStatus,
  owesCancelRollback,
  type WorkflowState,
  type WorkflowStatus,
} from "../workflow-state.ts";
import { hasCapability, type FenceGuard } from "../workflow-storage.ts";
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
import { wakeParentOfEndedRun } from "../child-wake.ts";
import {
  assertRunActive,
  cancelledError,
  rejectEndedRun,
  runFailure,
  runStartMs,
} from "./run-status.ts";
import { checkpointWrite } from "./step-checkpoint.ts";
import type { WorkflowStorage } from "../workflow-storage.ts";
import { errorMessage, errorTagOf } from "./step-body.ts";
import { failureFromError, workflowFailure, type WorkflowFailure } from "../workflow-failure.ts";
import {
  emitTrace,
  traceErrorOf,
  type WorkflowTraceRef,
  type WorkflowTraceRunOutcome,
} from "../workflow-tracer.ts";

/**
 * Default lock extension for orchestration runs. Matches `withLock`'s
 * `DEFAULT_LOCK_EXTENSION_MS` so a 30s heartbeat keeps the lock healthy
 * with a ~90s grace window for transient network hiccups.
 */
const DEFAULT_LOCK_DURATION_MS = 120_000;

/** Hard cap on chained continue-as-new restarts, to catch infinite loops in user code. */
const MAX_CONTINUE_AS_NEW_CHAIN = 1024;

interface RunParams {
  /**
   * Receives the structured failure when this run fails, so `runSafe` reads
   * the failure of its own run (the thrown error may be shared by runs).
   */
  readonly failureSink?: { failure?: WorkflowFailure };
  readonly workflowId: string;
  readonly input: unknown;
  readonly force?: boolean;
  readonly namespace?: string;
  readonly idempotencyKey?: string;
  readonly idempotencyExpiresAt?: Date;
  /**
   * The run's parent is driving it (`runChildWorkflow`) and reads its
   * outcome directly, so an ended run does not wake the parent.
   */
  readonly drivenByParent?: boolean;
  /**
   * Called once this invocation holds the run's lock, before anything is
   * read or written under it (`WorkflowRunner.start` resolves on it).
   */
  readonly onLocked?: () => void;
  /**
   * Leave a stored run that is still in flight (pending, running,
   * suspended, compensating) alone: reject with `WorkflowLockError`, as if
   * its lock were held elsewhere. Checked under the lock, so two
   * concurrent callers cannot both start the run.
   */
  readonly rejectInFlight?: boolean;
  /**
   * Abandon the run: once aborted, it stops at its next safe point as on a
   * lost lock (`WorkflowLockLostError`) and releases the lock, leaving the
   * run in flight for another driver.
   */
  readonly signal?: AbortSignal;
  /** Drive the stored run with its stored input; no stored run → `WorkflowRunNotFoundError`. */
  readonly useStoredInput?: boolean;
  /**
   * W3C `traceparent` of the request that starts or resumes the run, for the
   * tracer's `runStart`. With a tracer, a run created by this call also
   * stores it as `metadata.traceparent`, so a later resume joins the trace.
   */
  readonly traceparent?: string;
  /** The run is a child workflow of this run (`runChildWorkflow`). */
  readonly parentWorkflowId?: string;
}

/** The open trace of a run: its identity and when `runStart` was emitted. */
interface TraceRun {
  readonly ref: WorkflowTraceRef;
  readonly startedAt: Date;
}

/** Where a cycle is: executing (a cancel may roll back), its input, its trace. */
interface CyclePhase {
  executing: boolean;
  input: unknown;
  trace?: TraceRun;
  /** The run's usage as the DAG executor last folded it (for the run-end trace). */
  readonly usage: { current?: UsageTotals };
}

/** Statuses `rejectInFlight` treats as a run still in flight. */
const IN_FLIGHT_STATUSES: ReadonlySet<WorkflowStatus> = new Set([
  "pending",
  "running",
  "suspended",
  "compensating",
]);

/** The run's state as last loaded under the lock, for the parent wake. */
interface LoadedRun {
  state?: WorkflowState | null;
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
 * Query handlers registered by the run live in a scope the invocation opens
 * once it holds the lock: they are dropped when the run ends, kept for
 * `suspendedTtlMs` when it suspends, and never touched by a duplicate call
 * rejected with `WorkflowLockError`.
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
    return runWorkflowOrchestration(drainCtx, {
      workflowId,
      input,
      force,
      namespace,
      ...(params.drivenByParent && { drivenByParent: true }),
      ...(params.failureSink !== undefined && { failureSink: params.failureSink }),
      ...(params.onLocked !== undefined && { onLocked: params.onLocked }),
      ...(params.rejectInFlight === true && { rejectInFlight: true }),
      ...(params.signal !== undefined && { signal: params.signal }),
      ...(params.useStoredInput === true && { useStoredInput: true }),
      ...(params.traceparent !== undefined && { traceparent: params.traceparent }),
      ...(params.parentWorkflowId !== undefined && { parentWorkflowId: params.parentWorkflowId }),
    });
  }

  // Idempotency check — return the cached outcome if within TTL.
  if (idempotency) {
    const existing = await ctx.storage.loadWorkflow(workflowId);
    const cached = existing ? cachedIdempotentOutcome({ ctx, state: existing, clock }) : undefined;
    if (cached) return cached.result;
  }

  const loaded: LoadedRun = {};
  try {
    return await withLock({
      storage: ctx.storage,
      workflowId,
      // The run's state comes with the lock: one round trip instead of two.
      options: {
        lockDurationMs: ctx.lock?.durationMs ?? DEFAULT_LOCK_DURATION_MS,
        ...(ctx.lock?.heartbeatIntervalMs !== undefined && {
          heartbeatIntervalMs: ctx.lock.heartbeatIntervalMs,
        }),
        ...(ctx.lock?.maxHoldMs !== undefined && { maxHoldMs: ctx.lock.maxHoldMs }),
        clock,
        loadState: true,
        watchCancel: true,
        ...(params.signal !== undefined && { signal: params.signal }),
      },
      fn: async (lock) => {
        // Before the try: a rejected in-flight run is someone else's, and
        // its query handlers stay registered.
        if (params.rejectInFlight === true) {
          const stored =
            lock.state !== undefined ? lock.state : await ctx.storage.loadWorkflow(workflowId);
          if (stored && IN_FLIGHT_STATUSES.has(stored.status)) {
            throw new WorkflowLockError({
              workflowId,
              message: `Workflow "${workflowId}" is already running`,
            });
          }
        }
        params.onLocked?.();
        const stopTracingLock = traceLock({
          ctx,
          lock,
          clock,
          workflowId,
          namespace: params.namespace ?? lock.state?.namespace,
        });
        const queries = openQueryScope(workflowId);
        try {
          const result = await runChain({ ctx, params, lock, clock, loaded, queries });
          queries.close();
          stopTracingLock();
          return result;
        } catch (err) {
          stopTracingLock();
          // A suspended run keeps its handlers for a while: a resume here
          // re-registers them on replay, so they stay up across the wait.
          queries.close({ suspended: errorTag(err) === "WorkflowSuspendedError" });
          throw err;
        }
      },
    });
  } finally {
    // After the lock is released, so the woken parent can drive the child.
    if (!params.drivenByParent) await wakeParentIfEnded({ ctx, loaded, clock });
  }
}

/**
 * A child run that ended wakes its parent (`wakeParentOfEndedRun`), which
 * may be parked on it. The wake is retried like a checkpoint; one that
 * still fails is dropped, leaving the parent to its own wake time or to the
 * next run of this child, which delivers the wake again.
 */
async function wakeParentIfEnded(params: {
  ctx: WorkflowOrchestrationContext;
  loaded: LoadedRun;
  clock: WallClock;
}): Promise<void> {
  const { ctx, clock } = params;
  const state = params.loaded.state;
  if (!state || state.parentWorkflowId === undefined) return;
  try {
    await checkpointWrite({
      clock,
      workflowId: state.workflowId,
      operation: "wakeParent",
      write: async () => {
        const status = await ctx.storage.loadWorkflowStatus(state.workflowId);
        if (!status) return;
        await wakeParentOfEndedRun({
          storage: ctx.storage,
          state: { ...state, status: status.status },
        });
      },
    });
  } catch {
    // Best effort: see above.
  }
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
  loaded: LoadedRun;
  /** The query handlers of the runs of this chain. */
  queries: QueryScope;
}): Promise<unknown> {
  const { ctx, lock, clock, loaded } = params;
  const { workflowId } = params.params;
  let input = params.params.input;
  for (let chain = 0; chain < MAX_CONTINUE_AS_NEW_CHAIN; chain++) {
    try {
      return await runOneOrchestrationCycle({
        ctx,
        lock,
        clock,
        loaded,
        // The state read with the lock is current only for the first run.
        ...(chain === 0 && lock.state !== undefined && { preloaded: lock.state }),
        // A continued run is an internal restart: no idempotency cache,
        // and the key stays on the archived row.
        params:
          chain === 0
            ? { ...params.params, input }
            : {
                workflowId,
                input,
                force: true,
                namespace: params.params.namespace,
                ...(params.params.parentWorkflowId !== undefined && {
                  parentWorkflowId: params.params.parentWorkflowId,
                }),
                ...(params.params.failureSink !== undefined && {
                  failureSink: params.params.failureSink,
                }),
              },
      });
    } catch (err) {
      if (errorTag(err) !== "WorkflowContinueAsNewError") throw err;
      // An absent input continues as `null`: JSON stores keep no
      // `undefined`, and the next run must see what a later resume of it
      // reads back.
      const nextInput = (err as WorkflowContinueAsNewError).nextInput ?? null;
      // The next run's input is stored on the run, so a driver that resumes
      // it later (another worker, recovery) reads this input, not the first.
      await ctx.storage.startFreshRun({
        workflowId,
        guard: lock.fenceToken ? { fenceToken: lock.fenceToken } : undefined,
        ...(loaded.state?.run !== undefined && { expectedRun: loaded.state.run }),
        nextInput: { value: nextInput },
      });
      params.queries.reset();
      input = nextInput;
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
 * and runner-level hooks. `parentGuard` (the parent's lock) fences the
 * create, so a parent that lost its lock cannot start a child.
 */
export async function runChildWorkflow(params: {
  readonly runtime: OrchestrationRuntime;
  readonly parentWorkflowId: string;
  readonly parentGuard?: FenceGuard;
  readonly workflow: Workflow<unknown, unknown>;
  readonly workflowId: string;
  readonly input: unknown;
  /** The parent's namespace; a child created here is stored under it. */
  readonly namespace?: string;
  /** The parent's lock signal: the child stops when the parent loses (or abandons) its run. */
  readonly signal?: AbortSignal;
}): Promise<unknown> {
  const { runtime, workflow, workflowId, input } = params;
  const def = workflow._definition;
  const created = await runtime.storage.createWorkflow({
    workflowId,
    workflowName: workflow.name,
    input,
    workflowType: def.type,
    parentWorkflowId: params.parentWorkflowId,
    ...(params.namespace !== undefined && { namespace: params.namespace }),
    metadata: def.metadata,
    version: workflow.version,
    guard: params.parentGuard,
  });
  // A child that failed by itself is run again from scratch when its parent
  // step is retried. A completed or tripwired child answers as stored, and
  // so does one whose failure was a decision (cancelled, or marked failed
  // by an operator): the parent step observes that failure.
  const existing = created.created ? undefined : created.existing;
  const rerunFailed =
    existing !== undefined && existing.status === "failed" && !isFinalFailedRun(existing);
  return runWorkflowOrchestration(orchestrationContextFor({ workflow, runtime }), {
    workflowId,
    input,
    drivenByParent: true,
    parentWorkflowId: params.parentWorkflowId,
    ...(rerunFailed && { force: true }),
    ...(params.signal !== undefined && { signal: params.signal }),
  });
}

interface CycleParams {
  ctx: WorkflowOrchestrationContext;
  params: RunParams;
  lock: LockContext;
  clock: WallClock;
  loaded: LoadedRun;
  /** The run's state as read when the lock was taken, if it was. */
  preloaded?: WorkflowState | null;
}

/**
 * One run of the chain. With `compensate.onCancel: "rollback"`, a cancel
 * seen while the run executes (mid-step through the heartbeat, between
 * waves, or at a terminal write) rolls the completed steps back before the
 * run rejects with `WorkflowCancelledError`. A run found already cancelled
 * at the entry gate is not rolled back here: it ended, and was rolled back
 * (or not) by the driver that saw the cancel.
 */
async function runOneOrchestrationCycle(cycle: CycleParams): Promise<unknown> {
  const { ctx, lock, clock } = cycle;
  const phase: CyclePhase = { executing: false, input: cycle.params.input, usage: {} };
  try {
    const result = await runCycleBody({ ...cycle, phase });
    traceRunEnd({ ctx, clock, phase, params: cycle.params, error: undefined });
    return result;
  } catch (err) {
    try {
      if (phase.executing && errorTag(err) === "WorkflowCancelledError") {
        const settle = rollBackCancelled({
          ctx,
          lock,
          clock,
          workflowId: cycle.params.workflowId,
          input: phase.input,
          traceRef: phase.trace?.ref,
        });
        // Without `onCancel: "rollback"` there is nothing to undo: settling
        // is best effort (recovery settles a run left unsettled), and never
        // replaces the cancel the caller sees.
        if (ctx.compensateConfig?.onCancel === "rollback") await settle;
        else await settle.catch(() => undefined);
      }
    } finally {
      traceRunEnd({ ctx, clock, phase, params: cycle.params, error: err });
    }
    throw err;
  }
}

/**
 * Roll back a run that was cancelled while this driver held it. Runs only
 * when the stored run carries the cancel marker (a step failing with a
 * `WorkflowCancelledError`, a child's say, was rolled back as a failure). It goes through
 * `rollBackAndFail` as a cancel rollback: the run re-enters `compensating`
 * (durable, so a driver that stops mid-rollback leaves it for recovery to
 * finish), steps roll back in a failure's order with its retries and ledger,
 * and the run ends cancelled again, its cancel settled.
 */
async function rollBackCancelled(params: {
  ctx: WorkflowOrchestrationContext;
  lock: LockContext;
  clock: WallClock;
  workflowId: string;
  input: unknown;
  traceRef: WorkflowTraceRef | undefined;
}): Promise<void> {
  const { ctx, lock, workflowId } = params;
  if (lock.leaseSignal.aborted) throw lock.leaseSignal.reason;
  // The origin marker `cancelWorkflow` stored, not the error's tag: a step
  // that failed with `WorkflowCancelledError` ended the run as a failure.
  const state = await ctx.storage.loadWorkflow(workflowId);
  if (state === null || !owesCancelRollback(state)) return;
  await settleCancelledRun(params);
}

/**
 * Settle the cancel of a cancelled run this driver holds the lock of: roll
 * its completed steps back when `compensate.onCancel` is `"rollback"`, end
 * it cancelled again, and reject with the cancel. Without that option the
 * run only passes through the phase, so recovery does not find it again.
 */
async function settleCancelledRun(params: {
  ctx: WorkflowOrchestrationContext;
  lock: LockContext;
  clock: WallClock;
  workflowId: string;
  input: unknown;
  traceRef: WorkflowTraceRef | undefined;
}): Promise<never> {
  const { ctx, lock, clock, workflowId, input } = params;
  return rollBackAndFail({
    ctx,
    lock,
    clock,
    workflowId,
    input,
    dagNodes: ctx.steps.map((s) => ({ name: s.name, dependsOn: s.dependsOn })),
    guard: lock.fenceToken ? { fenceToken: lock.fenceToken } : undefined,
    workflowStartTime: clock.currentTimeMs(),
    error: cancelledError(workflowId),
    errorMsg: CANCELLED_ERROR,
    errorTag: CANCELLED_ERROR_TAG,
    resumed: false,
    cancelled: true,
    failureSink: undefined,
    traceRef: params.traceRef,
  });
}

async function runCycleBody(cycle: CycleParams & { phase: CyclePhase }): Promise<unknown> {
  const { ctx, lock, clock, loaded } = cycle;
  const { workflowId, force, namespace } = cycle.params;
  let input = cycle.params.input;
  const workflowStartTime = clock.currentTimeMs();
  const idempotency = force ? undefined : ctx.idempotency;
  const guard = lock.fenceToken ? { fenceToken: lock.fenceToken } : undefined;

  // 1. Load the run, unless it came with the lock.
  let state =
    cycle.preloaded !== undefined ? cycle.preloaded : await ctx.storage.loadWorkflow(workflowId);

  // Double-check idempotency under the lock — prevents a race with a
  // concurrent run that completed after the pre-check.
  if (idempotency && state?.completedAt) {
    const cached = cachedIdempotentOutcome({ ctx, state, clock });
    if (cached) return cached.result;
    const onExpiry = idempotency.onExpiry ?? "fresh-run";
    if (onExpiry === "fresh-run" && (state.status === "completed" || state.status === "failed")) {
      await ctx.storage.startFreshRun({ workflowId, guard, expectedRun: state.run });
      state = await ctx.storage.loadWorkflow(workflowId);
    }
  }

  // A driver of runs created elsewhere has no input to create one with.
  if (!state && cycle.params.useStoredInput === true) {
    throw new WorkflowRunNotFoundError({
      workflowId,
      message: `Workflow "${workflowId}" has no stored run to drive (useStoredInput)`,
    });
  }

  // 2. Create the run, or check the stored version of an existing one.
  // A run that never started (created by a parent or a producer) is not a resume.
  let resumedRun = state != null && state.status !== "pending";
  if (!state) {
    const createResult = await ctx.storage.createWorkflow({
      workflowId,
      workflowName: ctx.name,
      input,
      workflowType: ctx.type,
      ...(namespace !== undefined ? { namespace } : {}),
      metadata:
        ctx.tracer !== undefined && cycle.params.traceparent !== undefined
          ? { ...ctx.metadata, traceparent: cycle.params.traceparent }
          : ctx.metadata,
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

  // A stored run resumed for a driver that doesn't carry the input.
  if (cycle.params.useStoredInput === true && state) input = state.input;

  // 3. Entry gate — an ended run is not executed again: a completed run
  // answers with its result, decoded as a replay would; any other ended run
  // rejects with its stored outcome. `force` archives it and starts a fresh
  // run instead.
  loaded.state = state;
  if (state && isTerminalWorkflowStatus(state.status)) {
    if (!force) {
      // A run cancelled while no driver held it (suspended, or its worker
      // died) still owes its cancel: finish the rollback, or settle it when
      // the definition asks for none, and reject with the cancel.
      if (owesCancelRollback(state) && hasCapability(ctx.storage, "compensationLedger")) {
        const traceRef =
          ctx.tracer !== undefined ? traceRunStart({ cycle, state, resumedRun: true }) : undefined;
        return settleCancelledRun({
          ctx,
          lock,
          clock,
          workflowId,
          input: state.input,
          traceRef,
        });
      }
      rejectEndedRun(state);
      return completedRunResult({ ctx, state });
    }
    await ctx.storage.startFreshRun({ workflowId, guard, expectedRun: state.run });
    state = await ctx.storage.loadWorkflow(workflowId);
    loaded.state = state;
    resumedRun = false;
  }

  // From here the run executes under this driver: a cancel it sees may roll
  // back (see `runOneOrchestrationCycle`).
  cycle.phase.executing = true;
  cycle.phase.input = input;
  const traceRef =
    ctx.tracer !== undefined ? traceRunStart({ cycle, state, resumedRun }) : undefined;

  // 4. Validate DAG
  const dagNodes: DagNode[] = ctx.steps.map((s) => ({
    name: s.name,
    dependsOn: s.dependsOn,
  }));
  topologicalSort({ nodes: dagNodes, workflowId });

  // A run found mid-rollback (its driver stopped while compensating) is
  // not executed again: its rollback is finished and the run fails with
  // the failure that started it.
  if (state?.status === "compensating") {
    const cancelled = state.compensationOrigin === "cancel";
    // A cancel rollback ends by rejecting with the cancel: that must not
    // start a second one.
    if (cancelled) cycle.phase.executing = false;
    return rollBackAndFail({
      ctx,
      lock,
      clock,
      workflowId,
      input: state.input,
      dagNodes,
      guard,
      workflowStartTime,
      error: cancelled ? cancelledError(workflowId) : runFailure(state),
      errorMsg: state.error ?? `Workflow ${workflowId} failed`,
      errorTag: state.errorTag,
      resumed: true,
      cancelled,
      failureSink: cycle.params.failureSink,
      traceRef,
    });
  }

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
    ...(ctx.budget !== undefined && { budget: ctx.budget }),
    usageRef: cycle.phase.usage,
    stepExecutor: ctx.stepExecutor,
    guard,
    clock,
    signal: lock.signal,
    ...(ctx.tracer !== undefined && traceRef !== undefined && { tracer: ctx.tracer, traceRef }),
    workflowName: ctx.name,
    workflowQueue: ctx.queue,
    ...(ctx.executorId !== undefined && { executorId: ctx.executorId }),
    ...(state && { run: state.run }),
    ...(ctx.version !== undefined && { workflowVersion: ctx.version }),
    ...(ctx.patches !== undefined && { patches: ctx.patches }),
    runChild: (child) =>
      runChildWorkflow({
        runtime: runtimeOf(ctx),
        parentWorkflowId: workflowId,
        ...(guard !== undefined && { parentGuard: guard }),
        ...((state?.namespace ?? namespace) !== undefined && {
          namespace: state?.namespace ?? namespace,
        }),
        signal: lock.signal,
        ...child,
      }),
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
        // Stored in the codec's encoded form, like the step rows.
        write: () =>
          ctx.storage.completeWorkflow({
            workflowId,
            result: encodeWorkflowResult({ steps: ctx.steps, result: finalResult }),
            guard,
          }),
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
      if (!hasCapability(storage, "tripwire")) {
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
        write: () => storage.tripwireWorkflow({ workflowId, reason, guard }),
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

  // Not retryable or retries exhausted — roll back, then fail the run.
  return rollBackAndFail({
    ctx,
    lock,
    clock,
    workflowId,
    input,
    dagNodes,
    guard,
    workflowStartTime,
    error: lastStepError,
    errorMsg: errorMessage(lastStepError),
    errorTag: errorTagOf(lastStepError),
    resumed: false,
    cancelled: false,
    failureSink: cycle.params.failureSink,
    traceRef,
  });
}

/**
 * The failure path of a run: enter the `compensating` phase (when the
 * storage keeps a compensation ledger), roll the completed steps back,
 * fire `compensate.onComplete`, fail the run with `errorMsg` / `errorTag`,
 * fire `onWorkflowFailure`, publish to the DLQ, and reject with `error`.
 *
 * `resumed` finishes the rollback of a run found `compensating` (its
 * previous driver stopped mid-rollback): the phase is already persisted,
 * and the steps its ledger lists are not rolled back again.
 *
 * A run that ended before the phase could be entered (a cancel landed) is
 * not rolled back: it rejects with `WorkflowCancelledError`, or with
 * `error` for any other ending.
 */
async function rollBackAndFail(params: {
  ctx: WorkflowOrchestrationContext;
  lock: LockContext;
  clock: WallClock;
  workflowId: string;
  input: unknown;
  dagNodes: DagNode[];
  guard: FenceGuard | undefined;
  workflowStartTime: number;
  error: unknown;
  errorMsg: string;
  errorTag: string | undefined;
  resumed: boolean;
  /**
   * The rollback answers a cancel: the run ends cancelled again (not as a
   * failure: no `onWorkflowFailure`, no DLQ record), and `error` is the
   * `WorkflowCancelledError`.
   */
  cancelled: boolean;
  failureSink: { failure?: WorkflowFailure } | undefined;
  traceRef: WorkflowTraceRef | undefined;
}): Promise<never> {
  const { ctx, lock, clock, workflowId, input, dagNodes, guard, workflowStartTime } = params;
  const { errorMsg, errorTag: failedTag } = params;
  const lastStepError = params.error;
  const storage = ctx.storage;
  // Without a ledger a cancelled run is never reopened: it stays ended.
  const reopened = params.cancelled && hasCapability(storage, "compensationLedger");

  if (!params.resumed && hasCapability(storage, "compensationLedger")) {
    let entered = false;
    await checkpointWrite({
      clock,
      workflowId,
      operation: "beginCompensation",
      write: async () => {
        entered = await storage.beginCompensation({
          workflowId,
          error: errorMsg,
          ...(failedTag !== undefined && { errorTag: failedTag }),
          origin: params.cancelled ? "cancel" : "failure",
          guard,
        });
      },
    });
    if (!entered) {
      if (!params.cancelled) await assertNotCancelled({ ctx, workflowId });
      throw lastStepError;
    }
  }

  // A cancel only rolls steps back when asked to; otherwise the run just
  // passes through the phase so its cancel is settled.
  const rollBack = !params.cancelled || ctx.compensateConfig?.onCancel === "rollback";
  const compensationStartedAt = clock.now();
  if (rollBack && ctx.tracer !== undefined && params.traceRef !== undefined) {
    emitTrace(ctx.tracer, "compensationStart", {
      ...params.traceRef,
      at: compensationStartedAt,
      cancelled: params.cancelled,
      resumed: params.resumed,
    });
  }
  const compensationReport = !rollBack
    ? { compensated: [], failed: [] }
    : await compensateWorkflow({
        ...(ctx.tracer !== undefined &&
          params.traceRef !== undefined && { tracer: ctx.tracer, traceRef: params.traceRef }),
        storage,
        steps: ctx.steps,
        compensateConfig: ctx.compensateConfig,
        workflowId,
        input,
        dagNodes,
        guard,
        clock,
        signal: lock.leaseSignal,
        resumed: params.resumed,
        ...(ctx.executorId !== undefined && { executorId: ctx.executorId }),
      });
  if (rollBack && ctx.tracer !== undefined && params.traceRef !== undefined) {
    const endedAt = clock.now();
    emitTrace(ctx.tracer, "compensationEnd", {
      ...params.traceRef,
      startedAt: compensationStartedAt,
      endedAt,
      durationMs: endedAt.getTime() - compensationStartedAt.getTime(),
      compensated: compensationReport.compensated.length,
      failed: compensationReport.failed.length,
    });
  }

  // Fire workflow-level onComplete callback
  if (rollBack && ctx.compensateConfig?.onComplete) {
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

  // A cancel rollback has no failure to report: the run only ends again
  // (cancelled) when it was reopened.
  if (params.cancelled && !reopened) throw lastStepError;

  // Fail the workflow, keeping the error's tag with it.
  await checkpointWrite({
    clock,
    workflowId,
    operation: "failWorkflow",
    write: () =>
      ctx.storage.failWorkflow({
        workflowId,
        error: errorMsg,
        guard,
        ...(failedTag !== undefined && { errorTag: failedTag }),
        // The end of a cancel's rollback settles the cancel.
        ...(params.cancelled && { compensationOrigin: "cancel-settled" as const }),
      }),
  });
  // The structured failure: the stored run's failed step and attempt, the
  // live error as `cause`. A failed read degrades to the error alone.
  const failure = await loadRunFailure({
    storage: ctx.storage,
    workflowId,
    errorMsg,
    errorTag: failedTag,
    cause: lastStepError,
  });
  if (params.failureSink !== undefined) params.failureSink.failure = failure;
  if (params.cancelled) throw lastStepError;
  await fireHook({
    hooks: ctx.hooks,
    name: "onWorkflowFailure",
    event: {
      workflowId,
      error: errorMsg,
      failure,
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
      failure,
      clock,
      compensationReport,
      metadata: ctx.metadata,
    });
  }

  throw lastStepError;
}

/**
 * Open the run's trace: emit `runStart`, then `waitEnd` for every wait the
 * stored run was parked on (this call resumes it). Returns the run's
 * identity for the events that follow.
 */
function traceRunStart(params: {
  cycle: CycleParams & { phase: CyclePhase };
  state: WorkflowState | null | undefined;
  resumedRun: boolean;
}): WorkflowTraceRef {
  const { cycle, state } = params;
  const { ctx, clock } = cycle;
  const tracer = ctx.tracer!;
  const namespace = state?.namespace ?? cycle.params.namespace;
  const ref: WorkflowTraceRef = {
    workflowId: cycle.params.workflowId,
    workflowName: ctx.name,
    ...(namespace !== undefined && { namespace }),
    ...(state != null && { run: state.run }),
  };
  const startedAt = clock.now();
  cycle.phase.trace = { ref, startedAt };
  const stored = state?.metadata?.traceparent;
  const traceparent = cycle.params.traceparent ?? (typeof stored === "string" ? stored : undefined);
  const parentWorkflowId = cycle.params.parentWorkflowId ?? state?.parentWorkflowId;
  emitTrace(tracer, "runStart", {
    ...ref,
    startedAt,
    resumed: params.resumedRun,
    ...(ctx.version !== undefined && { version: ctx.version }),
    ...(parentWorkflowId !== undefined && { parentWorkflowId }),
    ...(traceparent !== undefined && { traceparent }),
    ...(ctx.executorId !== undefined && { executorId: ctx.executorId }),
  });
  if (params.resumedRun && state != null) {
    for (const [stepName, step] of Object.entries(state.steps)) {
      const event =
        step.status === "waiting_for_signal" && step.signalName !== undefined
          ? eventOfWakeSignal(step.signalName)
          : undefined;
      const kind =
        step.status === "sleeping"
          ? "sleep"
          : event !== undefined
            ? "event"
            : step.status === "waiting_for_signal"
              ? "signal"
              : undefined;
      if (kind === undefined) continue;
      emitTrace(tracer, "waitEnd", {
        ...ref,
        stepName,
        kind,
        ...(event !== undefined && {
          eventName: event.name,
          ...(event.key !== undefined && { eventKey: event.key }),
        }),
        since: state.updatedAt,
        endedAt: startedAt,
        waitedMs: Math.max(0, startedAt.getTime() - state.updatedAt.getTime()),
        ...(step.wakeAt !== undefined && { wakeAt: step.wakeAt }),
      });
    }
  }
  return ref;
}

/** Close the run's trace (if `runStart` was emitted) with how this orchestration ended. */
function traceRunEnd(params: {
  ctx: WorkflowOrchestrationContext;
  clock: WallClock;
  phase: CyclePhase;
  params: RunParams;
  error: unknown;
}): void {
  const { ctx, phase } = params;
  const trace = phase.trace;
  if (ctx.tracer === undefined || trace === undefined) return;
  const endedAt = params.clock.now();
  const error = params.error;
  const outcome = traceOutcomeOf(error);
  const failure =
    outcome === "failed"
      ? (params.params.failureSink?.failure ?? failureFromError(error))
      : undefined;
  emitTrace(ctx.tracer, "runEnd", {
    ...trace.ref,
    startedAt: trace.startedAt,
    endedAt,
    durationMs: endedAt.getTime() - trace.startedAt.getTime(),
    outcome,
    ...(failure !== undefined && { failure }),
    ...(phase.usage.current !== undefined && { usage: phase.usage.current }),
    ...(ctx.executorId !== undefined && { executorId: ctx.executorId }),
  });
}

/** How an orchestration that rejected with `error` (or returned, when `undefined`) ended. */
function traceOutcomeOf(error: unknown): WorkflowTraceRunOutcome {
  if (error === undefined) return "completed";
  switch (errorTag(error)) {
    case "WorkflowSuspendedError":
      return "suspended";
    case "WorkflowContinueAsNewError":
      return "continued";
    case "WorkflowTripwireError":
      return "tripwire";
    case "WorkflowCancelledError":
      return "cancelled";
  }
  return isAbandonRunExit(error) ? "abandoned" : "failed";
}

/**
 * Report the lock to the tracer: `lockAcquired` now, `lockLost` if the
 * lease is lost while the run is driven. Returns the function that stops
 * listening once the run is done with the lock.
 */
function traceLock(params: {
  ctx: WorkflowOrchestrationContext;
  lock: LockContext;
  clock: WallClock;
  workflowId: string;
  namespace: string | undefined;
}): () => void {
  const { ctx, lock, clock, workflowId, namespace } = params;
  const tracer = ctx.tracer;
  if (tracer === undefined) return noop;
  const base = { workflowId, ...(namespace !== undefined && { namespace }) };
  emitTrace(tracer, "lockAcquired", {
    ...base,
    at: clock.now(),
    fenced: lock.fenceToken !== undefined,
  });
  const onLost = (): void =>
    emitTrace(tracer, "lockLost", {
      ...base,
      at: clock.now(),
      error: traceErrorOf(lock.leaseSignal.reason),
    });
  if (lock.leaseSignal.aborted) {
    onLost();
    return noop;
  }
  lock.leaseSignal.addEventListener("abort", onLost, { once: true });
  return () => lock.leaseSignal.removeEventListener("abort", onLost);
}

function noop(): void {}

async function loadRunFailure(params: {
  storage: WorkflowStorage;
  workflowId: string;
  errorMsg: string;
  errorTag: string | undefined;
  cause: unknown;
}): Promise<WorkflowFailure> {
  const { storage, workflowId, errorMsg, errorTag, cause } = params;
  try {
    const state = await storage.loadWorkflow(workflowId);
    const stored = state
      ? workflowFailure({ ...state, status: "failed", error: errorMsg, errorTag }, cause)
      : undefined;
    if (stored !== undefined) return stored;
  } catch {
    // best effort: the failure is already stored
  }
  return failureFromError(cause);
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
  return decodeWorkflowResult({ steps: ctx.steps, stored: state.result });
}

/**
 * The workflow's result as stored: the last step's result through its codec.
 * Runs completed before this stored the plain value; decoding those through
 * the codec leaves JSON-plain values as they are.
 */
function encodeWorkflowResult(params: {
  steps: WorkflowOrchestrationContext["steps"];
  result: unknown;
}): unknown {
  const last = params.steps[params.steps.length - 1];
  return last ? last.codec.encode(params.result) : params.result;
}

/** The inverse of `encodeWorkflowResult`; `LosslessJsonCodec` when the steps are not at hand. */
export function decodeWorkflowResult(params: {
  steps?: WorkflowOrchestrationContext["steps"];
  stored: unknown;
}): unknown {
  const last = params.steps?.[params.steps.length - 1];
  return (last?.codec ?? LosslessJsonCodec).decode(params.stored);
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
      if (
        tag === "WorkflowDeadlineError" ||
        tag === "WorkflowCancelledError" ||
        tag === "WorkflowBudgetExceededError"
      )
        return false;
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
  if (state.status === "completed") {
    return { result: decodeWorkflowResult({ steps: ctx.steps, stored: state.result }) };
  }
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
