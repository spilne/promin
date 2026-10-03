// ---------------------------------------------------------------------------
// Orchestration loop — drives one workflow run end-to-end: the
// continue-as-new chain, version drain, idempotency cache, lock +
// heartbeat, load-or-create, workflow-level retry around the DAG executor,
// the terminal transitions (complete / tripwire / compensate + fail + DLQ)
// and the lifecycle hooks at each of them.
// ---------------------------------------------------------------------------

import type { TaggedError } from "../../shared/tagged-error.ts";
import { runHookResult } from "../../shared/eff.ts";
import { SystemWallClock } from "../../shared/wall-clock.ts";
import {
  TripwireStorageMissingError,
  WorkflowContinueAsNewError,
  WorkflowError,
  WorkflowTripwireError,
  WorkflowVersionMismatchError,
} from "../durable-pipeline-error.ts";
import { clearQueryHandlers } from "../query-registry.ts";
import { withLock } from "../with-lock.ts";
import { topologicalSort, type DagNode } from "../workflow-dag.ts";
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

/**
 * Default lock extension for orchestration runs. Matches `withLock`'s
 * `DEFAULT_LOCK_EXTENSION_MS` so a 30s heartbeat keeps the lock healthy
 * with a ~90s grace window for transient network hiccups.
 */
const DEFAULT_LOCK_DURATION_MS = 120_000;

/**
 * Run a workflow end-to-end. Orchestrates version-drain pre-check,
 * idempotency TTL, lock acquisition + heartbeat, workflow-level retry
 * around executeWorkflowDag, compensation cascade on exhausted retries,
 * DLQ publish on failure. Hooks fire at every natural boundary
 * (onWorkflowComplete / onWorkflowFailure / onStep*).
 *
 * Takes the full orchestration context directly, so any caller holding a
 * definition and a storage can drive the same loop.
 */
export async function runWorkflowOrchestration(
  ctx: WorkflowOrchestrationContext,
  params: {
    workflowId: string;
    input: unknown;
    force?: boolean;
    namespace?: string;
    idempotencyKey?: string;
    idempotencyExpiresAt?: Date;
  },
): Promise<unknown> {
  // Continue-as-new wrapper: catch WorkflowContinueAsNewError thrown out
  // of withLock, archive the current run via startFreshRun, then re-run
  // under the same workflowId with the carried input. Hard cap at 1024
  // chained continue-as-new calls to catch infinite loops in user code.
  //
  // Query-handler lifecycle: clear ONLY on terminal exit (success or
  // non-suspension failure). Suspension means "still hosting, just
  // paused" — the next resume re-runs the body and replay re-registers
  // handlers, so we want them to survive the wait. Skipping the clear
  // on WorkflowSuspendedError keeps handlers alive across signal /
  // sleep waits.
  let currentInput = params.input;
  for (let chain = 0; chain < 1024; chain++) {
    try {
      const result = await runOneOrchestrationCycle(ctx, {
        workflowId: params.workflowId,
        input: currentInput,
        force: chain > 0 ? true : params.force,
        namespace: params.namespace,
        // Only thread the key on the first cycle. Continue-as-new chains
        // are internal restarts; they shouldn't re-stamp the key onto the
        // archived row.
        ...(chain === 0 && params.idempotencyKey && params.idempotencyExpiresAt
          ? {
              idempotencyKey: params.idempotencyKey,
              idempotencyExpiresAt: params.idempotencyExpiresAt,
            }
          : {}),
      });
      clearQueryHandlers(params.workflowId);
      return result;
    } catch (err) {
      if (err instanceof WorkflowContinueAsNewError) {
        await ctx.storage.startFreshRun(params.workflowId);
        clearQueryHandlers(params.workflowId);
        currentInput = err.nextInput;
        continue;
      }
      // Suspension — keep handlers alive. Other errors are terminal.
      const tag = (err as { _tag?: string } | undefined)?._tag;
      if (tag !== "WorkflowSuspendedError") {
        clearQueryHandlers(params.workflowId);
      }
      throw err;
    }
  }
  clearQueryHandlers(params.workflowId);
  throw new Error(
    `Workflow "${params.workflowId}" exceeded continue-as-new chain limit (1024). ` +
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
  await runtime.storage.createWorkflow({
    workflowId,
    workflowName: workflow.name,
    input,
    workflowType: def.type,
    parentWorkflowId: params.parentWorkflowId,
    metadata: def.metadata,
    version: workflow.version,
  });
  return runWorkflowOrchestration(orchestrationContextFor({ workflow, runtime }), {
    workflowId,
    input,
  });
}

async function runOneOrchestrationCycle(
  ctx: WorkflowOrchestrationContext,
  params: {
    workflowId: string;
    input: unknown;
    force?: boolean;
    namespace?: string;
    idempotencyKey?: string;
    idempotencyExpiresAt?: Date;
  },
): Promise<unknown> {
  const { workflowId, input, force, namespace } = params;
  const clock = ctx.clock ?? SystemWallClock;
  const workflowStartTime = clock.currentTimeMs();
  const compensateTrigger = ctx.compensateConfig?.trigger ?? "after-retries";
  const maxWorkflowRetries = compensateTrigger === "immediate" ? 0 : (ctx.retry?.maxRetries ?? 0);
  const workflowRetryDelayMs = ctx.retry?.baseDelayMs ?? 1000;
  const idempotency = force ? undefined : ctx.idempotency;

  // Version drain — a run stored under an older version is driven by the
  // matching previousVersion definition.
  const drainCtx = await resolveDrainContext({ ctx, workflowId });
  if (drainCtx) {
    return runWorkflowOrchestration(drainCtx, { workflowId, input, force, namespace });
  }

  // 0. Idempotency check — return cached result if within TTL
  if (idempotency) {
    const existing = await ctx.storage.loadWorkflow(workflowId);
    if (existing?.completedAt) {
      const elapsed = clock.currentTimeMs() - existing.completedAt.getTime();
      const ttl = getIdempotencyTtl(idempotency, existing.status);
      if (ttl !== undefined && elapsed < ttl) {
        if (existing.status === "completed") return existing.result;
        if (existing.status === "failed")
          throw new WorkflowError({
            workflowId,
            message: existing.error ?? `Workflow "${workflowId}" failed (cached, TTL ${ttl}ms)`,
          });
      }
    }
  }

  // 1. Acquire lock with heartbeat — keeps lock alive during long steps
  return withLock({
    storage: ctx.storage,
    workflowId,
    options: { lockDurationMs: DEFAULT_LOCK_DURATION_MS, clock },
    fn: async ({ fenceToken }) => {
      const guard = fenceToken ? { fenceToken } : undefined;
      // 2. Load or create workflow state
      let state = await ctx.storage.loadWorkflow(workflowId);

      // Double-check idempotency after lock — prevents race
      if (idempotency && state?.completedAt) {
        const elapsed = clock.currentTimeMs() - state.completedAt.getTime();
        const ttl = getIdempotencyTtl(idempotency, state.status);
        if (ttl !== undefined && elapsed < ttl) {
          if (state.status === "completed") return state.result;
          if (state.status === "failed")
            throw new WorkflowError({
              workflowId,
              message: state.error ?? `Workflow "${workflowId}" failed (cached)`,
            });
        }
        // TTL expired — check if we should start a fresh run
        const onExpiry = idempotency.onExpiry ?? "fresh-run";
        if (
          onExpiry === "fresh-run" &&
          (state.status === "completed" || state.status === "failed")
        ) {
          await ctx.storage.startFreshRun(workflowId);
          state = await ctx.storage.loadWorkflow(workflowId);
        }
      }

      if (!state) {
        const createResult = await ctx.storage.createWorkflow({
          workflowId,
          workflowName: ctx.name,
          input,
          workflowType: ctx.type,
          ...(namespace !== undefined ? { namespace } : {}),
          metadata: ctx.metadata,
          version: ctx.version,
          ...(params.idempotencyKey && params.idempotencyExpiresAt
            ? {
                idempotencyKey: params.idempotencyKey,
                idempotencyExpiresAt: params.idempotencyExpiresAt,
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

      // 3. Validate DAG
      const dagNodes: DagNode[] = ctx.steps.map((s) => ({
        name: s.name,
        dependsOn: s.dependsOn,
      }));
      topologicalSort({ nodes: dagNodes, workflowId });

      // 4. Execute DAG with workflow-level retry
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
        workflowName: ctx.name,
        workflowQueue: ctx.queue,
        ...(ctx.executorId !== undefined && { executorId: ctx.executorId }),
        ...(ctx.version !== undefined && { workflowVersion: ctx.version }),
        ...(ctx.patches !== undefined && { patches: ctx.patches }),
        runChild: (child) =>
          runChildWorkflow({ runtime: runtimeOf(ctx), parentWorkflowId: workflowId, ...child }),
      };

      for (let workflowAttempt = 0; workflowAttempt <= maxWorkflowRetries; workflowAttempt++) {
        // On retry, wait before re-attempting
        if (workflowAttempt > 0) {
          const delay = workflowRetryDelayMs * Math.pow(2, workflowAttempt - 1);
          await new Promise((r) => clock.setTimeout(() => r(undefined), delay));
        }

        const dagResult = await executeWorkflowDag(dagCtx, {
          workflowId,
          input,
          dagNodes,
          state,
          workflowStartTime,
          stepAttempts,
          deadlineMs: ctx.timeoutMs != null ? workflowStartTime + ctx.timeoutMs : undefined,
        });

        if (dagResult.success) {
          // 5. Complete workflow
          const finalResult = dagResult.result;
          await ctx.storage.completeWorkflow(workflowId, finalResult, guard);
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
          const stepName = dagResult.stepName;
          const reason = dagResult.reason;
          if (!isTripwireCapableStorage(ctx.storage)) {
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
          await ctx.storage.tripwireWorkflow(workflowId, reason, guard);
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

        // DAG failed — suspension errors always propagate immediately.
        // Continue-as-new is also a clean unwind (no compensation, no
        // failure recording) — throw it out of withLock so the lock is
        // released by withLock's finally, then the outer wrapper catches
        // it and recurses with the carried input under the same workflowId.
        if (dagResult.suspension) {
          throw dagResult.error;
        }
        if (dagResult.continueAsNew) {
          throw dagResult.error;
        }

        lastStepError = dagResult.error;

        // Check if this error is retryable (workflow-level `when` predicate)
        const shouldRetry =
          workflowAttempt < maxWorkflowRetries &&
          (!ctx.retry?.when || ctx.retry.when(dagResult.error as TaggedError));

        if (shouldRetry) {
          // Reload state to pick up checkpointed steps
          state = await ctx.storage.loadWorkflow(workflowId);
        } else {
          // Not retryable or retries exhausted — break to compensation
          break;
        }
      }

      // All workflow retries exhausted — run compensation cascade
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

      // Fail the workflow
      const errorMsg =
        lastStepError instanceof globalThis.Error ? lastStepError.message : String(lastStepError);
      await ctx.storage.failWorkflow(workflowId, errorMsg, guard);
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
    },
  });
}
