// ---------------------------------------------------------------------------
// Compensation — the saga rollback run after a workflow exhausts its
// retries: reverses completed steps that carry a `compensate` function,
// records one attempt row per try, and keeps a durable ledger of the
// rollbacks that ran, so a rollback interrupted by a crash resumes where it
// stopped.
// ---------------------------------------------------------------------------

import type { Eff, Throws } from "@spilne/perfect-core";
import { runHookResult } from "../../shared/eff.ts";
import { retryAsync, type RetryPolicy } from "../../shared/retry-policy.ts";
import { SystemWallClock, type WallClock } from "../../shared/wall-clock.ts";
import type { CompensateConfig } from "../durable-pipeline.ts";
import { isAbandonRunExit } from "../step-policy.ts";
import type { WorkflowState } from "../workflow-state.ts";
import {
  hasCapability,
  type FenceGuard,
  type StepCompensationOutcome,
  type WorkflowStorage,
} from "../workflow-storage.ts";
import type { DagNode } from "../workflow-dag.ts";
import { errorMessage } from "./step-body.ts";
import { checkpointWrite } from "./step-checkpoint.ts";

/**
 * A step definition's view as needed by compensation: the name, the
 * rollback function and the codec its stored result is decoded with. A
 * full `StepDefinition` is a superset and passes this check via structural
 * typing; keeps compensation decoupled from the rest of the step shape.
 */
export interface CompensatableStep {
  readonly name: string;
  /**
   * Decodes the stored (codec-encoded) result before it reaches
   * `compensate`. Without one, `compensate` receives the stored form.
   */
  readonly codec?: { decode(value: unknown): unknown };
  readonly compensate?: (params: {
    result: unknown;
    input: unknown;
    workflowId: string;
  }) => Eff<unknown, Throws<unknown>> | Promise<void>;
}

/** What a rollback came to: the steps it reversed and the ones whose rollback failed. */
export interface CompensationReport {
  readonly compensated: string[];
  readonly failed: { stepName: string; error: unknown }[];
}

/**
 * The completed steps of `state` that carry a `compensate` function, in
 * the order they are rolled back:
 *
 * 1. Latest completion first (`StepState.completedAt`, descending), so
 *    parallel branches unwind in the reverse of the order they finished.
 * 2. Ties (same `completedAt`, or no `completedAt`) go to the step defined
 *    later in the workflow first.
 * 3. A step is never rolled back before a completed step that depends on
 *    it (`dagNodes`): when completion times disagree with the DAG (clock
 *    skew between executors), the dependent goes first.
 */
export function compensationOrder(params: {
  readonly steps: ReadonlyArray<CompensatableStep>;
  readonly state: WorkflowState;
  readonly dagNodes?: ReadonlyArray<DagNode>;
}): CompensatableStep[] {
  const { steps, state } = params;
  interface Entry {
    readonly step: CompensatableStep;
    readonly index: number;
    readonly completedMs: number;
    /** Completed steps depending on this one that are not rolled back yet. */
    dependents: number;
  }
  const entries = new Map<string, Entry>();
  steps.forEach((step, index) => {
    const row = state.steps[step.name];
    if (row?.status !== "completed") return;
    entries.set(step.name, {
      step,
      index,
      completedMs: row.completedAt?.getTime() ?? Number.NEGATIVE_INFINITY,
      dependents: 0,
    });
  });

  const dependsOn = new Map<string, readonly string[]>();
  for (const node of params.dagNodes ?? []) {
    if (!entries.has(node.name)) continue;
    const deps = node.dependsOn.filter((d) => d !== node.name && entries.has(d));
    dependsOn.set(node.name, deps);
    for (const dep of deps) entries.get(dep)!.dependents++;
  }

  const before = (a: Entry, b: Entry): boolean =>
    a.completedMs !== b.completedMs ? a.completedMs > b.completedMs : a.index > b.index;

  const available = [...entries.values()].filter((e) => e.dependents === 0);
  const order: CompensatableStep[] = [];
  while (available.length > 0) {
    let best = 0;
    for (let i = 1; i < available.length; i++) {
      if (before(available[i]!, available[best]!)) best = i;
    }
    const next = available[best]!;
    available.splice(best, 1);
    if (next.step.compensate) order.push(next.step);
    for (const dep of dependsOn.get(next.step.name) ?? []) {
      const entry = entries.get(dep)!;
      if (--entry.dependents === 0) available.push(entry);
    }
  }
  return order;
}

/**
 * Run the saga rollback for a workflow: reverses completed steps that carry
 * a `compensate` function, in `compensationOrder`, honoring the compensate
 * config's per-step retry policy. `compensate` receives the step's result
 * decoded through its codec, as downstream steps saw it.
 *
 * With a `CompensationLedgerStore`, each step's rollback is recorded
 * (fenced) as it settles, and steps the ledger already lists are skipped:
 * their earlier outcome goes into the report as it was recorded. A rollback
 * that was interrupted between a step's `compensate` and its ledger entry
 * runs that step's `compensate` again. Records an attempt row per try when
 * the storage supports `StepAttemptStore`; `resumed` numbers them after
 * the step's existing compensation attempts.
 *
 * A ledger or attempt write that keeps failing rejects with
 * `CheckpointError`, and a lost lock (`signal`) or fence rejection rejects
 * as itself: the run is abandoned mid-rollback, still `compensating`, for
 * its next driver to finish.
 */
export async function compensateWorkflow(params: {
  storage: WorkflowStorage;
  steps: ReadonlyArray<CompensatableStep>;
  compensateConfig?: CompensateConfig;
  workflowId: string;
  input: unknown;
  dagNodes: DagNode[];
  /** Optional fence guard — threaded to the ledger and attempt writes so a stale holder's rows are rejected. */
  guard?: FenceGuard;
  /** Time source. Drives compensation retry backoff + attempt timestamps. Default: SystemWallClock. */
  clock?: WallClock;
  /** Executor id stamped onto each compensation StepAttemptRecord. */
  executorId?: string;
  /** The run's lock signal: once aborted, no further step is rolled back. */
  signal?: AbortSignal;
  /** The rollback resumes an interrupted one (attempt rows continue its numbering). */
  resumed?: boolean;
}): Promise<CompensationReport> {
  const { storage, compensateConfig, workflowId, input, guard, executorId, signal } = params;
  const clock = params.clock ?? SystemWallClock;
  const compensated: string[] = [];
  const failed: { stepName: string; error: unknown }[] = [];

  const state = await storage.loadWorkflow(workflowId);
  if (!state) return { compensated, failed };

  const ledger = hasCapability(storage, "compensationLedger") ? storage : undefined;
  const attemptStorage = hasCapability(storage, "stepAttempts") ? storage : undefined;

  const pending: CompensatableStep[] = [];
  for (const step of compensationOrder({ steps: params.steps, state, dagNodes: params.dagNodes })) {
    const row = state.steps[step.name];
    if (ledger && row?.compensationStatus === "compensated") {
      compensated.push(step.name);
    } else if (ledger && row?.compensationStatus === "compensation_failed") {
      failed.push({ stepName: step.name, error: row.compensationError ?? "compensation failed" });
    } else {
      pending.push(step);
    }
  }

  // Compensation retries run on the shared retry loop: no retry unless
  // `compensate.retry` sets `maxRetries`, `RetryPolicy` defaults otherwise.
  // A write that abandons the run is never retried as a compensation.
  const retryConfig = compensateConfig?.retry;
  const policy: RetryPolicy<unknown> = {
    maxRetries: retryConfig?.maxRetries ?? 0,
    ...(retryConfig?.baseDelayMs !== undefined && { baseDelayMs: retryConfig.baseDelayMs }),
    when: (error) => !isAbandonRunExit(error),
  };

  const saveAttempt = async (record: {
    stepName: string;
    attempt: number;
    startedAt: Date;
    error?: unknown;
  }): Promise<void> => {
    if (!attemptStorage) return;
    const failedRecord = "error" in record;
    const completedAt = clock.now();
    await checkpointWrite({
      clock,
      workflowId,
      operation: "saveStepAttempt",
      stepName: record.stepName,
      write: () =>
        attemptStorage.saveStepAttempt({
          record: {
            workflowId,
            stepName: record.stepName,
            attempt: record.attempt,
            type: "compensation",
            status: failedRecord ? "failed" : "completed",
            ...(failedRecord && { error: errorMessage(record.error) }),
            durationMs: completedAt.getTime() - record.startedAt.getTime(),
            startedAt: record.startedAt,
            completedAt,
            ...(executorId !== undefined && { executorId }),
          },
          guard,
        }),
    });
  };

  const record = async (entry: {
    stepName: string;
    status: StepCompensationOutcome;
    error?: unknown;
  }): Promise<void> => {
    if (!ledger) return;
    await checkpointWrite({
      clock,
      workflowId,
      operation: "saveStepCompensation",
      stepName: entry.stepName,
      write: () =>
        ledger.saveStepCompensation({
          workflowId,
          stepName: entry.stepName,
          status: entry.status,
          ...(entry.error !== undefined && { error: errorMessage(entry.error) }),
          guard,
        }),
    });
  };

  for (const step of pending) {
    if (signal?.aborted) throw signal.reason;
    const firstAttempt =
      params.resumed === true ? await lastCompensationAttempt(storage, workflowId, step.name) : 0;
    try {
      const stored = state.steps[step.name]?.result;
      const result = step.codec ? step.codec.decode(stored) : stored;
      await retryAsync({
        policy,
        clock,
        run: async (retry) => {
          const startedAt = clock.now();
          const attempt = firstAttempt + retry + 1;
          try {
            await runHookResult(step.compensate!({ result, input, workflowId }));
          } catch (err) {
            await saveAttempt({ stepName: step.name, attempt, startedAt, error: err });
            throw err;
          }
          await saveAttempt({ stepName: step.name, attempt, startedAt });
        },
      });
    } catch (err) {
      if (isAbandonRunExit(err)) throw err;
      failed.push({ stepName: step.name, error: err });
      await record({ stepName: step.name, status: "compensation_failed", error: err });
      continue;
    }
    compensated.push(step.name);
    await record({ stepName: step.name, status: "compensated" });
  }

  return { compensated, failed };
}

/** Highest compensation attempt number already recorded for `stepName`; 0 when none. */
async function lastCompensationAttempt(
  storage: WorkflowStorage,
  workflowId: string,
  stepName: string,
): Promise<number> {
  if (!hasCapability(storage, "stepAttempts")) return 0;
  const attempts = await storage.loadStepAttempts({ workflowId, stepName });
  let last = 0;
  for (const a of attempts) {
    if (a.type === "compensation" && a.attempt > last) last = a.attempt;
  }
  return last;
}
