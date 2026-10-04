// ---------------------------------------------------------------------------
// Workflow observer — read-side views of a run: the `WorkflowHandle`
// returned by `start()` / `handle()`, the status snapshot behind
// `getStatus()`, and the polling event stream `subscribe()` falls back to
// when the storage has no native push.
// ---------------------------------------------------------------------------

import type { WallClock } from "../../shared/wall-clock.ts";
import type { WorkflowHandle, WorkflowStatusInfo } from "../durable-pipeline.ts";
import { wakeParentOfEndedRun } from "../child-wake.ts";
import { createWorkflowEventStream } from "../workflow-event-stream.ts";
import type { StepState, WorkflowRunEvent, WorkflowState } from "../workflow-state.ts";
import type { WorkflowStorage } from "../workflow-storage.ts";
import { findTripwireStep, storedRunError } from "./run-status.ts";

export { findTripwireStep } from "./run-status.ts";

/**
 * Build a `WorkflowHandle` over a known `workflowId`. `result()` rejects with
 * `WorkflowFailedError` for a failed run (carrying the stored `errorTag`),
 * `WorkflowCancelledError` for a cancelled one and `WorkflowTripwireError`
 * for a tripwired one. `getStatus` and `subscribe` back the handle's
 * `status` and `events`.
 */
export function createWorkflowHandle<Output>(params: {
  workflowId: string;
  storage: WorkflowStorage;
  clock: WallClock;
  getStatus: (p?: { includeStepResults?: boolean }) => Promise<WorkflowStatusInfo<unknown> | null>;
  subscribe: (options?: {
    signal?: AbortSignal;
    pollIntervalMs?: number;
  }) => AsyncIterable<WorkflowRunEvent>;
}): WorkflowHandle<Output> {
  const { workflowId, storage, clock } = params;
  return {
    workflowId,
    status: (p) => params.getStatus(p) as Promise<WorkflowStatusInfo<Output> | null>,
    signal: (signalName, payload) => storage.deliverSignal(workflowId, signalName, payload),
    cancel: async () => {
      await storage.cancelWorkflow(workflowId);
      // A cancelled child wakes a parent parked on it.
      const state = await storage.loadWorkflow(workflowId);
      if (state) await wakeParentOfEndedRun({ storage, state });
    },
    events: (opts) => params.subscribe(opts),
    result: async (p) => {
      const intervalMs = p?.intervalMs ?? 1_000;
      const timeoutMs = p?.timeoutMs ?? 60_000;
      const deadline = clock.currentTimeMs() + timeoutMs;

      while (clock.currentTimeMs() < deadline) {
        const state = await storage.loadWorkflow(workflowId);
        if (state?.status === "completed") return state.result as Output;
        const ended = state ? storedRunError(state) : undefined;
        if (ended !== undefined) throw ended;
        await new Promise((r) => clock.setTimeout(() => r(undefined), intervalMs));
      }
      throw new Error(`Workflow ${workflowId} did not complete within ${timeoutMs}ms`);
    },
  };
}

/**
 * Snapshot of a stored run for status endpoints / dashboards: active step,
 * suspended reason, per-step summary, timestamps.
 */
export function toStatusInfo(params: {
  state: WorkflowState;
  includeStepResults: boolean;
}): WorkflowStatusInfo<unknown> {
  const { state } = params;
  const includeResults = params.includeStepResults;

  let currentStep: string | undefined;
  let suspendedReason: "sleeping" | "waiting_for_signal" | undefined;

  for (const [name, step] of Object.entries(state.steps)) {
    if (step.status === "running" || step.status === "pending") {
      currentStep = currentStep ?? name;
    }
    if (step.status === "sleeping") {
      currentStep = name;
      suspendedReason = "sleeping";
    }
    if (step.status === "waiting_for_signal") {
      currentStep = name;
      suspendedReason = "waiting_for_signal";
    }
  }

  const steps: Record<string, { status: string; result?: unknown }> = {};
  for (const [name, step] of Object.entries(state.steps)) {
    steps[name] = includeResults
      ? { status: step.status, result: step.result }
      : { status: step.status };
  }

  return {
    state: state.status === "compensating" ? "failed" : state.status,
    result: state.status === "completed" ? state.result : undefined,
    error: state.error,
    ...(state.errorTag !== undefined && { errorTag: state.errorTag }),
    tripwire: state.status === "tripwire" ? state.tripwire : undefined,
    currentStep,
    suspendedReason,
    steps,
    createdAt: state.createdAt,
    startedAt: state.startedAt,
    updatedAt: state.updatedAt,
  };
}

/**
 * Event stream for storages without native push: polls `loadWorkflow`
 * every `pollIntervalMs` (default 500ms), diffs the step-state map and
 * synthesizes events. Closes on the first terminal workflow state or when
 * `signal` aborts.
 */
export function pollWorkflowEvents(params: {
  storage: WorkflowStorage;
  clock: WallClock;
  workflowId: string;
  options?: { signal?: AbortSignal; pollIntervalMs?: number };
}): AsyncIterable<WorkflowRunEvent> {
  const { storage, clock, workflowId, options } = params;
  const pollMs = options?.pollIntervalMs ?? 500;

  return createWorkflowEventStream((producer) => {
    let stopped = false;
    let prevSteps: Record<string, StepState> = {};
    let prevStatus: string | null = null;

    const stop = (): void => {
      stopped = true;
      producer.end();
    };
    const onAbort = (): void => stop();
    options?.signal?.addEventListener("abort", onAbort, { once: true });

    const tick = async (): Promise<void> => {
      while (!stopped && !producer.done) {
        let state;
        try {
          state = await storage.loadWorkflow(workflowId);
        } catch {
          // Transient storage error — keep polling; the workflow may still
          // materialize. Swallowing here keeps the stream alive in face
          // of network blips on remote storages.
          await new Promise<void>((r) => clock.setTimeout(() => r(), pollMs));
          continue;
        }
        if (state) {
          // Emit step transitions vs the last observed snapshot. Using
          // completedAt as the event timestamp so the ordering is stable
          // across polls; falls back to now when a storage omits it.
          for (const [stepName, step] of Object.entries(state.steps)) {
            const before = prevSteps[stepName];
            if (step.status === "completed" && (!before || before.status !== "completed")) {
              producer.push({
                type: "step-completed",
                stepName,
                result: step.result,
                durationMs: step.durationMs ?? 0,
                at: step.completedAt ?? clock.now(),
              });
            } else if (step.status === "failed" && (!before || before.status !== "failed")) {
              producer.push({
                type: "step-failed",
                stepName,
                error: step.error ?? "",
                at: step.completedAt ?? clock.now(),
              });
            }
          }
          // Workflow-terminal transitions close the stream.
          if (state.status !== prevStatus) {
            if (state.status === "completed") {
              producer.push({
                type: "workflow-completed",
                result: state.result,
                at: state.completedAt ?? clock.now(),
              });
              stop();
              return;
            } else if (state.status === "failed") {
              producer.push({
                type: "workflow-failed",
                error: state.error ?? "",
                at: state.completedAt ?? clock.now(),
              });
              stop();
              return;
            } else if (state.status === "tripwire") {
              const fired = findTripwireStep(state.steps);
              producer.push({
                type: "workflow-tripwire",
                stepName: fired?.stepName ?? "unknown",
                reason: state.tripwire,
                at: state.completedAt ?? clock.now(),
              });
              stop();
              return;
            }
          }
          prevSteps = state.steps;
          prevStatus = state.status;
        }
        await new Promise<void>((r) => clock.setTimeout(() => r(), pollMs));
      }
    };
    void tick();

    return () => {
      stopped = true;
      options?.signal?.removeEventListener("abort", onAbort);
    };
  });
}
