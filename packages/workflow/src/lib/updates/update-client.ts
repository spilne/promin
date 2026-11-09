// ---------------------------------------------------------------------------
// Update client — send an update to a run and wait for its outcome, over any
// `WorkflowStorage`. Everything here is signal reads and writes (see
// `update-protocol.ts`), so it works the same against every backend and
// against a `RemoteWorkflowStorage`; the run applies the update, wherever it
// is hosted.
// ---------------------------------------------------------------------------

import { SystemWallClock, type WallClock } from "../shared/wall-clock.ts";
import { sameStoredValue } from "../durable/storage/same-value.ts";
import { isTerminalWorkflowStatus, type SignalState } from "../durable/workflow-state.ts";
import type { WorkflowStorage } from "../durable/workflow-storage.ts";
import { validate } from "../schema/validator.ts";
import {
  UPDATE_HANDLERS_SIGNAL,
  UpdateFailedError,
  UpdateRejectedError,
  UpdateTimeoutError,
  parsePublishedHandlers,
  parseUpdateOutcome,
  parseUpdateRequest,
  updateRequestSignalName,
  updateResultSignalName,
  type UpdateArgsOf,
  type UpdateLike,
  type UpdateOutcome,
  type UpdateRejectionReason,
  type UpdateResultOf,
} from "./update-protocol.ts";

type Storage = Pick<WorkflowStorage, "deliverSignal" | "loadSignals" | "loadWorkflow">;

/** Options of waiting for an update's outcome. */
export interface AwaitUpdateOptions {
  /** How often to look for the outcome. Default 100 ms. */
  readonly intervalMs?: number;
  /** Give up after this long with `UpdateTimeoutError`. Default 60 000 ms. */
  readonly timeoutMs?: number;
  /** Time source. Default `SystemWallClock`. */
  readonly clock?: WallClock;
}

/** Params of `submitUpdate`. */
export interface SubmitUpdateParams {
  readonly storage: Storage;
  readonly workflowId: string;
  readonly update: UpdateLike | string;
  readonly args: unknown;
  /**
   * Idempotency key. Repeating a call with the same id and the same request
   * does not deliver it twice; the same id with a different name or args is
   * rejected (`id_conflict`). Default: a new random id.
   */
  readonly updateId?: string;
}

/** Answer of `submitUpdate`. */
export interface SubmittedUpdate {
  readonly updateId: string;
  /** Set when the run had already recorded this update's outcome (a repeated submit). */
  readonly outcome?: UpdateOutcome;
}

function rejected(params: {
  readonly workflowId: string;
  readonly updateId?: string;
  readonly updateName?: string;
  readonly reason: UpdateRejectionReason;
  readonly message: string;
}): UpdateRejectedError {
  return new UpdateRejectedError(params);
}

/**
 * Deliver an update request to a run. Rejects with `UpdateRejectedError`
 * when the run does not exist, has ended, serves no such handler, or the
 * args fail the schema the handler was registered with (or the definition's
 * own). Nothing is delivered then. A repeat of an earlier submit is a no-op.
 *
 * Delivery is only the request: the run applies it at its next update
 * point, and a suspended run is resumed by the signal scanner. Use
 * `awaitUpdate` (or `executeUpdate`) for the outcome.
 */
export async function submitUpdate(params: SubmitUpdateParams): Promise<SubmittedUpdate> {
  const { storage, workflowId, update, args } = params;
  const updateName = typeof update === "string" ? update : update.name;
  const updateId = params.updateId ?? crypto.randomUUID();
  const reject = (reason: UpdateRejectionReason, message: string): UpdateRejectedError =>
    rejected({ workflowId, updateId, updateName, reason, message });

  if (typeof update !== "string") {
    const check = validate(args, update.input.jsonSchema);
    if (!check.ok) throw reject("validation", describeErrors(check.errors));
  }

  const signals = await storage.loadSignals(workflowId);
  const existing = signals.find((x) => x.signalName === updateRequestSignalName(updateId));
  if (existing) {
    const request = parseUpdateRequest(existing.payload);
    if (
      !request ||
      request.updateName !== updateName ||
      !sameStoredValue({ stored: request.args ?? null, requested: args ?? null })
    ) {
      throw reject("id_conflict", `update id "${updateId}" is already used by another request`);
    }
    const outcome = outcomeIn({ signals, updateId });
    return { updateId, ...(outcome !== undefined && { outcome }) };
  }

  const state = await storage.loadWorkflow(workflowId);
  if (!state) throw reject("run_not_found", `workflow "${workflowId}" does not exist`);
  if (isTerminalWorkflowStatus(state.status)) {
    throw reject("run_terminal", `workflow "${workflowId}" has ${state.status}`);
  }

  const published = signals.find((x) => x.signalName === UPDATE_HANDLERS_SIGNAL);
  const handlers = parsePublishedHandlers(published?.payload).updates;
  if (handlers.length === 0) {
    throw reject("no_handlers", `workflow "${workflowId}" serves no update handlers right now`);
  }
  const handler = handlers.find((h) => h.name === updateName);
  if (!handler) {
    throw reject(
      "unknown_update",
      `workflow "${workflowId}" has no update handler "${updateName}" (serves: ${handlers
        .map((h) => h.name)
        .join(", ")})`,
    );
  }
  if (handler.jsonSchema !== undefined) {
    const check = validate(args, handler.jsonSchema);
    if (!check.ok) throw reject("validation", describeErrors(check.errors));
  }

  await storage.deliverSignal({
    workflowId,
    signalName: updateRequestSignalName(updateId),
    payload: { updateId, updateName, args: args ?? null },
  });
  return { updateId };
}

/**
 * The outcome recorded for `updateId`, or undefined while there is none.
 * Rejects with `UpdateRejectedError` (`not_applied`) when the request is
 * gone — the run restarted (continue-as-new) and dropped its signals.
 */
export async function getUpdateOutcome(params: {
  readonly storage: Pick<WorkflowStorage, "loadSignals">;
  readonly workflowId: string;
  readonly updateId: string;
}): Promise<UpdateOutcome | undefined> {
  const signals = await params.storage.loadSignals(params.workflowId);
  const outcome = outcomeIn({ signals, updateId: params.updateId });
  if (outcome !== undefined) return outcome;
  if (!signals.some((x) => x.signalName === updateRequestSignalName(params.updateId))) {
    throw rejected({
      workflowId: params.workflowId,
      updateId: params.updateId,
      reason: "not_applied",
      message: `update "${params.updateId}" is unknown to workflow "${params.workflowId}" (it may have restarted)`,
    });
  }
  return undefined;
}

/**
 * Wait for the outcome of a submitted update and return its result. Throws
 * `UpdateFailedError` when the handler threw, `UpdateRejectedError` when the
 * run rejected it or ended without applying it, `UpdateTimeoutError` when
 * nothing was recorded in time.
 */
export async function awaitUpdate<U extends UpdateLike | string = string>(
  params: {
    readonly storage: Storage;
    readonly workflowId: string;
    readonly updateId: string;
    readonly update?: U;
  } & AwaitUpdateOptions,
): Promise<UpdateResultOf<U>> {
  const { storage, workflowId, updateId } = params;
  const clock = params.clock ?? SystemWallClock;
  const intervalMs = params.intervalMs ?? 100;
  const timeoutMs = params.timeoutMs ?? 60_000;
  const updateName =
    params.update === undefined
      ? undefined
      : typeof params.update === "string"
        ? params.update
        : params.update.name;
  const deadline = clock.currentTimeMs() + timeoutMs;

  for (;;) {
    const outcome = await getUpdateOutcome({ storage, workflowId, updateId });
    if (outcome !== undefined) return unwrapOutcome({ workflowId, updateId, updateName, outcome });

    const state = await storage.loadWorkflow(workflowId);
    if (state && isTerminalWorkflowStatus(state.status)) {
      // Applied just before the run ended?
      const last = await getUpdateOutcome({ storage, workflowId, updateId });
      if (last !== undefined)
        return unwrapOutcome({ workflowId, updateId, updateName, outcome: last });
      throw rejected({
        workflowId,
        updateId,
        ...(updateName !== undefined && { updateName }),
        reason: "run_terminal",
        message: `workflow "${workflowId}" ended before update "${updateId}" was applied`,
      });
    }
    if (clock.currentTimeMs() >= deadline) {
      throw new UpdateTimeoutError({
        workflowId,
        updateId,
        message: `update "${updateId}" has no outcome after ${timeoutMs}ms`,
      });
    }
    await new Promise<void>((resolve) => clock.setTimeout(resolve, intervalMs));
  }
}

/** `submitUpdate`, then `awaitUpdate`: send an update and return the handler's result. */
export async function executeUpdate<U extends UpdateLike | string>(
  params: {
    readonly storage: Storage;
    readonly workflowId: string;
    readonly update: U;
    readonly args: UpdateArgsOf<U>;
    readonly updateId?: string;
  } & AwaitUpdateOptions,
): Promise<UpdateResultOf<U>> {
  const submitted = await submitUpdate({
    storage: params.storage,
    workflowId: params.workflowId,
    update: params.update,
    args: params.args,
    ...(params.updateId !== undefined && { updateId: params.updateId }),
  });
  if (submitted.outcome !== undefined) {
    return unwrapOutcome({
      workflowId: params.workflowId,
      updateId: submitted.updateId,
      updateName: typeof params.update === "string" ? params.update : params.update.name,
      outcome: submitted.outcome,
    }) as never;
  }
  return awaitUpdate({
    storage: params.storage,
    workflowId: params.workflowId,
    updateId: submitted.updateId,
    update: params.update,
    ...(params.intervalMs !== undefined && { intervalMs: params.intervalMs }),
    ...(params.timeoutMs !== undefined && { timeoutMs: params.timeoutMs }),
    ...(params.clock !== undefined && { clock: params.clock }),
  });
}

function outcomeIn(params: {
  readonly signals: readonly SignalState[];
  readonly updateId: string;
}): UpdateOutcome | undefined {
  const name = updateResultSignalName(params.updateId);
  const hit = params.signals.find((x) => x.signalName === name);
  return hit ? parseUpdateOutcome(hit.payload) : undefined;
}

function unwrapOutcome<R>(params: {
  readonly workflowId: string;
  readonly updateId: string;
  readonly updateName?: string | undefined;
  readonly outcome: UpdateOutcome;
}): R {
  const { workflowId, updateId, updateName, outcome } = params;
  switch (outcome.status) {
    case "completed":
      return outcome.result as R;
    case "failed":
      throw new UpdateFailedError({
        workflowId,
        updateId,
        ...(updateName !== undefined && { updateName }),
        ...(outcome.error.tag !== undefined && { errorTag: outcome.error.tag }),
        message: outcome.error.message,
      });
    case "rejected":
      throw rejected({
        workflowId,
        updateId,
        ...(updateName !== undefined && { updateName }),
        reason: outcome.reason,
        message: outcome.message,
      });
  }
}

function describeErrors(errors: ReadonlyArray<{ path: string; message: string }>): string {
  return errors.map((e) => (e.path ? `${e.path}: ${e.message}` : e.message)).join("; ");
}
