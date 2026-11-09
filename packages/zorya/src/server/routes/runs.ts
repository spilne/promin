// ---------------------------------------------------------------------------
// Runs route handlers — thin HTTP wrappers over RunsService and the
// workflows layer's trigger.
//
// Handlers validate params, query strings and bodies (schemas in
// `../api-types.ts`) and translate service results into responses. Errors
// they don't map are thrown to the router's crash barrier (JSON 500).
// ---------------------------------------------------------------------------

import type {
  NamespaceRegistry,
  WallClock,
  SignalPayloadSchema,
  Workflow,
  WorkflowState,
  WorkflowStorage,
} from "@promin/workflow";
import {
  awaitUpdate,
  getUpdateOutcome,
  isTerminalWorkflowStatus,
  submitUpdate,
  validate,
  UpdateFailedError,
  UpdateRejectedError,
  UpdateTimeoutError,
} from "@promin/workflow";
import { z } from "zod";
import { accepted, json, jsonError } from "../router.ts";
import {
  CancelRunRequestSchema,
  IdSchema,
  MarkFailedRequestSchema,
  NameSchema,
  NamespaceQuerySchema,
  RunListQuerySchema,
  SignalRequestSchema,
  TriggerRunRequestSchema,
  UpdateRequestSchema,
  type UpdateResponse,
  type RunActionResponse,
  type SignalDeliveryResponse,
  type TriggerRunResponse,
} from "../api-types.ts";
import { parseBody, parseParams, parseQuery } from "../request-validation.ts";
import { requireActiveNamespace } from "../namespace-guard.ts";
import { agentApprovalRefusal } from "./signals.ts";
import type { WorkflowAdvertisementRegistry } from "@promin/workflow/distributed";
import { RunsService, type RunActionResult } from "../services/runs-service.ts";
import { UnknownWorkflowError, WorkflowIdConflictError } from "../services/workflows/errors.ts";

export interface RunTrigger {
  (
    name: string,
    input: unknown,
    options?: {
      workflowId?: string;
      workflowType?: string;
      namespace?: string;
      metadata?: Record<string, unknown>;
      /** Workflow version to record on the run + route to. */
      version?: string;
      /** What kicked this run off (`"schedule"`, `"manual"`, …). */
      runSource?: import("@promin/workflow").RunSource;
      /** Producer id corresponding to `runSource` (e.g. `scheduleId`). */
      runSourceId?: string;
    },
  ): Promise<{ workflowId: string; existing?: boolean }>;
}

export interface RunRoutesDeps {
  storage: WorkflowStorage;
  /** Called to start a new run. Server doesn't know how to run workflows by name. */
  trigger?: RunTrigger;
  /** Static workflow definitions, used to surface planned steps. */
  workflows?: Readonly<Record<string, Workflow<unknown, unknown>>>;
  /** Worker-advertised step defs — fallback when `workflows` doesn't have the def. */
  advertisements?: WorkflowAdvertisementRegistry;
  /** When set, a run can only be triggered in a registered, active namespace. */
  namespaces?: NamespaceRegistry;
  /** Time source for a run's `waitingOnSummary`. */
  clock?: WallClock;
}

/** `:id` of a run route. */
export const RunParamsSchema = z.object({ id: IdSchema });
/** `:name` of a workflow route. */
export const WorkflowParamsSchema = z.object({ name: NameSchema });

function makeService(deps: RunRoutesDeps): RunsService {
  return new RunsService({
    storage: deps.storage,
    workflows: deps.workflows,
    advertisements: deps.advertisements,
    clock: deps.clock,
  });
}

/** `Location` of a run. */
export function runLocation(workflowId: string): string {
  return `/api/runs/${encodeURIComponent(workflowId)}`;
}

/** GET /api/runs — one page of runs, filtered, with `total` when storage can count. */
export function listRuns(deps: RunRoutesDeps) {
  const service = makeService(deps);
  return async (req: Request): Promise<Response> => {
    const { sort, ...query } = parseQuery({ req, schema: RunListQuerySchema });
    return json(200, await service.list({ ...query, ...sort }));
  };
}

export function getRun(deps: RunRoutesDeps) {
  const service = makeService(deps);
  return async (_req: Request, params: Record<string, string>): Promise<Response> => {
    const { id } = parseParams({ params, schema: RunParamsSchema });
    const dto = await service.get(id);
    if (!dto) return jsonError(404, "not_found");
    return json(200, dto);
  };
}

/**
 * POST /api/workflows/:name/runs — start a run. 202 + `Location` when a run
 * started; 200 `{ existing: true }` when the requested `workflowId` already
 * names a run of this workflow; 404 for an unknown workflow; 409 when the
 * `workflowId` belongs to another workflow's run. With a namespace registry,
 * an explicit `namespace` must be registered (404 `namespace_not_found`) and
 * active (409 `namespace_archived`).
 */
export function triggerRun(deps: RunRoutesDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    if (!deps.trigger) {
      return jsonError(
        501,
        "trigger_not_configured",
        "Server was constructed without a trigger fn",
      );
    }
    const { name } = parseParams({ params, schema: WorkflowParamsSchema });
    const body = (await parseBody({ req, schema: TriggerRunRequestSchema.optional() })) ?? {};
    await requireActiveNamespace({ registry: deps.namespaces, namespace: body.namespace });
    let result: { workflowId: string; existing?: boolean };
    try {
      result = await deps.trigger(name, body.input, {
        workflowId: body.workflowId,
        workflowType: body.workflowType,
        namespace: body.namespace,
        metadata: body.metadata,
        version: body.version,
        // A run started over HTTP without a tag is a manual fire.
        runSource: body.runSource ?? "manual",
        runSourceId: body.runSourceId,
      });
    } catch (err) {
      if (err instanceof UnknownWorkflowError) {
        return jsonError(404, "workflow_not_found", err.message);
      }
      if (err instanceof WorkflowIdConflictError) {
        return jsonError(409, "workflow_id_conflict", err.message);
      }
      throw err;
    }
    if (result.existing === true) {
      const response: TriggerRunResponse = { workflowId: result.workflowId, existing: true };
      return new Response(JSON.stringify(response), {
        status: 200,
        headers: { "content-type": "application/json", location: runLocation(result.workflowId) },
      });
    }
    const response: TriggerRunResponse = { workflowId: result.workflowId };
    return accepted(response, runLocation(result.workflowId));
  };
}

/**
 * POST /api/runs/:id/cancel — end an in-flight run as cancelled. Body
 * (optional) `{ cascade: true }` cancels its in-flight descendants too.
 */
export function cancelRun(deps: RunRoutesDeps) {
  const service = makeService(deps);
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const { id } = parseParams({ params, schema: RunParamsSchema });
    const body = await parseBody({ req, schema: CancelRunRequestSchema.optional() });
    const result = await service.cancel({ id, cascade: body?.cascade });
    return runActionResponse({ workflowId: id, result });
  };
}

/** POST /api/runs/:id/mark-success — operator override: end an in-flight run as completed. */
export function markRunSuccess(runs: RunsService) {
  return async (_req: Request, params: Record<string, string>): Promise<Response> => {
    const { id } = parseParams({ params, schema: RunParamsSchema });
    return runActionResponse({ workflowId: id, result: await runs.markSuccess(id) });
  };
}

/** POST /api/runs/:id/mark-failed — operator override: end an in-flight run as failed. */
export function markRunFailed(runs: RunsService) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const { id } = parseParams({ params, schema: RunParamsSchema });
    const body = await parseBody({ req, schema: MarkFailedRequestSchema.optional() });
    const reason = body?.reason ?? "marked failed";
    return runActionResponse({ workflowId: id, result: await runs.markFailed({ id, reason }) });
  };
}

/**
 * Map an operator action's outcome to a response: 200 with the run's
 * resulting status when it ended the run, 404 for an unknown run, 409 when
 * the run had already ended.
 */
export function runActionResponse(params: {
  workflowId: string;
  result: RunActionResult;
}): Response {
  const { workflowId, result } = params;
  switch (result.outcome) {
    case "ok": {
      const body: RunActionResponse = { workflowId, status: result.status };
      return json(200, body);
    }
    case "not_found":
      return jsonError(404, "not_found");
    case "ended":
      return jsonError(409, "run_ended", `Run has already ended (status: ${result.status})`);
  }
}

/** GET /api/workflows — distinct names / types / namespaces for filter dropdowns. */
export function listWorkflowNames(deps: RunRoutesDeps) {
  const service = makeService(deps);
  return async (req: Request): Promise<Response> => {
    const { namespace } = parseQuery({ req, schema: NamespaceQuerySchema });
    return json(200, await service.listNames({ namespace }));
  };
}

/**
 * POST /api/runs/:id/signal — store a signal value for the run to consume.
 *
 * Delivery is last-wins: until the run consumes the signal, a later
 * delivery of the same name replaces an earlier one. `ifAbsent: true`
 * refuses (409 `signal_already_delivered`) instead, so two operators
 * deciding on the same wait can't silently overwrite each other. 404 for
 * an unknown run, 409 `run_ended` for an ended one, 400 `schema_mismatch`
 * when the waiting step declared a payload schema the value fails.
 */
export function sendSignal(deps: RunRoutesDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const { id } = parseParams({ params, schema: RunParamsSchema });
    const body = await parseBody({ req, schema: SignalRequestSchema });
    const payload = body.payload ?? null;

    // A signal is only meaningful for a run that can still consume it.
    const run = await deps.storage.loadWorkflow(id);
    if (!run) return jsonError(404, "not_found");
    if (isTerminalWorkflowStatus(run.status)) {
      return jsonError(409, "run_ended", `Run has already ended (status: ${run.status})`);
    }

    const agentApproval = await agentApprovalRefusal({
      storage: deps.storage,
      workflowId: id,
      signalName: body.signalName,
    });
    if (agentApproval) return agentApproval;

    const mismatch = signalSchemaMismatch({ run, signalName: body.signalName, payload });
    if (mismatch !== undefined) return jsonError(400, "schema_mismatch", mismatch);

    if (body.ifAbsent === true) {
      const pending = await deps.storage.loadSignals(id);
      if (pending.some((s) => s.signalName === body.signalName)) {
        return jsonError(
          409,
          "signal_already_delivered",
          `A value for "${body.signalName}" is already waiting to be consumed`,
        );
      }
    }

    await deps.storage.deliverSignal({ workflowId: id, signalName: body.signalName, payload });
    const response: SignalDeliveryResponse = { workflowId: id, signalName: body.signalName };
    return accepted(response, runLocation(id));
  };
}

/** HTTP status of an update the caller could not deliver. */
const UPDATE_REJECTION_STATUS: Record<string, number> = {
  validation: 400,
  run_not_found: 404,
  run_terminal: 409,
  unknown_update: 409,
  no_handlers: 409,
  id_conflict: 409,
  not_applied: 409,
};

/**
 * POST /api/runs/:id/updates — send an update to the run's journaled body.
 *
 * Delivery stores the request (202); the run applies it at its next update
 * point (a suspended run is resumed by the signal scanner) and records an
 * outcome. With `wait: true` the answer is that outcome (200). Repeating
 * the call with the same `updateId` is idempotent. 404 for an unknown run;
 * 409 `run_terminal` / `unknown_update` / `no_handlers` / `id_conflict`
 * when it cannot be delivered; 400 `validation` when the args fail the
 * handler's schema.
 */
export function sendUpdate(deps: RunRoutesDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const { id } = parseParams({ params, schema: RunParamsSchema });
    const body = await parseBody({ req, schema: UpdateRequestSchema });
    let submitted;
    try {
      submitted = await submitUpdate({
        storage: deps.storage,
        workflowId: id,
        update: body.updateName,
        args: body.args ?? null,
        ...(body.updateId !== undefined && { updateId: body.updateId }),
      });
    } catch (error) {
      if (error instanceof UpdateRejectedError) return updateRejection(error);
      throw error;
    }
    const base = { workflowId: id, updateId: submitted.updateId };
    if (submitted.outcome !== undefined) {
      return json(200, { ...base, outcome: submitted.outcome } satisfies UpdateResponse);
    }
    if (body.wait !== true) return accepted(base satisfies UpdateResponse, runLocation(id));
    try {
      await awaitUpdate({
        storage: deps.storage,
        workflowId: id,
        updateId: submitted.updateId,
        timeoutMs: body.timeoutMs ?? 30_000,
        ...(deps.clock !== undefined && { clock: deps.clock }),
      });
    } catch (error) {
      if (error instanceof UpdateTimeoutError) return accepted(base satisfies UpdateResponse);
      if (error instanceof UpdateRejectedError && error.reason === "run_terminal") {
        return updateRejection(error);
      }
      // A failed or rejected outcome is the answer, not a transport error.
      if (!(error instanceof UpdateFailedError || error instanceof UpdateRejectedError))
        throw error;
    }
    const outcome = await getUpdateOutcome({
      storage: deps.storage,
      workflowId: id,
      updateId: submitted.updateId,
    });
    return json(200, {
      ...base,
      ...(outcome !== undefined && { outcome }),
    } satisfies UpdateResponse);
  };
}

/** GET /api/runs/:id/updates/:updateId — the outcome of an update (200), or 202 while it is pending. */
export function getUpdate(deps: RunRoutesDeps) {
  return async (_req: Request, params: Record<string, string>): Promise<Response> => {
    const { id } = parseParams({ params, schema: RunParamsSchema });
    const { updateId } = parseParams({ params, schema: z.object({ updateId: IdSchema }) });
    let outcome;
    try {
      outcome = await getUpdateOutcome({ storage: deps.storage, workflowId: id, updateId });
    } catch (error) {
      if (error instanceof UpdateRejectedError) return jsonError(404, "not_found", error.message);
      throw error;
    }
    const base = { workflowId: id, updateId };
    return outcome === undefined
      ? accepted(base satisfies UpdateResponse)
      : json(200, { ...base, outcome } satisfies UpdateResponse);
  };
}

function updateRejection(error: UpdateRejectedError): Response {
  return jsonError(UPDATE_REJECTION_STATUS[error.reason] ?? 409, error.reason, error.message);
}

/**
 * When a step of `run` waits on `signalName` with a payload schema (set up
 * by `ctx.validatedSignal` / `ctx.approval`), validate `payload` against it
 * and return the problems; `undefined` when it passes or no schema applies.
 * Plain `ctx.signal` waits carry no schema, and a delivery before the wait
 * starts has no step to check against.
 */
export function signalSchemaMismatch(params: {
  run: WorkflowState;
  signalName: string;
  payload: unknown;
}): string | undefined {
  const { run, signalName, payload } = params;
  for (const step of Object.values(run.steps)) {
    if (step.status === "waiting_for_signal" && step.signalName === signalName) {
      const schema = step.signalJsonSchema;
      if (schema === undefined || schema === null) return undefined;
      const result = validate(payload, schema as SignalPayloadSchema);
      if (result.ok) return undefined;
      return result.errors
        .map((e) => (e.path === "" ? e.message : `${e.path}: ${e.message}`))
        .join("; ");
    }
  }
  return undefined;
}
