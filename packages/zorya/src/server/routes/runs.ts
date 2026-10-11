// ---------------------------------------------------------------------------
// Runs route handlers — thin HTTP wrappers over RunsService + TriggerService.
//
// Handlers parse query params / request bodies and translate service results
// into Response objects. All real logic lives in `../services/runs-service.ts`
// and `../services/trigger-service.ts`.
// ---------------------------------------------------------------------------

import type {
  SignalPayloadSchema,
  Workflow,
  WorkflowOrderBy,
  WorkflowState,
  WorkflowStatus,
  WorkflowStorage,
} from "@promin/workflow";
import { isTerminalWorkflowStatus, validate } from "@promin/workflow";
import { json, jsonError, parseIntParam, readJson } from "../router.ts";
import type {
  RunListQuery,
  SignalRequest,
  TriggerRunRequest,
  TriggerRunResponse,
} from "../api-types.ts";
import type { WorkflowAdvertisementRegistry } from "../workflow-advertisements.ts";
import { RunsService, type RunActionResult } from "../services/runs-service.ts";
import { WorkflowIdConflictError } from "../services/workflows/errors.ts";

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
}

function makeService(deps: RunRoutesDeps): RunsService {
  return new RunsService({
    storage: deps.storage,
    workflows: deps.workflows,
    advertisements: deps.advertisements,
  });
}

export function listRuns(deps: RunRoutesDeps) {
  const service = makeService(deps);
  return async (req: Request): Promise<Response> => {
    const url = new URL(req.url);
    const sort = parseSortParam(url.searchParams.get("sort"));
    const q: RunListQuery = {
      status: (url.searchParams.get("status") as WorkflowStatus | null) ?? undefined,
      name: url.searchParams.get("name") ?? undefined,
      type: url.searchParams.get("type") ?? undefined,
      namespace: url.searchParams.get("namespace") ?? undefined,
      version: url.searchParams.get("version") ?? undefined,
      runSource:
        (url.searchParams.get("runSource") as RunListQuery["runSource"] | null) ?? undefined,
      runSourceId: url.searchParams.get("runSourceId") ?? undefined,
      metadata: parseMetadataParam(url.searchParams.get("metadata")),
      limit: parseLimitParam(url.searchParams.get("limit")) ?? 50,
      offset:
        parseIntParam({
          raw: url.searchParams.get("offset"),
          min: 0,
          max: Number.MAX_SAFE_INTEGER,
        }) ?? 0,
      orderBy: sort?.orderBy,
      orderDir: sort?.orderDir,
    };
    return json(200, await service.list(q));
  };
}

export function getRun(deps: RunRoutesDeps) {
  const service = makeService(deps);
  return async (_req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    if (!id) return jsonError(400, "missing_id");
    const dto = await service.get(id);
    if (!dto) return jsonError(404, "not_found");
    return json(200, dto);
  };
}

export function triggerRun(deps: RunRoutesDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    if (!deps.trigger) {
      return jsonError(
        501,
        "trigger_not_configured",
        "Server was constructed without a trigger fn",
      );
    }
    const name = params.name;
    if (!name) return jsonError(400, "missing_name");
    const body = (await readJson<TriggerRunRequest>(req)) ?? {};
    try {
      const result = await deps.trigger(name, body.input, {
        workflowId: body.workflowId,
        workflowType: body.workflowType,
        namespace: body.namespace,
        metadata: body.metadata,
        version: body.version,
        // Default tag for dashboard-initiated runs. Body fields can
        // override (e.g. an API client or webhook ingestor passing through
        // its own runSource), but the dashboard's "Trigger" button is
        // always a manual fire from a user's POV.
        runSource: body.runSource ?? "manual",
        runSourceId: body.runSourceId,
      });
      const response: TriggerRunResponse = {
        workflowId: result.workflowId,
        ...(result.existing === true && { existing: true }),
      };
      return json(200, response);
    } catch (err) {
      if (err instanceof WorkflowIdConflictError) {
        return jsonError(409, "workflow_id_conflict", err.message);
      }
      return jsonError(400, "trigger_failed", err instanceof Error ? err.message : String(err));
    }
  };
}

export function cancelRun(deps: RunRoutesDeps) {
  const service = makeService(deps);
  return async (_req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    if (!id) return jsonError(400, "missing_id");
    try {
      return runActionResponse(await service.cancel(id));
    } catch (err) {
      return jsonError(400, "cancel_failed", err instanceof Error ? err.message : String(err));
    }
  };
}

/**
 * Map an operator action's outcome to a response: 200 when it ended the
 * run, 404 for an unknown run, 409 when the run had already ended.
 */
export function runActionResponse(result: RunActionResult): Response {
  switch (result.outcome) {
    case "ok":
      return json(200, { ok: true });
    case "not_found":
      return jsonError(404, "not_found");
    case "ended":
      return jsonError(409, "run_ended", `Run has already ended (status: ${result.status})`);
  }
}

export function listWorkflowNames(deps: RunRoutesDeps) {
  const service = makeService(deps);
  return async (req: Request): Promise<Response> => {
    const url = new URL(req.url);
    const namespace = url.searchParams.get("namespace") ?? undefined;
    return json(200, await service.listNames({ namespace }));
  };
}

export function sendSignal(deps: RunRoutesDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    if (!id) return jsonError(400, "missing_id");
    const body = await readJson<SignalRequest>(req);
    if (!body || !body.signalName) return jsonError(400, "missing_signal_name");

    // A signal is only meaningful for a run that can still consume it.
    const run = await deps.storage.loadWorkflow(id);
    if (!run) return jsonError(404, "not_found");
    if (isTerminalWorkflowStatus(run.status)) {
      return jsonError(409, "run_ended", `Run has already ended (status: ${run.status})`);
    }

    // Schema check — when the suspend point was set up via
    // `ctx.validatedSignal` / `ctx.approval`, the schema snapshot lives on
    // the StepState. Reject mismatching payloads before delivery so the
    // workflow never resumes with junk. Plain `ctx.signal` suspends carry
    // no schema; the validator's absence-path is pass-through.
    const mismatch = checkSignalSchema({
      run,
      signalName: body.signalName,
      payload: body.payload ?? null,
    });
    if (mismatch) return mismatch;

    try {
      await deps.storage.deliverSignal({
        workflowId: id,
        signalName: body.signalName,
        payload: body.payload ?? null,
      });
      return json(200, { ok: true });
    } catch (err) {
      return jsonError(400, "signal_failed", err instanceof Error ? err.message : String(err));
    }
  };
}

/**
 * Walk the workflow's steps for one waiting on this signal name; if its
 * StepState carries a `signalJsonSchema`, validate the payload against it
 * and return a 400 response on mismatch. Returns null when there's no
 * schema (pass-through) or no matching step (let deliverSignal handle —
 * it queues the value for whenever the suspend lands).
 */
function checkSignalSchema(params: {
  run: WorkflowState;
  signalName: string;
  payload: unknown;
}): Response | null {
  const { run, signalName, payload } = params;
  for (const step of Object.values(run.steps)) {
    if (step.status === "waiting_for_signal" && step.signalName === signalName) {
      const schema = step.signalJsonSchema;
      if (schema === undefined || schema === null) return null;
      const result = validate(payload, schema as SignalPayloadSchema);
      if (!result.ok) {
        return jsonError(400, "schema_mismatch", formatValidationErrors(result.errors));
      }
      return null;
    }
  }
  return null;
}

function formatValidationErrors(errors: ReadonlyArray<{ path: string; message: string }>): string {
  return errors.map((e) => (e.path === "" ? e.message : `${e.path}: ${e.message}`)).join("; ");
}

/** Limit-style query parameter: an integer clamped to [1, 1000]. */
function parseLimitParam(raw: string | null): number | undefined {
  return parseIntParam({ raw, min: 1, max: 1_000 });
}

const VALID_ORDER_BY: ReadonlyArray<WorkflowOrderBy> = [
  "createdAt",
  "startedAt",
  "completedAt",
  "duration",
  "status",
  "name",
];

/**
 * Parse `?metadata=<json>`. Returns undefined for missing / malformed input
 * or non-object payloads — the URL stays forgiving when hand-edited.
 */
function parseMetadataParam(raw: string | null): Record<string, unknown> | undefined {
  if (!raw) return undefined;
  try {
    const parsed = JSON.parse(raw);
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return undefined;
    return parsed as Record<string, unknown>;
  } catch {
    return undefined;
  }
}

/**
 * Parse `?sort=createdAt:desc`. Bad / unknown columns silently fall back to
 * defaults — keeps the URL forgiving when users edit it by hand.
 */
function parseSortParam(
  raw: string | null,
): { orderBy?: WorkflowOrderBy; orderDir?: "asc" | "desc" } | undefined {
  if (!raw) return undefined;
  const [col, dir] = raw.split(":");
  const orderBy = VALID_ORDER_BY.find((c) => c === col);
  if (!orderBy) return undefined;
  const orderDir = dir === "asc" ? "asc" : dir === "desc" ? "desc" : undefined;
  return { orderBy, orderDir };
}
