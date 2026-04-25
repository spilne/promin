// ---------------------------------------------------------------------------
// Runs route handlers — thin HTTP wrappers over RunsService + TriggerService.
//
// Handlers parse query params / request bodies and translate service results
// into Response objects. All real logic lives in `../services/runs-service.ts`
// and `../services/trigger-service.ts`.
// ---------------------------------------------------------------------------

import type { Workflow, WorkflowOrderBy, WorkflowStatus, WorkflowStorage } from "@promin/workflow";
import { json, jsonError, readJson } from "../router.ts";
import type {
  RunListQuery,
  SignalRequest,
  TriggerRunRequest,
  TriggerRunResponse,
} from "../api-types.ts";
import type { WorkflowAdvertisementRegistry } from "../workflow-advertisements.ts";
import { RunsService } from "../services/runs-service.ts";

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
    },
  ): Promise<{ workflowId: string }>;
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
      limit: parseIntParam(url.searchParams.get("limit")) ?? 50,
      offset: parseIntParam(url.searchParams.get("offset")) ?? 0,
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
      });
      const response: TriggerRunResponse = { workflowId: result.workflowId };
      return json(200, response);
    } catch (err) {
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
      await service.cancel(id);
      return json(200, { ok: true });
    } catch (err) {
      return jsonError(400, "cancel_failed", err instanceof Error ? err.message : String(err));
    }
  };
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
    try {
      await deps.storage.deliverSignal(id, body.signalName, body.payload ?? null);
      return json(200, { ok: true });
    } catch (err) {
      return jsonError(400, "signal_failed", err instanceof Error ? err.message : String(err));
    }
  };
}

function parseIntParam(s: string | null): number | undefined {
  if (s === null) return undefined;
  const n = Number.parseInt(s, 10);
  return Number.isFinite(n) ? n : undefined;
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
