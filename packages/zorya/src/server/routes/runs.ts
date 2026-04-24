// ---------------------------------------------------------------------------
// Runs route handlers — list, get, trigger, cancel, signal.
// ---------------------------------------------------------------------------

import type { WorkflowStorage, WorkflowStatus } from "@promin/workflow";
import { json, jsonError, readJson } from "../router.ts";
import { runToDto, runToSummaryDto } from "../serialize.ts";
import type {
  RunListQuery,
  RunListResponse,
  SignalRequest,
  TriggerRunRequest,
  TriggerRunResponse,
} from "../api-types.ts";

export interface RunTrigger {
  (
    name: string,
    input: unknown,
    options?: {
      workflowId?: string;
      workflowType?: string;
      namespace?: string;
      metadata?: Record<string, unknown>;
    },
  ): Promise<{ workflowId: string }>;
}

export interface RunRoutesDeps {
  storage: WorkflowStorage;
  /** Called to start a new run. Server doesn't know how to run workflows by name. */
  trigger?: RunTrigger;
}

export function listRuns(deps: RunRoutesDeps) {
  return async (req: Request): Promise<Response> => {
    const url = new URL(req.url);
    const q: RunListQuery = {
      status: (url.searchParams.get("status") as WorkflowStatus | null) ?? undefined,
      name: url.searchParams.get("name") ?? undefined,
      type: url.searchParams.get("type") ?? undefined,
      namespace: url.searchParams.get("namespace") ?? undefined,
      limit: parseIntParam(url.searchParams.get("limit")) ?? 50,
      offset: parseIntParam(url.searchParams.get("offset")) ?? 0,
    };
    const rows = await deps.storage.listWorkflows(q);
    const response: RunListResponse = { runs: rows.map(runToSummaryDto) };
    return json(200, response);
  };
}

export function getRun(deps: RunRoutesDeps) {
  return async (_req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    if (!id) return jsonError(400, "missing_id");
    const state = await deps.storage.loadWorkflow(id);
    if (!state) return jsonError(404, "not_found");
    return json(200, runToDto(state));
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
      });
      const response: TriggerRunResponse = { workflowId: result.workflowId };
      return json(200, response);
    } catch (err) {
      return jsonError(400, "trigger_failed", err instanceof Error ? err.message : String(err));
    }
  };
}

export function cancelRun(deps: RunRoutesDeps) {
  return async (_req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    if (!id) return jsonError(400, "missing_id");
    try {
      await deps.storage.cancelWorkflow(id);
      return json(200, { ok: true });
    } catch (err) {
      return jsonError(400, "cancel_failed", err instanceof Error ? err.message : String(err));
    }
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
