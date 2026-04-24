// ---------------------------------------------------------------------------
// Runs route handlers — list, get, trigger, cancel, signal.
// ---------------------------------------------------------------------------

import type { WorkflowStorage, WorkflowStatus, Workflow } from "@promin/workflow";
import { json, jsonError, readJson } from "../router.ts";
import { runToDto, runToSummaryDto } from "../serialize.ts";
import type {
  RunDto,
  RunListQuery,
  RunListResponse,
  SignalRequest,
  StepDto,
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
  /** Registry of workflow definitions, used to augment /api/runs/:id with planned steps. */
  workflows?: Readonly<Record<string, Workflow<unknown, unknown>>>;
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
    const dto = runToDto(state);
    const def = deps.workflows?.[state.workflowName];
    if (def) {
      dto.steps = mergePlannedSteps(dto, def);
    }
    return json(200, dto);
  };
}

/**
 * Merge the workflow definition's static step list with the executed-step
 * DTOs so the UI can render not-yet-executed steps in the timeline. Executed
 * steps are kept as-is; definition steps missing from the executed set are
 * appended with `isPlanned: true` + status "pending".
 */
function mergePlannedSteps(dto: RunDto, wf: Workflow<unknown, unknown>): StepDto[] {
  const executedByName = new Map(dto.steps.map((s) => [s.stepName, s]));
  const out: StepDto[] = [];
  const seen = new Set<string>();

  for (const defStep of wf.dag.steps) {
    const executed = executedByName.get(defStep.name);
    if (executed) {
      out.push(executed);
    } else {
      out.push({
        stepName: defStep.name,
        run: dto.run,
        status: "pending",
        stepType: (defStep.kind as StepDto["stepType"]) ?? "single",
        dependsOn: [...defStep.dependsOn],
        attempt: 0,
        isPlanned: true,
      });
    }
    seen.add(defStep.name);
  }

  // Append any executed steps not in the definition (e.g. dynamic names).
  for (const s of dto.steps) {
    if (!seen.has(s.stepName)) out.push(s);
  }
  return out;
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

export function listWorkflowNames(deps: RunRoutesDeps) {
  return async (): Promise<Response> => {
    // Pull a wide page of recent workflows and distinct their names/types.
    // Good enough for "populate a dropdown"; a real backend should expose
    // dedicated distinct queries.
    const rows = await deps.storage.listWorkflows({ limit: 1000 });
    const names = Array.from(new Set(rows.map((r) => r.workflowName))).sort();
    const types = Array.from(
      new Set(rows.map((r) => r.workflowType).filter((t): t is string => !!t)),
    ).sort();
    return json(200, { names, types });
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
