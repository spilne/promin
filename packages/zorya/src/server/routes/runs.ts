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
import type { WorkflowAdvertisementRegistry } from "../workflow-advertisements.ts";

/** Minimal step-shape used by mergePlannedSteps — same fields on Workflow.dag.steps and AdvertisedWorkflow.steps. */
interface StepDefLike {
  readonly name: string;
  readonly kind: string;
  readonly dependsOn: readonly string[];
}

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
  /** Registry of workflow definitions, used to augment /api/runs/:id with planned steps. */
  workflows?: Readonly<Record<string, Workflow<unknown, unknown>>>;
  /**
   * Worker-advertised step defs. Used as a fallback when a workflow isn't
   * statically registered (i.e. split mode where workers own the code) so
   * graph edges can still render.
   */
  advertisements?: WorkflowAdvertisementRegistry;
}

export function listRuns(deps: RunRoutesDeps) {
  return async (req: Request): Promise<Response> => {
    const url = new URL(req.url);
    const version = url.searchParams.get("version") ?? undefined;
    const limit = parseIntParam(url.searchParams.get("limit")) ?? 50;
    const offset = parseIntParam(url.searchParams.get("offset")) ?? 0;
    const q: RunListQuery = {
      status: (url.searchParams.get("status") as WorkflowStatus | null) ?? undefined,
      name: url.searchParams.get("name") ?? undefined,
      type: url.searchParams.get("type") ?? undefined,
      namespace: url.searchParams.get("namespace") ?? undefined,
      version,
      limit,
      offset,
    };
    // Storage doesn't filter by version yet; over-fetch when the filter is
    // active and trim in JS so paginated views return roughly `limit` rows.
    const fetchLimit = version ? Math.min(limit * 5, 500) : limit;
    const rows = await deps.storage.listWorkflows({ ...q, limit: fetchLimit });
    const filtered = version ? rows.filter((r) => r.version === version) : rows;
    const response: RunListResponse = { runs: filtered.slice(0, limit).map(runToSummaryDto) };
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
    const stepDefs = await resolveStepDefs(deps, state.workflowName);
    if (stepDefs.length > 0) {
      dto.steps = mergePlannedSteps(dto, stepDefs);
    }
    return json(200, dto);
  };
}

/**
 * Resolve a workflow's step definitions from the in-process registry first,
 * falling back to worker advertisements. Empty array if nothing knows about
 * the workflow.
 */
async function resolveStepDefs(deps: RunRoutesDeps, name: string): Promise<readonly StepDefLike[]> {
  const def = deps.workflows?.[name];
  if (def) return def.dag.steps;
  if (deps.advertisements) {
    const distinct = await deps.advertisements.distinct();
    const adv = distinct.find((a) => a.name === name);
    if (adv) return adv.steps;
  }
  return [];
}

/**
 * Merge the workflow definition's static step list with the executed-step
 * DTOs so the UI can render not-yet-executed steps in the timeline. Executed
 * steps inherit `dependsOn` from the def when storage didn't persist it
 * (most backends only set dependsOn at insert time and saveStepResult /
 * saveStepFailure default it to []), so the run-detail Graph view shows
 * edges instead of disconnected nodes.
 */
function mergePlannedSteps(dto: RunDto, stepDefs: readonly StepDefLike[]): StepDto[] {
  const executedByName = new Map(dto.steps.map((s) => [s.stepName, s]));
  const out: StepDto[] = [];
  const seen = new Set<string>();

  for (const defStep of stepDefs) {
    const executed = executedByName.get(defStep.name);
    if (executed) {
      // Storage's executed rows often have dependsOn=[]; fill from the def
      // so graph edges render.
      if (executed.dependsOn.length === 0 && defStep.dependsOn.length > 0) {
        executed.dependsOn = [...defStep.dependsOn];
      }
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
    // Pull a wide page of recent workflows and distinct their names/types/
    // namespaces. Good enough for "populate a dropdown"; a real backend
    // should expose dedicated distinct queries.
    const rows = await deps.storage.listWorkflows({ limit: 1000 });
    const names = Array.from(new Set(rows.map((r) => r.workflowName))).sort();
    const types = Array.from(
      new Set(rows.map((r) => r.workflowType).filter((t): t is string => !!t)),
    ).sort();
    const namespaces = Array.from(
      new Set(rows.map((r) => r.namespace).filter((n): n is string => !!n)),
    ).sort();
    return json(200, { names, types, namespaces });
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
