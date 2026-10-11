// ---------------------------------------------------------------------------
// DAG gateway — operator-facing CRUD + run for AgenticDagRecipe.
//
// Routes:
//   GET    /api/dags                      — list (latest-per-id)
//   GET    /api/dags/:id                  — fetch (latest, or ?version=)
//   GET    /api/dags/:id/versions         — full version history
//   POST   /api/dags                      — register (or new version)
//   DELETE /api/dags/:id                  — unregister all versions
//   POST   /api/dags/:id/run              — start a durable run (202 + run id)
//
// A run names its scope (`namespaceId` + `resourceId | ownerId`) like an
// agent invocation; every node runs bound to it. Before the run starts,
// each node's agent must exist (404), be enabled (410) and pass the
// caller's `authorizeScope` for that scope (403), and the namespace must
// be active.
//
// A run is a run of the DAG workflow `ZoryaDags` registered at boot
// (`DAG_WORKFLOW_NAME`, internal: the generic trigger routes can't start
// it): the endpoint starts it in the scope's namespace and answers 202
// with its id at once. A `workflowId` naming an existing run answers with
// it only when it runs the same DAG, version and scope (else 409). Follow
// it like any other run (GET /api/runs/:id, its event stream); each node
// is a child run of it. Recovery resumes it by name after a restart.
//
// The scope checks live here, in the route. A row of the DAG workflow
// created another way (the worker plane's storage API) can still only run
// a DAG registered in the DAG registry, through node agents the resolver
// accepts — but its scope is not authorized: treat the worker storage
// plane as privileged.
// ---------------------------------------------------------------------------

import {
  DagValidationError,
  type AgentRegistry,
  type DagRegistry,
  type DurableDagInput,
  type RegisterDagInput,
} from "@promin/agent";
import type { NamespaceRegistry } from "@promin/workflow";
import { json, jsonError, readJson } from "../router.ts";
import { requireActiveNamespace } from "../namespace-guard.ts";
import { WorkflowIdConflictError } from "../services/workflows/errors.ts";
import type { TriggerResult, ZoryaWorkflows } from "../services/workflows/zorya-workflows.ts";
import type { ScopeAuthorizer } from "./agent-gateway-kit.ts";
import { agentNotFound, disabled410, parseScopeFields } from "./agents/agent-scope.ts";

export interface DagGatewayDeps {
  readonly registry: DagRegistry;
  /** Workflow service the DAG workflow is defined on; runs start through it. */
  readonly workflows: ZoryaWorkflows;
  /** Name the DAG workflow is defined under. */
  readonly workflowName: string;
  /** Agent recipes the DAG nodes reference — checked before a run starts. */
  readonly agents: AgentRegistry;
  /**
   * Binds a run's scope to the caller, once per node agent (403
   * `scope_forbidden`). See `ScopeAuthorizer`.
   */
  readonly authorizeScope?: ScopeAuthorizer;
  /**
   * When set, a run needs a registered, active namespace (404
   * `namespace_not_found`, 409 `namespace_archived`).
   */
  readonly namespaces?: NamespaceRegistry;
}

/**
 * Body of `POST /api/dags/:id/run`. A run acts in the caller's scope:
 * `namespaceId` plus exactly one of `resourceId` / `ownerId`, as for an
 * agent invocation.
 */
interface RunRequest {
  readonly namespaceId?: unknown;
  readonly resourceId?: unknown;
  readonly ownerId?: unknown;
  readonly version?: string;
  readonly initialInput?: Readonly<Record<string, unknown>>;
  /**
   * Override the run id. Defaults to a fresh one — pass an explicit id to
   * make a retried POST idempotent: an id that already has a run answers
   * with that run instead of starting another.
   */
  readonly workflowId?: string;
}

function asMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

export function listDags(deps: DagGatewayDeps) {
  return async (req: Request): Promise<Response> => {
    const url = new URL(req.url);
    const limit = url.searchParams.get("limit");
    const tag = url.searchParams.get("tag");
    const params: { limit?: number; tag?: string } = {};
    if (limit) {
      const n = Number.parseInt(limit, 10);
      if (Number.isFinite(n) && n > 0) params.limit = n;
    }
    if (tag) params.tag = tag;
    try {
      const dags = await deps.registry.list(params);
      return json(200, { dags });
    } catch (err) {
      return jsonError(500, "list_failed", asMessage(err));
    }
  };
}

export function getDag(deps: DagGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    if (!id) return jsonError(400, "missing_id");
    const url = new URL(req.url);
    const version = url.searchParams.get("version") ?? undefined;
    try {
      const dag = await deps.registry.get(id, version);
      if (!dag) return jsonError(404, "dag_not_found", `DAG "${id}" not registered.`);
      return json(200, dag);
    } catch (err) {
      return jsonError(500, "get_failed", asMessage(err));
    }
  };
}

export function listDagVersions(deps: DagGatewayDeps) {
  return async (_req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    if (!id) return jsonError(400, "missing_id");
    try {
      const versions = await deps.registry.versions(id);
      if (versions.length === 0) {
        return jsonError(404, "dag_not_found", `DAG "${id}" not registered.`);
      }
      return json(200, { versions });
    } catch (err) {
      return jsonError(500, "list_versions_failed", asMessage(err));
    }
  };
}

export function createDag(deps: DagGatewayDeps) {
  return async (req: Request): Promise<Response> => {
    const body = await readJson<RegisterDagInput>(req);
    if (!body) return jsonError(400, "missing_body");
    if (typeof body.id !== "string" || body.id.length === 0) {
      return jsonError(400, "missing_id");
    }
    try {
      const dag = await deps.registry.register(body);
      return json(201, dag);
    } catch (err) {
      if (err instanceof DagValidationError) {
        return json(400, { error: "invalid_dag", issues: err.issues });
      }
      return jsonError(500, "create_failed", asMessage(err));
    }
  };
}

export function deleteDag(deps: DagGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    if (!id) return jsonError(400, "missing_id");
    const url = new URL(req.url);
    const version = url.searchParams.get("version") ?? undefined;
    try {
      await deps.registry.unregister(id, version);
      return new Response(null, { status: 204 });
    } catch (err) {
      return jsonError(500, "delete_failed", asMessage(err));
    }
  };
}

export function runDag(deps: DagGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    if (!id) return jsonError(400, "missing_id");
    const body = (await readJson<RunRequest>(req)) ?? {};
    const scope = parseScopeFields(body);
    if ("error" in scope) return jsonError(400, scope.error);
    const dag = await deps.registry.get(id, body.version);
    if (!dag) return jsonError(404, "dag_not_found", `DAG "${id}" not registered.`);

    // Every node runs in the caller's scope, so the caller must be allowed
    // to invoke each node's agent there — the same checks as a direct
    // invocation. Nodes are re-checked for the kill switch as they run.
    for (const agentId of new Set(dag.nodes.map((n) => n.agentId))) {
      const recipe = await deps.agents.get(agentId);
      if (!recipe) return agentNotFound(agentId);
      const disabled = disabled410(recipe);
      if (disabled) return disabled;
      if (deps.authorizeScope) {
        const allowed = await deps.authorizeScope({ req, scope: { ...scope, agentId } });
        if (!allowed) return jsonError(403, "scope_forbidden");
      }
    }
    await requireActiveNamespace({ registry: deps.namespaces, namespace: scope.namespaceId });

    // The run names the DAG by reference; the workflow reads the recipe
    // from the registry, so a run only ever executes a registered DAG.
    const input: DurableDagInput = {
      dagId: dag.id,
      version: dag.version,
      initialInput: body.initialInput ?? {},
      scope,
    };
    let started: TriggerResult;
    try {
      started = await deps.workflows.trigger(deps.workflowName, input, {
        ...(body.workflowId !== undefined && { workflowId: body.workflowId }),
        namespace: scope.namespaceId,
        runSource: "api",
        metadata: { dagId: dag.id, dagVersion: dag.version },
        internal: true,
      });
    } catch (err) {
      if (err instanceof WorkflowIdConflictError) {
        return jsonError(409, "workflow_id_conflict", err.message);
      }
      return jsonError(500, "run_failed", asMessage(err));
    }
    if (started.existing === true) {
      // An explicit id naming an existing run answers with that run only
      // when it is this very run: same DAG, version and scope.
      const existing = await deps.workflows.storage.loadWorkflow(started.workflowId);
      if (!existing || !sameRun({ stored: existing.input, input })) {
        return jsonError(
          409,
          "workflow_id_conflict",
          `Run "${started.workflowId}" exists for another DAG or scope.`,
        );
      }
    }
    return json(202, {
      workflowId: started.workflowId,
      ...(started.existing === true && { existing: true }),
    });
  };
}

/** Whether a stored DAG run input is `input`'s DAG, version and scope. */
function sameRun(params: { stored: unknown; input: DurableDagInput }): boolean {
  const stored = params.stored as Partial<DurableDagInput> | null;
  const { input } = params;
  return (
    stored?.dagId === input.dagId &&
    stored.version === input.version &&
    stored.scope?.namespaceId === input.scope.namespaceId &&
    stored.scope?.resourceId === input.scope.resourceId &&
    stored.scope?.ownerId === input.scope.ownerId
  );
}
