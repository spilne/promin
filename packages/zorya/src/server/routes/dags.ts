// ---------------------------------------------------------------------------
// DAG gateway — operator-facing CRUD + run for AgenticDagRecipe.
//
// Routes (versioning per `versioned-resource.ts`):
//   GET    /api/dags                      — active version per DAG (tag filter,
//                                           `limit` + `cursor` paging)
//   GET    /api/dags/:id                  — fetch (active, or ?version=)
//   GET    /api/dags/:id/versions         — full version history
//   POST   /api/dags                      — register a new DAG (409 when it exists)
//   PATCH  /api/dags/:id                  — new version / kill switch; `If-Match` required
//   POST   /api/dags/:id/promote          — make a version active
//   DELETE /api/dags/:id                  — remove all versions (or ?version=)
//   POST   /api/dags/:id/run              — start a durable run (202 + run id;
//                                           410 while the DAG is disabled)
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
  type DagRecipe,
  type DagRegistry,
  type DurableDagInput,
  type RegisteredDag,
} from "@promin/agent";
import type { NamespaceRegistry } from "@promin/workflow";
import { json, jsonError, parseIntParam, readJson } from "../router.ts";
import { requireActiveNamespace } from "../namespace-guard.ts";
import { WorkflowIdConflictError } from "../services/workflows/errors.ts";
import type { TriggerResult, ZoryaWorkflows } from "../services/workflows/zorya-workflows.ts";
import type { ScopeAuthorizer } from "./agent-gateway-kit.ts";
import { agentNotFound, disabled410, parseScopeFields } from "./agents/agent-scope.ts";
import {
  matchActiveVersion,
  createOnly,
  deleteRoute,
  entryResponse,
  ifMatchVersions,
  listVersionsRoute,
  optionalVersion,
  promoteRoute,
  registryCall,
} from "./versioned-resource.ts";

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
    const limit = parseIntParam({ raw: url.searchParams.get("limit"), min: 1, max: 1_000 });
    const cursor = url.searchParams.get("cursor") ?? undefined;
    const tag = url.searchParams.get("tag") ?? undefined;
    const page = await deps.registry.list({
      ...(tag !== undefined && { filter: { tag } }),
      ...(limit !== undefined && { limit }),
      ...(cursor !== undefined && { cursor }),
    });
    return json(200, { dags: page.items, nextCursor: page.nextCursor });
  };
}

function dagNotFound(id: string): Response {
  return jsonError(404, "dag_not_found", `DAG "${id}" not registered.`);
}

export function getDag(deps: DagGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    if (!id) return jsonError(400, "missing_id");
    const version = new URL(req.url).searchParams.get("version") ?? undefined;
    const dag = await deps.registry.get({ id, ...(version !== undefined && { version }) });
    if (!dag) return dagNotFound(id);
    return entryResponse({ status: 200, entry: dag });
  };
}

export function listDagVersions(deps: DagGatewayDeps) {
  return listVersionsRoute({ registry: deps.registry, notFound: dagNotFound });
}

export function promoteDag(deps: DagGatewayDeps) {
  return promoteRoute({ registry: deps.registry, notFound: dagNotFound });
}

export function deleteDag(deps: DagGatewayDeps) {
  return deleteRoute({ registry: deps.registry, notFound: dagNotFound });
}

/** The graph fields of a request body; validation happens in the registry. */
function dagBody(raw: Record<string, unknown>): DagRecipe {
  return {
    nodes: (raw.nodes ?? []) as DagRecipe["nodes"],
    edges: (raw.edges ?? []) as DagRecipe["edges"],
    entry: (raw.entry ?? []) as DagRecipe["entry"],
    terminals: (raw.terminals ?? []) as DagRecipe["terminals"],
    ...(raw.metadata !== undefined && { metadata: raw.metadata as DagRecipe["metadata"] }),
  };
}

/** A registry write that answers 400 with the issues of an invalid graph. */
async function dagWrite<T>(fn: () => Promise<T>): Promise<T | Response> {
  try {
    return await registryCall(fn);
  } catch (err) {
    if (err instanceof DagValidationError) {
      return json(400, { error: "invalid_dag", issues: err.issues });
    }
    throw err;
  }
}

/** `POST /api/dags` — register a new DAG (409 `dag_exists` when the id is taken). */
export function createDag(deps: DagGatewayDeps) {
  return async (req: Request): Promise<Response> => {
    const body = await readJson<Record<string, unknown>>(req);
    if (!body) return jsonError(400, "missing_body");
    if (typeof body.id !== "string" || body.id.length === 0) {
      return jsonError(400, "missing_id");
    }
    const id = body.id;
    const version = optionalVersion(body.version);
    const created = await dagWrite(() =>
      createOnly({
        register: () =>
          deps.registry.register({
            id,
            ...(version !== undefined && { version }),
            ...dagBody(body),
            expectedActive: null,
          }),
        onExists: () => jsonError(409, "dag_exists", `DAG "${id}" already exists.`),
      }),
    );
    if (created instanceof Response) return created;
    return entryResponse({ status: 201, entry: created });
  };
}

/**
 * `PATCH /api/dags/:id` — `If-Match` required. A graph in the body becomes
 * a new version (`version` names it; `publish: false` keeps it a draft);
 * `enabled` flips the DAG's kill switch.
 */
export function updateDag(deps: DagGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    if (!id) return jsonError(400, "missing_id");
    const body = await readJson<Record<string, unknown>>(req);
    if (!body) return jsonError(400, "missing_body");
    const ifMatch = ifMatchVersions({ req, required: true });
    if (body.enabled !== undefined && typeof body.enabled !== "boolean") {
      return jsonError(400, "invalid_enabled");
    }
    const active = await deps.registry.get({ id });
    if (!active) return dagNotFound(id);
    matchActiveVersion({ tags: ifMatch, active: active.version });

    // One write: the new version and the kill switch land together or not at all.
    const enabled = typeof body.enabled === "boolean" ? body.enabled : undefined;
    let current: RegisteredDag = active;
    if (body.nodes !== undefined) {
      const version = optionalVersion(body.version);
      const written = await dagWrite(() =>
        deps.registry.register({
          id,
          ...(version !== undefined && { version }),
          ...dagBody(body),
          promote: body.publish !== false,
          expectedActive: active.version,
          ...(enabled !== undefined && { enabled }),
        }),
      );
      if (written instanceof Response) return written;
      current = written;
    } else if (enabled !== undefined) {
      current = await registryCall(() =>
        deps.registry.setEnabled({ id, enabled, expectedActive: active.version }),
      );
    }
    return entryResponse({ status: 200, entry: current });
  };
}

export function runDag(deps: DagGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    if (!id) return jsonError(400, "missing_id");
    const body = (await readJson<RunRequest>(req)) ?? {};
    const scope = parseScopeFields(body);
    if ("error" in scope) return jsonError(400, scope.error);
    const dag = await deps.registry.get({
      id,
      ...(body.version !== undefined && { version: body.version }),
    });
    if (!dag) return dagNotFound(id);
    if (!dag.enabled) {
      return jsonError(
        410,
        "dag_disabled",
        `DAG "${id}" is disabled. Re-enable it before running.`,
      );
    }

    // Every node runs in the caller's scope, so the caller must be allowed
    // to invoke each node's agent there — the same checks as a direct
    // invocation. Nodes are re-checked for the kill switch as they run.
    for (const agentId of new Set(dag.nodes.map((n) => n.agentId))) {
      const recipe = await deps.agents.get({ id: agentId });
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
