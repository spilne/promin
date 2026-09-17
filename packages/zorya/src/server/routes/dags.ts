// ---------------------------------------------------------------------------
// DAG gateway — operator-facing CRUD + run for AgenticDagRecipe.
//
// Routes:
//   GET    /api/dags                      — list (latest-per-id)
//   GET    /api/dags/:id                  — fetch (latest, or ?version=)
//   GET    /api/dags/:id/versions         — full version history
//   POST   /api/dags                      — register (or new version)
//   DELETE /api/dags/:id                  — unregister all versions
//   POST   /api/dags/:id/run              — durable execute via the workflow runner
//
// The run endpoint kicks off `createDagWorkflow` against the configured
// agent resolver. v0 returns the synchronous DAG-execution result;
// async + cancellable runs land on top of the workflow runner's
// existing run-cancellation primitives later.
// ---------------------------------------------------------------------------

import {
  createDagWorkflow,
  DagValidationError,
  type AgentResolver,
  type AgenticDagRecipe,
  type DagRegistry,
  type RegisterDagInput,
  type RegisteredDag,
} from "@promin/agent";
import type { WorkflowRunner } from "@promin/workflow";
import { json, jsonError, readJson } from "../router.ts";
import { randomUUID } from "node:crypto";

export interface DagGatewayDeps {
  readonly registry: DagRegistry;
  readonly resolver: AgentResolver;
  readonly runner: WorkflowRunner;
}

interface RunRequest {
  readonly version?: string;
  readonly initialInput?: Readonly<Record<string, unknown>>;
  /**
   * Override the workflowId. Defaults to a fresh UUID — pass an explicit
   * id to make replays idempotent (re-POST with the same id returns the
   * journaled result without re-firing any node activity).
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
    const dag = await deps.registry.get(id, body.version);
    if (!dag) return jsonError(404, "dag_not_found", `DAG "${id}" not registered.`);

    const wf = createDagWorkflow({
      resolver: deps.resolver,
      name: `dag-${dag.id}`,
      type: "agentic-dag",
    });
    const workflowId = body.workflowId ?? `dag-${dag.id}-${randomUUID().slice(0, 8)}`;

    try {
      const result = await deps.runner.run({
        workflow: wf,
        workflowId,
        input: {
          dag: stripStorageMeta(dag),
          initialInput: body.initialInput ?? {},
        },
      });
      return json(200, { workflowId, result });
    } catch (err) {
      return jsonError(500, "run_failed", asMessage(err));
    }
  };
}

/**
 * Strip server-assigned timestamps before passing the recipe to the
 * workflow input — keeps the input deterministic for replay (timestamps
 * would otherwise force non-deterministic input hashing).
 */
function stripStorageMeta(dag: RegisteredDag): AgenticDagRecipe {
  return {
    id: dag.id,
    version: dag.version,
    nodes: dag.nodes,
    edges: dag.edges,
    entry: dag.entry,
    terminals: dag.terminals,
    ...(dag.metadata !== undefined && { metadata: dag.metadata }),
  };
}
