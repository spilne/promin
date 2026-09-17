// ---------------------------------------------------------------------------
// Remote-deployment self-registration routes — POST/heartbeat/DELETE/list.
//
// External Zorya deployments call POST /register at startup to expose
// their RemoteAgentBackend recipes on this host's AgentRegistry.
// Heartbeat extends the TTL; DELETE is graceful unregister; a periodic
// sweep prunes stale registrations.
//
// Side-effects on AgentRegistry:
//   - register   → upsert RemoteAgentBackend recipe per declared agent id
//   - unregister → delete those recipes
//   - sweep      → delete the recipes of expired registrations
//
// Phase 1 scope: in-memory registry default; sweep runs on each list()
// call (lazy GC) so a real production deploy can layer a periodic loop
// on top via the existing scheduler pattern. Persistent backends
// (Sqlite / Postgres) and an explicit sweep loop are tracked as
// follow-ups.
// ---------------------------------------------------------------------------

import type {
  AgentRegistry,
  RegisterDeploymentInput,
  RegisteredDeployment,
  RemoteDeploymentRegistry,
} from "@promin/agent";
import { json, jsonError, readJson } from "../router.ts";

export interface RemoteDeploymentsGatewayDeps {
  readonly registry: RemoteDeploymentRegistry;
  readonly agents: AgentRegistry;
  /** Optional clock for tests. Defaults to `Date.now`. */
  readonly now?: () => number;
}

interface RegisterRequest {
  readonly endpoint?: unknown;
  readonly agents?: unknown;
  readonly auth?: unknown;
  readonly ttlMs?: unknown;
}

function parseRegisterBody(
  body: RegisterRequest | null,
): RegisterDeploymentInput | { error: string } {
  if (!body) return { error: "missing_body" };
  if (typeof body.endpoint !== "string" || body.endpoint.length === 0) {
    return { error: "missing_endpoint" };
  }
  if (!Array.isArray(body.agents) || body.agents.length === 0) {
    return { error: "missing_agents" };
  }
  const agents: string[] = [];
  for (const a of body.agents) {
    if (typeof a !== "string" || a.length === 0) return { error: "invalid_agent_id" };
    agents.push(a);
  }
  let auth: RegisterDeploymentInput["auth"] | undefined;
  if (body.auth !== undefined && body.auth !== null) {
    if (typeof body.auth !== "object") return { error: "invalid_auth" };
    const authObj = body.auth as { kind?: unknown; token?: unknown };
    if (
      authObj.kind !== "bearer" ||
      typeof authObj.token !== "string" ||
      authObj.token.length === 0
    ) {
      return { error: "invalid_auth" };
    }
    auth = { kind: "bearer", token: authObj.token };
  }
  const ttlMs = typeof body.ttlMs === "number" && body.ttlMs > 0 ? body.ttlMs : undefined;
  return {
    endpoint: body.endpoint,
    agents,
    ...(auth !== undefined && { auth }),
    ...(ttlMs !== undefined && { ttlMs }),
  };
}

/**
 * Upsert a RemoteAgentBackend recipe for each declared agent id,
 * pointing at this deployment's endpoint. Idempotent — the AgentRegistry
 * register() upserts on (id, version) so re-running is a no-op.
 */
async function upsertRecipes(
  agents: AgentRegistry,
  deployment: RegisteredDeployment,
): Promise<void> {
  for (const agentId of deployment.agents) {
    await agents.register({
      id: agentId,
      backend: {
        type: "remote",
        endpoint: deployment.endpoint,
        remoteAgentId: agentId,
        ...(deployment.auth !== undefined && { auth: deployment.auth }),
      },
      metadata: {
        description: `Auto-registered from ${deployment.endpoint} (deployment ${deployment.deploymentId})`,
        capabilities: [],
        tags: ["remote", "auto-registered"],
      },
    });
  }
}

/**
 * Delete the recipes that were created for a deployment. Best-effort —
 * a missing recipe (already deleted, manually unregistered) is fine.
 */
async function deleteRecipes(
  agents: AgentRegistry,
  deployment: RegisteredDeployment,
): Promise<void> {
  for (const agentId of deployment.agents) {
    await agents.unregister(agentId).catch(() => {});
  }
}

export interface RegisterDeploymentResponse {
  readonly deploymentId: string;
  readonly ttlMs: number;
  readonly heartbeatPath: string;
  readonly unregisterPath: string;
}

export function registerDeployment(deps: RemoteDeploymentsGatewayDeps) {
  return async (req: Request): Promise<Response> => {
    const body = await readJson<RegisterRequest>(req);
    const parsed = parseRegisterBody(body);
    if ("error" in parsed) return jsonError(400, parsed.error);

    try {
      const deployment = await deps.registry.register(parsed);
      await upsertRecipes(deps.agents, deployment);
      const response: RegisterDeploymentResponse = {
        deploymentId: deployment.deploymentId,
        ttlMs: deployment.ttlMs,
        heartbeatPath: `/api/remote-deployments/${encodeURIComponent(deployment.deploymentId)}/heartbeat`,
        unregisterPath: `/api/remote-deployments/${encodeURIComponent(deployment.deploymentId)}`,
      };
      return json(201, response);
    } catch (err) {
      return jsonError(500, "register_failed", err instanceof Error ? err.message : String(err));
    }
  };
}

export function heartbeatDeployment(deps: RemoteDeploymentsGatewayDeps) {
  return async (_req: Request, params: Record<string, string>): Promise<Response> => {
    const deploymentId = params.deploymentId;
    if (!deploymentId) return jsonError(400, "missing_deploymentId");
    try {
      const updated = await deps.registry.heartbeat(deploymentId);
      if (!updated) {
        // Caller should re-register. Surface 410 Gone so the client
        // distinguishes 'unknown id' from a transient 5xx.
        return jsonError(
          410,
          "deployment_not_found",
          `Deployment ${deploymentId} is unknown — re-register.`,
        );
      }
      return json(200, {
        deploymentId: updated.deploymentId,
        ttlMs: updated.ttlMs,
        lastHeartbeat: updated.lastHeartbeat,
      });
    } catch (err) {
      return jsonError(500, "heartbeat_failed", err instanceof Error ? err.message : String(err));
    }
  };
}

export function unregisterDeployment(deps: RemoteDeploymentsGatewayDeps) {
  return async (_req: Request, params: Record<string, string>): Promise<Response> => {
    const deploymentId = params.deploymentId;
    if (!deploymentId) return jsonError(400, "missing_deploymentId");
    try {
      const removed = await deps.registry.unregister(deploymentId);
      if (removed) {
        await deleteRecipes(deps.agents, removed);
      }
      // Idempotent — 204 even when the deployment was already gone.
      return new Response(null, { status: 204 });
    } catch (err) {
      return jsonError(500, "unregister_failed", err instanceof Error ? err.message : String(err));
    }
  };
}

export interface ListDeploymentsResponse {
  readonly deployments: ReadonlyArray<RegisteredDeployment>;
}

export function listDeployments(deps: RemoteDeploymentsGatewayDeps) {
  return async (): Promise<Response> => {
    try {
      // Lazy sweep: every list() call expires stale registrations and
      // cleans up their recipes. A periodic loop in production
      // deployments would replace this; for Phase 1 single-process
      // demos, lazy GC is enough.
      const now = (deps.now ?? (() => Date.now()))();
      const expired = await deps.registry.expireStale({ now });
      for (const dep of expired) {
        await deleteRecipes(deps.agents, dep);
      }
      const deployments = await deps.registry.list();
      const response: ListDeploymentsResponse = { deployments };
      return json(200, response);
    } catch (err) {
      return jsonError(500, "list_failed", err instanceof Error ? err.message : String(err));
    }
  };
}
