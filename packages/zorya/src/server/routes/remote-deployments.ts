// ---------------------------------------------------------------------------
// Remote-deployment self-registration routes — POST/heartbeat/DELETE/list.
//
// External Zorya deployments call POST /register at startup to expose
// their agents on this host's AgentRegistry as RemoteAgentBackend recipes.
// Heartbeat extends the TTL; DELETE is graceful unregister; expired
// registrations are swept on a clock-driven timer (and on every route
// call), removing their recipes.
//
// Ownership:
//   Every recipe version a deployment writes is tagged
//   `remote-deployment:<id>` and promoted to the agent's active version.
//   Register refuses (409 `agent_id_conflict`) an agent id that already
//   belongs to anything else — a hand-authored or file-scanned recipe, or
//   a live deployment at a different endpoint — so a registration can
//   never shadow or hijack an existing agent. A live deployment at the
//   SAME endpoint (the remote restarted) and an expired one hand the id
//   over. Unregister and expiry delete only the recipe versions still
//   tagged with that deployment.
//
// Outbound safety:
//   The endpoint must pass the `OutboundPolicy` (public http(s) hosts
//   only by default); see `remote-endpoint-policy.ts` for the call-time
//   side. Bearer tokens are stored but never returned:
//   every response redacts them.
// ---------------------------------------------------------------------------

import {
  isRegistryConflict,
  listAllVersioned,
  type AgentRegistry,
  type OutboundPolicy,
  type RegisterDeploymentInput,
  type RegisteredAgent,
  type RegisteredDeployment,
  type RemoteDeploymentRegistry,
} from "@promin/agent";
import { SystemWallClock, type TimerHandle, type WallClock } from "@promin/workflow";
import { json, jsonError, readJson } from "../router.ts";
import { redactDeployment } from "./agent-gateway-kit.ts";
import { checkRemoteEndpoint } from "./remote-endpoint-policy.ts";

/** Tag prefix marking a recipe as written by a remote deployment. */
export const REMOTE_DEPLOYMENT_TAG_PREFIX = "remote-deployment:";

/** Accepted `ttlMs` range on register. */
export const MIN_DEPLOYMENT_TTL_MS = 1_000;
export const MAX_DEPLOYMENT_TTL_MS = 60 * 60 * 1_000;

/** Most agents one deployment may register. */
const MAX_AGENTS_PER_DEPLOYMENT = 100;

/** Agent ids a deployment may claim: no reserved `_` prefix, URL-safe. */
const AGENT_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$/;

export interface RemoteDeploymentsGatewayDeps {
  readonly registry: RemoteDeploymentRegistry;
  readonly agents: AgentRegistry;
  /** Outbound endpoint policy. Default: public http(s) hosts only. */
  readonly endpointPolicy?: OutboundPolicy;
}

interface RegisterRequest {
  readonly endpoint?: unknown;
  readonly agents?: unknown;
  readonly auth?: unknown;
  readonly ttlMs?: unknown;
}

function parseRegisterBody(
  body: RegisterRequest | null,
): RegisterDeploymentInput | { error: string; message?: string } {
  if (!body) return { error: "missing_body" };
  if (typeof body.endpoint !== "string" || body.endpoint.length === 0) {
    return { error: "missing_endpoint" };
  }
  if (!Array.isArray(body.agents) || body.agents.length === 0) {
    return { error: "missing_agents" };
  }
  if (body.agents.length > MAX_AGENTS_PER_DEPLOYMENT) {
    return { error: "too_many_agents", message: `At most ${MAX_AGENTS_PER_DEPLOYMENT} agents.` };
  }
  const agents = new Set<string>();
  for (const a of body.agents) {
    if (typeof a !== "string" || !AGENT_ID_PATTERN.test(a)) return { error: "invalid_agent_id" };
    agents.add(a);
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
  let ttlMs: number | undefined;
  if (body.ttlMs !== undefined) {
    if (
      typeof body.ttlMs !== "number" ||
      !Number.isInteger(body.ttlMs) ||
      body.ttlMs < MIN_DEPLOYMENT_TTL_MS ||
      body.ttlMs > MAX_DEPLOYMENT_TTL_MS
    ) {
      return {
        error: "invalid_ttlMs",
        message: `ttlMs must be an integer in [${MIN_DEPLOYMENT_TTL_MS}, ${MAX_DEPLOYMENT_TTL_MS}].`,
      };
    }
    ttlMs = body.ttlMs;
  }
  return {
    endpoint: body.endpoint,
    agents: [...agents],
    ...(auth !== undefined && { auth }),
    ...(ttlMs !== undefined && { ttlMs }),
  };
}

function ownerTag(deploymentId: string): string {
  return `${REMOTE_DEPLOYMENT_TAG_PREFIX}${deploymentId}`;
}

/** The deployment that wrote `recipe`, or null for any other recipe. */
function recipeOwner(recipe: RegisteredAgent): string | null {
  if (recipe.backend.type !== "remote") return null;
  const tag = recipe.metadata.tags.find((t) => t.startsWith(REMOTE_DEPLOYMENT_TAG_PREFIX));
  return tag ? tag.slice(REMOTE_DEPLOYMENT_TAG_PREFIX.length) : null;
}

/**
 * Agent ids `input` may not claim: ids with a version not written by a
 * deployment, or owned by a live deployment at another endpoint. For the
 * others, the active version this check saw (`null`: none) — the recipe
 * write is conditional on it.
 */
async function claimableIds(
  deps: RemoteDeploymentsGatewayDeps,
  input: RegisterDeploymentInput,
): Promise<{ conflicts: string[]; seen: Map<string, string | null> }> {
  const conflicts: string[] = [];
  const seen = new Map<string, string | null>();
  for (const agentId of input.agents) {
    const versions = await deps.agents.versions({ id: agentId });
    seen.set(agentId, versions.find((v) => v.active)?.version ?? null);
    for (const version of versions) {
      const owner = recipeOwner(version);
      if (owner === null) {
        conflicts.push(agentId);
        break;
      }
      const live = await deps.registry.get(owner);
      if (live && live.endpoint !== input.endpoint) {
        conflicts.push(agentId);
        break;
      }
    }
  }
  return { conflicts, seen };
}

/**
 * Write a RemoteAgentBackend recipe per declared agent, tagged with its
 * owner, as the agent's active version — each write conditional on the
 * active version the ownership check saw, so an agent created or changed
 * in between (a hand-authored recipe, another deployment's claim) is never
 * overwritten. Returns the ids that lost that race.
 */
async function upsertRecipes(params: {
  agents: AgentRegistry;
  deployment: RegisteredDeployment;
  seen: ReadonlyMap<string, string | null>;
}): Promise<string[]> {
  const { agents, deployment, seen } = params;
  const lost: string[] = [];
  for (const agentId of deployment.agents) {
    try {
      await registerRecipe({
        agents,
        deployment,
        agentId,
        expectedActive: seen.get(agentId) ?? null,
      });
    } catch (err) {
      if (!isRegistryConflict(err) || err.code !== "active_mismatch") throw err;
      lost.push(agentId);
    }
  }
  return lost;
}

async function registerRecipe(params: {
  agents: AgentRegistry;
  deployment: RegisteredDeployment;
  agentId: string;
  expectedActive: string | null;
}): Promise<void> {
  const { agents, deployment, agentId } = params;
  await agents.register({
    id: agentId,
    promote: true,
    expectedActive: params.expectedActive,
    backend: {
      type: "remote",
      endpoint: deployment.endpoint,
      remoteAgentId: agentId,
      ...(deployment.auth !== undefined && { auth: deployment.auth }),
    },
    metadata: {
      description: `Auto-registered from ${deployment.endpoint} (deployment ${deployment.deploymentId})`,
      capabilities: [],
      tags: ["remote", "auto-registered", ownerTag(deployment.deploymentId)],
    },
  });
}

/** Delete the recipe versions `deployment` still owns; others are left alone. */
async function deleteOwnedRecipes(
  agents: AgentRegistry,
  deployment: RegisteredDeployment,
): Promise<void> {
  for (const agentId of deployment.agents) {
    for (const version of await agents.versions({ id: agentId })) {
      if (recipeOwner(version) === deployment.deploymentId) {
        await agents.unregister({ id: agentId, version: version.version });
      }
    }
  }
}

/**
 * Delete every recipe version written by a deployment that is no longer
 * registered — left behind when the registry lost its rows (an in-memory
 * registry across a restart) or a process died between removing a
 * deployment and its recipes. Returns how many versions were removed.
 */
export async function sweepOrphanedRemoteRecipes(
  deps: RemoteDeploymentsGatewayDeps,
): Promise<number> {
  const live = new Map<string, boolean>();
  let removed = 0;
  // Any version may be owned, not just the active one — so every agent.
  for (const { id } of await listAllVersioned({ registry: deps.agents })) {
    for (const version of await deps.agents.versions({ id })) {
      const owner = recipeOwner(version);
      if (owner === null) continue;
      if (!live.has(owner)) live.set(owner, (await deps.registry.get(owner)) !== null);
      if (live.get(owner)) continue;
      if (await deps.agents.unregister({ id, version: version.version })) removed += 1;
    }
  }
  return removed;
}

/** Expire stale registrations and remove their recipes. */
export async function sweepExpiredDeployments(deps: RemoteDeploymentsGatewayDeps): Promise<number> {
  const expired = await deps.registry.expireStale();
  for (const dep of expired) await deleteOwnedRecipes(deps.agents, dep);
  return expired.length;
}

export interface RegisterDeploymentResponse {
  readonly deploymentId: string;
  readonly ttlMs: number;
  readonly heartbeatPath: string;
  readonly unregisterPath: string;
}

export function registerDeployment(deps: RemoteDeploymentsGatewayDeps) {
  return async (req: Request): Promise<Response> => {
    const parsed = parseRegisterBody(await readJson<RegisterRequest>(req));
    if ("error" in parsed) return jsonError(400, parsed.error, parsed.message);

    const endpoint = await checkRemoteEndpoint({
      raw: parsed.endpoint,
      ...(deps.endpointPolicy !== undefined && { policy: deps.endpointPolicy }),
    });
    if (!endpoint.ok) return jsonError(400, endpoint.error, endpoint.message);
    const input: RegisterDeploymentInput = { ...parsed, endpoint: endpoint.url };

    await sweepExpiredDeployments(deps);
    const claim = await claimableIds(deps, input);
    const conflict409 = (agents: string[]) =>
      json(409, {
        error: "agent_id_conflict",
        message: "These agent ids already belong to another agent or deployment.",
        agents,
      });
    if (claim.conflicts.length > 0) return conflict409(claim.conflicts);

    const deployment = await deps.registry.register(input);
    const rollback = async () => {
      await deps.registry.unregister(deployment.deploymentId).catch(() => {});
      await deleteOwnedRecipes(deps.agents, deployment).catch(() => {});
    };
    let lost: string[];
    try {
      lost = await upsertRecipes({ agents: deps.agents, deployment, seen: claim.seen });
    } catch (err) {
      await rollback();
      throw err;
    }
    if (lost.length > 0) {
      // Another writer took these ids after the check: no partial claim.
      await rollback();
      return conflict409(lost);
    }
    const id = encodeURIComponent(deployment.deploymentId);
    const response: RegisterDeploymentResponse = {
      deploymentId: deployment.deploymentId,
      ttlMs: deployment.ttlMs,
      heartbeatPath: `/api/remote-deployments/${id}/heartbeat`,
      unregisterPath: `/api/remote-deployments/${id}`,
    };
    return json(201, response);
  };
}

export function heartbeatDeployment(deps: RemoteDeploymentsGatewayDeps) {
  return async (_req: Request, params: Record<string, string>): Promise<Response> => {
    const deploymentId = params.deploymentId;
    if (!deploymentId) return jsonError(400, "missing_deploymentId");
    // Sweep first: an expired registration is removed (with its recipes)
    // rather than revived.
    await sweepExpiredDeployments(deps);
    const updated = await deps.registry.heartbeat(deploymentId);
    if (!updated) {
      // 410 Gone so the client distinguishes "re-register" from a transient 5xx.
      return jsonError(
        410,
        "deployment_not_found",
        `Deployment ${deploymentId} is unknown or expired — re-register.`,
      );
    }
    return json(200, {
      deploymentId: updated.deploymentId,
      ttlMs: updated.ttlMs,
      lastHeartbeat: updated.lastHeartbeat,
    });
  };
}

export function unregisterDeployment(deps: RemoteDeploymentsGatewayDeps) {
  return async (_req: Request, params: Record<string, string>): Promise<Response> => {
    const deploymentId = params.deploymentId;
    if (!deploymentId) return jsonError(400, "missing_deploymentId");
    const removed = await deps.registry.unregister(deploymentId);
    if (removed) await deleteOwnedRecipes(deps.agents, removed);
    // Idempotent — 204 even when the deployment was already gone.
    return new Response(null, { status: 204 });
  };
}

export interface ListDeploymentsResponse {
  /** Live deployments; `auth.token` is always redacted. */
  readonly deployments: ReadonlyArray<RegisteredDeployment>;
}

export function listDeployments(deps: RemoteDeploymentsGatewayDeps) {
  return async (): Promise<Response> => {
    await sweepExpiredDeployments(deps);
    const deployments = (await deps.registry.list()).map(redactDeployment);
    return json(200, { deployments } satisfies ListDeploymentsResponse);
  };
}

// ---------------------------------------------------------------------------
// Expiry sweep loop
// ---------------------------------------------------------------------------

export interface RemoteDeploymentSweeperConfig extends RemoteDeploymentsGatewayDeps {
  /** Sweep period. Default: 15s. */
  readonly intervalMs?: number;
  /** Timer source. Default: `SystemWallClock`. */
  readonly clock?: WallClock;
  /** Sweep failures land here. Default: console. */
  readonly onError?: (err: unknown) => void;
}

/**
 * Periodic expiry of stale deployments, so a remote that died without
 * unregistering stops receiving traffic within one sweep period even
 * when nobody opens the deployments page. `start()` first runs a boot
 * sweep that also removes recipes whose deployment is gone.
 */
export class RemoteDeploymentSweeper {
  private readonly deps: RemoteDeploymentsGatewayDeps;
  private readonly intervalMs: number;
  private readonly clock: WallClock;
  private readonly onError: (err: unknown) => void;
  private timer: TimerHandle | null = null;
  private running = false;

  constructor(config: RemoteDeploymentSweeperConfig) {
    this.deps = config;
    this.intervalMs = config.intervalMs ?? 15_000;
    this.clock = config.clock ?? SystemWallClock;
    this.onError =
      config.onError ?? ((err) => console.error("[zorya] remote-deployment sweep failed", err));
  }

  start(): void {
    if (this.timer) return;
    this.timer = this.clock.setInterval(() => void this.tick(), this.intervalMs);
    this.timer.unref?.();
    void this.bootSweep();
  }

  /**
   * Expire stale deployments, then remove every recipe whose deployment is
   * gone: a restart may have lost registrations (an in-memory registry) or
   * died between removing a deployment and its recipes.
   */
  async bootSweep(): Promise<void> {
    await this.guarded(async () => {
      await sweepExpiredDeployments(this.deps);
      await sweepOrphanedRemoteRecipes(this.deps);
    });
  }

  stop(): void {
    this.timer?.clear();
    this.timer = null;
  }

  /** One expiry sweep; overlapping sweeps are skipped. */
  async tick(): Promise<void> {
    await this.guarded(() => sweepExpiredDeployments(this.deps));
  }

  private async guarded(fn: () => Promise<unknown>): Promise<void> {
    if (this.running) return;
    this.running = true;
    try {
      await fn();
    } catch (err) {
      this.onError(err);
    } finally {
      this.running = false;
    }
  }
}
