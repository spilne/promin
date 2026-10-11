// ---------------------------------------------------------------------------
// Request scoping shared by every agent route: the `(namespaceId,
// resourceId | ownerId)` parser, the one place a request becomes a scoped
// Agent, thread ownership checks, turn leases and the common error answers.
// ---------------------------------------------------------------------------

import type { Agent, AgentThread, AgentTurnGate, RegisteredAgent } from "@promin/agent";
import {
  composeAgentInstanceId,
  THREAD_AGENT_METADATA_KEY,
  ThreadNotFoundError,
  TurnInProgressError,
} from "@promin/agent";
import { SystemWallClock } from "@promin/workflow";
import { json, jsonError, parseIntParam } from "../../router.ts";
import { requireActiveNamespace } from "../../namespace-guard.ts";
import { internalError } from "../agent-gateway-kit.ts";
import type { AgentGatewayDeps } from "./agent-gateway-deps.ts";

/**
 * Reject invocations on disabled recipes with 410 Gone. Read and
 * housekeeping routes (list threads, messages, trace, rename, archive,
 * metadata edits, versions, clone) still work on disabled agents —
 * operators inspect and re-enable a kill-switched agent.
 */
export function disabled410(recipe: RegisteredAgent): Response | null {
  if (recipe.metadata.enabled === false) {
    return jsonError(
      410,
      "agent_disabled",
      `Agent "${recipe.id}" is disabled. Re-enable it before invoking.`,
    );
  }
  return null;
}

export function agentNotFound(id: string): Response {
  return jsonError(404, "agent_not_found", `Agent "${id}" is not registered.`);
}

export function threadNotFound(): Response {
  return jsonError(404, "thread_not_found");
}

/**
 * Translate a TurnInProgressError into a 409 response with structured
 * body. Returns null when the error is something else.
 */
function turnInProgress409(err: unknown): Response | null {
  if (err instanceof TurnInProgressError) {
    return json(409, {
      error: "turn_in_progress",
      message: err.message,
      currentOwner: err.currentLease.ownerId,
      expiresAt: err.currentLease.expiresAt,
    });
  }
  return null;
}

/** Body shape for invoke / stream / thread send. */
export interface InvokeRequest {
  readonly task?: unknown;
  /** Thread sends only: idempotency key of the turn (see `parseTurnKey`). */
  readonly turnKey?: unknown;
  readonly namespaceId?: unknown;
  readonly resourceId?: unknown;
  readonly ownerId?: unknown;
  readonly metadata?: unknown;
}

export interface ParsedInvoke extends ScopeFields {
  readonly task: string;
}

export interface ScopeFields {
  readonly namespaceId: string;
  readonly resourceId?: string;
  readonly ownerId?: string;
}

/**
 * Parse + validate the `(namespaceId, resourceId | ownerId)` scope
 * triple from any agent-invocation body or thread-route query. The parser
 * rejects input that omits the user-scope field, so a caller can never
 * act "as namespace acme" without saying "for which user" — there's no
 * implicit "default user."
 *
 * Errors:
 *   missing_namespaceId        — namespaceId absent or empty.
 *   missing_scope_identity     — neither resourceId nor ownerId set.
 *   conflicting_identity       — both resourceId and ownerId set.
 *
 * @internal — exported for tests; not part of the public route surface.
 */
export function parseScopeFields(body: {
  readonly namespaceId?: unknown;
  readonly resourceId?: unknown;
  readonly ownerId?: unknown;
}): ScopeFields | { error: string } {
  if (typeof body.namespaceId !== "string" || body.namespaceId.length === 0) {
    return { error: "missing_namespaceId" };
  }
  const resourceId =
    typeof body.resourceId === "string" && body.resourceId.length > 0 ? body.resourceId : undefined;
  const ownerId =
    typeof body.ownerId === "string" && body.ownerId.length > 0 ? body.ownerId : undefined;
  if (resourceId === undefined && ownerId === undefined) {
    return { error: "missing_scope_identity" };
  }
  if (resourceId !== undefined && ownerId !== undefined) {
    return { error: "conflicting_identity" };
  }
  return {
    namespaceId: body.namespaceId,
    ...(resourceId !== undefined && { resourceId }),
    ...(ownerId !== undefined && { ownerId }),
  };
}

export function parseInvokeBody(body: InvokeRequest | null): ParsedInvoke | { error: string } {
  if (!body) return { error: "missing_body" };
  if (typeof body.task !== "string" || body.task.length === 0) {
    return { error: "missing_task" };
  }
  const scope = parseScopeFields(body);
  if ("error" in scope) return scope;
  return { task: body.task, ...scope };
}

/** Longest accepted turn key. */
const MAX_TURN_KEY_LENGTH = 200;

/**
 * Idempotency key of a thread turn: the body's `turnKey`, else the
 * `Idempotency-Key` header. A send retried with the same key on the same
 * thread is the same turn — answered from (or continuing) the first
 * attempt instead of running the model again. Empty when neither is set
 * (every send is a new turn).
 */
export function parseTurnKey(params: {
  readonly req: Request;
  readonly body: { readonly turnKey?: unknown } | null;
}): { turnKey?: string } | { error: string } {
  const raw = params.body?.turnKey ?? params.req.headers.get("idempotency-key") ?? undefined;
  if (raw === undefined) return {};
  if (typeof raw !== "string" || raw.length === 0 || raw.length > MAX_TURN_KEY_LENGTH) {
    return { error: "invalid_turnKey" };
  }
  return { turnKey: raw };
}

/** Scope fields from a GET route's query string. */
export function scopeFromQuery(url: URL): ScopeFields | { error: string } {
  return parseScopeFields({
    namespaceId: url.searchParams.get("namespaceId") ?? undefined,
    resourceId: url.searchParams.get("resourceId") ?? undefined,
    ownerId: url.searchParams.get("ownerId") ?? undefined,
  });
}

// ---------------------------------------------------------------------------
// loadScopedAgent — the one place a request turns into a scoped Agent:
// recipe lookup, kill switch, caller authorization, instance resolution,
// resolve + withScope.
// ---------------------------------------------------------------------------

export interface ScopedAgent {
  readonly recipe: RegisteredAgent;
  readonly agent: Agent;
  readonly namespaceId: string;
  readonly resourceId: string;
  /** Set when the scope was resolved from `ownerId`. */
  readonly instanceId?: string;
}

export async function loadScopedAgent(params: {
  readonly deps: AgentGatewayDeps;
  readonly req: Request;
  readonly id: string;
  readonly scope: ScopeFields;
  /** Invocation routes reject a disabled recipe with 410. */
  readonly invoking: boolean;
}): Promise<ScopedAgent | Response> {
  const { deps, req, id, scope, invoking } = params;
  const recipe = await deps.registry.get(id);
  if (!recipe) return agentNotFound(id);
  if (invoking) {
    const disabled = disabled410(recipe);
    if (disabled) return disabled;
  }
  if (deps.authorizeScope) {
    const allowed = await deps.authorizeScope({ req, scope: { ...scope, agentId: id } });
    if (!allowed) return jsonError(403, "scope_forbidden");
  }
  // Invocations write into the namespace (threads, memory, instances);
  // reads of an unknown or archived namespace just find nothing new.
  if (invoking) {
    await requireActiveNamespace({ registry: deps.namespaces, namespace: scope.namespaceId });
  }

  let resourceId: string;
  let instanceId: string | undefined;
  if (scope.ownerId !== undefined) {
    if (!deps.instanceRegistry) return jsonError(400, "ownerId_unsupported");
    if (invoking) {
      const instance = await deps.instanceRegistry.resolveOrCreate({
        registeredAgentId: id,
        namespaceId: scope.namespaceId,
        ownerId: scope.ownerId,
      });
      instanceId = instance.id;
    } else {
      // Read routes never create an instance row: the deterministic id
      // is enough to address the owner's threads (none, if it has none).
      instanceId = composeAgentInstanceId({
        namespaceId: scope.namespaceId,
        registeredAgentId: id,
        ownerId: scope.ownerId,
      });
    }
    resourceId = instanceId;
  } else {
    resourceId = scope.resourceId!;
  }

  let agent: Agent;
  try {
    const resolved = await deps.resolve(recipe, { namespaceId: scope.namespaceId, resourceId });
    // The owner comes from the authorized request, never from parsing
    // the instance id, so peer delegation runs under the same owner.
    agent = resolved.withScope({
      namespaceId: scope.namespaceId,
      resourceId,
      ...(scope.ownerId !== undefined && { ownerId: scope.ownerId }),
    });
  } catch (err) {
    return internalError({ code: "resolve_failed", err });
  }
  return {
    recipe,
    agent,
    namespaceId: scope.namespaceId,
    resourceId,
    ...(instanceId !== undefined && { instanceId }),
  };
}

/**
 * Open a thread the caller owns. `create: true` creates a missing thread
 * (stamped with this agent's id); `false` requires it to exist. A thread
 * owned by another resource or agent answers like a missing one.
 */
export async function openOwnedThread(params: {
  readonly scoped: ScopedAgent;
  readonly threadId: string;
  readonly create: boolean;
}): Promise<AgentThread | Response> {
  const { scoped, threadId, create } = params;
  const agentId = scoped.recipe.id;
  let thread: AgentThread;
  try {
    thread = await scoped.agent.thread(
      threadId,
      create ? { metadata: { [THREAD_AGENT_METADATA_KEY]: agentId } } : { createIfMissing: false },
    );
  } catch (err) {
    if (err instanceof ThreadNotFoundError) return threadNotFound();
    throw err;
  }
  // Remote backends own their thread storage; the agent stamp
  // lives with local (memory-store) threads only.
  if (scoped.recipe.backend.type === "local" && !thread.isNew) {
    const metadata = await thread.metadata();
    if (metadata[THREAD_AGENT_METADATA_KEY] !== agentId) return threadNotFound();
  }
  return thread;
}

// ---------------------------------------------------------------------------
// Turn leases
// ---------------------------------------------------------------------------

export interface HeldTurnLease {
  /** Release the lease (idempotent) and stop extending it. */
  release(): Promise<void>;
}

/**
 * Take the thread's turn lease when a gate is configured. While held,
 * the lease is extended on the gateway clock every half TTL so a long
 * turn (or stream) never loses it mid-flight. Returns a 409 response on
 * contention.
 */
export async function acquireTurnLease(params: {
  readonly deps: AgentGatewayDeps;
  readonly key: { readonly namespaceId: string; readonly threadId: string };
}): Promise<HeldTurnLease | Response> {
  const { deps, key } = params;
  const gate = deps.turnGate;
  if (!gate) return { release: async () => {} };
  let lease: Awaited<ReturnType<AgentTurnGate["acquire"]>>;
  try {
    lease = await gate.acquire({
      key,
      ownerId: deps.workerId ?? "gateway",
      policy: deps.defaultTurnPolicy ?? "strict",
    });
  } catch (err) {
    const conflict = turnInProgress409(err);
    if (conflict) return conflict;
    throw err;
  }
  const clock = deps.clock ?? SystemWallClock;
  const ttlMs = Math.max(1, lease.expiresAt - lease.acquiredAt);
  const periodMs = Math.max(1, Math.floor(ttlMs / 2));
  const timer = clock.setInterval(() => {
    // A failed extend means the lease already expired or was stolen;
    // the store reports it and the turn simply finishes without it.
    lease.extend(periodMs).catch(() => {});
  }, periodMs);
  timer.unref?.();
  let released = false;
  return {
    release: async () => {
      if (released) return;
      released = true;
      timer.clear();
      await gate.release(lease.leaseId);
    },
  };
}

/** Load the scoped agent and the caller's thread for a read route (query scope). */
export async function openThreadFromQuery(args: {
  readonly deps: AgentGatewayDeps;
  readonly req: Request;
  readonly params: Record<string, string>;
}): Promise<{ thread: AgentThread; url: URL } | Response> {
  const { deps, req, params } = args;
  const id = params.id;
  const threadId = params.threadId;
  if (!id) return jsonError(400, "missing_id");
  if (!threadId) return jsonError(400, "missing_threadId");
  const url = new URL(req.url);
  const scope = scopeFromQuery(url);
  if ("error" in scope) return jsonError(400, scope.error);
  const scoped = await loadScopedAgent({ deps, req, id, scope, invoking: false });
  if (scoped instanceof Response) return scoped;
  const thread = await openOwnedThread({ scoped, threadId, create: false });
  if (thread instanceof Response) return thread;
  return { thread, url };
}

/** Scope + thread for a body-scoped route (rename / archive / distill / compact). */
export async function openThreadFromBody(args: {
  readonly deps: AgentGatewayDeps;
  readonly req: Request;
  readonly params: Record<string, string>;
  readonly body: {
    readonly namespaceId?: unknown;
    readonly resourceId?: unknown;
    readonly ownerId?: unknown;
  };
}): Promise<{ thread: AgentThread; scoped: ScopedAgent } | Response> {
  const { deps, req, params, body } = args;
  const id = params.id;
  const threadId = params.threadId;
  if (!id) return jsonError(400, "missing_id");
  if (!threadId) return jsonError(400, "missing_threadId");
  const scope = parseScopeFields(body);
  if ("error" in scope) return jsonError(400, scope.error);
  const scoped = await loadScopedAgent({ deps, req, id, scope, invoking: false });
  if (scoped instanceof Response) return scoped;
  const thread = await openOwnedThread({ scoped, threadId, create: false });
  if (thread instanceof Response) return thread;
  return { thread, scoped };
}

/** Limit-style query parameter: an integer clamped to [1, 1000]. */
export function parseLimitParam(raw: string | null): number | undefined {
  return parseIntParam({ raw, min: 1, max: 1_000 });
}
