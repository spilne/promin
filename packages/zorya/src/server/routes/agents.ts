// ---------------------------------------------------------------------------
// Agent gateway routes — single entry point external apps call to invoke a
// registered agent.
//
// Routes:
//   GET  /api/agents                          — list registered agents
//   GET  /api/agents/:id                      — get one (active version)
//   POST /api/agents/:id/invoke               — one-shot invocation
//   POST /api/agents/:id/stream               — one-shot, SSE response
//   POST /api/agents/:id/threads/:threadId    — conversational turn (one-shot per call)
//   POST /api/agents/:id/threads/:threadId/stream — conversational, SSE
//   GET  /api/agents/:id/threads              — list the caller's threads
//   GET  /api/agents/:id/threads/:threadId/messages — read message history
//
// Scope binding:
//   Every invocation AND every thread route requires `namespaceId` and
//   exactly ONE OF `resourceId | ownerId` (body for POST/PATCH, query for
//   GET). `resourceId` = raw scope key; `ownerId` = "resolve via
//   AgentInstance, use its id as the scope key". Both at once is rejected
//   (`conflicting_identity`); neither is rejected (`missing_scope_identity`).
//   Use the exported `AgentInvokeBody` discriminated-union type to get the
//   constraint enforced at compile time.
//
// Thread ownership:
//   A thread belongs to the (namespace, resource, agent) that created it.
//   The resource half is enforced by the agent backend (`LocalAgent.thread`
//   refuses a thread owned by another resource); the agent half by the
//   gateway, which stamps `metadata.agentId` when a thread is created and
//   only lists / opens threads carrying its own agent id. A thread owned by
//   someone else answers exactly like a missing one (404), so ids can't be
//   probed. Hosts that authenticate end users bind the requested scope to
//   the caller with `authorizeScope` (403 on mismatch).
//
// Resolver injection:
//   The route doesn't know how to construct an Agent from a recipe — that's
//   the host's job. Pass `resolve(recipe) → Agent` in `AgentGatewayDeps`.
//   For LocalAgent backends, that's `(r) => resolveLocalAgent(r, deps)`.
//
// Errors:
//   Expected outcomes map to 4xx with a stable `error` code. Unexpected
//   failures answer `{ error: <code>, requestId }` with status 500; the
//   detail is logged under the request id and never sent to the caller.
// ---------------------------------------------------------------------------

import type {
  Agent,
  AgentInstanceRegistry,
  AgentRegistry,
  AgentRunOutput,
  AgentThread,
  AgentThreadSummary,
  AgentTurnGate,
  AgentTurnPolicy,
  Message,
  RegisteredAgent,
  RoleRegistry,
  SecretsStorage,
} from "@promin/agent";
import {
  buildAgentTrace,
  composeAgentInstanceId,
  ConsolidatorRateLimitError,
  NothingToConsolidateError,
  SecretScope,
  ThreadNotFoundError,
  TurnInProgressError,
} from "@promin/agent";
import { SystemWallClock, WorkflowSuspendedError, type WallClock } from "@promin/workflow";
import { json, jsonError, parseIntParam, readJson } from "../router.ts";
import {
  internalError,
  mergeAgentMetadata,
  parseBackend,
  parseMetadataPatch,
  redactRecipe,
  restoreRedactedSecrets,
  type ScopeAuthorizer,
} from "./agent-gateway-kit.ts";
import { isDraftId } from "./agent-drafts.ts";
import { SECRET_KEY_PATTERN, SECRET_MAX_VALUE_BYTES } from "./secrets.ts";

// ---------------------------------------------------------------------------
// Response DTOs — UI imports these for typing API responses. Re-export the
// underlying agent types so the UI bundle doesn't import @promin/agent
// directly (keeps the import surface narrow and matches schedules.ts).
// `ThreadSummary` here is the agent-facing shape (with `id`), not the
// memory-store shape (which uses `threadId`).
// ---------------------------------------------------------------------------

export type { AgentThreadSummary as ThreadSummary, RegisteredAgent, Message } from "@promin/agent";

export interface AgentsListResponse {
  agents: RegisteredAgent[];
}

export interface AgentThreadsResponse {
  threads: AgentThreadSummary[];
}

export interface ThreadMessagesResponse {
  threadId: string;
  messages: Message[];
}

export interface RenameThreadResponse {
  threadId: string;
  title: string | null;
  metadata: Record<string, unknown>;
}

export interface InvokeResponse {
  text: string;
  finishReason: string;
  usage: {
    inputTokens: number;
    outputTokens: number;
    cacheReadTokens?: number;
    cacheWriteTokens?: number;
  };
  /** Set when the gateway resolved an AgentInstance from `ownerId`. */
  instanceId?: string;
}

export interface ThreadInvokeResponse extends InvokeResponse {
  threadId: string;
  isNew: boolean;
}

/**
 * Thread-metadata key the gateway stamps with the owning agent's id when
 * it creates a thread. Listing filters on it and every thread route
 * checks it, so one agent's threads are never reachable through another.
 */
export const THREAD_AGENT_METADATA_KEY = "agentId";

export interface AgentGatewayDeps {
  readonly registry: AgentRegistry;
  /**
   * Optional role registry. When set, `extract-role` can lift an agent's
   * inline role into it (and rebind the agent to a ref). Agent resolution
   * of `ref` bindings happens in the host's `resolve` callback, which is
   * wired with the same registry.
   */
  readonly roles?: RoleRegistry;
  /**
   * Materialize a live `Agent` from a registered recipe. The gateway
   * calls this per request, then `.withScope()` for tenancy. The
   * result is unscoped — the gateway handles tenant scoping itself.
   *
   * Optional `scope` carries the per-request namespace + resource so
   * the host can resolve recipe-level credentialRefs (BYOK) at this
   * boundary.
   *
   * Returns `Agent` synchronously OR `Promise<Agent>` when the host
   * does async secret resolution (the typical BYOK case).
   */
  readonly resolve: (
    recipe: RegisteredAgent,
    scope?: { readonly namespaceId?: string; readonly resourceId?: string },
  ) => Agent | Promise<Agent>;
  /**
   * Optional. When set, callers may pass `ownerId` instead of
   * `resourceId`. The gateway resolves-or-creates an AgentInstance via
   * this registry (read routes only compute its id) and uses
   * `instance.id` as the resourceId. The response carries the resolved
   * `instanceId` so clients can skip the lookup on subsequent calls.
   *
   * When unset, `ownerId` in the body is rejected — the server can't
   * honor the implied semantics without somewhere to record the row.
   */
  readonly instanceRegistry?: AgentInstanceRegistry;
  /**
   * Optional per-thread coordination gate. Wraps thread-bound routes
   * (`POST /threads/:threadId`, `POST /threads/:threadId/stream`,
   * `POST /threads/:threadId/approve`) so two replicas processing the
   * same conversation atomically: only one runs the turn at a time;
   * concurrent attempts are rejected with 409 Conflict + current owner.
   * The lease is extended on the gateway clock while a turn runs and
   * released when it ends, fails, or the client disconnects.
   *
   * When unset, no coordination is enforced — fine for single-process
   * deployments. Multi-replica deployments (sharing a PgMemoryStore)
   * should always set this.
   */
  readonly turnGate?: AgentTurnGate;
  /**
   * Identifier for THIS gateway process. Used as `ownerId` when the
   * gate acquires a lease, so 409 responses can name which replica
   * holds the conversation. Default: `"gateway"`.
   */
  readonly workerId?: string;
  /**
   * Default policy when contention happens. Today only 'strict' is
   * implemented. Default: 'strict'.
   */
  readonly defaultTurnPolicy?: AgentTurnPolicy;
  /**
   * Optional scoped secrets vault. When set, the clone endpoint
   * persists secrets supplied in the request body at the requested
   * scope; agent invocations resolve `model.credentialRef` against
   * this vault (BYOK). Without it, a clone request carrying secrets is
   * rejected with 400 `secrets_unsupported`.
   */
  readonly secrets?: SecretsStorage;
  /**
   * Agent ids backed by a recipe file on disk. Edits and deletes of
   * those ids are refused (409 `file_managed`): the next scan tick would
   * silently overwrite them.
   */
  readonly fileManagedIds?: () => ReadonlyArray<string>;
  /** Binds a request's scope to its authenticated caller. See `ScopeAuthorizer`. */
  readonly authorizeScope?: ScopeAuthorizer;
  /**
   * Time source for the distill rate-limit `retry-after`, the default
   * thread `archivedAt` stamp and turn-lease extension.
   * Default: `SystemWallClock`.
   */
  readonly clock?: WallClock;
}

/**
 * Reject invocations on disabled recipes with 410 Gone. Read and
 * housekeeping routes (list threads, messages, trace, rename, archive,
 * metadata edits, versions, clone) still work on disabled agents —
 * operators inspect and re-enable a kill-switched agent.
 */
function disabled410(recipe: RegisteredAgent): Response | null {
  if (recipe.metadata.enabled === false) {
    return jsonError(
      410,
      "agent_disabled",
      `Agent "${recipe.id}" is disabled. Re-enable it before invoking.`,
    );
  }
  return null;
}

function agentNotFound(id: string): Response {
  return jsonError(404, "agent_not_found", `Agent "${id}" is not registered.`);
}

function threadNotFound(): Response {
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
interface InvokeRequest {
  readonly task?: unknown;
  readonly namespaceId?: unknown;
  readonly resourceId?: unknown;
  readonly ownerId?: unknown;
  readonly metadata?: unknown;
}

interface ParsedInvoke extends ScopeFields {
  readonly task: string;
}

/**
 * Typed shape callers can use to construct an invoke body with the
 * `(namespaceId, resourceId | ownerId)` constraint enforced at the type
 * level. The discriminated union refuses bodies that omit both
 * identity-scope fields — TypeScript clients catch the mistake at
 * compile time; the runtime parser catches anyone bypassing TS.
 *
 * ```ts
 * const body: AgentInvokeBody = {
 *   task: "summarize",
 *   namespaceId: "acme",
 *   ownerId: "u-9",   // OR resourceId; never both, never neither
 * };
 * ```
 */
export type AgentInvokeBody =
  | {
      readonly task: string;
      readonly namespaceId: string;
      readonly resourceId: string;
      readonly ownerId?: never;
      readonly metadata?: Record<string, unknown>;
    }
  | {
      readonly task: string;
      readonly namespaceId: string;
      readonly resourceId?: never;
      readonly ownerId: string;
      readonly metadata?: Record<string, unknown>;
    };

interface ScopeFields {
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

function parseInvokeBody(body: InvokeRequest | null): ParsedInvoke | { error: string } {
  if (!body) return { error: "missing_body" };
  if (typeof body.task !== "string" || body.task.length === 0) {
    return { error: "missing_task" };
  }
  const scope = parseScopeFields(body);
  if ("error" in scope) return scope;
  return { task: body.task, ...scope };
}

/** Scope fields from a GET route's query string. */
function scopeFromQuery(url: URL): ScopeFields | { error: string } {
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

interface ScopedAgent {
  readonly recipe: RegisteredAgent;
  readonly agent: Agent;
  readonly namespaceId: string;
  readonly resourceId: string;
  /** Set when the scope was resolved from `ownerId`. */
  readonly instanceId?: string;
}

async function loadScopedAgent(
  deps: AgentGatewayDeps,
  params: {
    readonly req: Request;
    readonly id: string;
    readonly scope: ScopeFields;
    /** Invocation routes reject a disabled recipe with 410. */
    readonly invoking: boolean;
  },
): Promise<ScopedAgent | Response> {
  const { req, id, scope, invoking } = params;
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
    agent = resolved.withScope({ namespaceId: scope.namespaceId, resourceId });
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
async function openOwnedThread(params: {
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
  // Remote and cursor backends own their thread storage; the agent stamp
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

interface HeldTurnLease {
  /** Release the lease (idempotent) and stop extending it. */
  release(): Promise<void>;
}

/**
 * Take the thread's turn lease when a gate is configured. While held,
 * the lease is extended on the gateway clock every half TTL so a long
 * turn (or stream) never loses it mid-flight. Returns a 409 response on
 * contention.
 */
async function acquireTurnLease(
  deps: AgentGatewayDeps,
  key: { readonly namespaceId: string; readonly threadId: string },
): Promise<HeldTurnLease | Response> {
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

// ---------------------------------------------------------------------------
// Discovery — list + get
// ---------------------------------------------------------------------------

export interface AgentSourcesResponse {
  /** Agent ids currently backed by a recipe file on disk — read-only in the UI. */
  fileManaged: string[];
}

/**
 * Report which agent recipes are file-managed (scanned from disk) vs
 * operator-authored. The editor uses this to disable in-place Save on a
 * file-scanned recipe; the server refuses such edits as well.
 */
export function listAgentSources(deps: { readonly fileManagedIds: () => string[] }) {
  return async (): Promise<Response> => {
    return json(200, { fileManaged: deps.fileManagedIds() } satisfies AgentSourcesResponse);
  };
}

export function listAgents(deps: AgentGatewayDeps) {
  return async (req: Request): Promise<Response> => {
    const url = new URL(req.url);
    const capability = url.searchParams.get("capability") ?? undefined;
    const tag = url.searchParams.get("tag") ?? undefined;
    const limit = parseLimitParam(url.searchParams.get("limit"));
    const cursor = url.searchParams.get("cursor") ?? undefined;
    const agents = await deps.registry.list({ capability, tag, limit, cursor });
    // Hide ephemeral Designer drafts — they're not real recipes and
    // shouldn't clutter the agent list.
    return json(200, {
      agents: agents.filter((a) => !isDraftId(a.id)).map(redactRecipe),
    } satisfies AgentsListResponse);
  };
}

/** Weak ETag for a recipe row — its `updatedAt`. */
function recipeEtag(recipe: RegisteredAgent): string {
  return `W/"${recipe.version}:${recipe.updatedAt}"`;
}

function recipeResponse(status: number, recipe: RegisteredAgent): Response {
  const res = json(status, redactRecipe(recipe));
  res.headers.set("etag", recipeEtag(recipe));
  return res;
}

export function getAgent(deps: AgentGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    if (!id) return jsonError(400, "missing_id");
    const url = new URL(req.url);
    const version = url.searchParams.get("version") ?? undefined;
    const recipe = await deps.registry.get(id, version);
    if (!recipe) return agentNotFound(id);
    return recipeResponse(200, recipe);
  };
}

// ---------------------------------------------------------------------------
// Recipe CRUD — author / edit / clone agents through HTTP.
//
// Semantics:
//   - POST /api/agents is create-only: 409 when (id, version) exists.
//     Publishing a new version of an existing id is a POST with a new
//     `version`; that version becomes the active one.
//   - PATCH edits the active version in place. Editing an older version
//     is refused (409 `version_not_active`) so an edit can never change
//     which version is live. Metadata merges field by field; `If-Match`
//     (the GET `etag`) makes the edit conditional (412 on mismatch).
//   - Clone is create-only on the target id.
// ---------------------------------------------------------------------------

interface CreateAgentRequest {
  readonly id?: unknown;
  readonly version?: unknown;
  readonly backend?: unknown;
  readonly metadata?: unknown;
}

interface UpdateAgentRequest {
  readonly version?: unknown; // version to update; must be the active one
  readonly backend?: unknown; // replaced wholesale when present
  readonly metadata?: unknown; // merged field by field when present
}

interface CloneAgentRequest {
  readonly targetId?: unknown;
  readonly targetVersion?: unknown;
  /**
   * Optional secrets to provision alongside the clone, stored in
   * SecretsStorage at `secretsScope`. Keys follow the `/api/secrets`
   * rules. Requires a configured SecretsStorage (else 400).
   */
  readonly secrets?: unknown;
  /**
   * Where to store the supplied `secrets`. Three shapes match
   * SecretScope:
   *   { kind: 'global' }                            — host-wide
   *   { kind: 'namespace', namespaceId }            — tenant-scope
   *   { kind: 'resource', namespaceId, resourceId } — per-user
   * Default: global.
   */
  readonly secretsScope?: unknown;
}

function isFileManaged(deps: AgentGatewayDeps, id: string): boolean {
  return deps.fileManagedIds?.().includes(id) ?? false;
}

function fileManaged409(id: string): Response {
  return jsonError(
    409,
    "file_managed",
    `Agent "${id}" is managed by a recipe file; edit the file instead.`,
  );
}

function optionalString(raw: unknown): string | undefined {
  return typeof raw === "string" && raw.length > 0 ? raw : undefined;
}

export function createAgent(deps: AgentGatewayDeps) {
  return async (req: Request): Promise<Response> => {
    const body = await readJson<CreateAgentRequest>(req);
    if (!body) return jsonError(400, "missing_body");
    if (typeof body.id !== "string" || body.id.length === 0) {
      return jsonError(400, "missing_id");
    }
    if (body.id.startsWith("_")) {
      // The `_catalog/*` URL prefix is reserved; an agent named `_x`
      // would shadow it once the route has the recipe in registry.
      return jsonError(400, "reserved_id_prefix", "Agent ids starting with `_` are reserved.");
    }
    if (isFileManaged(deps, body.id)) return fileManaged409(body.id);
    if (body.backend === undefined || body.backend === null) {
      return jsonError(400, "missing_backend");
    }
    const backend = parseBackend(body.backend);
    if ("error" in backend) return jsonError(400, backend.error, backend.message);
    const restored = restoreRedactedSecrets({ next: backend.backend });
    if ("error" in restored) return jsonError(400, restored.error);
    let metadata: ReturnType<typeof mergeAgentMetadata> | undefined;
    if (body.metadata !== undefined && body.metadata !== null) {
      const patch = parseMetadataPatch(body.metadata);
      if ("error" in patch) return jsonError(400, patch.error, patch.message);
      metadata = mergeAgentMetadata({
        existing: { description: null, capabilities: [], tags: [] },
        patch: patch.metadata,
      });
    }
    const version = optionalString(body.version) ?? "v1";
    if (await deps.registry.get(body.id, version)) {
      return jsonError(409, "agent_exists", `Agent "${body.id}@${version}" already exists.`);
    }
    const recipe = await deps.registry.register({
      id: body.id,
      version,
      backend: restored,
      ...(metadata !== undefined && { metadata }),
    });
    return recipeResponse(201, recipe);
  };
}

export function updateAgent(deps: AgentGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    if (!id) return jsonError(400, "missing_id");
    if (isFileManaged(deps, id)) return fileManaged409(id);
    const body = await readJson<UpdateAgentRequest>(req);
    if (!body) return jsonError(400, "missing_body");

    const active = await deps.registry.get(id);
    if (!active) return agentNotFound(id);
    const version = optionalString(body.version);
    if (version !== undefined && version !== active.version) {
      if (!(await deps.registry.get(id, version))) return agentNotFound(id);
      return jsonError(
        409,
        "version_not_active",
        `Only the active version (${active.version}) can be edited; publish a new version instead.`,
      );
    }
    const ifMatch = req.headers.get("if-match");
    if (ifMatch !== null && ifMatch !== "*" && ifMatch !== recipeEtag(active)) {
      return jsonError(412, "precondition_failed", "The recipe changed since it was read.");
    }

    let nextBackend = active.backend;
    if (body.backend !== undefined && body.backend !== null) {
      const parsed = parseBackend(body.backend);
      if ("error" in parsed) return jsonError(400, parsed.error, parsed.message);
      const restored = restoreRedactedSecrets({ next: parsed.backend, existing: active.backend });
      if ("error" in restored) return jsonError(400, restored.error);
      nextBackend = restored;
    }
    let nextMetadata = active.metadata;
    if (body.metadata !== undefined && body.metadata !== null) {
      const patch = parseMetadataPatch(body.metadata);
      if ("error" in patch) return jsonError(400, patch.error, patch.message);
      nextMetadata = mergeAgentMetadata({ existing: active.metadata, patch: patch.metadata });
    }
    const updated = await deps.registry.register({
      id,
      version: active.version,
      backend: nextBackend,
      metadata: nextMetadata,
    });
    return recipeResponse(200, updated);
  };
}

export function deleteAgent(deps: AgentGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    if (!id) return jsonError(400, "missing_id");
    if (isFileManaged(deps, id)) return fileManaged409(id);
    const url = new URL(req.url);
    const version = url.searchParams.get("version") ?? undefined;
    if (!(await deps.registry.get(id, version))) return agentNotFound(id);
    await deps.registry.unregister(id, version);
    return new Response(null, { status: 204 });
  };
}

export function listAgentVersions(deps: AgentGatewayDeps) {
  return async (_req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    if (!id) return jsonError(400, "missing_id");
    const versions = await deps.registry.versions(id);
    if (versions.length === 0) return agentNotFound(id);
    return json(200, { versions: versions.map(redactRecipe) });
  };
}

export function cloneAgent(deps: AgentGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const sourceId = params.id;
    if (!sourceId) return jsonError(400, "missing_id");
    const body = await readJson<CloneAgentRequest>(req);
    if (!body) return jsonError(400, "missing_body");
    if (typeof body.targetId !== "string" || body.targetId.length === 0) {
      return jsonError(400, "missing_targetId");
    }
    if (body.targetId.startsWith("_")) {
      return jsonError(400, "reserved_id_prefix", "Agent ids starting with `_` are reserved.");
    }

    const url = new URL(req.url);
    const sourceVersion = url.searchParams.get("version") ?? undefined;
    const source = await deps.registry.get(sourceId, sourceVersion);
    if (!source) return agentNotFound(sourceId);

    const secretsScope = parseCloneSecretsScope(body.secretsScope);
    if ("error" in secretsScope) return jsonError(400, secretsScope.error);
    const secrets = parseCloneSecrets(body.secrets);
    if ("error" in secrets) return jsonError(400, secrets.error, secrets.message);
    const provided = Object.entries(secrets.secrets);
    if (provided.length > 0 && !deps.secrets) {
      return jsonError(400, "secrets_unsupported", "This server has no secrets storage wired.");
    }

    if (await deps.registry.get(body.targetId)) {
      return jsonError(409, "agent_exists", `Agent "${body.targetId}" already exists.`);
    }

    const targetVersion = optionalString(body.targetVersion);
    const clone = await deps.registry.register({
      id: body.targetId,
      ...(targetVersion !== undefined && { version: targetVersion }),
      backend: source.backend,
      metadata: {
        description: source.metadata.description,
        capabilities: [...source.metadata.capabilities],
        tags: [...source.metadata.tags],
      },
    });
    for (const [key, value] of provided) {
      await deps.secrets!.set({ scope: secretsScope, key, value });
    }
    // `acceptedSecrets` echoes back the stored key names; values are never echoed.
    return json(201, {
      recipe: redactRecipe(clone),
      acceptedSecrets: provided.map(([key]) => key),
    });
  };
}

/**
 * Validate the clone `secrets` map with the same rules as `/api/secrets`:
 * an object of `key → non-empty string`, keys matching the key pattern,
 * values within the size cap.
 */
function parseCloneSecrets(
  raw: unknown,
): { secrets: Record<string, string> } | { error: string; message: string } {
  if (raw === undefined || raw === null) return { secrets: {} };
  if (typeof raw !== "object" || Array.isArray(raw)) {
    return { error: "invalid_secrets", message: "secrets must be an object of key → value." };
  }
  const out: Record<string, string> = {};
  for (const [key, value] of Object.entries(raw)) {
    if (!SECRET_KEY_PATTERN.test(key)) {
      return {
        error: "invalid_key",
        message: "Secret keys must match [A-Za-z][A-Za-z0-9_\\-.]{0,127}.",
      };
    }
    if (typeof value !== "string" || value.length === 0) {
      return { error: "invalid_secrets", message: `Secret "${key}" must be a non-empty string.` };
    }
    if (Buffer.byteLength(value, "utf8") > SECRET_MAX_VALUE_BYTES) {
      return { error: "value_too_large", message: `Secret "${key}" is too large.` };
    }
    out[key] = value;
  }
  return { secrets: out };
}

/**
 * Parse the `secretsScope` field from a clone request. Defaults to
 * global when omitted; validates the discriminated union shape.
 */
function parseCloneSecretsScope(raw: unknown): SecretScope | { error: string } {
  if (raw === undefined || raw === null) return SecretScope.global();
  if (typeof raw !== "object") return { error: "invalid_secretsScope" };
  const obj = raw as { kind?: unknown; namespaceId?: unknown; resourceId?: unknown };
  if (obj.kind === "global") return SecretScope.global();
  if (obj.kind === "namespace") {
    if (typeof obj.namespaceId !== "string" || obj.namespaceId.length === 0) {
      return { error: "missing_namespaceId" };
    }
    return SecretScope.namespace(obj.namespaceId);
  }
  if (obj.kind === "resource") {
    if (typeof obj.namespaceId !== "string" || obj.namespaceId.length === 0) {
      return { error: "missing_namespaceId" };
    }
    if (typeof obj.resourceId !== "string" || obj.resourceId.length === 0) {
      return { error: "missing_resourceId" };
    }
    return SecretScope.resource(obj.namespaceId, obj.resourceId);
  }
  return { error: "invalid_secretsScope_kind" };
}

// ---------------------------------------------------------------------------
// Extract role — "Save as role". Lifts the active version's INLINE role
// into the RoleRegistry under a NEW role id, then rebinds the agent to a
// `ref` so the two share a live link. Create-only on the role; if the
// rebind fails the freshly written role is removed again.
// ---------------------------------------------------------------------------

interface ExtractRoleRequest {
  readonly roleId?: unknown;
  readonly roleVersion?: unknown;
  readonly metadata?: unknown;
}

export function extractRole(deps: AgentGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const agentId = params.id;
    if (!agentId) return jsonError(400, "missing_id");
    if (!deps.roles) {
      return jsonError(501, "roles_not_configured", "This server has no role registry wired.");
    }
    const roles = deps.roles;
    if (isFileManaged(deps, agentId)) return fileManaged409(agentId);
    const body = await readJson<ExtractRoleRequest>(req);
    if (!body) return jsonError(400, "missing_body");
    if (typeof body.roleId !== "string" || body.roleId.length === 0) {
      return jsonError(400, "missing_roleId");
    }
    if (body.roleId.startsWith("_")) {
      return jsonError(400, "reserved_id_prefix", "Role ids starting with `_` are reserved.");
    }

    const recipe = await deps.registry.get(agentId);
    if (!recipe) return agentNotFound(agentId);
    if (recipe.backend.type !== "local" || !("inline" in recipe.backend.role)) {
      return jsonError(
        400,
        "no_inline_role",
        "Only an agent that carries an inline role can be extracted.",
      );
    }
    if ((await roles.versions(body.roleId)).length > 0) {
      return jsonError(409, "role_exists", `Role "${body.roleId}" already exists.`);
    }

    const roleVersion = optionalString(body.roleVersion);
    const metadata =
      typeof body.metadata === "object" && body.metadata !== null
        ? (body.metadata as { description?: string | null; tags?: string[] })
        : undefined;

    const role = await roles.register({
      id: body.roleId,
      ...(roleVersion !== undefined && { version: roleVersion }),
      definition: recipe.backend.role.inline,
      ...(metadata !== undefined && { metadata }),
    });
    try {
      // Rebind the agent to a ref at the same agent version. Pin the role
      // version just written so the link is reproducible.
      const agent = await deps.registry.register({
        id: recipe.id,
        version: recipe.version,
        backend: { ...recipe.backend, role: { ref: { id: role.id, version: role.version } } },
        metadata: recipe.metadata,
      });
      return json(201, { role, agent: redactRecipe(agent) });
    } catch (err) {
      await roles.unregister(role.id, role.version).catch(() => {});
      throw err;
    }
  };
}

// ---------------------------------------------------------------------------
// One-shot invoke + stream
// ---------------------------------------------------------------------------

export function invokeAgent(deps: AgentGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    if (!id) return jsonError(400, "missing_id");

    const parsed = parseInvokeBody(await readJson<InvokeRequest>(req));
    if ("error" in parsed) return jsonError(400, parsed.error);
    const scoped = await loadScopedAgent(deps, { req, id, scope: parsed, invoking: true });
    if (scoped instanceof Response) return scoped;

    try {
      const out = await scoped.agent.invoke({ task: parsed.task });
      const response: InvokeResponse = {
        text: await out.text,
        finishReason: await out.finishReason,
        usage: await out.usage,
      };
      if (scoped.instanceId !== undefined) response.instanceId = scoped.instanceId;
      return json(200, response);
    } catch (err) {
      return internalError({ code: "invoke_failed", err });
    }
  };
}

export function streamAgent(deps: AgentGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    if (!id) return jsonError(400, "missing_id");

    const parsed = parseInvokeBody(await readJson<InvokeRequest>(req));
    if ("error" in parsed) return jsonError(400, parsed.error);
    const scoped = await loadScopedAgent(deps, { req, id, scope: parsed, invoking: true });
    if (scoped instanceof Response) return scoped;

    const out = scoped.agent.stream({ task: parsed.task });
    silenceOutput(out);
    const sse = createSseChannel();
    const stream = new ReadableStream<Uint8Array>({
      async start(controller) {
        sse.attach(controller);
        try {
          for await (const delta of out.textStream) {
            if (sse.closed) break;
            sse.send(null, { delta });
          }
          if (!sse.closed) {
            const finishPayload: Record<string, unknown> = {
              text: await out.text,
              finishReason: await out.finishReason,
              usage: await out.usage,
            };
            if (scoped.instanceId !== undefined) finishPayload.instanceId = scoped.instanceId;
            sse.send("finish", finishPayload);
          }
        } catch (err) {
          sse.send("error", streamErrorPayload(err));
        } finally {
          sse.close();
        }
      },
      cancel: async () => {
        sse.markClosed();
        await out.cancel().catch(() => {});
      },
    });
    return sseResponse(stream);
  };
}

// ---------------------------------------------------------------------------
// Threaded conversation
// ---------------------------------------------------------------------------

export function sendThreadMessage(deps: AgentGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    const threadId = params.threadId;
    if (!id) return jsonError(400, "missing_id");
    if (!threadId) return jsonError(400, "missing_threadId");

    const parsed = parseInvokeBody(await readJson<InvokeRequest>(req));
    if ("error" in parsed) return jsonError(400, parsed.error);
    const scoped = await loadScopedAgent(deps, { req, id, scope: parsed, invoking: true });
    if (scoped instanceof Response) return scoped;

    const lease = await acquireTurnLease(deps, { namespaceId: scoped.namespaceId, threadId });
    if (lease instanceof Response) return lease;
    try {
      const thread = await openOwnedThread({ scoped, threadId, create: true });
      if (thread instanceof Response) return thread;
      const out = await thread.send({ task: parsed.task });
      const response: ThreadInvokeResponse = {
        threadId: thread.id,
        isNew: thread.isNew,
        text: await out.text,
        finishReason: await out.finishReason,
        usage: await out.usage,
      };
      if (scoped.instanceId !== undefined) response.instanceId = scoped.instanceId;
      return json(200, response);
    } catch (err) {
      return internalError({ code: "send_failed", err });
    } finally {
      await lease.release();
    }
  };
}

export function streamThreadMessage(deps: AgentGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    const threadId = params.threadId;
    if (!id) return jsonError(400, "missing_id");
    if (!threadId) return jsonError(400, "missing_threadId");

    const parsed = parseInvokeBody(await readJson<InvokeRequest>(req));
    if ("error" in parsed) return jsonError(400, parsed.error);
    const scoped = await loadScopedAgent(deps, { req, id, scope: parsed, invoking: true });
    if (scoped instanceof Response) return scoped;

    // The lease outlives this handler: the stream is consumed after the
    // response returns. Every exit before the stream takes ownership of
    // the lease releases it here; afterwards the stream releases it when
    // it finishes, fails or the client disconnects.
    const lease = await acquireTurnLease(deps, { namespaceId: scoped.namespaceId, threadId });
    if (lease instanceof Response) return lease;
    try {
      const thread = await openOwnedThread({ scoped, threadId, create: true });
      if (thread instanceof Response) {
        await lease.release();
        return thread;
      }
      const out = thread.stream({ task: parsed.task });
      return streamAgentRunResponse({
        out,
        threadMeta: threadMetaOf(thread, scoped),
        onClose: () => lease.release(),
      });
    } catch (err) {
      await lease.release();
      return internalError({ code: "stream_failed", err });
    }
  };
}

function threadMetaOf(thread: AgentThread, scoped: ScopedAgent): Record<string, unknown> {
  return {
    threadId: thread.id,
    isNew: thread.isNew,
    ...(scoped.instanceId !== undefined && { instanceId: scoped.instanceId }),
  };
}

/**
 * Continue a turn that suspended on `approve:<callId>`. Body shape:
 *   { toolCallId: string, approved: boolean, reason?: string,
 *     namespaceId, resourceId | ownerId }
 *
 * Takes the thread's turn lease like a send (so an approve can't race a
 * new turn), answers 404 when the thread or the suspended approval is
 * unknown, then streams the resumed turn's events in the same SSE shape
 * as the user-task stream.
 */
export function streamThreadApproval(deps: AgentGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    const threadId = params.threadId;
    if (!id) return jsonError(400, "missing_id");
    if (!threadId) return jsonError(400, "missing_threadId");

    const parsed = parseApprovalBody(await readJson<ApprovalRequest>(req));
    if ("error" in parsed) return jsonError(400, parsed.error);
    const scoped = await loadScopedAgent(deps, { req, id, scope: parsed, invoking: true });
    if (scoped instanceof Response) return scoped;

    const lease = await acquireTurnLease(deps, { namespaceId: scoped.namespaceId, threadId });
    if (lease instanceof Response) return lease;
    try {
      const thread = await openOwnedThread({ scoped, threadId, create: false });
      if (thread instanceof Response) {
        await lease.release();
        return thread;
      }
      if (!thread.resumeStream) {
        await lease.release();
        return jsonError(
          501,
          "resume_unsupported",
          "This agent backend doesn't support approval resume.",
        );
      }
      if (thread.hasPendingApproval && !(await thread.hasPendingApproval(parsed.toolCallId))) {
        await lease.release();
        return jsonError(404, "approval_not_found");
      }
      const out = thread.resumeStream(parsed.toolCallId, {
        approved: parsed.approved,
        ...(parsed.reason !== undefined && { reason: parsed.reason }),
      });
      return streamAgentRunResponse({
        out,
        threadMeta: threadMetaOf(thread, scoped),
        onClose: () => lease.release(),
      });
    } catch (err) {
      await lease.release();
      return internalError({ code: "resume_failed", err });
    }
  };
}

interface ApprovalRequest {
  readonly toolCallId?: unknown;
  readonly approved?: unknown;
  readonly reason?: unknown;
  readonly namespaceId?: unknown;
  readonly resourceId?: unknown;
  readonly ownerId?: unknown;
}

interface ParsedApproval extends ScopeFields {
  readonly toolCallId: string;
  readonly approved: boolean;
  readonly reason?: string;
}

function parseApprovalBody(body: ApprovalRequest | null): ParsedApproval | { error: string } {
  if (!body) return { error: "missing_body" };
  if (typeof body.toolCallId !== "string" || body.toolCallId.length === 0) {
    return { error: "missing_toolCallId" };
  }
  if (typeof body.approved !== "boolean") return { error: "missing_approved" };
  const scope = parseScopeFields(body);
  if ("error" in scope) return scope;
  const reason = typeof body.reason === "string" ? body.reason : undefined;
  return {
    toolCallId: body.toolCallId,
    approved: body.approved,
    ...scope,
    ...(reason !== undefined && { reason }),
  };
}

// ---------------------------------------------------------------------------
// SSE plumbing
// ---------------------------------------------------------------------------

const SSE_HEADERS = {
  "content-type": "text/event-stream",
  "cache-control": "no-cache, no-transform",
  connection: "keep-alive",
} as const;

function sseResponse(stream: ReadableStream<Uint8Array>): Response {
  return new Response(stream, { status: 200, headers: SSE_HEADERS });
}

/**
 * Guarded SSE writer: after the client disconnects (or the stream is
 * closed) every send is a no-op instead of a throw.
 */
function createSseChannel() {
  const enc = new TextEncoder();
  let controller: ReadableStreamDefaultController<Uint8Array> | null = null;
  let closed = false;
  return {
    get closed() {
      return closed;
    },
    attach(c: ReadableStreamDefaultController<Uint8Array>) {
      controller = c;
    },
    send(event: string | null, data: unknown) {
      if (closed || !controller) return;
      const prefix = event ? `event: ${event}\n` : "";
      try {
        controller.enqueue(enc.encode(`${prefix}data: ${JSON.stringify(data)}\n\n`));
      } catch {
        closed = true;
      }
    },
    markClosed() {
      closed = true;
    },
    close() {
      if (closed) return;
      closed = true;
      try {
        controller?.close();
      } catch {
        // already closed
      }
    },
  };
}

/**
 * Attach a no-op rejection handler to every promise on a run output so
 * a failed run that nobody awaits (the client went away, or the stream
 * reported the failure through `fullStream`) never surfaces as an
 * unhandled rejection.
 */
function silenceOutput(out: AgentRunOutput): void {
  for (const value of Object.values(out)) {
    if (value instanceof Promise) value.catch(() => {});
  }
}

/** An `event: error` payload: a request id, never the error's text. */
function streamErrorPayload(err: unknown): { error: string; requestId: string } {
  const requestId = crypto.randomUUID();
  try {
    console.error(`[zorya] agent stream failed requestId=${requestId}`, err);
  } catch {
    // ignore
  }
  return { error: "stream_failed", requestId };
}

/**
 * Shared SSE writer for thread.stream / thread.resumeStream — fans the
 * agent's `fullStream` into typed SSE events so the client sees text
 * deltas, approval requests, and finish/suspend signals in one channel.
 *
 * Suspension semantics: when the workflow suspends on an approval gate
 * the underlying `out.text` promise rejects with `WorkflowSuspendedError`.
 * The `approval-requested` event already fired through `fullStream`, so
 * we map the rejection to a clean `event: suspended` and let the client
 * react (render the approval banner). Any other error becomes
 * `event: error`.
 *
 * `onClose` runs exactly once, when the run has settled (completed,
 * failed or suspended) — also when the client disconnected earlier: the
 * run keeps writing to the thread until it settles, so the turn lease is
 * held until then.
 */
function streamAgentRunResponse(params: {
  out: AgentRunOutput;
  threadMeta: Record<string, unknown>;
  onClose?: () => Promise<void>;
}): Response {
  const { out, threadMeta, onClose } = params;
  silenceOutput(out);
  const runSettled = Promise.allSettled([out.text]);
  let cleaned = false;
  const cleanup = async () => {
    if (cleaned) return;
    cleaned = true;
    await onClose?.().catch(() => {});
  };
  const cleanupWhenSettled = () => {
    void runSettled.then(cleanup);
  };
  const sse = createSseChannel();
  const stream = new ReadableStream<Uint8Array>({
    async start(controller) {
      sse.attach(controller);
      sse.send("thread", threadMeta);

      let suspended: { toolCallId: string; toolName: string } | null = null;
      try {
        for await (const event of out.fullStream) {
          if (sse.closed) break;
          switch (event.type) {
            case "text-delta":
              sse.send(null, { delta: event.delta });
              break;
            case "approval-requested":
              suspended = { toolCallId: event.toolCallId, toolName: event.toolName };
              sse.send("approval-requested", suspended);
              break;
            case "error":
              sse.send("error", { message: event.error.message });
              break;
            default:
              break;
          }
        }
        if (sse.closed) {
          // Client went away mid-run; nothing left to report.
        } else if (!suspended) {
          const text = await out.text;
          const finishReason = await out.finishReason;
          const usage = await out.usage;
          sse.send("finish", { ...threadMeta, text, finishReason, usage });
        } else {
          sse.send("suspended", { ...threadMeta, ...suspended });
        }
      } catch (err) {
        if (err instanceof WorkflowSuspendedError) {
          sse.send("suspended", { ...threadMeta, ...suspended });
        } else {
          sse.send("error", streamErrorPayload(err));
        }
      } finally {
        sse.close();
        cleanupWhenSettled();
      }
    },
    cancel: async () => {
      sse.markClosed();
      await out.cancel().catch(() => {});
      cleanupWhenSettled();
    },
  });
  return sseResponse(stream);
}

// ---------------------------------------------------------------------------
// Thread reads + housekeeping. Allowed on a disabled agent.
// ---------------------------------------------------------------------------

export function listAgentThreads(deps: AgentGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    if (!id) return jsonError(400, "missing_id");
    const url = new URL(req.url);
    const scope = scopeFromQuery(url);
    if ("error" in scope) return jsonError(400, scope.error);
    const scoped = await loadScopedAgent(deps, { req, id, scope, invoking: false });
    if (scoped instanceof Response) return scoped;

    const threads = await scoped.agent.listThreads({
      resourceId: scoped.resourceId,
      ...(scoped.recipe.backend.type === "local" && {
        metadataFilter: { [THREAD_AGENT_METADATA_KEY]: id },
      }),
      limit: parseLimitParam(url.searchParams.get("limit")),
      cursor: url.searchParams.get("cursor") ?? undefined,
      q: url.searchParams.get("q") ?? undefined,
    });
    return json(200, { threads } satisfies AgentThreadsResponse);
  };
}

/** Load the scoped agent and the caller's thread for a read route (query scope). */
async function openThreadFromQuery(
  deps: AgentGatewayDeps,
  req: Request,
  params: Record<string, string>,
): Promise<{ thread: AgentThread; url: URL } | Response> {
  const id = params.id;
  const threadId = params.threadId;
  if (!id) return jsonError(400, "missing_id");
  if (!threadId) return jsonError(400, "missing_threadId");
  const url = new URL(req.url);
  const scope = scopeFromQuery(url);
  if ("error" in scope) return jsonError(400, scope.error);
  const scoped = await loadScopedAgent(deps, { req, id, scope, invoking: false });
  if (scoped instanceof Response) return scoped;
  const thread = await openOwnedThread({ scoped, threadId, create: false });
  if (thread instanceof Response) return thread;
  return { thread, url };
}

export function getThreadTrace(deps: AgentGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const opened = await openThreadFromQuery(deps, req, params);
    if (opened instanceof Response) return opened;
    const messages = await opened.thread.messages({ limit: TRACE_MESSAGE_LIMIT, order: "desc" });
    const trace = buildAgentTrace([...messages].reverse());
    return json(200, { threadId: opened.thread.id, trace });
  };
}

/** Newest messages a trace is built from. */
const TRACE_MESSAGE_LIMIT = 1_000;

export function listThreadMessages(deps: AgentGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const opened = await openThreadFromQuery(deps, req, params);
    if (opened instanceof Response) return opened;
    const { thread, url } = opened;
    const limit = parseLimitParam(url.searchParams.get("limit"));
    const fromSeq = parseIntParam({
      raw: url.searchParams.get("fromSeq"),
      min: 0,
      max: Number.MAX_SAFE_INTEGER,
    });
    const toSeq = parseIntParam({
      raw: url.searchParams.get("toSeq"),
      min: 0,
      max: Number.MAX_SAFE_INTEGER,
    });
    const messages = await thread.messages({ limit, fromSeq, toSeq });
    return json(200, { threadId: thread.id, messages } satisfies ThreadMessagesResponse);
  };
}

/** Scope + thread for a body-scoped route (rename / archive / distill / compact). */
async function openThreadFromBody(
  deps: AgentGatewayDeps,
  req: Request,
  params: Record<string, string>,
  body: {
    readonly namespaceId?: unknown;
    readonly resourceId?: unknown;
    readonly ownerId?: unknown;
  },
): Promise<{ thread: AgentThread; scoped: ScopedAgent } | Response> {
  const id = params.id;
  const threadId = params.threadId;
  if (!id) return jsonError(400, "missing_id");
  if (!threadId) return jsonError(400, "missing_threadId");
  const scope = parseScopeFields(body);
  if ("error" in scope) return jsonError(400, scope.error);
  const scoped = await loadScopedAgent(deps, { req, id, scope, invoking: false });
  if (scoped instanceof Response) return scoped;
  const thread = await openOwnedThread({ scoped, threadId, create: false });
  if (thread instanceof Response) return thread;
  return { thread, scoped };
}

// ---------------------------------------------------------------------------
// Rename thread — sets the thread's first-class display title. The
// response carries the thread's metadata bag, which the title doesn't touch.
// ---------------------------------------------------------------------------

interface RenameThreadRequest {
  readonly namespaceId?: unknown;
  readonly resourceId?: unknown;
  readonly ownerId?: unknown;
  readonly title?: unknown;
}

export function renameAgentThread(deps: AgentGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const body = await readJson<RenameThreadRequest>(req);
    if (!body) return jsonError(400, "missing_body");
    if (body.title !== undefined && body.title !== null && typeof body.title !== "string") {
      return jsonError(400, "invalid_title");
    }
    // Empty / null title clears the rename — back to the threadId default.
    const title =
      typeof body.title === "string" && body.title.trim().length > 0 ? body.title.trim() : null;
    const opened = await openThreadFromBody(deps, req, params, body);
    if (opened instanceof Response) return opened;
    await opened.thread.setTitle(title);
    const metadata = { ...(await opened.thread.metadata()) };
    return json(200, {
      threadId: opened.thread.id,
      title,
      metadata,
    } satisfies RenameThreadResponse);
  };
}

// ---------------------------------------------------------------------------
// Memory consolidation — delegates to the agent's Consolidator.
// ---------------------------------------------------------------------------

interface ConsolidateRequest {
  readonly namespaceId?: unknown;
  readonly resourceId?: unknown;
  readonly ownerId?: unknown;
  readonly force?: unknown;
  readonly keepRecent?: unknown;
}

function nothingToConsolidate409(err: unknown): Response | null {
  if (err instanceof NothingToConsolidateError) {
    return json(409, {
      error: err.operation === "compact" ? "nothing_to_compact" : "nothing_to_distill",
      messageCount: err.messageCount,
    });
  }
  return null;
}

export function distillThread(deps: AgentGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const body = (await readJson<ConsolidateRequest>(req)) ?? {};
    const opened = await openThreadFromBody(deps, req, params, body);
    if (opened instanceof Response) return opened;

    try {
      const episode = await opened.scoped.agent.distillThread(opened.thread.id, {
        force: body.force === true,
      });
      return json(200, { episode });
    } catch (err) {
      if (err instanceof ConsolidatorRateLimitError) {
        const retryAfter = err.retryAfterSeconds((deps.clock ?? SystemWallClock).currentTimeMs());
        return new Response(
          JSON.stringify({
            error: "distill_rate_limited",
            message: err.message,
            scope: err.scope,
            windowMs: err.windowMs,
            max: err.max,
            seen: err.seen,
            retryAfterSeconds: retryAfter,
          }),
          {
            status: 429,
            headers: {
              "content-type": "application/json",
              "retry-after": String(retryAfter),
            },
          },
        );
      }
      const nothing = nothingToConsolidate409(err);
      if (nothing) return nothing;
      return internalError({ code: "distill_failed", err });
    }
  };
}

export function compactThread(deps: AgentGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const body = (await readJson<ConsolidateRequest>(req)) ?? {};
    let keepRecent: number | undefined;
    if (body.keepRecent !== undefined) {
      if (
        typeof body.keepRecent !== "number" ||
        !Number.isInteger(body.keepRecent) ||
        body.keepRecent < 0
      ) {
        return jsonError(400, "invalid_keepRecent", "keepRecent must be a non-negative integer.");
      }
      keepRecent = body.keepRecent;
    }
    const opened = await openThreadFromBody(deps, req, params, body);
    if (opened instanceof Response) return opened;

    try {
      const episode = await opened.scoped.agent.compactThread(opened.thread.id, { keepRecent });
      return json(200, { episode });
    } catch (err) {
      const nothing = nothingToConsolidate409(err);
      if (nothing) return nothing;
      return internalError({ code: "compact_failed", err });
    }
  };
}

// ---------------------------------------------------------------------------
// Archive / unarchive a thread — sets archivedAt on the thread row so
// the sidebar hides it from the default view without deleting it.
// ---------------------------------------------------------------------------

interface ArchiveThreadRequest {
  readonly namespaceId?: unknown;
  readonly resourceId?: unknown;
  readonly ownerId?: unknown;
  /** Unix ms timestamp to archive at, or null to restore. Defaults to the gateway clock's now when omitted. */
  readonly archivedAt?: unknown;
}

export function archiveAgentThread(deps: AgentGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const body = (await readJson<ArchiveThreadRequest>(req)) ?? {};
    let archivedAt: number | null;
    if (body.archivedAt === null) {
      archivedAt = null;
    } else if (body.archivedAt === undefined) {
      archivedAt = (deps.clock ?? SystemWallClock).currentTimeMs();
    } else if (
      typeof body.archivedAt === "number" &&
      Number.isSafeInteger(body.archivedAt) &&
      body.archivedAt >= 0
    ) {
      archivedAt = body.archivedAt;
    } else {
      return jsonError(
        400,
        "invalid_archivedAt",
        "archivedAt must be a unix-ms timestamp, null, or omitted.",
      );
    }
    const opened = await openThreadFromBody(deps, req, params, body);
    if (opened instanceof Response) return opened;
    await opened.thread.setArchived(archivedAt);
    return json(200, { threadId: opened.thread.id, archivedAt });
  };
}

// ---------------------------------------------------------------------------

/** Limit-style query parameter: an integer clamped to [1, 1000]. */
function parseLimitParam(raw: string | null): number | undefined {
  return parseIntParam({ raw, min: 1, max: 1_000 });
}
