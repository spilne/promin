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
//   `metadata.agentId` stamp (`THREAD_AGENT_METADATA_KEY`): the agent backend
//   stamps it on every thread it creates — through this gateway,
//   programmatically or from a scheduled dispatch — and the gateway only
//   lists / opens threads carrying its own agent id. A thread owned by
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
  AgentThreadSummary,
  AgentTurnGate,
  AgentTurnPolicy,
  Message,
  RegisteredAgent,
  RoleRegistry,
  SecretsStorage,
} from "@promin/agent";
import type { NamespaceRegistry, WallClock } from "@promin/workflow";
import type { ScopeAuthorizer } from "../agent-gateway-kit.ts";

// ---------------------------------------------------------------------------
// Response DTOs — UI imports these for typing API responses. Re-export the
// underlying agent types so the UI bundle doesn't import @promin/agent
// directly (keeps the import surface narrow and matches schedules.ts).
// `ThreadSummary` here is the agent-facing shape (with `id`), not the
// memory-store shape (which uses `threadId`).
// ---------------------------------------------------------------------------

export type { AgentThreadSummary as ThreadSummary, RegisteredAgent, Message } from "@promin/agent";

/** One row of `GET /api/agents`: the agent's active version plus its version count. */
export type AgentListEntry = RegisteredAgent & {
  /** How many versions the agent has (`GET /api/agents/:id/versions` lists them). */
  versionCount: number;
};

export interface AgentSourcesResponse {
  /** Agent ids currently backed by a recipe file on disk — read-only in the UI. */
  fileManaged: string[];
}

export interface AgentsListResponse {
  agents: AgentListEntry[];
  /** Agents matching the filters across all pages. */
  total: number;
}

export interface AgentVersionsResponse {
  /** Every version of the agent, oldest first. */
  versions: RegisteredAgent[];
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
   * Optional namespace registry. When set, invocations (and the clone
   * endpoint's namespace- or resource-scoped secrets) need a registered,
   * active `namespaceId`: 404 `namespace_not_found`, 409
   * `namespace_archived`. Read routes are not checked.
   */
  readonly namespaces?: NamespaceRegistry;
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
