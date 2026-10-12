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
//   GET  /api/agents/:id/threads/:threadId/stream  — reattach to a turn's SSE
//   POST /api/agents/:id/threads/:threadId/cancel  — cancel a running turn
//   GET  /api/agents/:id/threads              — list the caller's threads
//   GET  /api/agents/:id/threads/:threadId/messages — read message history
//   POST /api/agents/:id/threads/:threadId/approve  — decide a pending approval, SSE
//   GET  /api/agents/:id/threads/:threadId/approvals — the thread's pending approvals
//   GET  /api/agents/:id/threads/:threadId/usage — the thread's spend vs its budgets
//   GET  /api/agents/_usage?namespaceId=       — the namespace's spend vs its budget
//
// Budgets (`ZoryaAgents.budgets`):
//   A turn on a thread or namespace whose budget is used up answers 429
//   `budget_exceeded` (with `scope`, `dimension`, `limit`, `used`) before it
//   runs; an admitted turn is capped by what is left and ends with
//   finishReason `budget_exceeded` when it runs out.
//
// Thread turns:
//   A send may carry `turnKey` (body) or `Idempotency-Key` (header): the
//   same key on the same thread is the same turn, answered from (or
//   continuing) the first attempt. A send on a thread whose turn waits for
//   an approval answers 409 `approval_pending` — decide the approval first,
//   so the suspended turn's output lands before the next message.
//
// Streams (see agent-sse.ts):
//   Every SSE event carries `id: <turnId>:<seq>`, the server sends a
//   heartbeat comment while idle, and a client that disconnects detaches:
//   the turn runs on and commits. Reattach with `GET .../stream` and
//   `Last-Event-ID`; cancel only with `POST .../cancel`.
//
// Input caps:
//   `task` is at most MAX_AGENT_TASK_CHARS (413 `task_too_large`), a thread
//   title at most MAX_THREAD_TITLE_CHARS (400 `title_too_long`).
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
  AgentBudgets,
  AgentInstanceRegistry,
  AgentRegistry,
  AgentThreadSummary,
  AgentTurnGate,
  AgentTurnPolicy,
  BudgetUsage,
  Message,
  RegisteredAgent,
  RoleRegistry,
  SecretsStorage,
  TurnBudget,
} from "@promin/agent";
import type { NamespaceRegistry, WallClock } from "@promin/workflow";
import type { McpRecipeChecker, ScopeAuthorizer } from "../agent-gateway-kit.ts";
import type { TurnStreamHub } from "./agent-sse.ts";

// ---------------------------------------------------------------------------
// Response DTOs — UI imports these for typing API responses. Re-export the
// underlying agent types so the UI bundle doesn't import @promin/agent
// directly (keeps the import surface narrow and matches schedules.ts).
// `ThreadSummary` here is the agent-facing shape (with `id`), not the
// memory-store shape (which uses `threadId`).
// ---------------------------------------------------------------------------

export type {
  AgentThreadSummary as ThreadSummary,
  BudgetUsage,
  RegisteredAgent,
  Message,
  TurnBudget,
} from "@promin/agent";

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
  /** Pass as `?cursor=` for the next page; absent on the last page. */
  nextCursor?: string;
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

/** A budget's limits next to what was spent; `limits` is absent when no limit is set. */
export interface BudgetStandingDto {
  spent: BudgetUsage;
  limits?: TurnBudget;
}

/** `GET /api/agents/:id/threads/:threadId/usage`. */
export interface ThreadUsageResponse {
  threadId: string;
  /** False when the server has no budgets configured: nothing is accounted. */
  enabled: boolean;
  /** The thread's spend over all its turns, and its cumulative budget. */
  thread?: BudgetStandingDto;
  /** The namespace's spend in its window, and its budget. Absent without a namespace budget. */
  namespace?: BudgetStandingDto & { windowMs: number };
}

/** `GET /api/agents/_usage?namespaceId=`. */
export interface NamespaceUsageResponse {
  namespaceId: string;
  enabled: boolean;
  /** Absent without a namespace budget. */
  namespace?: BudgetStandingDto & { windowMs: number };
}

/** 429 body of a turn refused because a budget is used up. */
export interface BudgetExceededResponse {
  error: "budget_exceeded";
  message: string;
  scope: "thread" | "namespace";
  dimension: keyof TurnBudget;
  limit: number;
  used: number;
}

export interface ThreadInvokeResponse extends InvokeResponse {
  threadId: string;
  isNew: boolean;
}

/** A tool approval a thread's turn is suspended on. */
export interface PendingApprovalDto {
  toolCallId: string;
  toolName: string;
  input: unknown;
  /** Unix ms. */
  requestedAt: number;
  /** Unix ms; null when it never expires. */
  expiresAt: number | null;
  /**
   * `pending`, or the decision (`approved` / `rejected` / `expired`) when it
   * is recorded but has not reached the turn yet — approve again to finish.
   */
  status: "pending" | "approved" | "rejected" | "expired";
}

/** 202 of a thread send whose turn suspended on a tool approval. */
export interface ThreadSuspendedResponse {
  threadId: string;
  suspended: true;
  approvals: PendingApprovalDto[];
  instanceId?: string;
}

export interface ThreadApprovalsResponse {
  approvals: PendingApprovalDto[];
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
   * The operator's MCP allowlist (`ZoryaServerConfig.mcp`). A created or
   * edited recipe may attach only the servers it allows (400
   * `invalid_mcp_servers` otherwise); without it, no MCP servers at all.
   */
  readonly mcp?: McpRecipeChecker;
  /**
   * Optional namespace registry. When set, invocations (and the clone
   * endpoint's namespace- or resource-scoped secrets) need a registered,
   * active `namespaceId`: 404 `namespace_not_found`, 409
   * `namespace_archived`. Read routes are not checked.
   */
  readonly namespaces?: NamespaceRegistry;
  /** Binds a request's scope to its authenticated caller. See `ScopeAuthorizer`. */
  readonly authorizeScope?: ScopeAuthorizer;
  /**
   * The turn streams of this process: every streamed turn's events go
   * through it, so clients can detach, reattach and resume by event id.
   */
  readonly turnStreams: TurnStreamHub;
  /** Thread and namespace budgets (`ZoryaAgents.budgets`): 429 refusals and the usage routes. */
  readonly budgets?: AgentBudgets;
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
