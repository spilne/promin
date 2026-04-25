// ---------------------------------------------------------------------------
// AgentIdentity — the per-(registeredAgentId, namespaceId, userId) record
// that turns "an agent" from a per-request session into a long-lived
// instance with state across invocations.
//
// What the identity actually stores
// ---------------------------------
// Identity is metadata + a deterministic id. The heavy lifting (working
// memory, facts, episodes, threads) stays in `MemoryStore`, scoped by
// `resourceId = identity.id`. That gives us:
//   - per-(agent, user) memory for free, via the existing cascade
//   - resolveContext continues to work unchanged
//   - operator can wipe an identity by deleting the registry row PLUS
//     clearing the matching resource scope from the memory store
//
// Why a separate registry instead of just using resourceId
// --------------------------------------------------------
// The memory store doesn't know which resourceIds map to which agents,
// or which user owns them. The identity registry adds:
//   - enumeration ("list all identities for user alice")
//   - reverse lookup ("what user owns this resourceId?")
//   - displayName + metadata (UI affordances that don't fit in memory)
//   - lastActiveAt (for sorting / staleness reports without scanning
//     the message log)
//
// Without it, callers would have to scan threads to enumerate identities,
// which doesn't scale. With it, the registry is a thin index over the
// memory store.
// ---------------------------------------------------------------------------

export interface AgentIdentity {
  /**
   * Stable opaque id. Convention: `${registeredAgentId}::${userId}` so the
   * id is deterministic from the create input — but treat it as opaque,
   * since alternative implementations may pick different formats.
   *
   * Used as `resourceId` when invoking the agent so the memory cascade
   * scopes working memory + facts + episodes per-(agent, user).
   */
  readonly id: string;
  readonly registeredAgentId: string;
  readonly namespaceId: string;
  /**
   * External user identifier — whatever the caller's auth system uses.
   * Treated as opaque by the registry (no shape constraints beyond
   * non-empty string).
   */
  readonly userId: string;
  /** Optional human-friendly label. UI can fall back to `${registeredAgentId} for ${userId}`. */
  readonly displayName: string | null;
  readonly metadata: Readonly<Record<string, unknown>>;
  readonly createdAt: number;
  readonly lastActiveAt: number;
}

export interface CreateAgentIdentityInput {
  readonly registeredAgentId: string;
  readonly namespaceId: string;
  readonly userId: string;
  readonly displayName?: string | null;
  readonly metadata?: Readonly<Record<string, unknown>>;
}

export interface ListAgentIdentitiesParams {
  readonly namespaceId?: string;
  readonly userId?: string;
  readonly registeredAgentId?: string;
  readonly limit?: number;
  /** Defaults to "lastActiveDesc". */
  readonly order?: "lastActiveDesc" | "createdAsc" | "createdDesc";
}

export interface UpdateAgentIdentityPatch {
  readonly displayName?: string | null;
  readonly metadata?: Readonly<Record<string, unknown>>;
}

/**
 * Index of long-lived agent instances, keyed by (registeredAgentId, namespaceId, userId).
 *
 * Implementations: `InMemoryAgentIdentityRegistry` (reference) and
 * `SqliteAgentIdentityRegistry` (persistent). Both must pass the shared
 * conformance suite.
 */
export interface AgentIdentityRegistry {
  /**
   * Idempotent: returns the existing identity for the same triple, or
   * creates one. `displayName` and `metadata` from the input only apply
   * on creation — call `update` to change them later.
   */
  resolveOrCreate(input: CreateAgentIdentityInput): Promise<AgentIdentity>;

  get(id: string): Promise<AgentIdentity | null>;

  list(params?: ListAgentIdentitiesParams): Promise<AgentIdentity[]>;

  /** Bumps `lastActiveAt`. Called from the agent action on each successful turn. */
  touch(id: string, lastActiveAt?: number): Promise<void>;

  update(id: string, patch: UpdateAgentIdentityPatch): Promise<AgentIdentity>;

  /**
   * Removes the identity row only. Memory under `resourceId = id` is the
   * caller's responsibility — see `wipeAgentIdentity` for the cascading
   * helper that drops both the row and the resource-scope state.
   */
  delete(id: string): Promise<void>;
}

/**
 * Compose a deterministic identity id from (namespaceId, registeredAgentId, userId).
 * Exported so callers that don't want to round-trip through the registry
 * (e.g. when invoking an agent for the first time) can compute the id
 * themselves and pass it directly as `resourceId`.
 *
 * Namespace is part of the id so two tenants with the same userId for
 * the same agent recipe don't collide on the registry row — multi-tenant
 * is the default, not an opt-in.
 *
 * Delimiter `::` was chosen to avoid collision with characters that
 * commonly appear in agent ids (`/`, `-`, `:`, alphanumerics).
 */
export function composeAgentIdentityId(input: {
  readonly namespaceId: string;
  readonly registeredAgentId: string;
  readonly userId: string;
}): string {
  return `${input.namespaceId}::${input.registeredAgentId}::${input.userId}`;
}
