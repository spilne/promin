// ---------------------------------------------------------------------------
// AgentIdentity — a stable per-(agent, tenant, user) record that turns
// "an agent" from a per-request session into a long-lived instance with
// memory across all invocations.
//
//
// The problem
// -----------
// Without identity, an HTTP caller invokes an agent like this:
//
//     POST /api/agents/writer/invoke
//       body: { task, namespaceId: "acme", resourceId: "alice" }
//
// Keeping `resourceId: "alice"` stable across calls gets you persistent
// state via the memory cascade — but ALL agents share that state. Alice's
// `writer` agent and her `reviewer` agent both see her single resource
// working memory. There's also no way to enumerate "what agents has
// alice instantiated?" or to wipe everything alice has with the writer
// without scanning threads.
//
// That's a session model. We want an instance model.
//
//
// The shape
// ---------
//     AgentIdentity = {
//       id: "acme::writer::alice",        // deterministic, used as resourceId
//       registeredAgentId: "writer",      // which recipe
//       namespaceId: "acme",              // which tenant
//       userId: "alice",                  // which end-user
//       displayName: string | null,
//       metadata: {},
//       createdAt, lastActiveAt
//     }
//
// `id` is just `${namespaceId}::${registeredAgentId}::${userId}` (see
// `composeAgentIdentityId`). Deterministic — you can compute it from the
// inputs without round-tripping through the registry.
//
//
// The trick: id is reused as resourceId
// -------------------------------------
// This is the whole mechanism. When the agent is invoked, `identity.id`
// is passed as `resourceId`:
//
//     namespaceId: "acme"
//     resourceId:  "acme::writer::alice"   ← identity.id
//     threadId:    "t-2026-04-27-abc"
//
// The existing MemoryStore cascade reads/writes against that resourceId.
// So:
//   - working memory at (acme, acme::writer::alice) is private to the
//     alice's writer
//   - facts at (acme, acme::writer::alice) are private to alice's writer
//   - (acme, acme::reviewer::alice) is a different row → reviewer can't
//     see writer's notes
//
// No new memory tier. The identity is metadata + a clever resourceId
// convention, nothing more.
//
//
// Two layers
// ----------
//     ┌────────────────────────────────────────┐
//     │  AgentIdentityRegistry                  │  ← thin index
//     │  - who has what agent                   │
//     │  - displayName, lastActiveAt, metadata  │
//     └────────────────────────────────────────┘
//                       │ identity.id used as
//                       ▼ resourceId
//     ┌────────────────────────────────────────┐
//     │  MemoryStore                            │  ← does the heavy lifting
//     │  - working memory, facts, episodes,     │
//     │    threads, messages                    │
//     │  - keyed by (namespaceId, resourceId)   │
//     └────────────────────────────────────────┘
//
// The registry is bookkeeping. The memory store is reality.
//
//
// Lifecycle
// ---------
//     // 1. Resolve-or-create (idempotent — same triple = same row)
//     const identity = await registry.resolveOrCreate({
//       registeredAgentId: "writer",
//       namespaceId: "acme",
//       userId: "alice",
//     });
//
//     // 2. Invoke the agent using identity.id as resourceId
//     await agent.run({ namespaceId, resourceId: identity.id, threadId, task });
//
//     // 3. Touch lastActiveAt so list() ordering reflects real activity
//     await registry.touch(identity.id);
//
//     // 4. Wipe everything (registry row + resource memory + threads)
//     await wipeAgentIdentity({ registry, memory, identityId: identity.id });
//
//
// Why a separate registry instead of just using resourceId
// --------------------------------------------------------
// The memory store doesn't know which resourceIds map to which agents
// or which user owns them. The identity registry adds:
//
//   - enumeration ("list all identities for alice across agents")
//   - reverse lookup ("what user owns this resourceId?")
//   - displayName + metadata (UI affordances that don't fit in memory)
//   - lastActiveAt (sorting / staleness without scanning messages)
//   - cascading wipe (delete row + clear matching resource scope in one call)
//
// Without it, callers would have to scan threads to enumerate identities,
// which doesn't scale. With it, the registry is a thin index over the
// memory store.
// ---------------------------------------------------------------------------

export interface AgentIdentity {
  /**
   * Stable opaque id. Convention: `${namespaceId}::${registeredAgentId}::${userId}`
   * (see `composeAgentIdentityId`) so the id is deterministic from the
   * create input — but treat it as opaque, since alternative
   * implementations may pick different formats.
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
