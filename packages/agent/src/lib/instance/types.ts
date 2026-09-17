// ---------------------------------------------------------------------------
// AgentInstance — a long-lived per-(agent, tenant, owner) instance of an
// agent recipe. Class/instance, where `RegisteredAgent` is the class and
// `AgentInstance` is the live thing with its own state.
//
//     RegisteredAgent (recipe)         AgentInstance (live)
//     ----------------                 -------------------
//     id: "writer"                     id: "acme::writer::alice"
//     model, systemPrompt, tools       ownerId: "alice"
//     metadata.capabilities            displayName, metadata
//
// `ownerId` is intentionally generic — it can be a human user, a team, a
// project, a device, or any other addressable entity the caller's system
// recognises. The registry treats it as an opaque non-empty string.
//
//
// The problem
// -----------
// Without instances, an HTTP caller invokes an agent like this:
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
//     AgentInstance = {
//       id: "acme::writer::alice",        // deterministic, used as resourceId
//       registeredAgentId: "writer",      // which recipe
//       namespaceId: "acme",              // which tenant
//       ownerId: "alice",                 // for whom (user, team, project, ...)
//       displayName: string | null,
//       metadata: {},
//       createdAt
//     }
//
// `id` is just `${namespaceId}::${registeredAgentId}::${ownerId}` (see
// `composeAgentInstanceId`). Deterministic — you can compute it from the
// inputs without round-tripping through the registry.
//
//
// The trick: id is reused as resourceId
// -------------------------------------
// This is the whole mechanism. When the agent is invoked, `instance.id`
// is passed as `resourceId`:
//
//     namespaceId: "acme"
//     resourceId:  "acme::writer::alice"   ← instance.id
//     threadId:    "t-2026-04-27-abc"
//
// The existing MemoryStore cascade reads/writes against that resourceId.
// So:
//   - working memory at (acme, acme::writer::alice) is private to
//     alice's writer
//   - facts at (acme, acme::writer::alice) are private to alice's writer
//   - (acme, acme::reviewer::alice) is a different row → reviewer can't
//     see writer's notes
//
// No new memory tier. The instance is metadata + a clever resourceId
// convention, nothing more.
//
//
// Two layers
// ----------
//     ┌────────────────────────────────────────┐
//     │  AgentInstanceRegistry                  │  ← thin index
//     │  - who has what agent                   │
//     │  - displayName, metadata                │
//     └────────────────────────────────────────┘
//                       │ instance.id used as
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
//     const instance = await registry.resolveOrCreate({
//       registeredAgentId: "writer",
//       namespaceId: "acme",
//       ownerId: "alice",
//     });
//
//     // 2. Invoke the agent using instance.id as resourceId
//     await agent.run({ namespaceId, resourceId: instance.id, threadId, task });
//
//     // 3. Wipe everything (registry row + resource memory + threads)
//     await wipeAgentInstance({ registry, memory, instanceId: instance.id });
//
//
// Why a separate registry instead of just using resourceId
// --------------------------------------------------------
// The memory store doesn't know which resourceIds map to which agents
// or which owner they belong to. The instance registry adds:
//
//   - enumeration ("list all instances for owner alice across agents")
//   - reverse lookup ("what owner does this resourceId belong to?")
//   - displayName + metadata (UI affordances that don't fit in memory)
//   - cascading wipe (delete row + clear matching resource scope in one call)
//
// Without it, callers would have to scan threads to enumerate instances,
// which doesn't scale. With it, the registry is a thin index over the
// memory store.
//
//
// This is opt-in — both models are first-class
// --------------------------------------------
// Instances are one valid memory model, not the only one. The agent
// runtime accepts any string as `resourceId`; the registry just gives
// you the per-(agent, owner) flavour with bookkeeping. Pick per call:
//
//   resourceId: "alice"                  → owner-centric (shared)
//   ----------------------------------------------------------------------
//   alice has ONE working memory + facts row in the namespace. Every agent
//   she talks to reads and writes the same row. If the writer learns
//   "alice prefers terse", the reviewer sees it on its next turn. Good
//   when the org wants a single shared mental model of the owner.
//
//   resourceId: instance.id              → agent-centric (isolated)
//   ----------------------------------------------------------------------
//   alice has a separate working memory + facts row per (agent, owner)
//   pair. The writer's scratchpad is invisible to the reviewer. Good
//   when each agent has its own job and shouldn't pollute its peers'
//   context — or when the owner wants to keep multiple long-lived
//   instances of the same agent that drift independently.
//
// Both can coexist in one deployment. A "shared org memory of alice" can
// live at resourceId="alice" while specialised long-lived instances live
// at composed instance ids. Namespace scope already gives you a third
// tier above all of this (org-wide policy + facts that flow into every
// turn regardless of resource). See `packages/agent/src/lib/memory/types.ts`
// for the cascade.
// ---------------------------------------------------------------------------

export interface AgentInstance {
  /**
   * Stable opaque id. Convention: `${namespaceId}::${registeredAgentId}::${ownerId}`
   * (see `composeAgentInstanceId`) so the id is deterministic from the
   * create input — but treat it as opaque, since alternative
   * implementations may pick different formats.
   *
   * Used as `resourceId` when invoking the agent so the memory cascade
   * scopes working memory + facts + episodes per-(agent, owner).
   */
  readonly id: string;
  readonly registeredAgentId: string;
  readonly namespaceId: string;
  /**
   * External owner identifier — the entity this agent instance is for.
   * Can be a user, team, project, device, or any other addressable
   * entity. Treated as opaque by the registry (no shape constraints
   * beyond non-empty string).
   */
  readonly ownerId: string;
  /** Optional human-friendly label. UI can fall back to `${registeredAgentId} for ${ownerId}`. */
  readonly displayName: string | null;
  readonly metadata: Readonly<Record<string, unknown>>;
  readonly createdAt: number;
}

export interface CreateAgentInstanceInput {
  readonly registeredAgentId: string;
  readonly namespaceId: string;
  readonly ownerId: string;
  readonly displayName?: string | null;
  readonly metadata?: Readonly<Record<string, unknown>>;
}

export interface ListAgentInstancesParams {
  readonly namespaceId?: string;
  readonly ownerId?: string;
  readonly registeredAgentId?: string;
  readonly limit?: number;
  /** Defaults to "createdDesc". */
  readonly order?: "createdAsc" | "createdDesc";
}

export interface UpdateAgentInstancePatch {
  readonly displayName?: string | null;
  readonly metadata?: Readonly<Record<string, unknown>>;
}

/**
 * Index of long-lived agent instances, keyed by (registeredAgentId, namespaceId, ownerId).
 *
 * Implementations: `InMemoryAgentInstanceRegistry` (reference) and
 * `SqliteAgentInstanceRegistry` (persistent). Both must pass the shared
 * conformance suite.
 */
export interface AgentInstanceRegistry {
  /**
   * Idempotent: returns the existing instance for the same triple, or
   * creates one. `displayName` and `metadata` from the input only apply
   * on creation — call `update` to change them later.
   */
  resolveOrCreate(input: CreateAgentInstanceInput): Promise<AgentInstance>;

  get(id: string): Promise<AgentInstance | null>;

  list(params?: ListAgentInstancesParams): Promise<AgentInstance[]>;

  update(id: string, patch: UpdateAgentInstancePatch): Promise<AgentInstance>;

  /**
   * Removes the instance row only. Memory under `resourceId = id` is the
   * caller's responsibility — see `wipeAgentInstance` for the cascading
   * helper that drops both the row and the resource-scope state.
   */
  delete(id: string): Promise<void>;
}

/**
 * Compose a deterministic instance id from (namespaceId, registeredAgentId, ownerId).
 * Exported so callers that don't want to round-trip through the registry
 * (e.g. when invoking an agent for the first time) can compute the id
 * themselves and pass it directly as `resourceId`.
 *
 * Namespace is part of the id so two tenants with the same ownerId for
 * the same agent recipe don't collide on the registry row — multi-tenant
 * is the default, not an opt-in.
 *
 * Delimiter `::` was chosen to avoid collision with characters that
 * commonly appear in agent ids (`/`, `-`, `:`, alphanumerics).
 */
export function composeAgentInstanceId(input: {
  readonly namespaceId: string;
  readonly registeredAgentId: string;
  readonly ownerId: string;
}): string {
  return `${input.namespaceId}::${input.registeredAgentId}::${input.ownerId}`;
}
