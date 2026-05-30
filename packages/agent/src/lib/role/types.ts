// ---------------------------------------------------------------------------
// `RoleRegistry` — the "what this agent is and can do" table.
//
// A role is a reusable, version-keyed BEHAVIORAL bundle: a persona prompt
// (plain or layered over fragments), the tools it uses, and the skills it
// may load. It deliberately does NOT name a model, a credential, or a token
// budget — those are deployment concerns owned by the agent that binds the
// role. "Create a new agent WITH role X" → the agent references the role.
//
// This sits one tier above fragments (a fragment is a single shared text
// block; a role composes fragments and adds tools + skills) and one tier
// below the agent (an agent = role + model + credentials + runtime).
//
// Design choices mirror `AgentRegistry` on purpose — same versioning,
// upsert, and list semantics — so the two stores share a conformance
// shape and operators reason about them the same way:
//
//   1. `version` is part of the primary key, defaulting to `"v1"`. Lets a
//      role evolve while agents pin a specific version for reproducibility
//      (or float to latest for live fixes).
//   2. No tenant column — roles are tenant-agnostic definitions. Tenancy
//      binds at the agent/instance layer.
// ---------------------------------------------------------------------------

/**
 * The behavioral definition a role carries. JSON-serializable; the runtime
 * expands `systemPrompt` layers through the `FragmentRegistry` and resolves
 * `skills` against the `SkillRegistry` at agent-resolve time.
 */
export interface RoleDefinition {
  /**
   * Persona. Either a plain string OR the layered `{ base, layers }` form,
   * where `layers` are fragment keys concatenated onto `base` (same shape
   * `LocalAgentBackend.systemPrompt` accepts today). `null` means "no
   * persona of its own" — the agent runs on memory cascade alone.
   */
  readonly systemPrompt:
    | string
    | { readonly base: string; readonly layers?: ReadonlyArray<string> }
    | null;
  /** Tool names the host must have wired (same contract as the agent recipe). */
  readonly tools: ReadonlyArray<string>;
  /**
   * Skill catalog refs. Each pins a skill `(id, version?)`; the resolver
   * injects each skill's description + whenToUse into the prompt and
   * auto-attaches `loadSkill`. Omitted/empty means no skills.
   */
  readonly skills?: ReadonlyArray<{ readonly id: string; readonly version?: string }>;
  /**
   * Capabilities this behavior claims/needs. Carried here (not on the
   * agent) because they describe the role; the agent that binds the role
   * still enforces them at resolve time via `filterToolsByCapability`.
   */
  readonly capabilities?: ReadonlyArray<string>;
}

export interface RoleMetadata {
  readonly description: string | null;
  readonly tags: ReadonlyArray<string>;
  /**
   * UX hint: credential names an agent built from this role will typically
   * need to supply (e.g. `ANTHROPIC_API_KEY`). Purely advisory — the role
   * never holds a credential itself; the agent's `model.credentialRef`
   * does. Lets the "create agent from role" flow prompt for the right keys.
   */
  readonly suggestedSecrets?: ReadonlyArray<string>;
}

/**
 * A registered role — JSON-serializable, durable, version-keyed. An agent
 * binds one of these by reference (live link) or carries one inline.
 */
export interface RegisteredRole {
  readonly id: string;
  readonly version: string;
  readonly definition: RoleDefinition;
  readonly metadata: RoleMetadata;
  readonly createdAt: number;
  readonly updatedAt: number;
}

export interface RegisterRoleInput {
  readonly id: string;
  /** Defaults to `"v1"` when omitted. */
  readonly version?: string;
  readonly definition: RoleDefinition;
  readonly metadata?: Partial<RoleMetadata>;
}

/** Filter / pagination params for `list`. */
export interface ListRolesParams {
  /** Restrict to roles whose `definition.capabilities` include this. */
  readonly capability?: string;
  /** Restrict to roles whose `metadata.tags` include this. */
  readonly tag?: string;
  readonly limit?: number;
  readonly cursor?: string;
  /** Defaults to "createdDesc" (most recent first). */
  readonly order?: "createdAsc" | "createdDesc" | "idAsc";
}

/**
 * Versioned registry of role definitions. Implementations:
 *   - `InMemoryRoleRegistry` (reference, in this package)
 *   - `SqliteRoleRegistry` (SQLite, in `@promin/sqlite`)
 *   - `PostgresRoleRegistry` (Postgres, in `@promin/postgres`)
 *
 * All must pass `roleRegistryTestSuite`.
 */
export interface RoleRegistry {
  /**
   * Register or replace a role at `(id, version)`. When the row exists,
   * `definition` and `metadata` are replaced; `createdAt` is preserved and
   * `updatedAt` advances.
   */
  register(input: RegisterRoleInput): Promise<RegisteredRole>;

  /**
   * Look up by id. When `version` is omitted, returns the most recently
   * registered version of that id (sorted by `updatedAt` desc).
   */
  get(id: string, version?: string): Promise<RegisteredRole | null>;

  /** List, optionally filtered + paginated. */
  list(params?: ListRolesParams): Promise<RegisteredRole[]>;

  /** All versions of one id, in ascending `createdAt` order. */
  versions(id: string): Promise<RegisteredRole[]>;

  /**
   * Remove a row. When `version` is omitted, removes ALL versions of `id`.
   * No-op when nothing matches.
   */
  unregister(id: string, version?: string): Promise<void>;
}

/** Default version assigned to new registrations when caller doesn't specify. */
export const DEFAULT_ROLE_VERSION = "v1";

/** Default `RoleMetadata` shape for new registrations. */
export const DEFAULT_ROLE_METADATA: RoleMetadata = {
  description: null,
  tags: [],
};
