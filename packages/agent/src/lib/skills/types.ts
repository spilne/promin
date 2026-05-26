// ---------------------------------------------------------------------------
// `SkillRegistry` — the "what reusable instruction blocks can an agent pull
// in on demand" table. Sibling of `AgentRegistry`.
//
// A skill is a versioned, JSON-serializable instruction block — a framework
// for HOW to do something (structured debugging, a writing rubric, a review
// checklist). It is NOT a tool and carries no code: `body` is markdown the
// model reads. An agent recipe references a CATALOG of skills it may use;
// the resolver injects only their `description` + `whenToUse` into the
// system prompt, and the model pulls a full `body` into context on demand
// via the `loadSkill` tool.
//
// Design choices (deliberately parallel to `AgentRegistry`):
//
// 1. `version` is part of the primary key, defaulting to `"v1"`. The
//    agent's catalog pins `(id, version)` so a recipe's skill set is
//    reproducible.
//
// 2. Skill versions are IMMUTABLE BY CONVENTION — publish a new version
//    rather than mutating an existing one. This is what lets a future
//    "lean reference-journaling" loadSkill (journal `id@version`, re-resolve
//    the body live) stay replay-safe. The MVP registry does not yet enforce
//    immutability or retention; `register` replaces on the same key, exactly
//    like `AgentRegistry`. Enforcement (soft-delete + retention) is tracked
//    separately.
//
// 3. The content fields (`description`, `whenToUse`, `body`) sit at the top
//    level rather than under `metadata` — unlike `AgentMetadata.description`.
//    For a skill these ARE the substance, and the catalog builder reads
//    `description` + `whenToUse` directly. `metadata` carries only the
//    cross-cutting bits (capabilities gate, tags, kill-switch).
// ---------------------------------------------------------------------------

/**
 * A registered skill — a versioned, JSON-serializable instruction block.
 * The catalog surfaces `description` + `whenToUse`; `loadSkill` delivers
 * `body` into the conversation on demand.
 */
export interface RegisteredSkill {
  readonly id: string;
  readonly version: string;
  /** One-line summary shown in the agent's skill catalog. Keep it short. */
  readonly description: string;
  /**
   * Guidance for the model on WHEN to reach for this skill, shown in the
   * catalog beside `description`. This is what the model matches against
   * its current task to decide whether to call `loadSkill`.
   */
  readonly whenToUse: string;
  /**
   * The full instruction block (markdown). Never injected into the system
   * prompt — delivered only as the `loadSkill` tool result, so it is pinned
   * in the workflow journal and survives replay. Keeping it out of the
   * catalog is deliberate: see the loadSkill design notes.
   */
  readonly body: string;
  readonly metadata: SkillMetadata;
  readonly createdAt: number;
  readonly updatedAt: number;
}

export interface SkillMetadata {
  /**
   * Capability gate. When non-empty, a host may choose to surface this
   * skill in an agent's catalog only if the agent's
   * `metadata.capabilities` grant one of these — same fail-closed spirit
   * as elevated-tool gating. Empty means "ungated".
   */
  readonly capabilities: ReadonlyArray<string>;
  readonly tags: ReadonlyArray<string>;
  /**
   * Operator kill-switch. `enabled !== false` (default) means the skill is
   * catalogable and loadable; setting `enabled: false` keeps the row (so
   * history / references stay browsable) but hides it from new catalogs and
   * makes `loadSkill` refuse it. Mirrors `AgentMetadata.enabled` and the
   * tool `enabled` flag.
   */
  readonly enabled?: boolean;
}

/**
 * Caller-facing input to `register`. The store assigns `createdAt` /
 * `updatedAt` and defaults `version` to `"v1"` when omitted.
 */
export interface RegisterSkillInput {
  readonly id: string;
  /** Defaults to `"v1"` when omitted. */
  readonly version?: string;
  readonly description: string;
  /**
   * When the model should reach for this skill. Optional: the markdown
   * ecosystem (SKILL.md) folds "when to use" into a single `description`,
   * so a skill may omit this — the store falls back to `description`. When
   * authored explicitly it's the more specific trigger the catalog shows.
   */
  readonly whenToUse?: string;
  readonly body: string;
  readonly metadata?: Partial<SkillMetadata>;
}

/** Filter / pagination params for `list`. */
export interface ListSkillsParams {
  /** Restrict to skills whose `metadata.capabilities` include this. */
  readonly capability?: string;
  /** Restrict to skills whose `metadata.tags` include this. */
  readonly tag?: string;
  readonly limit?: number;
  readonly cursor?: string;
  /** Defaults to "createdDesc" (most recent first). */
  readonly order?: "createdAsc" | "createdDesc" | "idAsc";
}

/**
 * Versioned registry of skill manifests. Sibling of `AgentRegistry`.
 * Implementations:
 *   - `InMemorySkillRegistry` (reference, in this package)
 *   - future Postgres parity (cf. the agent-registry Postgres ticket)
 *
 * Every implementation must pass `skillRegistryTestSuite`.
 */
export interface SkillRegistry {
  /**
   * Register or replace a skill at `(id, version)`. When the row exists,
   * the content fields + `metadata` are replaced; `createdAt` is preserved
   * and `updatedAt` advances.
   *
   * NOTE the immutability convention (see module header): callers SHOULD
   * publish a new `version` rather than re-registering an existing one.
   * Replace semantics exist for parity with `AgentRegistry` and for
   * fixing a freshly-authored skill before anything references it.
   */
  register(input: RegisterSkillInput): Promise<RegisteredSkill>;

  /**
   * Look up by id. When `version` is omitted, returns the most recently
   * updated version of that id.
   */
  get(id: string, version?: string): Promise<RegisteredSkill | null>;

  /** List, optionally filtered + paginated. */
  list(params?: ListSkillsParams): Promise<RegisteredSkill[]>;

  /** All versions of one id, in ascending `createdAt` order. */
  versions(id: string): Promise<RegisteredSkill[]>;

  /**
   * Remove a row. When `version` is omitted, removes ALL versions of `id`.
   * No-op when nothing matches.
   */
  unregister(id: string, version?: string): Promise<void>;
}

/**
 * A reference to a skill from an agent recipe's catalog. `version` pins a
 * specific revision; omit it to resolve the registry's latest at
 * catalog-resolution time (the concrete version is then recorded so the
 * catalog stays reproducible for `loadSkill`).
 */
export interface SkillRef {
  readonly id: string;
  readonly version?: string;
}

/** Default version assigned to new registrations when caller doesn't specify. */
export const DEFAULT_SKILL_VERSION = "v1";

/** Default `SkillMetadata` shape for new registrations. */
export const DEFAULT_SKILL_METADATA: SkillMetadata = {
  capabilities: [],
  tags: [],
};
