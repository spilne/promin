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
// Skills live in a `VersionedRegistry` like agents and roles. Versions
// are immutable, so a role's skill catalog pins `(id, version)` and
// `loadSkill` delivers exactly the reviewed text. The content fields
// (`description`, `whenToUse`, `body`) sit at the top level — for a skill
// they ARE the substance; `metadata` carries the cross-cutting bits
// (capability gate, tags). The kill switch (`enabled`, per skill id) and
// review state (`trust`, per version) are registry state on the entry.
// ---------------------------------------------------------------------------

import type {
  RegisterVersionInput,
  Versioned,
  VersionedRegistry,
} from "../versioned-registry/types.ts";

/** What a skill version stores. */
export interface SkillRecipe {
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
   * in the workflow journal and survives replay.
   */
  readonly body: string;
  readonly metadata: SkillMetadata;
}

/** `register` input body. */
export interface SkillRecipeInput {
  readonly description: string;
  /**
   * When the model should reach for this skill. Optional: the markdown
   * ecosystem (SKILL.md) folds "when to use" into a single `description`,
   * so a skill may omit this — the store falls back to `description`.
   */
  readonly whenToUse?: string;
  readonly body: string;
  readonly metadata?: Partial<SkillMetadata>;
}

/**
 * A stored skill version. The catalog surfaces `description` + `whenToUse`;
 * `loadSkill` delivers `body`. `trust: "needs-review"` versions are hidden
 * from catalogs and refused by `loadSkill` until an operator reviews them;
 * `enabled: false` turns the whole skill off.
 */
export type RegisteredSkill = Versioned<SkillRecipe>;

export interface SkillMetadata {
  /**
   * Capability gate. When non-empty, a host may choose to surface this
   * skill in an agent's catalog only if the agent's capabilities grant one
   * of these — same fail-closed spirit as elevated-tool gating. Empty
   * means "ungated".
   */
  readonly capabilities: ReadonlyArray<string>;
  readonly tags: ReadonlyArray<string>;
}

/** `SkillRegistry.register` input. */
export type RegisterSkillInput = RegisterVersionInput<SkillRecipeInput>;

/** `SkillRegistry.list` filter; every field narrows. */
export interface SkillFilter {
  /** Only skills whose `metadata.capabilities` include this. */
  readonly capability?: string;
  /** Only skills whose `metadata.tags` include this. */
  readonly tag?: string;
}

/** Versioned store of skill manifests. */
export type SkillRegistry = VersionedRegistry<SkillRecipe, SkillFilter, SkillRecipeInput>;

/**
 * A reference to a skill from a role's catalog. `version` pins a specific
 * revision; omit it to resolve the skill's active version at
 * catalog-resolution time (the concrete version is then recorded so the
 * catalog stays reproducible for `loadSkill`).
 */
export interface SkillRef {
  readonly id: string;
  readonly version?: string;
}

/** Default `SkillMetadata` shape for new registrations. */
export const DEFAULT_SKILL_METADATA: SkillMetadata = {
  capabilities: [],
  tags: [],
};
