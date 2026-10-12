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
// Roles live in a `VersionedRegistry` like agents, skills, DAGs and
// fragments: immutable versions, an active pointer, no tenant column
// (tenancy binds at the agent/instance layer). Agents pin a role version
// for reproducibility or float to the active one for live fixes.
// ---------------------------------------------------------------------------

import type {
  RegisterVersionInput,
  Versioned,
  VersionedRegistry,
} from "../versioned-registry/types.ts";

/**
 * The behavioral definition a role carries. JSON-serializable; the runtime
 * expands `systemPrompt` layers through the `FragmentRegistry` and resolves
 * `skills` against the `SkillRegistry` at agent-resolve time.
 */
export interface RoleDefinition {
  /**
   * Persona. Either a plain string OR the layered `{ base, layers }` form,
   * where `layers` are fragment ids concatenated onto `base`. A layer
   * `"<id>@<version>"` pins that fragment version; a bare `"<id>"` follows
   * the fragment's active version. `null` means "no persona of its own" —
   * the agent runs on memory cascade alone.
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

/**
 * How an agent binds a role. Two shapes, one resolved code path:
 *   - `ref`    — points at a `(id, version?)` in the RoleRegistry. Omitting
 *                `version` follows the role's active version, so promoting
 *                a new role version changes every agent that refs it. A
 *                pinned version fixes the definition; layers it names by
 *                bare fragment id still follow those fragments' active
 *                versions (pin them as `"<id>@<version>"` too).
 *   - `inline` — carries the definition directly on the agent. A one-off,
 *                no shared link. "Save as role" lifts it into the registry
 *                and swaps the binding to a `ref`.
 */
export type RoleBinding =
  | { readonly ref: { readonly id: string; readonly version?: string } }
  | { readonly inline: RoleDefinition };

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

/** What a role version stores. */
export interface RoleRecipe {
  readonly definition: RoleDefinition;
  readonly metadata: RoleMetadata;
}

/** `register` input body: metadata fields default when omitted. */
export interface RoleRecipeInput {
  readonly definition: RoleDefinition;
  readonly metadata?: Partial<RoleMetadata>;
}

/**
 * A stored role version — JSON-serializable, immutable. An agent binds one
 * by reference or carries a definition inline.
 */
export type RegisteredRole = Versioned<RoleRecipe>;

/** `RoleRegistry.register` input. */
export type RegisterRoleInput = RegisterVersionInput<RoleRecipeInput>;

/** `RoleRegistry.list` filter; every field narrows. */
export interface RoleFilter {
  /** Only roles whose `definition.capabilities` include this. */
  readonly capability?: string;
  /** Only roles whose `metadata.tags` include this. */
  readonly tag?: string;
}

/** Versioned store of role definitions. */
export type RoleRegistry = VersionedRegistry<RoleRecipe, RoleFilter, RoleRecipeInput>;

/** Default `RoleMetadata` shape for new registrations. */
export const DEFAULT_ROLE_METADATA: RoleMetadata = {
  description: null,
  tags: [],
};
