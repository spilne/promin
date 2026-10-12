// ---------------------------------------------------------------------------
// Prompt fragments — small, operator-curated markdown blocks that roles
// compose into their system prompt (`systemPrompt.layers`). Always-on;
// concatenated at resolve time. Distinct from skills (load-on-demand).
//
// Fragments live in a `VersionedRegistry` like every other recipe kind, so
// every replica reads the same durable store (no per-process cache to
// drift) and a role can pin a fragment version with `"<id>@<version>"`.
// File-scanned and operator-authored fragments share the store; scanned
// versions carry `source: "file"`, and scanner sync only removes those.
// ---------------------------------------------------------------------------

import type {
  RegisterVersionInput,
  Versioned,
  VersionedRegistry,
} from "../versioned-registry/types.ts";

/** What a fragment version stores. */
export interface FragmentRecipe {
  /** The markdown block concatenated into the system prompt. */
  readonly content: string;
}

/** A stored fragment version; `id` is the fragment key roles layer by. */
export type RegisteredFragment = Versioned<FragmentRecipe>;

/** `FragmentRegistry.register` input. */
export type RegisterFragmentInput = RegisterVersionInput<FragmentRecipe>;

/** Fragments have no list filter. */
export type FragmentFilter = Readonly<Record<string, never>>;

/** Versioned store of prompt fragments. */
export type FragmentRegistry = VersionedRegistry<FragmentRecipe, FragmentFilter>;

/** Separator of a pinned layer reference: `"<fragment id>@<version>"`. */
export const FRAGMENT_VERSION_SEPARATOR = "@";
