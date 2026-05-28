// ---------------------------------------------------------------------------
// Resolve an agent recipe's skill catalog and render it for the system
// prompt. This is the async pre-step that lets the synchronous
// `resolveLocalAgent` inject a catalog block — same pattern as
// `resolveCredentialRef` for BYOK keys: the gateway resolves the catalog
// once per request and passes the result into `resolveLocalAgent` deps.
//
// CRITICAL: the catalog carries ONLY `description` + `whenToUse` (+ the
// pinned version). It never carries `body`. Bodies are reconstructed from
// live config on every workflow replay (the system prompt is not journaled),
// so inlining a body would let a later skill edit retroactively rewrite the
// prompt that replayed turns are rebuilt with. Bodies travel ONLY through
// the journaled `loadSkill` tool result. See `load-skill-tool.ts`.
// ---------------------------------------------------------------------------

import type { RegisteredAgent } from "../registry/types.ts";
import type { SkillRef, SkillRegistry } from "./types.ts";

/**
 * A catalog entry as surfaced to the model — the pinned identity plus the
 * two fields the model matches against its task. Deliberately omits `body`.
 */
export interface ResolvedSkillEntry {
  readonly id: string;
  /** The version actually resolved — pins the recipe's catalog. */
  readonly version: string;
  readonly description: string;
  readonly whenToUse: string;
}

export interface ResolveSkillCatalogParams {
  readonly recipe: RegisteredAgent;
  readonly registry: SkillRegistry;
  /**
   * Behavior for a recipe skill ref that resolves to nothing — either no
   * such skill/version, or the row is kill-switched (`enabled: false`).
   * Default `"throw"`. `"skip"` quietly drops it, mirroring
   * `resolveLocalAgent`'s `onUnknownTool: "skip"` for forward-looking refs.
   */
  readonly onMissing?: "throw" | "skip";
}

/**
 * Read each `(id, version?)` the recipe references out of the registry and
 * return the resolved catalog (pinned version + description + whenToUse).
 * Returns `[]` when the backend isn't local or declares no skills.
 *
 * Version pinning: when a ref omits `version`, the registry's latest is
 * used and its concrete version is recorded in the entry — so downstream
 * `loadSkill` fetches exactly the version the catalog showed.
 */
export async function resolveSkillCatalog(
  params: ResolveSkillCatalogParams,
): Promise<ResolvedSkillEntry[]> {
  const { recipe, registry } = params;
  const onMissing = params.onMissing ?? "throw";
  if (recipe.backend.type !== "local") return [];
  const refs: ReadonlyArray<SkillRef> = recipe.backend.skills ?? [];
  if (refs.length === 0) return [];

  const out: ResolvedSkillEntry[] = [];
  for (const ref of refs) {
    const row = await registry.get(ref.id, ref.version);
    if (!row) {
      if (onMissing === "skip") continue;
      throw new Error(
        `resolveSkillCatalog: skill "${ref.id}"${ref.version ? `@${ref.version}` : ""} ` +
          `referenced by recipe "${recipe.id}" but not found in the SkillRegistry. ` +
          "Register the skill or set onMissing: 'skip'.",
      );
    }
    if (row.metadata.enabled === false) {
      if (onMissing === "skip") continue;
      throw new Error(
        `resolveSkillCatalog: skill "${row.id}@${row.version}" is disabled (enabled: false). ` +
          "Re-enable it or remove the reference from the recipe.",
      );
    }
    // Capability gate (fail-closed, SILENT — not subject to onMissing): a
    // skill that declares capabilities is surfaced only when the agent's
    // capabilities grant at least one. Empty skill capabilities = ungated.
    // A policy exclusion, not a misconfiguration — so we drop it quietly,
    // mirroring how `filterToolsByCapability` drops elevated tools.
    if (!skillAllowedByCapabilities(row.metadata.capabilities, recipe.metadata.capabilities)) {
      continue;
    }
    // Trust gate (fail-closed, SILENT). A needs-review skill is in the
    // registry but hasn't been approved by an operator yet — silently keep
    // it out of agent context. The skills manager surfaces these for review.
    if (row.metadata.trust === "needs-review") continue;
    out.push({
      id: row.id,
      version: row.version,
      description: row.description,
      whenToUse: row.whenToUse,
    });
  }
  return out;
}

/**
 * Whether a skill is allowed for an agent given each side's capabilities.
 * A skill with no declared capabilities is ungated (always allowed). When it
 * declares capabilities, the agent must hold at least one of them — the same
 * "grant one of these" / fail-closed rule elevated tools use.
 */
export function skillAllowedByCapabilities(
  skillCapabilities: ReadonlyArray<string>,
  agentCapabilities: ReadonlyArray<string>,
): boolean {
  if (skillCapabilities.length === 0) return true;
  return skillCapabilities.some((c) => agentCapabilities.includes(c));
}

/**
 * Render a resolved catalog as a system-prompt block. Returns `""` when the
 * catalog is empty so callers can unconditionally append. The block teaches
 * the model that skills are load-on-demand and lists each with its
 * "use when" so the model can decide relevance without seeing bodies.
 */
export function buildSkillCatalogPrompt(entries: ReadonlyArray<ResolvedSkillEntry>): string {
  if (entries.length === 0) return "";
  const lines = entries.map((e) => {
    // A SKILL.md-style skill folds the trigger into its description, so
    // whenToUse == description — don't print "Use when:" twice.
    const trigger = e.whenToUse && e.whenToUse !== e.description ? ` Use when: ${e.whenToUse}` : "";
    return `- \`${e.id}\` (${e.version}): ${e.description}${trigger}`;
  });
  return [
    "## Skills",
    "",
    "You have skills available — reusable instruction blocks you can pull into the " +
      "conversation on demand. Each skill below lists when it's useful. When your current " +
      "task matches a skill's \"use when\", call the `loadSkill` tool with that skill's id to " +
      "load its full instructions, then follow them. Don't load skills you don't need.",
    "",
    ...lines,
  ].join("\n");
}
