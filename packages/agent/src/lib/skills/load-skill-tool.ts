// ---------------------------------------------------------------------------
// `loadSkill` — the on-demand skill loader. A plain `AgentTool` the model
// calls with a skill id to pull that skill's full instruction body into the
// conversation.
//
// DETERMINISM (Option A): loadSkill is an ordinary tool, so the agent loop
// runs it inside a journaled tool-execution activity. Its return value (the
// body) is journaled exactly like any other tool result, so a workflow
// replay returns the body bit-identically — no special replay machinery.
// (A leaner variant that journals only `id@version` and re-resolves the body
// from an immutable registry is a deferred optimization.)
//
// The tool is bound to the agent's RESOLVED catalog: it can only load skills
// the recipe pinned, and it loads exactly the pinned version. The model
// passes only an `id`; it cannot reach skills outside the catalog or pick a
// version. This keeps the loadable surface equal to what the catalog block
// in the system prompt advertised.
// ---------------------------------------------------------------------------

import { z } from "zod";
import { tool, type AgentTool } from "../tool.ts";
import type { ResolvedSkillEntry } from "./resolve-skill-catalog.ts";
import type { SkillRegistry } from "./types.ts";

export const LOAD_SKILL_TOOL_NAME = "loadSkill";

export interface LoadSkillToolConfig {
  /** Registry the tool reads bodies from at call time. */
  readonly registry: SkillRegistry;
  /**
   * The agent's resolved catalog — pins `id → version` and bounds which
   * skills are loadable. Typically the output of `resolveSkillCatalog`.
   */
  readonly catalog: ReadonlyArray<ResolvedSkillEntry>;
}

export interface LoadSkillOutput {
  readonly id: string;
  readonly version: string;
  readonly body: string;
}

/**
 * Build a `loadSkill` tool bound to a resolved catalog. Returns a tool whose
 * model-facing output is the skill body, so the loaded instructions land in
 * the conversation for the model to follow on the next step.
 */
export function createLoadSkillTool(
  config: LoadSkillToolConfig,
): AgentTool<{ id: string }, LoadSkillOutput | { error: string }> {
  // Pin id → version from the catalog so the model can't pick a version and
  // can't load anything the catalog didn't advertise.
  const pinned = new Map(config.catalog.map((e) => [e.id, e.version] as const));
  const ids = config.catalog.map((e) => e.id);

  return tool({
    name: LOAD_SKILL_TOOL_NAME,
    description:
      "Load a skill's full instructions into the conversation. Skills are listed under " +
      "'## Skills' in your system prompt with a 'use when' for each. Call this with the " +
      "skill's id when your task matches; then follow the instructions it returns.",
    usage:
      ids.length > 0
        ? `Available skill ids: ${ids.map((i) => `"${i}"`).join(", ")}.`
        : "No skills are available for this agent.",
    parameters: z.object({
      id: z.string().min(1).describe("The id of the skill to load, as shown in '## Skills'."),
    }),
    toModelOutput: (out) => ("error" in out ? out.error : out.body),
    toResultMetadata: (out) =>
      "error" in out ? undefined : { skillId: out.id, skillVersion: out.version },
    execute: async ({ id }) => {
      const version = pinned.get(id);
      if (version === undefined) {
        const known = ids.length > 0 ? ids.map((i) => `"${i}"`).join(", ") : "(none)";
        return {
          error:
            `Unknown skill "${id}". This agent's loadable skills are: ${known}. ` +
            "Use one of those ids exactly as listed under '## Skills'.",
        };
      }
      const row = await config.registry.get(id, version);
      if (!row) {
        // The catalog pinned this version at resolve time but the registry
        // no longer has it (e.g. unregistered mid-session). Surface clearly
        // rather than throwing — the model gets a usable tool result.
        return {
          error:
            `Skill "${id}@${version}" was in your catalog but is no longer available in the ` +
            "registry. Proceed without it or tell the user it's unavailable.",
        };
      }
      return { id: row.id, version: row.version, body: row.body };
    },
  });
}
