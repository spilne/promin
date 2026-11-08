// ---------------------------------------------------------------------------
// Procedural learning. The consolidator notices instructions a user gives
// the agent ("keep answers short", "don't use emoji") and records them as
// rule proposals with the threads they came from. A proposal never reaches
// a prompt on its own: `staticRules` are instructions, so only a person
// promotes one, through `approveRuleProposal`.
// ---------------------------------------------------------------------------

import {
  MAX_STATIC_RULES_CHARS,
  MemoryLimitError,
  type MemoryStore,
  type RuleProposal,
  type ScopedKey,
} from "./types.ts";

/** Most pending proposals one resource holds; the consolidator stops adding past it. */
export const MAX_PENDING_RULE_PROPOSALS = 50;

/** Proposals compare case-insensitively, ignoring spacing and trailing punctuation. */
export function normalizeRuleText(text: string): string {
  return text
    .toLowerCase()
    .replace(/\s+/g, " ")
    .trim()
    .replace(/[.!?;:,\s]+$/u, "");
}

/**
 * Approve a proposal: add it to the resource's `staticRules` as a bullet and
 * mark it approved, as one atomic store step. Returns the proposal, or null
 * when the id is unknown or already decided (a proposal someone rejected a
 * moment earlier never becomes a rule). Throws `MemoryLimitError`, changing
 * nothing, when the rule would not fit in `MAX_STATIC_RULES_CHARS`. This is
 * the one step that turns learned text into an instruction, so call it only
 * from a person's action.
 */
export async function approveRuleProposal(params: {
  readonly store: MemoryStore;
  readonly key: ScopedKey;
  readonly proposalId: string;
}): Promise<RuleProposal | null> {
  return params.store.approveResourceRuleProposal(params.key, {
    id: params.proposalId,
    maxChars: MAX_STATIC_RULES_CHARS,
  });
}

/**
 * `existing` rules with `text` added as a bullet, unless a bullet already says
 * the same. Throws `MemoryLimitError` when the result passes `maxChars`.
 * Stores call this inside their atomic `appendResourceStaticRule`.
 */
export function appendStaticRule(params: {
  readonly existing: string | null;
  readonly text: string;
  readonly maxChars: number;
}): string {
  const existing = params.existing?.trim() ?? "";
  const wanted = normalizeRuleText(params.text);
  const present = existing
    .split("\n")
    .some((line) => normalizeRuleText(line.replace(/^-\s*/, "")) === wanted);
  if (present) return existing;
  const bullet = `- ${params.text.replace(/\s+/g, " ").trim()}`;
  const rules = existing ? `${existing}\n${bullet}` : bullet;
  if (rules.length > params.maxChars) {
    throw new MemoryLimitError(
      `Cannot add the rule: staticRules would exceed ${params.maxChars} characters.`,
    );
  }
  return rules;
}
