// ---------------------------------------------------------------------------
// Procedural learning. The consolidator notices instructions a user gives
// the agent ("keep answers short", "don't use emoji") and records them as
// rule proposals with the threads they came from. A proposal never reaches
// a prompt on its own: `staticRules` are instructions, so only a person
// promotes one, through `approveRuleProposal`.
// ---------------------------------------------------------------------------

import {
  MAX_STATIC_RULES_CHARS,
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
 * Approve a proposal: mark it approved, then append it to the resource's
 * `staticRules` as a bullet. Returns the proposal, or null when the id is
 * unknown or already decided (nothing changes then; the status update is the
 * store's atomic pending check, so two approvals of one proposal append it
 * once). Throws, changing nothing, when the rule would not fit in
 * `MAX_STATIC_RULES_CHARS`. This is the one step that turns learned text into
 * an instruction, so call it only from a person's action.
 *
 * The `staticRules` write is a read-modify-write: approving two proposals
 * at the same instant, or while an operator edits the rules, can lose one
 * of the writes. Serialize approvals for a resource.
 */
export async function approveRuleProposal(params: {
  readonly store: MemoryStore;
  readonly key: ScopedKey;
  readonly proposalId: string;
}): Promise<RuleProposal | null> {
  const { store, key, proposalId } = params;
  const pending = (await store.listResourceRuleProposals(key, { status: "pending" })).find(
    (p) => p.id === proposalId,
  );
  if (!pending) return null;
  const existing = (await store.getResource(key))?.staticRules?.trim() ?? "";
  const rules = appendRule({ existing, text: pending.text });
  if (rules.length > MAX_STATIC_RULES_CHARS) {
    throw new Error(
      `Cannot approve the rule: staticRules would exceed ${MAX_STATIC_RULES_CHARS} characters.`,
    );
  }
  const decided = await store.decideResourceRuleProposal(key, {
    id: proposalId,
    status: "approved",
  });
  if (decided && rules !== existing) await store.upsertResource(key, { staticRules: rules });
  return decided;
}

/** `existing` rules with `text` added as a bullet, unless a bullet already says the same. */
function appendRule(params: { readonly existing: string; readonly text: string }): string {
  const { existing, text } = params;
  const wanted = normalizeRuleText(text);
  const present = existing
    .split("\n")
    .some((line) => normalizeRuleText(line.replace(/^-\s*/, "")) === wanted);
  if (present) return existing;
  const bullet = `- ${text.replace(/\s+/g, " ").trim()}`;
  return existing ? `${existing}\n${bullet}` : bullet;
}
