// ---------------------------------------------------------------------------
// Procedural learning. The consolidator notices instructions a user gives
// the agent ("keep answers short", "don't use emoji") and records them as
// rule proposals with the threads they came from. A proposal never reaches
// a prompt on its own: `staticRules` are instructions, so only a person
// promotes one, through `approveRuleProposal`.
// ---------------------------------------------------------------------------

import type { MemoryStore, RuleProposal, ScopedKey } from "./types.ts";

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
 * Approve a proposal: append it to the resource's `staticRules` as a bullet
 * and mark it approved. Returns the proposal, or null when the id is unknown
 * or already decided (nothing changes then). This is the one step that turns
 * learned text into an instruction, so call it only from a person's action.
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
  const resource = await store.getResource(key);
  const existing = resource?.staticRules?.trim() ?? "";
  const line = `- ${pending.text.replace(/\s+/g, " ").trim()}`;
  if (
    !existing
      .split("\n")
      .some((l) => normalizeRuleText(l.replace(/^-\s*/, "")) === normalizeRuleText(pending.text))
  ) {
    await store.upsertResource(key, {
      staticRules: existing ? `${existing}\n${line}` : line,
    });
  }
  return store.decideResourceRuleProposal(key, { id: proposalId, status: "approved" });
}
