// ---------------------------------------------------------------------------
// Route parts of the memory to different backends: the entity graph to a
// graph database, rule proposals to another store, everything else to the
// main store. Each part is a narrow slice of `MemoryStore`, so a backend
// implements only its slice.
//
// Backends are not joined: there is no transaction across them, and removing
// a resource from the base store does not remove its rows in another one.
// ---------------------------------------------------------------------------

import { appendStaticRule } from "./rule-proposals.ts";
import type { MemoryStore } from "./types.ts";

/** The entity-graph slice of `MemoryStore`. */
export type RelationStore = Pick<
  MemoryStore,
  "appendResourceRelation" | "listResourceRelations" | "deleteResourceRelation"
>;

/** The rule-proposal slice of `MemoryStore`. */
export type RuleProposalStore = Pick<
  MemoryStore,
  "recordResourceRuleProposal" | "listResourceRuleProposals" | "decideResourceRuleProposal"
>;

const RELATION_METHODS: ReadonlyArray<keyof RelationStore> = [
  "appendResourceRelation",
  "listResourceRelations",
  "deleteResourceRelation",
];

const RULE_PROPOSAL_METHODS: ReadonlyArray<keyof RuleProposalStore> = [
  "recordResourceRuleProposal",
  "listResourceRuleProposals",
  "decideResourceRuleProposal",
];

/**
 * `base` with its relation and/or rule-proposal methods served by the given
 * stores. With neither, returns `base` itself.
 */
export function composeMemoryStore(params: {
  readonly base: MemoryStore;
  readonly relations?: RelationStore;
  readonly ruleProposals?: RuleProposalStore;
}): MemoryStore {
  const { base, relations, ruleProposals } = params;
  if (!relations && !ruleProposals) return base;

  const owners = new Map<PropertyKey, object>();
  const overrides = new Map<PropertyKey, unknown>();
  if (relations) for (const name of RELATION_METHODS) owners.set(name, relations);
  if (ruleProposals) {
    for (const name of RULE_PROPOSAL_METHODS) owners.set(name, ruleProposals);
    overrides.set("approveResourceRuleProposal", approveAcrossStores({ base, ruleProposals }));
  }

  return new Proxy(base, {
    get(target, name) {
      if (overrides.has(name)) return overrides.get(name);
      const owner = owners.get(name) ?? target;
      const value: unknown = Reflect.get(owner, name);
      return typeof value === "function" ? value.bind(owner) : value;
    },
  });
}

/**
 * Approval when proposals live apart from the resource: no transaction spans
 * both, so the proposal is decided first (its atomic pending check lets only
 * one approval or rejection win) and the rule is appended after. A failure in
 * between leaves the proposal approved without its rule, never a rule from a
 * rejected proposal. The size limit is checked before deciding.
 */
function approveAcrossStores(params: {
  readonly base: MemoryStore;
  readonly ruleProposals: RuleProposalStore;
}): MemoryStore["approveResourceRuleProposal"] {
  const { base, ruleProposals } = params;
  return async (key, { id, maxChars }) => {
    const pending = (
      await ruleProposals.listResourceRuleProposals(key, { status: "pending" })
    ).find((p) => p.id === id);
    if (!pending) return null;
    // Throws MemoryLimitError before anything is decided.
    appendStaticRule({
      existing: (await base.getResource(key))?.staticRules ?? null,
      text: pending.text,
      maxChars,
    });
    const decided = await ruleProposals.decideResourceRuleProposal(key, { id, status: "approved" });
    if (decided) await base.appendResourceStaticRule(key, { text: decided.text, maxChars });
    return decided;
  };
}
