// ---------------------------------------------------------------------------
// distillThread records the instructions a user gave the agent as rule
// proposals, counted per distinct thread. Nothing becomes a rule until a
// person approves it.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { DefaultConsolidator } from "../consolidator.ts";
import { InMemoryMemoryStore } from "../in-memory-memory-store.ts";
import { approveRuleProposal, MAX_PENDING_RULE_PROPOSALS } from "../rule-proposals.ts";
import type { LLMProvider } from "../../llm-provider.ts";

const RESOURCE = { namespaceId: "acme", resourceId: "alice" };
const thread = (threadId: string) => ({ ...RESOURCE, threadId });

async function setup(params: { corrections: () => string[]; ruleProposals?: boolean }) {
  const store = new InMemoryMemoryStore();
  const llm: LLMProvider = {
    chat: async () => ({
      content: JSON.stringify({
        summary: "chat",
        outcome: null,
        salience: 0.6,
        facts: [],
        corrections: params.corrections(),
      }),
      finishReason: "stop",
    }),
  };
  const consolidator = new DefaultConsolidator({
    store,
    llm,
    minDistillMessages: 1,
    ...(params.ruleProposals !== undefined && { ruleProposals: params.ruleProposals }),
  });
  const talk = async (threadId: string) => {
    await store.appendMessages(thread(threadId), [
      { role: "user", content: "keep it short please" },
      { role: "assistant", content: "ok" },
    ]);
    await consolidator.distillThread(thread(threadId));
  };
  return { store, talk };
}

describe("rule proposals from distillation", () => {
  it("counts distinct threads, ignores a re-distill, and stays inert until approved", async () => {
    const { store, talk } = await setup({
      corrections: () => ["Keep answers short.", "keep answers short"],
    });
    await talk("t1");
    await talk("t2");
    await store.appendMessages(thread("t2"), [{ role: "user", content: "again" }]);
    await new DefaultConsolidator({
      store,
      llm: {
        chat: async () => ({
          content: '{"summary":"s","corrections":["KEEP answers short!"]}',
          finishReason: "stop",
        }),
      },
      minDistillMessages: 1,
    }).distillThread(thread("t2"), { force: true });

    const [proposal, ...rest] = await store.listResourceRuleProposals(RESOURCE);
    expect(rest).toEqual([]);
    expect(proposal!.occurrences).toBe(2);
    expect(proposal!.evidenceThreadIds).toEqual(["t1", "t2"]);
    expect(await store.listResourceRuleProposals(RESOURCE, { minOccurrences: 3 })).toEqual([]);

    // Pending: no rule yet, in the prompt or in the resource row.
    expect((await store.getResource(RESOURCE))?.staticRules ?? null).toBeNull();
    const before = await store.resolveContext(thread("t3"), { maxMessageTokens: 100_000 });
    expect(JSON.stringify(before)).not.toContain("Keep answers short");

    const approved = await approveRuleProposal({ store, key: RESOURCE, proposalId: proposal!.id });
    expect(approved?.status).toBe("approved");
    expect((await store.getResource(RESOURCE))?.staticRules).toBe("- Keep answers short.");
    expect(
      await approveRuleProposal({ store, key: RESOURCE, proposalId: proposal!.id }),
    ).toBeNull();
    const context = await store.resolveContext(thread("t3"), { maxMessageTokens: 100_000 });
    expect(JSON.stringify(context)).toContain("Keep answers short.");
  });

  it("keeps a rejected proposal rejected when it comes up again", async () => {
    const { store, talk } = await setup({ corrections: () => ["Never use emoji"] });
    await talk("t1");
    const [p] = await store.listResourceRuleProposals(RESOURCE);
    await store.decideResourceRuleProposal(RESOURCE, { id: p!.id, status: "rejected" });
    await talk("t2");
    const [again] = await store.listResourceRuleProposals(RESOURCE);
    expect(again!.status).toBe("rejected");
    expect(again!.occurrences).toBe(2);
    expect(await approveRuleProposal({ store, key: RESOURCE, proposalId: p!.id })).toBeNull();
  });

  it("stops adding new proposals at the pending cap, and can be switched off", async () => {
    let n = 0;
    const capped = await setup({ corrections: () => [`Rule number ${n++}`] });
    for (let i = 0; i < MAX_PENDING_RULE_PROPOSALS + 3; i++) await capped.talk(`t${i}`);
    expect(await capped.store.listResourceRuleProposals(RESOURCE)).toHaveLength(
      MAX_PENDING_RULE_PROPOSALS,
    );

    const off = await setup({ corrections: () => ["Be brief"], ruleProposals: false });
    await off.talk("t1");
    expect(await off.store.listResourceRuleProposals(RESOURCE)).toEqual([]);
  });
});
