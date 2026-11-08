// ---------------------------------------------------------------------------
// Memory profile: presets set different behavior, the builder is immutable and
// rejects combinations that cannot work, and routed stores split the memory
// across backends while the rest stays on the base store.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { InMemoryMemoryStore } from "../in-memory-memory-store.ts";
import { memoryProfile } from "../memory-profile.ts";
import { composeMemoryStore } from "../compose-memory-store.ts";

const KEY = { namespaceId: "acme", resourceId: "alice" };

describe("memoryProfile", () => {
  it("presets turn on progressively more, and the builder never mutates", () => {
    const store = new InMemoryMemoryStore();
    const minimal = memoryProfile({ store, preset: "minimal" }).build();
    const standard = memoryProfile({ store }).build();
    const full = memoryProfile({ store, preset: "full" }).build();

    expect(minimal.consolidation).toMatchObject({ extractRelations: false, ruleProposals: false });
    expect(minimal.context.maxEpisodeTokens).toBe(0);
    expect(standard.consolidation).toMatchObject({ extractRelations: true, ruleProposals: false });
    expect(standard.context.maxEpisodeTokens).toBeGreaterThan(0);
    expect(full.consolidation).toMatchObject({ ruleProposals: true });
    expect(minimal.context.maxMessageTokens).toBe(16_000);

    const base = memoryProfile({ store, preset: "minimal" });
    const tuned = base.consolidate({ ruleProposals: true }).context({ maxMessageTokens: 4_000 });
    expect(tuned.build().consolidation).toMatchObject({ ruleProposals: true });
    expect(tuned.build().context.maxMessageTokens).toBe(4_000);
    expect(base.build().consolidation).toMatchObject({ ruleProposals: false });
  });

  it("turns the consolidator off and back on, and rejects options that cannot work", () => {
    const store = new InMemoryMemoryStore();
    const off = memoryProfile({ store, preset: "full" }).consolidate(false);
    expect(off.build().consolidation).toBe(false);
    expect(off.consolidate({ reconcileFacts: true }).build().consolidation).toEqual({
      reconcileFacts: true,
    });

    const base = memoryProfile({ store });
    expect(() => base.recall({ recencyHalfLifeMs: 0 }).build()).toThrow("recencyHalfLifeMs");
    expect(() => base.context({ episodeHalfLifeMs: -1 }).build()).toThrow("episodeHalfLifeMs");
    expect(() => base.recall({ sources: [] }).build()).toThrow("at least one source");
    expect(() => base.consolidate({ distillPrompt: "x", extractRelations: true }).build()).toThrow(
      "distillPrompt",
    );
    expect(() => base.consolidate({ distillPrompt: "x", ruleProposals: true }).build()).toThrow(
      "distillPrompt",
    );
  });

  it("routes relations and proposals to their own stores and everything else to the base", async () => {
    const base = new InMemoryMemoryStore();
    const graph = new InMemoryMemoryStore();
    const proposals = new InMemoryMemoryStore();
    const store = memoryProfile({ store: base })
      .relationsIn(graph)
      .ruleProposalsIn(proposals)
      .build().store;

    await store.appendResourceFact(KEY, { text: "likes tea", source: "user" });
    await store.appendResourceRelation(KEY, {
      subject: "Alice",
      predicate: "works at",
      object: "Acme",
      source: "user",
    });
    await store.recordResourceRuleProposal(KEY, { text: "keep it short", threadId: "t1" });

    expect(await base.listResourceFacts(KEY)).toHaveLength(1);
    expect(await base.listResourceRelations(KEY)).toHaveLength(0);
    expect(await base.listResourceRuleProposals(KEY)).toHaveLength(0);
    expect(await graph.listResourceRelations(KEY)).toHaveLength(1);
    expect(await proposals.listResourceRuleProposals(KEY)).toHaveLength(1);
    expect(await store.listResourceRelations(KEY)).toHaveLength(1);
    expect(composeMemoryStore({ base })).toBe(base);
  });
});
