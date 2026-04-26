// ---------------------------------------------------------------------------
// `resolveContext` — pure cascade builder, no I/O.
//
// Pins:
//   - cascade order: namespace → boundary → resource → thread
//   - inheritFromParent severs cleanly (resource cuts namespace; thread
//     cuts both)
//   - facts render numbered + dated; empty layers omit their headers
//   - the cache boundary marker is always present (even when a side is
//     empty), so prompt-cache providers can split on it deterministically
//   - messages trim from oldest first to fit the token budget
//   - static layers (rules, facts, working memory) are never trimmed
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { resolveContext } from "../resolve-context.ts";
import {
  PROMPT_CACHE_BOUNDARY,
  type EpisodicRecord,
  type Fact,
  type NamespaceRow,
  type ResourceRow,
  type StoredMessage,
  type ThreadRow,
  type TokenBudget,
} from "../types.ts";

const T0 = Date.parse("2026-04-01T00:00:00Z");

function makeNamespace(patch: Partial<NamespaceRow> = {}): NamespaceRow {
  return {
    namespaceId: "acme",
    staticRules: null,
    workingMemory: null,
    inheritFromParent: true,
    metadata: {},
    createdAt: T0,
    updatedAt: T0,
    ...patch,
  };
}

function makeResource(patch: Partial<ResourceRow> = {}): ResourceRow {
  return {
    namespaceId: "acme",
    resourceId: "alice",
    staticRules: null,
    workingMemory: null,
    inheritFromParent: true,
    metadata: {},
    createdAt: T0,
    updatedAt: T0,
    ...patch,
  };
}

function makeThread(patch: Partial<ThreadRow> = {}): ThreadRow {
  return {
    namespaceId: "acme",
    resourceId: "alice",
    threadId: "alice-default",
    workingMemory: null,
    inheritFromParent: true,
    metadata: {},
    createdAt: T0,
    updatedAt: T0,
    ...patch,
  };
}

function fact(id: string, text: string, dayOffset = 0): Fact {
  const ts = T0 + dayOffset * 24 * 60 * 60 * 1000;
  return { id, text, createdAt: ts, updatedAt: ts };
}

function userMsg(content: string, seq: number): StoredMessage {
  return { role: "user", content, seq, createdAt: T0 + seq };
}

const looseBudget: TokenBudget = { maxMessageTokens: 1_000_000 };

describe("resolveContext — cascade order + boundary", () => {
  it("emits namespace rules, facts, and working memory above the cache boundary", () => {
    const out = resolveContext({
      namespace: makeNamespace({
        staticRules: "Acme is a payments company.",
        workingMemory: "Holiday: Q1 release on 2026-01-15.",
      }),
      namespaceFacts: [fact("n1", "Default currency is USD")],
      thread: makeThread(),
      messages: [],
      budget: looseBudget,
    });

    const idxRules = out.systemPrompt.indexOf("Acme is a payments company.");
    const idxFacts = out.systemPrompt.indexOf("Default currency is USD");
    const idxWorking = out.systemPrompt.indexOf("Holiday: Q1 release on 2026-01-15.");
    const idxBoundary = out.systemPrompt.indexOf(PROMPT_CACHE_BOUNDARY);

    expect(idxRules).toBeGreaterThan(-1);
    expect(idxFacts).toBeGreaterThan(idxRules);
    expect(idxWorking).toBeGreaterThan(idxFacts);
    expect(idxBoundary).toBeGreaterThan(idxWorking);
  });

  it("emits resource rules, facts, and working memory below the boundary", () => {
    const out = resolveContext({
      resource: makeResource({
        staticRules: "User prefers terse answers.",
        workingMemory: "Currently working on tax-2025.",
      }),
      resourceFacts: [fact("r1", "User is a TypeScript dev")],
      thread: makeThread(),
      messages: [],
      budget: looseBudget,
    });

    const idxBoundary = out.systemPrompt.indexOf(PROMPT_CACHE_BOUNDARY);
    const idxRules = out.systemPrompt.indexOf("User prefers terse answers.");
    const idxFacts = out.systemPrompt.indexOf("User is a TypeScript dev");
    const idxWorking = out.systemPrompt.indexOf("Currently working on tax-2025.");

    expect(idxRules).toBeGreaterThan(idxBoundary);
    expect(idxFacts).toBeGreaterThan(idxRules);
    expect(idxWorking).toBeGreaterThan(idxFacts);
  });

  it("emits thread working memory after resource layer (most volatile, last)", () => {
    const out = resolveContext({
      resource: makeResource({ staticRules: "RESOURCE" }),
      thread: makeThread({ workingMemory: "THREAD-SCRATCH" }),
      messages: [],
      budget: looseBudget,
    });

    const idxResource = out.systemPrompt.indexOf("RESOURCE");
    const idxThread = out.systemPrompt.indexOf("THREAD-SCRATCH");

    expect(idxResource).toBeGreaterThan(-1);
    expect(idxThread).toBeGreaterThan(idxResource);
  });

  it("always emits the cache boundary, even with no namespace contribution", () => {
    const out = resolveContext({
      thread: makeThread({ workingMemory: "x" }),
      messages: [],
      budget: looseBudget,
    });
    expect(out.systemPrompt.includes(PROMPT_CACHE_BOUNDARY)).toBe(true);
  });
});

describe("resolveContext — fact rendering", () => {
  it("renders facts as numbered, ISO-dated lines", () => {
    const out = resolveContext({
      thread: makeThread(),
      threadFacts: [fact("t1", "first", 0), fact("t2", "second", 5)],
      messages: [],
      budget: looseBudget,
    });

    expect(out.systemPrompt).toContain("1. [2026-04-01] first");
    expect(out.systemPrompt).toContain("2. [2026-04-06] second");
  });

  it("omits the facts header when no facts exist on a layer", () => {
    const out = resolveContext({
      namespace: makeNamespace({ staticRules: "rules-only" }),
      thread: makeThread(),
      messages: [],
      budget: looseBudget,
    });

    expect(out.systemPrompt).toContain("rules-only");
    expect(out.systemPrompt).not.toContain("## Namespace Facts");
  });
});

describe("resolveContext — inheritance severance", () => {
  it("skips namespace when resource.inheritFromParent is false", () => {
    const out = resolveContext({
      namespace: makeNamespace({ staticRules: "NAMESPACE-RULES" }),
      namespaceFacts: [fact("n1", "ns-secret")],
      resource: makeResource({
        inheritFromParent: false,
        staticRules: "RESOURCE-RULES",
      }),
      thread: makeThread(),
      messages: [],
      budget: looseBudget,
    });

    expect(out.systemPrompt).not.toContain("NAMESPACE-RULES");
    expect(out.systemPrompt).not.toContain("ns-secret");
    expect(out.systemPrompt).toContain("RESOURCE-RULES");
  });

  it("skips both namespace and resource when thread.inheritFromParent is false", () => {
    const out = resolveContext({
      namespace: makeNamespace({ staticRules: "NS" }),
      resource: makeResource({ staticRules: "RES" }),
      thread: makeThread({ inheritFromParent: false, workingMemory: "THREAD-ONLY" }),
      messages: [],
      budget: looseBudget,
    });

    expect(out.systemPrompt).not.toContain("NS");
    expect(out.systemPrompt).not.toContain("RES");
    expect(out.systemPrompt).toContain("THREAD-ONLY");
  });
});

describe("resolveContext — message trimming", () => {
  const m1 = userMsg("a".repeat(40), 1); // ~10 tokens
  const m2 = userMsg("b".repeat(40), 2); // ~10 tokens
  const m3 = userMsg("c".repeat(40), 3); // ~10 tokens

  it("returns all messages when total fits the budget", () => {
    const out = resolveContext({
      thread: makeThread(),
      messages: [m1, m2, m3],
      budget: { maxMessageTokens: 100 },
    });
    expect(out.messages.map((m) => m.seq)).toEqual([1, 2, 3]);
  });

  it("drops oldest messages first to fit the budget", () => {
    const out = resolveContext({
      thread: makeThread(),
      messages: [m1, m2, m3],
      budget: { maxMessageTokens: 15 },
    });
    expect(out.messages.map((m) => m.seq)).toEqual([3]);
  });

  it("respects a caller-supplied estimator", () => {
    const out = resolveContext({
      thread: makeThread(),
      messages: [m1, m2, m3],
      budget: { maxMessageTokens: 2, estimate: () => 1 },
    });
    // estimate=1 each, budget=2 → keep last two
    expect(out.messages.map((m) => m.seq)).toEqual([2, 3]);
  });

  it("never mutates the input message array", () => {
    const input: StoredMessage[] = [m1, m2, m3];
    resolveContext({
      thread: makeThread(),
      messages: input,
      budget: { maxMessageTokens: 1 },
    });
    expect(input.map((m) => m.seq)).toEqual([1, 2, 3]);
  });
});

function episode(summary: string, salience: number, dayOffset = 0): EpisodicRecord {
  const ts = T0 + dayOffset * 24 * 60 * 60 * 1000;
  return {
    id: `ep-${summary}`,
    summary,
    outcome: null,
    salience,
    embedding: null,
    sourceThreadId: null,
    sourceMessageRange: null,
    occurredAt: ts,
    createdAt: ts,
    metadata: {},
  };
}

describe("resolveContext — episodes (L2)", () => {
  it("ignores episodes when maxEpisodeTokens is unset", () => {
    const out = resolveContext({
      thread: makeThread(),
      resourceEpisodes: [episode("HIDDEN", 0.9)],
      messages: [],
      budget: looseBudget,
    });
    expect(out.systemPrompt).not.toContain("HIDDEN");
  });

  it("renders episodes salience-tagged + dated under 'Recent Episodes'", () => {
    const out = resolveContext({
      thread: makeThread(),
      resourceEpisodes: [episode("worked on tax filing", 0.85, 3)],
      messages: [],
      budget: { maxMessageTokens: 1000, maxEpisodeTokens: 1000 },
    });
    expect(out.systemPrompt).toContain("## Recent Episodes");
    expect(out.systemPrompt).toContain("[2026-04-04, salience=0.85] worked on tax filing");
  });

  it("renders the outcome line when present", () => {
    const out = resolveContext({
      thread: makeThread(),
      resourceEpisodes: [{ ...episode("did the thing", 0.6), outcome: "shipped on time" }],
      messages: [],
      budget: { maxMessageTokens: 1000, maxEpisodeTokens: 1000 },
    });
    expect(out.systemPrompt).toContain("Outcome: shipped on time");
  });

  it("trims lowest-salience first when over budget", () => {
    const big = (s: string) => s + " ".repeat(60);
    const out = resolveContext({
      thread: makeThread(),
      resourceEpisodes: [episode(big("HIGH"), 0.9), episode(big("LOW"), 0.3)],
      messages: [],
      // Budget fits roughly one episode summary (~16 tokens).
      budget: { maxMessageTokens: 1000, maxEpisodeTokens: 18 },
    });
    expect(out.systemPrompt).toContain("HIGH");
    expect(out.systemPrompt).not.toContain("LOW");
  });

  it("places episodes after resource layer and before thread working memory", () => {
    const out = resolveContext({
      resource: makeResource({ staticRules: "RESOURCE-RULES" }),
      resourceEpisodes: [episode("EPISODE-X", 0.8)],
      thread: makeThread({ workingMemory: "THREAD-SCRATCH" }),
      messages: [],
      budget: { maxMessageTokens: 1000, maxEpisodeTokens: 1000 },
    });
    const idxResource = out.systemPrompt.indexOf("RESOURCE-RULES");
    const idxEpisode = out.systemPrompt.indexOf("EPISODE-X");
    const idxThread = out.systemPrompt.indexOf("THREAD-SCRATCH");
    expect(idxResource).toBeGreaterThan(-1);
    expect(idxEpisode).toBeGreaterThan(idxResource);
    expect(idxThread).toBeGreaterThan(idxEpisode);
  });

  it("does not render episodes when thread.inheritFromParent is false", () => {
    const out = resolveContext({
      resourceEpisodes: [episode("CUT", 1)],
      thread: makeThread({ inheritFromParent: false }),
      messages: [],
      budget: { maxMessageTokens: 1000, maxEpisodeTokens: 1000 },
    });
    expect(out.systemPrompt).not.toContain("CUT");
  });
});

describe("resolveContext — empty edge case", () => {
  it("produces a valid (boundary-only) prompt and empty messages when nothing is configured", () => {
    const out = resolveContext({
      thread: makeThread(),
      messages: [],
      budget: looseBudget,
    });
    expect(out.systemPrompt).toBe(PROMPT_CACHE_BOUNDARY);
    expect(out.messages).toEqual([]);
  });
});
