// ---------------------------------------------------------------------------
// Recipe `backend.knowledge` validation — the gateway accepts well-formed
// retriever bindings and rejects bad modes, tool names, duplicate ids and
// unknown fields with `invalid_backend`.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import type { AgentKnowledgeRecipe } from "@promin/agent";
import { parseBackend } from "../agent-gateway-kit.ts";

const backend = (knowledge: unknown) => ({
  type: "local",
  model: { provider: "anthropic", id: "claude-sonnet-4-6" },
  role: { inline: { systemPrompt: "Helpful", tools: [] } },
  knowledge,
});

describe("parseBackend — knowledge bindings", () => {
  it("accepts well-formed bindings", () => {
    const knowledge: AgentKnowledgeRecipe[] = [
      { id: "docs" },
      {
        id: "faq",
        name: "search_faq",
        description: "FAQ",
        topK: 4,
        includeSources: false,
        includeScores: true,
        maxChunkCharacters: 400,
        filter: { tags: ["public"], metadata: { tenant: "acme", tier: 2 } },
        mode: "tool-and-context",
      },
    ];
    const parsed = parseBackend({ raw: backend(knowledge) });
    if (!("backend" in parsed) || parsed.backend.type !== "local") {
      throw new Error(`expected a local backend, got ${JSON.stringify(parsed)}`);
    }
    expect(parsed.backend.knowledge).toEqual(knowledge);
  });

  it.each([
    ["an empty id", [{ id: "" }]],
    ["an unknown mode", [{ id: "docs", mode: "always" }]],
    ["a tool name with spaces", [{ id: "docs", name: "search docs" }]],
    ["a non-positive topK", [{ id: "docs", topK: 0 }]],
    ["duplicate ids", [{ id: "docs" }, { id: "docs", mode: "context" }]],
    ["an unknown field", [{ id: "docs", retriever: {} }]],
  ])("rejects %s", (_label, knowledge) => {
    const parsed = parseBackend({ raw: backend(knowledge) });
    expect("error" in parsed && parsed.error).toBe("invalid_backend");
  });
});
