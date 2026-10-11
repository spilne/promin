// ---------------------------------------------------------------------------
// recipe-knowledge-form — form-state ⇄ recipe round-trips for the
// edit-drawer "Knowledge / RAG" section.
// Pins: defaults are omitted on save, blank ids are dropped, numeric fields
// stay unset when blank or invalid, and the uneditable `filter` survives.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { buildKnowledge, emptyKnowledgeForm, initKnowledge } from "../recipe-knowledge-form.ts";

describe("recipe-knowledge-form", () => {
  it("round-trips a fully specified binding", () => {
    const recipe = [
      {
        id: "docs",
        name: "search_docs_v2",
        description: "Product docs",
        topK: 4,
        includeSources: false,
        includeScores: true,
        maxChunkCharacters: 500,
        filter: { tags: ["public"], metadata: { tenant: "acme" } },
        mode: "tool-and-context" as const,
      },
    ];
    expect(buildKnowledge(initKnowledge(recipe))).toEqual(recipe);
  });

  it("omits defaults and drops rows without an id", () => {
    const forms = [
      { ...emptyKnowledgeForm({ id: "  docs  " }), topK: "0", maxChunkCharacters: "abc" },
      emptyKnowledgeForm(),
    ];
    expect(buildKnowledge(forms)).toEqual([{ id: "docs" }]);
  });

  it("pre-fills a catalog row's description", () => {
    expect(emptyKnowledgeForm({ id: "docs", description: "Company docs" })).toMatchObject({
      id: "docs",
      description: "Company docs",
      mode: "tool",
      includeSources: true,
    });
  });

  it("initializes an absent knowledge list to no rows", () => {
    expect(initKnowledge(undefined)).toEqual([]);
  });
});
