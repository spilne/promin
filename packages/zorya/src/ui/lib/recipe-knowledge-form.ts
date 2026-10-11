// ---------------------------------------------------------------------------
// recipe-knowledge-form — pure form-state ⇄ recipe mapping for the agent
// edit-drawer's "Knowledge / RAG" section (`backend.knowledge[]`).
//
// Numeric fields stay strings so "blank" (retriever default) is distinct
// from a number; the builder omits blank and default-valued fields so a
// saved recipe only carries what the operator changed. A binding's
// `filter` has no form control and round-trips untouched.
// ---------------------------------------------------------------------------

import type { AgentKnowledgeMode, AgentKnowledgeRecipe } from "@promin/agent/browser";
import { parsePositiveInt } from "./recipe-memory-form.ts";

export interface KnowledgeForm {
  id: string;
  name: string;
  description: string;
  topK: string;
  includeSources: boolean;
  includeScores: boolean;
  maxChunkCharacters: string;
  mode: AgentKnowledgeMode;
  filter?: AgentKnowledgeRecipe["filter"];
}

/** A blank binding row, optionally pre-filled from a catalog entry. */
export function emptyKnowledgeForm(
  params: { id?: string; description?: string } = {},
): KnowledgeForm {
  return {
    id: params.id ?? "",
    name: "",
    description: params.description ?? "",
    topK: "",
    includeSources: true,
    includeScores: false,
    maxChunkCharacters: "",
    mode: "tool",
  };
}

export function initKnowledge(
  bindings: ReadonlyArray<AgentKnowledgeRecipe> | undefined,
): KnowledgeForm[] {
  return (bindings ?? []).map((k) => ({
    id: k.id,
    name: k.name ?? "",
    description: k.description ?? "",
    topK: k.topK !== undefined ? String(k.topK) : "",
    includeSources: k.includeSources !== false,
    includeScores: k.includeScores === true,
    maxChunkCharacters: k.maxChunkCharacters !== undefined ? String(k.maxChunkCharacters) : "",
    mode: k.mode ?? "tool",
    ...(k.filter !== undefined && { filter: k.filter }),
  }));
}

/** Form rows → recipe bindings. Rows with a blank id are dropped. */
export function buildKnowledge(forms: ReadonlyArray<KnowledgeForm>): AgentKnowledgeRecipe[] {
  return forms
    .filter((k) => k.id.trim().length > 0)
    .map((k) => {
      const name = k.name.trim();
      const description = k.description.trim();
      const topK = parsePositiveInt(k.topK);
      const maxChunkCharacters = parsePositiveInt(k.maxChunkCharacters);
      return {
        id: k.id.trim(),
        ...(name && { name }),
        ...(description && { description }),
        ...(topK !== undefined && { topK }),
        ...(!k.includeSources && { includeSources: false }),
        ...(k.includeScores && { includeScores: true }),
        ...(maxChunkCharacters !== undefined && { maxChunkCharacters }),
        ...(k.filter !== undefined && { filter: k.filter }),
        ...(k.mode !== "tool" && { mode: k.mode }),
      };
    });
}
