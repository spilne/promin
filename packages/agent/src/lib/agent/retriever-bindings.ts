// ---------------------------------------------------------------------------
// RAG bindings of a `LocalAgent`: each binding is a search tool the model
// can call, retrieved context injected before every turn, or both.
// ---------------------------------------------------------------------------

import type { AgentTool } from "../tool.ts";
import { fenceReferenceData } from "../reference-data.ts";
import { createRetrieverTool, formatRetrievedChunks } from "../rag/retriever-tool.ts";
import { retrieverToolName } from "../rag/retriever-tool-name.ts";
import type { Retriever, RetrieverFilter } from "../rag/types.ts";
import type { AgentKnowledgeMode } from "../registry/types.ts";

/** A retriever plus how it is exposed to the model. */
export interface AgentRetrieverBinding {
  readonly retriever: Retriever;
  /** LLM-facing tool name. Default `retrieverToolName(id)`. */
  readonly name?: string;
  /** LLM-facing tool description. */
  readonly description?: string;
  /** Results per search. Default: the retriever's own default. */
  readonly topK?: number;
  /** Include source metadata in tool output. Default true. */
  readonly includeSources?: boolean;
  /** Include relevance scores in the model-visible tool output. Default false. */
  readonly includeScores?: boolean;
  /** Truncate each returned chunk to this many characters. */
  readonly maxChunkCharacters?: number;
  /** Filter applied to every search (tool calls and context injection). */
  readonly filter?: RetrieverFilter;
  /**
   * `tool` (default) exposes a search tool; `context` searches with the
   * turn's task before every turn and injects the hits as a fenced
   * reference-data message (user role, never system — the chunks are
   * untrusted); `tool-and-context` does both.
   */
  readonly mode?: AgentKnowledgeMode;
}

/** Retriever bindings keyed by binding id; a bare `Retriever` is a `tool` binding. */
export type AgentRetrievers = Readonly<Record<string, Retriever | AgentRetrieverBinding>>;

interface NormalizedRetrieverBinding extends AgentRetrieverBinding {
  readonly id: string;
  readonly toolName: string;
  readonly mode: AgentKnowledgeMode;
}

function retrieverBindings(retrievers: AgentRetrievers): NormalizedRetrieverBinding[] {
  return Object.entries(retrievers).map(([id, bindingLike]) => {
    const binding: AgentRetrieverBinding =
      "retrieve" in bindingLike ? { retriever: bindingLike } : bindingLike;
    return {
      ...binding,
      id,
      toolName: binding.name ?? retrieverToolName(id),
      mode: binding.mode ?? "tool",
    };
  });
}

/**
 * `tools` plus one search tool per binding whose mode includes `tool`.
 * A tool already in `tools` under the same name wins.
 */
export function withRetrieverTools(params: {
  readonly retrievers: AgentRetrievers;
  readonly tools: Record<string, AgentTool<any, any>>;
}): Record<string, AgentTool<any, any>> {
  const merged = { ...params.tools };
  for (const binding of retrieverBindings(params.retrievers)) {
    if (binding.mode === "context") continue;
    if (binding.toolName in merged) continue;
    merged[binding.toolName] = createRetrieverTool({
      retriever: binding.retriever,
      name: binding.toolName,
      ...(binding.description !== undefined && { description: binding.description }),
      ...(binding.topK !== undefined && { topK: binding.topK }),
      ...(binding.includeSources !== undefined && { includeSources: binding.includeSources }),
      ...(binding.includeScores !== undefined && { includeScores: binding.includeScores }),
      ...(binding.maxChunkCharacters !== undefined && {
        maxChunkCharacters: binding.maxChunkCharacters,
      }),
      ...(binding.filter !== undefined && { filter: binding.filter }),
    });
  }
  return merged;
}

/**
 * Search every context-mode binding with `query` and render the hits as
 * one fenced reference-data block (one section per binding). Returns
 * `undefined` when no binding is in context mode or nothing matched.
 */
export async function buildRetrieverContext(params: {
  retrievers: AgentRetrievers | undefined;
  query: string;
}): Promise<string | undefined> {
  if (!params.retrievers) return undefined;
  const contextBindings = retrieverBindings(params.retrievers).filter((b) => b.mode !== "tool");
  if (contextBindings.length === 0) return undefined;
  const batches = await Promise.all(
    contextBindings.map(async (binding) => ({
      binding,
      results: await binding.retriever.retrieve({
        query: params.query,
        ...(binding.topK !== undefined && { topK: binding.topK }),
        ...(binding.filter !== undefined && { filter: binding.filter }),
        ...(binding.maxChunkCharacters !== undefined && {
          maxChunkCharacters: binding.maxChunkCharacters,
        }),
      }),
    })),
  );
  return fenceReferenceData({
    source: "knowledge",
    sections: batches
      .filter(({ results }) => results.length > 0)
      .map(({ binding, results }) => ({
        title: `Knowledge results from ${binding.id}`,
        body: formatRetrievedChunks({
          results: results.map((r) => ({
            text: r.chunk.text,
            score: r.score,
            ...(binding.includeSources !== false && { source: r.chunk.source }),
          })),
          ...(binding.includeScores !== undefined && { includeScores: binding.includeScores }),
        }),
      })),
  });
}
