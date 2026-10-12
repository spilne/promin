// ---------------------------------------------------------------------------
// RAG bindings of a `LocalAgent`: each binding is a search tool the model
// can call, retrieved context injected before every turn, or both.
// ---------------------------------------------------------------------------

import { SystemWallClock, type WallClock } from "@promin/workflow";
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
  /**
   * Longest a context-mode search may take before the turn goes on
   * without it, in ms. Default `DEFAULT_RETRIEVER_TIMEOUT_MS`.
   */
  readonly timeoutMs?: number;
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

/** A context-mode binding whose search failed or timed out this turn. */
export interface RetrieverFailure {
  readonly bindingId: string;
  readonly error: string;
  readonly timedOut: boolean;
}

/** Knowledge injected before a turn, and the bindings that could not contribute. */
export interface RetrieverContext {
  /** One fenced reference-data block, or `undefined` when there is nothing to inject. */
  readonly text: string | undefined;
  readonly failures: ReadonlyArray<RetrieverFailure>;
}

/** Default `AgentRetrieverBinding.timeoutMs` of a context-mode search. */
export const DEFAULT_RETRIEVER_TIMEOUT_MS = 10_000;

/**
 * Search every context-mode binding with `query` and render the hits as
 * one fenced reference-data block (one section per binding). A binding
 * that throws or exceeds its `timeoutMs` (measured on `clock`) is left
 * out: the turn goes on without it, the block notes the missing source,
 * and the failure is returned for the caller to report. `text` is
 * `undefined` when no binding is in context mode or nothing matched.
 */
export async function buildRetrieverContext(params: {
  retrievers: AgentRetrievers | undefined;
  query: string;
  /** Time source of the per-binding timeout. Default `SystemWallClock`. */
  clock?: WallClock;
}): Promise<RetrieverContext> {
  if (!params.retrievers) return { text: undefined, failures: [] };
  const contextBindings = retrieverBindings(params.retrievers).filter((b) => b.mode !== "tool");
  if (contextBindings.length === 0) return { text: undefined, failures: [] };
  const clock = params.clock ?? SystemWallClock;
  const batches = await Promise.all(
    contextBindings.map(async (binding) => {
      try {
        const results = await withTimeout({
          clock,
          ms: binding.timeoutMs ?? DEFAULT_RETRIEVER_TIMEOUT_MS,
          run: (signal) =>
            binding.retriever.retrieve({
              query: params.query,
              signal,
              ...(binding.topK !== undefined && { topK: binding.topK }),
              ...(binding.filter !== undefined && { filter: binding.filter }),
              ...(binding.maxChunkCharacters !== undefined && {
                maxChunkCharacters: binding.maxChunkCharacters,
              }),
            }),
        });
        return { binding, results, failure: undefined };
      } catch (err) {
        const failure: RetrieverFailure = {
          bindingId: binding.id,
          error: err instanceof Error ? err.message : String(err),
          timedOut: err instanceof RetrieverTimeoutError,
        };
        return { binding, results: [], failure };
      }
    }),
  );
  const failures = batches.flatMap(({ failure }) => (failure ? [failure] : []));
  const sections = batches
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
    }));
  if (failures.length > 0) {
    sections.push({
      title: "Knowledge sources unavailable this turn",
      body: failures
        .map(
          (f) =>
            `- ${f.bindingId}: ${f.timedOut ? "the search timed out" : "the search failed"}; ` +
            "its results are missing.",
        )
        .join("\n"),
    });
  }
  return { text: fenceReferenceData({ source: "knowledge", sections }), failures };
}

class RetrieverTimeoutError extends Error {
  constructor(ms: number) {
    super(`retriever search timed out after ${ms}ms`);
    this.name = "RetrieverTimeoutError";
  }
}

/**
 * `run(signal)`, or a `RetrieverTimeoutError` once `ms` pass on `clock` —
 * then `signal` aborts so the search stops instead of running on.
 */
function withTimeout<T>(params: {
  clock: WallClock;
  ms: number;
  run: (signal: AbortSignal) => Promise<T>;
}): Promise<T> {
  const { clock, ms } = params;
  const controller = new AbortController();
  return new Promise<T>((resolve, reject) => {
    const timer = clock.setTimeout(() => {
      const timeout = new RetrieverTimeoutError(ms);
      controller.abort(timeout);
      reject(timeout);
    }, ms);
    let running: Promise<T>;
    try {
      running = params.run(controller.signal);
    } catch (err) {
      // A retriever that throws before returning its promise.
      timer.clear();
      reject(err);
      return;
    }
    running.then(
      (value) => {
        timer.clear();
        resolve(value);
      },
      (err: unknown) => {
        timer.clear();
        reject(err);
      },
    );
  });
}
