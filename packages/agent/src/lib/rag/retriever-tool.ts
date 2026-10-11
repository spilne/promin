import { z } from "zod";
import { fenceReferenceData, oneLine } from "../reference-data.ts";
import { tool, type AgentTool } from "../tool.ts";
import { DEFAULT_RETRIEVER_TOOL_NAME } from "./retriever-tool-name.ts";
import type { RetrieveResult, Retriever, RetrieverFilter } from "./types.ts";

export interface RetrieverToolConfig {
  readonly retriever: Retriever;
  /** LLM-facing tool name. Default: `search_knowledge_base`. */
  readonly name?: string;
  /** LLM-facing tool description. */
  readonly description?: string;
  /** Result count when the model asks for none. Default: the retriever's own default. */
  readonly topK?: number;
  /** Include source metadata in the tool output. Default: true. */
  readonly includeSources?: boolean;
  /** Include relevance scores in the model-visible text. Default: false. */
  readonly includeScores?: boolean;
  /** Truncate chunk text before returning it. */
  readonly maxChunkCharacters?: number;
  /**
   * Filter applied to every search. The model's own `filter` argument is
   * merged on top: tags are unioned (all must match), metadata keys from
   * the binding win so the model cannot widen a tenant/ownership scope.
   */
  readonly filter?: RetrieverFilter;
}

export interface RetrieverToolSource {
  readonly id: string;
  readonly title?: string;
  readonly uri?: string;
  readonly tags?: ReadonlyArray<string>;
  readonly metadata?: Readonly<Record<string, unknown>>;
}

export interface RetrieverToolOutput {
  readonly results: ReadonlyArray<{
    readonly id: string;
    readonly text: string;
    readonly score: number;
    readonly source?: RetrieverToolSource;
  }>;
}

export interface RetrieverToolInput {
  readonly query: string;
  readonly topK?: number;
  readonly filter?: {
    readonly tags?: string[];
    readonly metadata?: Record<string, string | number | boolean>;
  };
}

const RetrieverToolFilterSchema = z
  .object({
    tags: z.array(z.string()).optional(),
    metadata: z.record(z.string(), z.union([z.string(), z.number(), z.boolean()])).optional(),
  })
  .optional();

/**
 * Wrap a `Retriever` as an agent tool.
 *
 * Tool-based RAG: the model decides when to search, and the tool returns
 * grounded chunks with source metadata for citation. The sources also land
 * on the tool result message's metadata (`sources`) for traces and UIs.
 */
export function createRetrieverTool(
  config: RetrieverToolConfig,
): AgentTool<RetrieverToolInput, RetrieverToolOutput> {
  const includeSources = config.includeSources ?? true;
  const includeScores = config.includeScores ?? false;
  return tool<RetrieverToolInput, RetrieverToolOutput>({
    name: config.name ?? DEFAULT_RETRIEVER_TOOL_NAME,
    description:
      config.description ??
      "Search the knowledge base for relevant source-backed context before answering.",
    parameters: z.object({
      query: z.string().min(1).describe("Natural-language search query."),
      topK: z.number().int().positive().max(50).optional().describe("Maximum chunks to return."),
      filter: RetrieverToolFilterSchema.describe("Optional tag or metadata filters."),
    }),
    execute: async ({ query, topK, filter }) => {
      const results = await config.retriever.retrieve({
        query,
        ...((topK ?? config.topK) !== undefined && { topK: topK ?? config.topK }),
        ...mergeFilter({ base: config.filter, requested: filter }),
        ...(config.maxChunkCharacters !== undefined && {
          maxChunkCharacters: config.maxChunkCharacters,
        }),
      });
      return { results: results.map((r) => toToolResult({ result: r, includeSources })) };
    },
    toModelOutput: (output) => formatRetrieverToolOutput({ output, includeScores }),
    toResultMetadata: (output) => ({
      sources: output.results
        .map((r) => r.source)
        .filter((source): source is RetrieverToolSource => source !== undefined),
    }),
  });
}

function mergeFilter(params: {
  base: RetrieverFilter | undefined;
  requested: RetrieverToolInput["filter"];
}): { filter?: RetrieverFilter } {
  const { base, requested } = params;
  if (!base && !requested) return {};
  const tags = [...new Set([...(requested?.tags ?? []), ...(base?.tags ?? [])])];
  const metadata = { ...requested?.metadata, ...base?.metadata };
  return {
    filter: {
      ...(tags.length > 0 && { tags }),
      ...(Object.keys(metadata).length > 0 && { metadata }),
    },
  };
}

function toToolResult(params: {
  result: RetrieveResult;
  includeSources: boolean;
}): RetrieverToolOutput["results"][number] {
  const { result, includeSources } = params;
  const base = {
    id: result.chunk.id,
    text: result.chunk.text,
    score: result.score,
  };
  if (!includeSources) return base;
  const source = result.chunk.source;
  return {
    ...base,
    source: {
      id: source.id,
      ...(source.title !== undefined && { title: source.title }),
      ...(source.uri !== undefined && { uri: source.uri }),
      ...(source.tags !== undefined && { tags: source.tags }),
      ...(source.metadata !== undefined && { metadata: source.metadata }),
    },
  };
}

/**
 * Model-visible rendering of retrieved chunks: one numbered block per
 * chunk with its source (title or id, plus uri) so the model can cite it.
 * The output is raw chunk text — callers put it inside a
 * `fenceReferenceData` block before it reaches the model.
 */
export function formatRetrievedChunks(params: {
  results: ReadonlyArray<{
    readonly text: string;
    readonly score: number;
    readonly source?: { readonly id: string; readonly title?: string; readonly uri?: string };
  }>;
  includeScores?: boolean;
}): string {
  return params.results
    .map((result, i) => {
      const source = result.source
        ? `source=${oneLine(result.source.title ?? result.source.id)}${result.source.uri ? ` (${oneLine(result.source.uri)})` : ""}`
        : "source=hidden";
      const score = params.includeScores ? ` score=${result.score.toFixed(3)}` : "";
      return `[${i + 1}] ${source}${score}\n${result.text}`;
    })
    .join("\n\n");
}

function formatRetrieverToolOutput(params: {
  output: RetrieverToolOutput;
  includeScores: boolean;
}): string {
  if (params.output.results.length === 0) return "No relevant knowledge-base results found.";
  return fenceReferenceData({
    source: "knowledge",
    sections: [
      {
        title: "Knowledge results",
        body: formatRetrievedChunks({
          results: params.output.results,
          includeScores: params.includeScores,
        }),
      },
    ],
  })!;
}
