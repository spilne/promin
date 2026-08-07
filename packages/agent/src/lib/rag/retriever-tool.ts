import { z } from "zod";
import { tool, type AgentTool } from "../tool.ts";
import type { RetrieveResult, Retriever, RetrieverFilter } from "./types.ts";

export interface RetrieverToolConfig {
  readonly retriever: Retriever;
  /** LLM-facing tool name. Default: `search_knowledge_base`. */
  readonly name?: string;
  /** LLM-facing tool description. */
  readonly description?: string;
  /** Default result count. Default: 8. */
  readonly topK?: number;
  /** Include source metadata in the model-visible output. Default: true. */
  readonly includeSources?: boolean;
  /** Include similarity/relevance scores in model-visible output. Default: false. */
  readonly includeScores?: boolean;
  /** Truncate chunk text before returning it. */
  readonly maxChunkCharacters?: number;
}

export interface RetrieverToolOutput {
  readonly results: Array<{
    readonly id: string;
    readonly text: string;
    readonly score: number;
    readonly source?: {
      readonly id: string;
      readonly title?: string;
      readonly uri?: string;
      readonly tags?: ReadonlyArray<string>;
      readonly metadata?: Readonly<Record<string, unknown>>;
    };
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

const filterSchema = z
  .object({
    tags: z.array(z.string()).optional(),
    metadata: z.record(z.string(), z.union([z.string(), z.number(), z.boolean()])).optional(),
  })
  .optional();

/**
 * Wrap a `Retriever` as an agent tool.
 *
 * This is the tool-based RAG pattern: the model decides when to search, and
 * the tool returns grounded chunks with optional source metadata for citation.
 */
export function createRetrieverTool(
  config: RetrieverToolConfig,
): AgentTool<RetrieverToolInput, RetrieverToolOutput> {
  const includeSources = config.includeSources ?? true;
  const includeScores = config.includeScores ?? false;
  return tool({
    name: config.name ?? "search_knowledge_base",
    description:
      config.description ??
      "Search the knowledge base for relevant source-backed context before answering.",
    parameters: z.object({
      query: z.string().min(1).describe("Natural-language search query."),
      topK: z.number().int().positive().max(50).optional().describe("Maximum chunks to return."),
      filter: filterSchema.describe("Optional tag or metadata filters."),
    }),
    execute: async ({ query, topK, filter }) => {
      const results = await config.retriever.retrieve({
        query,
        topK: topK ?? config.topK,
        filter: filter as RetrieverFilter | undefined,
        maxChunkCharacters: config.maxChunkCharacters,
      });
      return { results: results.map((r) => toToolResult(r, includeSources)) };
    },
    toModelOutput: (output) => formatRetrieverToolOutput(output, { includeScores }),
    toResultMetadata: (output) => ({
      sources: output.results
        .map((r) => r.source)
        .filter((source): source is NonNullable<typeof source> => source !== undefined),
    }),
  });
}

function toToolResult(
  result: RetrieveResult,
  includeSources: boolean,
): RetrieverToolOutput["results"][number] {
  const base = {
    id: result.chunk.id,
    text: result.chunk.text,
    score: result.score,
  };
  if (!includeSources) return base;
  return {
    ...base,
    source: {
      id: result.chunk.source.id,
      title: result.chunk.source.title,
      uri: result.chunk.source.uri,
      tags: result.chunk.source.tags,
      metadata: result.chunk.source.metadata,
    },
  };
}

function formatRetrieverToolOutput(
  output: RetrieverToolOutput,
  opts: { includeScores: boolean },
): string {
  if (output.results.length === 0) return "No relevant knowledge-base results found.";
  return output.results
    .map((result, i) => {
      const source = result.source
        ? `source=${result.source.title ?? result.source.id}${result.source.uri ? ` (${result.source.uri})` : ""}`
        : "source=hidden";
      const score = opts.includeScores ? ` score=${result.score.toFixed(3)}` : "";
      return `[${i + 1}] ${source}${score}\n${result.text}`;
    })
    .join("\n\n");
}
