// ---------------------------------------------------------------------------
// retrievalRecall — did the run retrieve the documents it should have?
//
// Reads the run's trace: every tool result that carries `sources` metadata
// (what a knowledge-base retriever tool attaches) contributes its source
// ids. The case's `expected.sources` lists the source ids a good answer
// needs; the score is the fraction of them that were retrieved.
// ---------------------------------------------------------------------------

import type { AgentTrace } from "@promin/agent";
import type { Scorer } from "../types.ts";

export interface RetrievalRecallConfig {
  /** Only count results of these tools. Default: every tool result with `sources`. */
  readonly toolNames?: ReadonlyArray<string>;
  /** Scorer id — default `"retrievalRecall"`. */
  readonly id?: string;
  /** Pass cutoff on recall. Default 1 (every expected source retrieved). */
  readonly threshold?: number;
  readonly required?: boolean;
}

/** Build a scorer of retrieval recall against `expected.sources`. */
export function retrievalRecall(config: RetrievalRecallConfig = {}): Scorer {
  const id = config.id ?? "retrievalRecall";
  const toolNames = config.toolNames !== undefined ? new Set(config.toolNames) : undefined;
  return {
    id,
    threshold: config.threshold ?? 1,
    ...(config.required !== undefined && { required: config.required }),
    async score({ output, expected }) {
      const wanted = expectedSources(expected);
      if (wanted === undefined) {
        throw new Error(`${id}: the case's expected value needs a "sources" array of source ids`);
      }
      if (output.trace === undefined) {
        return { scorerId: id, value: 0, reason: "output carries no trace to inspect" };
      }
      const retrieved = retrievedSourceIds({
        trace: output.trace,
        ...(toolNames !== undefined && { toolNames }),
      });
      if (wanted.length === 0) return { scorerId: id, value: 1 };
      const missing = wanted.filter((source) => !retrieved.has(source));
      const value = (wanted.length - missing.length) / wanted.length;
      return {
        scorerId: id,
        value,
        ...(missing.length > 0 && {
          reason: `missing [${missing.join(", ")}]; retrieved [${[...retrieved].join(", ")}]`,
        }),
      };
    },
  };
}

/** Source ids of every retrieval result in the trace, across all turns. */
export function retrievedSourceIds(params: {
  readonly trace: AgentTrace;
  readonly toolNames?: ReadonlySet<string>;
}): Set<string> {
  const ids = new Set<string>();
  for (const turn of params.trace.turns) {
    for (const child of turn.children) {
      if (child.kind !== "assistant") continue;
      for (const call of child.toolCalls) {
        if (params.toolNames !== undefined && !params.toolNames.has(call.name)) continue;
        const sources = call.result?.metadata?.sources;
        if (!Array.isArray(sources)) continue;
        for (const source of sources) {
          const sourceId = (source as { id?: unknown } | null)?.id;
          if (typeof sourceId === "string") ids.add(sourceId);
        }
      }
    }
  }
  return ids;
}

function expectedSources(expected: unknown): string[] | undefined {
  const sources = (expected as { sources?: unknown } | null | undefined)?.sources;
  if (!Array.isArray(sources) || !sources.every((s) => typeof s === "string")) return undefined;
  return sources as string[];
}
