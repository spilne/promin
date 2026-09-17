// ---------------------------------------------------------------------------
// traceDataset — source eval cases from production threads in a MemoryStore.
//
// Each thread becomes one case: `input` is the thread's first user message,
// `id` is the thread id. Cases have no `expected` — production traffic has
// no ground truth — so pair this dataset with reference-free scorers
// (llmJudge without a reference, toolTrajectory, budget).
// ---------------------------------------------------------------------------

import type { MemoryStore } from "@promin/agent";
import type { EvalCase, EvalDataset } from "../types.ts";

export interface TraceDatasetConfig {
  readonly namespaceId: string;
  /** When set, restrict to threads owned by this resource. */
  readonly resourceId?: string;
  /** Cap on the number of threads turned into cases. */
  readonly limit?: number;
  /** Dataset id — default `"trace:<namespaceId>"`. */
  readonly id?: string;
}

/** Build an `EvalDataset` backed by a `MemoryStore`'s threads. */
export function traceDataset(memory: MemoryStore, config: TraceDatasetConfig): EvalDataset {
  const datasetId = config.id ?? `trace:${config.namespaceId}`;
  return {
    id: datasetId,
    async *cases() {
      const threads = await memory.listThreads({
        namespaceId: config.namespaceId,
        ...(config.resourceId !== undefined && { resourceId: config.resourceId }),
      });
      const selected = config.limit !== undefined ? threads.slice(0, config.limit) : threads;
      for (const thread of selected) {
        const messages = await memory.getMessages(
          {
            namespaceId: thread.namespaceId,
            ...(thread.resourceId !== null && { resourceId: thread.resourceId }),
            threadId: thread.threadId,
          },
          { order: "asc" },
        );
        const firstUser = messages.find((message) => message.role === "user");
        if (firstUser === undefined) continue; // no user turn — nothing to evaluate
        const evalCase: EvalCase = {
          id: thread.threadId,
          input: firstUser.content,
          metadata: { threadId: thread.threadId, namespaceId: thread.namespaceId },
        };
        yield evalCase;
      }
    },
  };
}
