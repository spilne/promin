// ---------------------------------------------------------------------------
// traceDataset — source eval cases from production threads in a MemoryStore.
//
// Each thread becomes one case: `input` is the thread's first user message,
// `id` is the thread id. Cases have no `expected` — production traffic has
// no ground truth — so pair this dataset with reference-free scorers
// (llmJudge without a reference, toolTrajectory, budget).
// ---------------------------------------------------------------------------

import { nextThreadCursor, type MemoryStore } from "@promin/agent";
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
      // Active threads page by page, newest activity first, up to `limit`.
      let yielded = 0;
      let cursor: string | undefined;
      for (;;) {
        const page = await memory.listThreads({
          namespaceId: config.namespaceId,
          ...(config.resourceId !== undefined && { resourceId: config.resourceId }),
          archived: false,
          limit: THREAD_PAGE_SIZE,
          ...(cursor !== undefined && { cursor }),
        });
        for (const thread of page) {
          if (config.limit !== undefined && yielded >= config.limit) return;
          yielded += 1;
          // The case input is the thread's first user message, near the start.
          const messages = await memory.getMessages(
            {
              namespaceId: thread.namespaceId,
              ...(thread.resourceId !== null && { resourceId: thread.resourceId }),
              threadId: thread.threadId,
            },
            { order: "asc", limit: FIRST_MESSAGES },
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
        const last = page.at(-1);
        if (!last || page.length < THREAD_PAGE_SIZE) return;
        cursor = nextThreadCursor({ thread: last });
      }
    },
  };
}

/** Threads read per page. */
const THREAD_PAGE_SIZE = 200;
/** Messages read from the start of a thread to find its first user message. */
const FIRST_MESSAGES = 50;
