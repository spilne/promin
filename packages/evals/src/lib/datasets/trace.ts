// ---------------------------------------------------------------------------
// traceDataset — source eval cases from production threads in a MemoryStore.
//
// Each active thread becomes one case: `input` is the thread's first user
// message, `id` is the thread id. Cases have no `expected` — production
// traffic has no ground truth — so pair this dataset with reference-free
// scorers (llmJudge without a reference, toolTrajectory, budget).
//
// Threads are read a page at a time (keyset cursor) in creation order — a
// stable order, so a thread that gets a new message mid-read is neither
// skipped nor read twice — and only the head of each thread is loaded to
// find its first user message.
// ---------------------------------------------------------------------------

import { nextThreadCursor, type MemoryStore, type ThreadKey } from "@promin/agent";
import type { EvalCase, EvalDataset } from "../types.ts";

/** Threads per `listThreads` page. */
export const DEFAULT_TRACE_PAGE_SIZE = 100;
/** Messages read from a thread's head per lookup of its first user message. */
const HEAD_MESSAGES = 20;
const ORDER = "createdAsc";

export interface TraceDatasetConfig {
  readonly memory: MemoryStore;
  readonly namespaceId: string;
  /** When set, restrict to threads owned by this resource. */
  readonly resourceId?: string;
  /** Cap on the number of cases produced. */
  readonly limit?: number;
  /** Threads per page. Default `DEFAULT_TRACE_PAGE_SIZE`. */
  readonly pageSize?: number;
  /** Dataset id — default `"trace:<namespaceId>"`. */
  readonly id?: string;
}

/** Build an `EvalDataset` backed by a `MemoryStore`'s active threads. */
export function traceDataset(config: TraceDatasetConfig): EvalDataset {
  const { memory } = config;
  const datasetId = config.id ?? `trace:${config.namespaceId}`;
  const pageSize = config.pageSize ?? DEFAULT_TRACE_PAGE_SIZE;
  if (!Number.isInteger(pageSize) || pageSize < 1) {
    throw new RangeError(`traceDataset: pageSize must be a positive integer, got ${pageSize}`);
  }
  const limit = config.limit;
  if (limit !== undefined && !(Number.isInteger(limit) && limit >= 0)) {
    throw new RangeError(`traceDataset: limit must be a non-negative integer, got ${limit}`);
  }
  return {
    id: datasetId,
    async *cases() {
      let produced = 0;
      let cursor: string | undefined;
      while (limit === undefined || produced < limit) {
        const page = await memory.listThreads({
          namespaceId: config.namespaceId,
          ...(config.resourceId !== undefined && { resourceId: config.resourceId }),
          archived: false,
          order: ORDER,
          limit: pageSize,
          ...(cursor !== undefined && { cursor }),
        });
        for (const thread of page) {
          if (limit !== undefined && produced >= limit) return;
          const key: ThreadKey = {
            namespaceId: thread.namespaceId,
            ...(thread.resourceId !== null && { resourceId: thread.resourceId }),
            threadId: thread.threadId,
          };
          const firstUser = await firstUserMessage({ memory, key });
          if (firstUser === undefined) continue; // no user turn — nothing to evaluate
          const evalCase: EvalCase = {
            id: thread.threadId,
            input: firstUser,
            metadata: { threadId: thread.threadId, namespaceId: thread.namespaceId },
          };
          produced += 1;
          yield evalCase;
        }
        const last = page.at(-1);
        if (last === undefined || page.length < pageSize) return;
        cursor = nextThreadCursor({ thread: last, order: ORDER });
      }
    },
  };
}

/** The first user message of a thread, reading its head a slice at a time. */
async function firstUserMessage(params: {
  readonly memory: MemoryStore;
  readonly key: ThreadKey;
}): Promise<string | undefined> {
  let fromSeq: number | undefined;
  for (;;) {
    const head = await params.memory.getMessages(params.key, {
      order: "asc",
      limit: HEAD_MESSAGES,
      ...(fromSeq !== undefined && { fromSeq }),
    });
    const user = head.find((message) => message.role === "user");
    if (user !== undefined) return user.content;
    if (head.length < HEAD_MESSAGES) return undefined;
    fromSeq = head[head.length - 1]!.seq + 1;
  }
}
