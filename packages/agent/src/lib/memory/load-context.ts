// ---------------------------------------------------------------------------
// `loadResolvedContext` — the read side of `MemoryStore.resolveContext`,
// shared by every backend. It reads only what the prompt can hold:
//
//   - the thread's latest compaction episode (one row), and the messages
//     after it — never the folded ones;
//   - of those, the newest pages until the token budget is exceeded by one
//     message (so the trimmer can cut on a turn boundary), never the whole
//     transcript;
//   - resource episodes (when the budget opts in), top salience first, at
//     most as many as the episode budget could render.
//
// Then it hands the rows to the pure `resolveContext` cascade builder.
// ---------------------------------------------------------------------------

import { defaultEstimate, resolveContext } from "./resolve-context.ts";
import {
  COMPACT_EPISODE_KIND,
  type MemoryStore,
  type ResolvedContext,
  type StoredMessage,
  type ThreadKey,
  type ThreadRow,
  type TokenBudget,
} from "./types.ts";

/** Messages read per page while walking the tail back. */
const TAIL_PAGE_SIZE = 100;
/** Smallest token cost assumed per episode when sizing the episode read. */
const MIN_EPISODE_TOKENS = 8;
/** Upper bound on episodes read for one prompt, whatever the budget. */
const MAX_EPISODES_READ = 500;

/**
 * Resolve `key`'s prompt-ready context from `store` with bounded reads
 * (see the module comment). A thread without a row resolves as an empty
 * thread under the namespace / resource layers of `key`.
 */
export async function loadResolvedContext(params: {
  readonly store: MemoryStore;
  readonly key: ThreadKey;
  readonly budget: TokenBudget;
}): Promise<ResolvedContext> {
  const { store, key, budget } = params;
  const stored = await store.getThread(key);
  const thread: ThreadRow = stored ?? emptyThread(key);
  const resourceId = thread.resourceId ?? key.resourceId;
  const resourceKey = resourceId ? { namespaceId: key.namespaceId, resourceId } : null;
  const maxEpisodeTokens = budget.maxEpisodeTokens ?? 0;

  const [namespace, namespaceFacts, resource, resourceFacts, resourceEpisodes, threadFacts] =
    await Promise.all([
      store.getNamespace(key.namespaceId),
      store.listNamespaceFacts(key.namespaceId),
      resourceKey ? store.getResource(resourceKey) : null,
      resourceKey ? store.listResourceFacts(resourceKey) : [],
      resourceKey && maxEpisodeTokens > 0
        ? store.listResourceEpisodes(resourceKey, {
            order: "salienceDesc",
            limit: Math.min(MAX_EPISODES_READ, Math.ceil(maxEpisodeTokens / MIN_EPISODE_TOKENS)),
          })
        : [],
      stored ? store.listThreadFacts(key) : [],
    ]);

  const compaction = stored
    ? ((
        await store.listThreadEpisodes(key, {
          kind: COMPACT_EPISODE_KIND,
          order: "sourceRangeDesc",
          limit: 1,
        })
      )[0] ?? null)
    : null;
  const messages = stored
    ? await readMessageTail({
        store,
        key,
        afterSeq: compaction?.sourceMessageRange?.toSeq ?? 0,
        budget,
      })
    : [];

  return resolveContext({
    namespace,
    namespaceFacts,
    resource,
    resourceFacts,
    resourceEpisodes,
    thread,
    threadFacts,
    compaction,
    messages,
    budget,
  });
}

/**
 * The newest messages after `afterSeq`, oldest first: pages are read
 * backwards until their token estimate exceeds `budget.maxMessageTokens`
 * (the message that tips it over is included, so the turn-aware trimmer
 * still sees where the partial oldest turn starts) or the range runs out.
 */
export async function readMessageTail(params: {
  readonly store: MemoryStore;
  readonly key: ThreadKey;
  readonly afterSeq: number;
  readonly budget: TokenBudget;
}): Promise<StoredMessage[]> {
  const { store, key, afterSeq, budget } = params;
  const estimate = budget.estimate ?? defaultEstimate;
  const newestFirst: StoredMessage[] = [];
  let total = 0;
  let beforeSeq: number | undefined;
  for (;;) {
    const page = await store.getMessages(key, {
      fromSeq: afterSeq + 1,
      ...(beforeSeq !== undefined && { toSeq: beforeSeq - 1 }),
      order: "desc",
      limit: TAIL_PAGE_SIZE,
    });
    for (const m of page) {
      newestFirst.push(m);
      total += estimate(m);
      if (total > budget.maxMessageTokens) return newestFirst.reverse();
    }
    if (page.length < TAIL_PAGE_SIZE) return newestFirst.reverse();
    beforeSeq = page[page.length - 1]!.seq;
  }
}

function emptyThread(key: ThreadKey): ThreadRow {
  return {
    namespaceId: key.namespaceId,
    resourceId: key.resourceId ?? null,
    threadId: key.threadId,
    title: null,
    workingMemory: null,
    inheritFromParent: true,
    metadata: {},
    archivedAt: null,
    createdAt: 0,
    updatedAt: 0,
  };
}
