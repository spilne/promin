// ---------------------------------------------------------------------------
// Memory inspector — read-only snapshot of a thread's memory cascade.
//
// Route:
//   GET /api/memory/inspect?namespaceId=&resourceId=&threadId=
//
// The operator's debugging question is "what does the model actually see?"
// The answer needs four things in one round-trip:
//
//   1. The composite system prompt (resolveContext output) — what the LLM
//      will read after the cascade collapses.
//   2. Per-scope rows + working memory + facts + episodes — so the operator
//      can locate which layer a wrong/stale piece of context came from.
//   3. The thread message history.
//
// Tenant binding: `namespaceId` is required. `resourceId` opts the resource
// layer in. `threadId` opts the thread layer in (and unlocks resolveContext
// since the cascade is thread-rooted).
//
// CRUD / audit / GDPR bulk-delete / retrieval attribution are intentionally
// out of scope — they belong to the larger promin-w7tc spec which assumes a
// permission gradient + audit log infra that isn't in place yet.
// ---------------------------------------------------------------------------

import type {
  EpisodicRecord,
  Fact,
  MemoryStore,
  NamespaceRow,
  ResourceRow,
  ResolvedContext,
  StoredMessage,
  ThreadRow,
} from "@promin/agent";
import { json, jsonError } from "../router.ts";

export interface MemoryInspectorDeps {
  readonly memory: MemoryStore;
}

// ---------------------------------------------------------------------------
// Response DTOs — re-exported so the UI imports the API shape from here.
// ---------------------------------------------------------------------------

export type {
  EpisodicRecord,
  Fact,
  NamespaceRow,
  ResourceRow,
  StoredMessage,
  ThreadRow,
} from "@promin/agent";

export interface NamespaceSnapshot {
  readonly row: NamespaceRow | null;
  readonly facts: Fact[];
  readonly episodes: EpisodicRecord[];
}

export interface ResourceSnapshot {
  readonly row: ResourceRow | null;
  readonly facts: Fact[];
  readonly episodes: EpisodicRecord[];
}

export interface ThreadSnapshot {
  readonly row: ThreadRow | null;
  readonly facts: Fact[];
  readonly episodes: EpisodicRecord[];
  readonly messages: StoredMessage[];
}

/**
 * Slimmed-down ResolvedContext — drops the full message tail (the inspector
 * already shows it under thread.messages) but keeps the prompt and the
 * count of trimmed-in messages so the operator sees what made the cut.
 */
export interface ResolvedContextSummary {
  readonly systemPrompt: string;
  readonly messageCount: number;
}

export interface MemoryInspectResponse {
  readonly namespaceId: string;
  readonly resourceId: string | null;
  readonly threadId: string | null;
  readonly namespace: NamespaceSnapshot;
  readonly resource: ResourceSnapshot | null;
  readonly thread: ThreadSnapshot | null;
  /** Present only when `threadId` is given. */
  readonly resolved: ResolvedContextSummary | null;
}

// Default budget used by `resolveContext` for inspection. Generous enough
// to see most thread tails; the inspector is a debugging tool, not a
// runtime cost-sensitive path.
const DEFAULT_INSPECT_BUDGET = { maxMessageTokens: 16_000 };

export function inspectMemory(deps: MemoryInspectorDeps) {
  return async (req: Request): Promise<Response> => {
    const url = new URL(req.url);
    const namespaceId = url.searchParams.get("namespaceId") ?? undefined;
    const resourceId = url.searchParams.get("resourceId") ?? undefined;
    const threadId = url.searchParams.get("threadId") ?? undefined;
    if (!namespaceId) return jsonError(400, "missing_namespaceId");

    try {
      const namespace = await loadNamespaceSnapshot(deps.memory, namespaceId);
      const resource = resourceId
        ? await loadResourceSnapshot(deps.memory, { namespaceId, resourceId })
        : null;
      const thread = threadId
        ? await loadThreadSnapshot(deps.memory, { namespaceId, resourceId, threadId })
        : null;
      const resolved = threadId
        ? await loadResolvedSummary(deps.memory, { namespaceId, resourceId, threadId })
        : null;

      const response: MemoryInspectResponse = {
        namespaceId,
        resourceId: resourceId ?? null,
        threadId: threadId ?? null,
        namespace,
        resource,
        thread,
        resolved,
      };
      return json(200, response);
    } catch (err) {
      return jsonError(500, "inspect_failed", err instanceof Error ? err.message : String(err));
    }
  };
}

async function loadNamespaceSnapshot(
  store: MemoryStore,
  namespaceId: string,
): Promise<NamespaceSnapshot> {
  const [row, facts, episodes] = await Promise.all([
    store.getNamespace(namespaceId),
    store.listNamespaceFacts(namespaceId),
    store.listNamespaceEpisodes(namespaceId, { order: "createdDesc" }).catch(() => []),
  ]);
  return { row, facts, episodes };
}

async function loadResourceSnapshot(
  store: MemoryStore,
  key: { namespaceId: string; resourceId: string },
): Promise<ResourceSnapshot> {
  const [row, facts, episodes] = await Promise.all([
    store.getResource(key),
    store.listResourceFacts(key),
    store.listResourceEpisodes(key, { order: "createdDesc" }).catch(() => []),
  ]);
  return { row, facts, episodes };
}

async function loadThreadSnapshot(
  store: MemoryStore,
  key: { namespaceId: string; resourceId?: string; threadId: string },
): Promise<ThreadSnapshot> {
  const [row, facts, episodes, messages] = await Promise.all([
    store.getThread(key),
    store.listThreadFacts(key).catch(() => []),
    store.listThreadEpisodes(key, { order: "createdDesc" }).catch(() => []),
    store.getMessages(key, { order: "asc" }).catch(() => []),
  ]);
  return { row, facts, episodes, messages };
}

async function loadResolvedSummary(
  store: MemoryStore,
  key: { namespaceId: string; resourceId?: string; threadId: string },
): Promise<ResolvedContextSummary | null> {
  try {
    const ctx: ResolvedContext = await store.resolveContext(key, DEFAULT_INSPECT_BUDGET);
    return {
      systemPrompt: ctx.systemPrompt,
      messageCount: ctx.messages.length,
    };
  } catch {
    // Thread doesn't exist or store rejected the call — surface as null
    // rather than failing the whole inspection (the per-scope panes still
    // give useful info).
    return null;
  }
}
