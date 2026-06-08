// ---------------------------------------------------------------------------
// Memory inspector — snapshot + targeted edits over a thread's memory cascade.
//
// Routes:
//   GET    /api/memory/inspect?namespaceId=&resourceId=&threadId=&agentId=
//   PATCH  /api/memory/namespace/:namespaceId          { staticRules?, workingMemory? }
//   POST   /api/memory/namespace/:namespaceId/facts    { text }
//   DELETE /api/memory/namespace/:namespaceId/facts/:factId
//
// The operator's debugging question is "what does the model actually see?"
// The answer needs four things in one round-trip:
//
//   1. The persona (role system prompt) + the composite cascade
//      (resolveContext output) — together, what the LLM actually reads:
//      persona first, then the cascade.
//   2. Per-scope rows + working memory + facts + episodes — so the operator
//      can locate which layer a wrong/stale piece of context came from.
//   3. The thread message history.
//
// Tenant binding: `namespaceId` is required. `resourceId` opts the resource
// layer in. `threadId` opts the thread layer in (and unlocks resolveContext
// since the cascade is thread-rooted). `agentId` opts the persona in.
//
// Namespace mutations are operator-only — the agent-side memory tool does
// not expose namespace writes. Episodes stay read-only here because they're
// LLM-generated artefacts of distillation; hand-writing them would defeat
// the consolidator's salience scoring.
// ---------------------------------------------------------------------------

import type {
  AgentRegistry,
  EpisodicRecord,
  Fact,
  FragmentRegistry,
  MemoryStore,
  NamespaceRow,
  RegisteredAgent,
  ResourceRow,
  ResolvedContext,
  StoredMessage,
  ThreadRow,
} from "@promin/agent";
import { inlineRoleDefinition, resolveSystemPrompt } from "@promin/agent";
import { json, jsonError } from "../router.ts";
import type { NamespaceService } from "../services/namespaces.ts";
import { resolveRequiredNamespaceId } from "./namespace-validation.ts";

export interface MemoryInspectorDeps {
  readonly memory: MemoryStore;
  readonly namespaces?: NamespaceService;
  /**
   * Recipe store. When set (and the request carries `agentId`), the
   * inspector resolves the agent's persona — its role system prompt — so
   * the Prompt tab can show the full picture the model receives: persona
   * first, then the memory cascade. Without it, only the cascade is shown.
   */
  readonly registry?: AgentRegistry;
  /**
   * Fragment registry. Needed to expand layered (`{ base, layers }`)
   * personas exactly as `resolveLocalAgent` does at run time. Recipes
   * using the plain-string form resolve without it.
   */
  readonly fragments?: FragmentRegistry;
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
  /**
   * The agent's resolved persona — its role system prompt, with layered
   * `{ base, layers }` forms expanded through the fragment registry. The
   * model sees this BEFORE the memory cascade (`resolved.systemPrompt`).
   * `null` when no `agentId` was given, no registry is wired, the agent
   * isn't found, or the recipe has no system prompt.
   *
   * Note: this is the role prompt only — the auto-generated skill-catalog
   * block that `resolveLocalAgent` appends at run time is NOT included.
   */
  readonly persona: string | null;
}

// Default budget used by `resolveContext` for inspection. The inspector
// is a debugging tool — its job is to show the operator what an agent
// turn WOULD see. Both numbers are generous so the rendered prompt
// matches what a real cross-thread recall would produce; episodes are
// rendered (maxEpisodeTokens > 0) so the operator can verify
// distillThread-written rollups actually flow into the cascade.
const DEFAULT_INSPECT_BUDGET = {
  maxMessageTokens: 16_000,
  maxEpisodeTokens: 8_000,
};

export function inspectMemory(deps: MemoryInspectorDeps) {
  return async (req: Request): Promise<Response> => {
    const url = new URL(req.url);
    const namespaceId = url.searchParams.get("namespaceId") ?? undefined;
    const resourceId = url.searchParams.get("resourceId") ?? undefined;
    const threadId = url.searchParams.get("threadId") ?? undefined;
    const agentId = url.searchParams.get("agentId") ?? undefined;
    const namespace = await resolveRequiredNamespaceId(deps.namespaces, namespaceId);
    if ("response" in namespace) return namespace.response;

    try {
      const namespaceSnapshot = await loadNamespaceSnapshot(deps.memory, namespace.namespaceId);
      const resource = resourceId
        ? await loadResourceSnapshot(deps.memory, {
            namespaceId: namespace.namespaceId,
            resourceId,
          })
        : null;
      const thread = threadId
        ? await loadThreadSnapshot(deps.memory, {
            namespaceId: namespace.namespaceId,
            resourceId,
            threadId,
          })
        : null;
      const resolved = threadId
        ? await loadResolvedSummary(deps.memory, {
            namespaceId: namespace.namespaceId,
            resourceId,
            threadId,
          })
        : null;
      const persona = agentId ? await loadPersona(deps, agentId) : null;

      const response: MemoryInspectResponse = {
        namespaceId: namespace.namespaceId,
        resourceId: resourceId ?? null,
        threadId: threadId ?? null,
        namespace: namespaceSnapshot,
        resource,
        thread,
        resolved,
        persona,
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

// ---------------------------------------------------------------------------
// Namespace mutations — operator-only writes. The agent-side memory tool
// stops at thread + resource scope; namespace is policy that lives above
// individual user state, so it gets a separate gate.
// ---------------------------------------------------------------------------

export function patchNamespace(deps: MemoryInspectorDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const namespaceId = params.namespaceId;
    const namespace = await resolveRequiredNamespaceId(deps.namespaces, namespaceId);
    if ("response" in namespace) return namespace.response;
    const body = (await req.json().catch(() => null)) as {
      staticRules?: string | null;
      workingMemory?: string | null;
    } | null;
    if (!body) return jsonError(400, "invalid_body");

    // Pass through only the fields the caller actually set, so omitting
    // workingMemory in the payload doesn't accidentally clear it.
    const patch: { staticRules?: string | null; workingMemory?: string | null } = {};
    if ("staticRules" in body) patch.staticRules = body.staticRules ?? null;
    if ("workingMemory" in body) patch.workingMemory = body.workingMemory ?? null;
    if (Object.keys(patch).length === 0) return jsonError(400, "empty_patch");

    try {
      const row = await deps.memory.upsertNamespace(namespace.namespaceId, patch);
      return json(200, { row });
    } catch (err) {
      return jsonError(500, "patch_failed", err instanceof Error ? err.message : String(err));
    }
  };
}

export function addNamespaceFact(deps: MemoryInspectorDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const namespaceId = params.namespaceId;
    const namespace = await resolveRequiredNamespaceId(deps.namespaces, namespaceId);
    if ("response" in namespace) return namespace.response;
    const body = (await req.json().catch(() => null)) as { text?: unknown } | null;
    const text = typeof body?.text === "string" ? body.text.trim() : "";
    if (!text) return jsonError(400, "missing_text");

    try {
      const fact = await deps.memory.appendNamespaceFact(namespace.namespaceId, text);
      return json(201, { fact });
    } catch (err) {
      return jsonError(500, "add_failed", err instanceof Error ? err.message : String(err));
    }
  };
}

export function deleteNamespaceFact(deps: MemoryInspectorDeps) {
  return async (_req: Request, params: Record<string, string>): Promise<Response> => {
    const namespaceId = params.namespaceId;
    const factId = params.factId;
    const namespace = await resolveRequiredNamespaceId(deps.namespaces, namespaceId);
    if ("response" in namespace) return namespace.response;
    if (!factId) return jsonError(400, "missing_factId");

    try {
      await deps.memory.deleteNamespaceFact(namespace.namespaceId, factId);
      return json(200, { ok: true });
    } catch (err) {
      return jsonError(500, "delete_failed", err instanceof Error ? err.message : String(err));
    }
  };
}

/**
 * Resolve an agent's persona (role system prompt) the same way the runtime
 * does in `resolveLocalAgent` — flattening the plain-string / layered /
 * null shapes through the fragment registry. Only the role prompt: the
 * skill-catalog block appended at run time is intentionally left out (it's
 * auto-generated boilerplate and resolving it needs the async skill
 * catalog, which the inspector doesn't carry). Returns `null` when the
 * agent can't be resolved to a persona, so a missing recipe degrades
 * gracefully rather than failing the whole inspection.
 */
async function loadPersona(deps: MemoryInspectorDeps, agentId: string): Promise<string | null> {
  if (!deps.registry) return null;
  try {
    const recipe: RegisteredAgent | null = await deps.registry.get(agentId);
    if (!recipe || recipe.backend.type !== "local") return null;
    // Behavioral fields live on the role binding. A `ref` binding resolves
    // to undefined here (no I/O) → null persona for now; a follow-up will
    // resolve refs.
    const def = inlineRoleDefinition(recipe.backend.role);
    const persona = resolveSystemPrompt({
      systemPrompt: def?.systemPrompt ?? null,
      ...(deps.fragments !== undefined && { fragments: deps.fragments }),
    });
    return persona?.trim() ? persona : null;
  } catch {
    return null;
  }
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
