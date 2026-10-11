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
  RoleDefinition,
  RoleRegistry,
  StoredMessage,
  ThreadRow,
} from "@promin/agent";
import { inlineRoleDefinition, resolveSystemPrompt } from "@promin/agent";
import { z } from "zod";
import { json, jsonError } from "../router.ts";
import { internalError, type ScopeAuthorizer } from "./agent-gateway-kit.ts";
import { THREAD_AGENT_METADATA_KEY } from "./agents.ts";

export interface MemoryInspectorDeps {
  readonly memory: MemoryStore;
  /** Role registry — resolves the persona of an agent bound to a role by `ref`. */
  readonly roles?: RoleRegistry;
  /** Binds the inspected scope to the caller. See `ScopeAuthorizer`. */
  readonly authorizeScope?: ScopeAuthorizer;
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

/** Newest thread messages the inspector returns. */
const INSPECT_MESSAGE_LIMIT = 500;

export function inspectMemory(deps: MemoryInspectorDeps) {
  return async (req: Request): Promise<Response> => {
    const url = new URL(req.url);
    const namespaceId = url.searchParams.get("namespaceId") || undefined;
    const resourceId = url.searchParams.get("resourceId") || undefined;
    const threadId = url.searchParams.get("threadId") || undefined;
    const agentId = url.searchParams.get("agentId") || undefined;
    if (!namespaceId) return jsonError(400, "missing_namespaceId");
    if (deps.authorizeScope) {
      const allowed = await deps.authorizeScope({
        req,
        scope: {
          namespaceId,
          ...(resourceId !== undefined && { resourceId }),
          ...(agentId !== undefined && { agentId }),
        },
      });
      if (!allowed) return jsonError(403, "scope_forbidden");
    }

    try {
      const namespace = await loadNamespaceSnapshot(deps.memory, namespaceId);
      const resource = resourceId
        ? await loadResourceSnapshot(deps.memory, { namespaceId, resourceId })
        : null;
      // A thread is shown only through the resource (and agent, when one is
      // named) that owns it; anyone else sees it as absent.
      const ownsThread = threadId
        ? await callerOwnsThread(deps.memory, { namespaceId, resourceId, threadId, agentId })
        : false;
      const thread = threadId
        ? ownsThread
          ? await loadThreadSnapshot(deps.memory, { namespaceId, resourceId, threadId })
          : { row: null, facts: [], episodes: [], messages: [] }
        : null;
      const resolved =
        threadId && ownsThread
          ? await loadResolvedSummary(deps.memory, { namespaceId, resourceId, threadId })
          : null;
      const persona = agentId ? await loadPersona(deps, agentId) : null;

      const response: MemoryInspectResponse = {
        namespaceId,
        resourceId: resourceId ?? null,
        threadId: threadId ?? null,
        namespace,
        resource,
        thread,
        resolved,
        persona,
      };
      return json(200, response);
    } catch (err) {
      return internalError({ code: "inspect_failed", err });
    }
  };
}

async function callerOwnsThread(
  store: MemoryStore,
  params: { namespaceId: string; resourceId?: string; threadId: string; agentId?: string },
): Promise<boolean> {
  const row = await store.getThread({ namespaceId: params.namespaceId, threadId: params.threadId });
  if (!row) return false;
  if (row.resourceId !== (params.resourceId ?? null)) return false;
  if (params.agentId !== undefined && row.metadata[THREAD_AGENT_METADATA_KEY] !== params.agentId) {
    return false;
  }
  return true;
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
  const [row, facts, episodes, newest] = await Promise.all([
    store.getThread(key),
    store.listThreadFacts(key).catch(() => []),
    store.listThreadEpisodes(key, { order: "createdDesc" }).catch(() => []),
    store.getMessages(key, { order: "desc", limit: INSPECT_MESSAGE_LIMIT }).catch(() => []),
  ]);
  return { row, facts, episodes, messages: [...newest].reverse() };
}

// ---------------------------------------------------------------------------
// Namespace mutations — operator-only writes. The agent-side memory tool
// stops at thread + resource scope; namespace is policy that lives above
// individual user state, so it gets a separate gate.
// ---------------------------------------------------------------------------

const NamespacePatchSchema = z
  .object({
    staticRules: z.string().nullable().optional(),
    workingMemory: z.string().nullable().optional(),
  })
  .strict();

/** Largest namespace fact accepted, in characters. */
const MAX_FACT_LENGTH = 10_000;

export function patchNamespace(deps: MemoryInspectorDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const namespaceId = params.namespaceId;
    if (!namespaceId) return jsonError(400, "missing_namespaceId");
    const parsed = NamespacePatchSchema.safeParse(await req.json().catch(() => null));
    if (!parsed.success) {
      return jsonError(
        400,
        "invalid_body",
        "Expected { staticRules?: string | null, workingMemory?: string | null }.",
      );
    }

    // Pass through only the fields the caller actually set, so omitting
    // workingMemory in the payload doesn't accidentally clear it.
    const patch: { staticRules?: string | null; workingMemory?: string | null } = {};
    if (parsed.data.staticRules !== undefined) patch.staticRules = parsed.data.staticRules;
    if (parsed.data.workingMemory !== undefined) patch.workingMemory = parsed.data.workingMemory;
    if (Object.keys(patch).length === 0) return jsonError(400, "empty_patch");

    const row = await deps.memory.upsertNamespace(namespaceId, patch);
    return json(200, { row });
  };
}

export function addNamespaceFact(deps: MemoryInspectorDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const namespaceId = params.namespaceId;
    if (!namespaceId) return jsonError(400, "missing_namespaceId");
    const body = (await req.json().catch(() => null)) as { text?: unknown } | null;
    const text = typeof body?.text === "string" ? body.text.trim() : "";
    if (!text) return jsonError(400, "missing_text");
    if (text.length > MAX_FACT_LENGTH) {
      return jsonError(400, "text_too_long", `A fact is at most ${MAX_FACT_LENGTH} characters.`);
    }
    const fact = await deps.memory.appendNamespaceFact(namespaceId, text);
    return json(201, { fact });
  };
}

export function deleteNamespaceFact(deps: MemoryInspectorDeps) {
  return async (_req: Request, params: Record<string, string>): Promise<Response> => {
    const namespaceId = params.namespaceId;
    const factId = params.factId;
    if (!namespaceId) return jsonError(400, "missing_namespaceId");
    if (!factId) return jsonError(400, "missing_factId");
    const facts = await deps.memory.listNamespaceFacts(namespaceId);
    if (!facts.some((f) => f.id === factId)) return jsonError(404, "fact_not_found");
    await deps.memory.deleteNamespaceFact(namespaceId, factId);
    return json(200, { ok: true });
  };
}

/**
 * Resolve an agent's persona (role system prompt) the same way the runtime
 * does in `resolveLocalAgent` — an inline role directly, a `ref` role
 * through the role registry, then the plain-string / layered / null
 * prompt shapes through the fragment registry. Only the role prompt: the
 * skill-catalog block appended at run time is intentionally left out.
 * Returns `null` when the agent can't be resolved to a persona, so a
 * missing recipe degrades gracefully rather than failing the inspection.
 */
async function loadPersona(deps: MemoryInspectorDeps, agentId: string): Promise<string | null> {
  if (!deps.registry) return null;
  try {
    const recipe: RegisteredAgent | null = await deps.registry.get(agentId);
    if (!recipe || recipe.backend.type !== "local") return null;
    const binding = recipe.backend.role;
    let def: RoleDefinition | undefined = inlineRoleDefinition(binding);
    if (!def && "ref" in binding && deps.roles) {
      const role = await deps.roles.get(binding.ref.id, binding.ref.version);
      def = role?.definition;
    }
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
    // Store rejected the call — surface as null rather than failing the
    // whole inspection (the per-scope panes still give useful info).
    return null;
  }
}
