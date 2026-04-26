// ---------------------------------------------------------------------------
// Tools for the model to interact with the layered `MemoryStore`.
//
// Three operations the model needs:
//   - set         → append a fact at thread or resource scope
//   - setWorking  → overwrite the markdown scratchpad at thread or resource
//   - recall      → search facts + episodes by keyword (or via SemanticRecall
//                   capability if the store implements it)
//
// Reads beyond `recall` happen automatically via `MemoryStore.resolveContext`
// at prompt build — facts, working memory, and (opt-in) episodes are
// already in the system prompt every turn. `recall` is for the cases that
// don't fit the budget: "what did we decide last month about X?".
//
// Scope keys (`namespaceId` / `resourceId` / `threadId`) come from the
// runtime task envelope, not the model. The tool binds them at construction
// so the model just picks `"thread"` or `"resource"` by name.
//
// Namespace-scope writes are intentionally NOT exposed to the model —
// namespace facts are org-wide policy, operator-only. A separate
// `setNamespaceMemory` tool can wrap that path for privileged contexts.
// ---------------------------------------------------------------------------

import { z } from "zod";
import { multiTool, command } from "../multi-tool.ts";
import {
  isSemanticRecall,
  type EpisodicRecord,
  type Fact,
  type MemoryStore,
  type RecallHit,
  type ScopedKey,
  type ThreadKey,
} from "../memory/types.ts";

export interface LayeredMemoryToolConfig {
  readonly store: MemoryStore;
  readonly namespaceId: string;
  /** Optional — when absent, `"resource"` scope is rejected at runtime. */
  readonly resourceId?: string;
  readonly threadId: string;
  /**
   * Default sources searched by `recall` when the model does not specify.
   * Default: `["fact", "episode"]` — messages are not searched by default
   * since recent ones are already in context.
   */
  readonly defaultRecallSources?: ReadonlyArray<"fact" | "episode" | "message">;
}

const ModelScope = z.enum(["thread", "resource"]);
const RecallScope = z.enum(["thread", "resource", "namespace"]);
const RecallSource = z.enum(["fact", "episode", "message"]);

/**
 * `memory.set` / `memory.setWorking` / `memory.recall` bundled into one tool
 * so agents use a single tool slot. Pass the result into your tool map:
 *
 *   const memory = createLayeredMemoryTool({
 *     store, namespaceId, resourceId, threadId,
 *   });
 *   agentLoop({ tools: { memory } });
 */
export function createLayeredMemoryTool(config: LayeredMemoryToolConfig) {
  const threadKey: ThreadKey = {
    namespaceId: config.namespaceId,
    resourceId: config.resourceId,
    threadId: config.threadId,
  };
  const resourceKey: ScopedKey | null = config.resourceId
    ? { namespaceId: config.namespaceId, resourceId: config.resourceId }
    : null;
  const defaultSources = config.defaultRecallSources ?? ["fact", "episode"];

  return multiTool({
    name: "memory",
    description:
      "Read and write durable agent memory. Pick the right command for the kind " +
      "of information:\n" +
      "  - `set`: a small, lasting fact about the user or this conversation\n" +
      "  - `setWorking`: current state / focus / scratch — overwrites in place\n" +
      "  - `recall`: search past facts and session summaries that aren't already " +
      "in your context.",
    commands: {
      set: command({
        description:
          "Save a small, durable claim that should survive this conversation. " +
          "One line, atomic, true.\n\n" +
          "Use scope='thread' (default) for facts about THIS conversation only:\n" +
          "  - 'we decided to use Postgres for the audit log'\n" +
          "  - 'the bug only happens on staging'\n\n" +
          "Use scope='resource' for facts that should follow the user across ALL " +
          "their conversations:\n" +
          "  - 'the user lives in PST'\n" +
          "  - 'the user is allergic to peanuts'\n" +
          "  - 'the user prefers terse answers'\n\n" +
          "DO NOT use this for current state, in-progress reasoning, or anything " +
          "that will change soon — that's what `setWorking` is for. Don't repeat " +
          "facts that are already saved.",
        parameters: z.object({
          scope: ModelScope.default("thread").describe(
            "thread = this conversation only. resource = this user, all conversations.",
          ),
          text: z.string().min(1).describe("The fact, in one line."),
        }),
        execute: async ({ scope, text }) => {
          if (scope === "resource") {
            if (!resourceKey) {
              return "Cannot write resource-scope memory: no resourceId is bound to this task.";
            }
            const f = await config.store.appendResourceFact(resourceKey, text);
            return `Saved to resource memory (id: ${shortId(f.id)}).`;
          }
          const f = await config.store.appendThreadFact(threadKey, text);
          return `Saved to thread memory (id: ${shortId(f.id)}).`;
        },
      }),

      setWorking: command({
        description:
          "Replace the markdown scratchpad with current state. Use this for " +
          "things that change frequently and aren't worth saving as durable " +
          "facts:\n" +
          "  - 'currently debugging the login flow, blocked on token refresh'\n" +
          "  - 'next step: write the schema migration, then the rollback test'\n" +
          "  - 'open questions: do we need pgvector or is FTS enough?'\n\n" +
          "The scratchpad is injected into the system prompt every turn, so " +
          "keep it SHORT (a few lines) and CURRENT. Each call REPLACES the " +
          "previous scratchpad — it's not append. To remember something " +
          "permanently, use `set` instead.",
        parameters: z.object({
          scope: ModelScope.default("thread").describe(
            "thread = this conversation. resource = persists across this user's conversations.",
          ),
          markdown: z.string().describe("New scratchpad content. Replaces whatever was there."),
        }),
        execute: async ({ scope, markdown }) => {
          if (scope === "resource") {
            if (!resourceKey) {
              return "Cannot write resource-scope working memory: no resourceId is bound.";
            }
            await config.store.upsertResource(resourceKey, { workingMemory: markdown });
            return "Updated resource working memory.";
          }
          // Auto-create the thread if it doesn't exist yet — symmetric with
          // `appendMessages` / `appendThreadFact` which also auto-create.
          if (!(await config.store.getThread(threadKey))) {
            await config.store.createThread(threadKey);
          }
          await config.store.setThreadWorking(threadKey, markdown);
          return "Updated thread working memory.";
        },
      }),

      recall: command({
        description:
          "Search durable memory for things you don't already see in your " +
          "context. Use this when the user references something from a past " +
          "conversation:\n" +
          "  - 'remember that thing about my partner's allergies?'\n" +
          "  - 'what did we decide about the database last month?'\n" +
          "  - 'didn't we already talk about this somewhere?'\n\n" +
          "Returns the top matching facts and past-session summaries " +
          "(episodes) ranked by relevance + salience. Don't use this for " +
          "things already injected into your system prompt — those are " +
          "already visible to you.",
        parameters: z.object({
          query: z.string().min(1).describe("Natural-language search query."),
          scopes: z
            .array(RecallScope)
            .optional()
            .describe("Layers to search. Default: thread + resource."),
          sources: z
            .array(RecallSource)
            .optional()
            .describe("Source types. Default: fact + episode."),
          limit: z.number().int().min(1).max(20).default(5),
        }),
        execute: async ({ query, scopes, sources, limit }) => {
          const wantScopes = scopes ?? defaultSearchScopes(resourceKey != null);
          const wantSources = sources ?? [...defaultSources];

          // If the store implements SemanticRecall, prefer it.
          if (isSemanticRecall(config.store)) {
            const hits = await config.store.recall(threadKey, query, {
              limit,
              sources: wantSources,
            });
            return formatHits(hits, query);
          }

          // Fallback: keyword scan over facts + episodes at the requested scopes.
          const hits = await keywordSearch({
            store: config.store,
            namespaceId: config.namespaceId,
            resourceKey,
            threadKey,
            query,
            scopes: wantScopes,
            sources: wantSources,
            limit,
          });
          return formatHits(hits, query);
        },
      }),
    },
  });
}

function defaultSearchScopes(hasResource: boolean): Array<"thread" | "resource" | "namespace"> {
  return hasResource ? ["thread", "resource"] : ["thread"];
}

interface KeywordSearchInput {
  readonly store: MemoryStore;
  readonly namespaceId: string;
  readonly resourceKey: ScopedKey | null;
  readonly threadKey: ThreadKey;
  readonly query: string;
  readonly scopes: ReadonlyArray<"thread" | "resource" | "namespace">;
  readonly sources: ReadonlyArray<"fact" | "episode" | "message">;
  readonly limit: number;
}

async function keywordSearch(input: KeywordSearchInput): Promise<RecallHit[]> {
  const q = input.query.toLowerCase();
  const out: RecallHit[] = [];

  const wantFacts = input.sources.includes("fact");
  const wantEpisodes = input.sources.includes("episode");

  // Per scope: load + filter + score by keyword overlap (with salience tiebreak for episodes).
  for (const scope of input.scopes) {
    if (scope === "namespace") {
      if (wantFacts) {
        const facts = await input.store.listNamespaceFacts(input.namespaceId);
        out.push(...factsToHits(facts, "namespace", q));
      }
      if (wantEpisodes) {
        const eps = await input.store.listNamespaceEpisodes(input.namespaceId);
        out.push(...episodesToHits(eps, "namespace", q));
      }
    } else if (scope === "resource" && input.resourceKey) {
      if (wantFacts) {
        const facts = await input.store.listResourceFacts(input.resourceKey);
        out.push(...factsToHits(facts, "resource", q));
      }
      if (wantEpisodes) {
        const eps = await input.store.listResourceEpisodes(input.resourceKey);
        out.push(...episodesToHits(eps, "resource", q));
      }
    } else if (scope === "thread") {
      if (wantFacts) {
        const facts = await input.store.listThreadFacts(input.threadKey);
        out.push(...factsToHits(facts, "thread", q));
      }
      if (wantEpisodes) {
        const eps = await input.store.listThreadEpisodes(input.threadKey);
        out.push(...episodesToHits(eps, "thread", q));
      }
    }
  }

  return out
    .filter((h) => h.score > 0)
    .sort((a, b) => b.score - a.score)
    .slice(0, input.limit);
}

function factsToHits(
  facts: ReadonlyArray<Fact>,
  scope: RecallHit["scope"],
  q: string,
): RecallHit[] {
  return facts.flatMap((f) => {
    const score = keywordScore(q, f.text);
    if (score <= 0) return [];
    return [
      {
        source: "fact" as const,
        scope,
        id: f.id,
        text: f.text,
        score,
        createdAt: f.createdAt,
      },
    ];
  });
}

function episodesToHits(
  eps: ReadonlyArray<EpisodicRecord>,
  scope: RecallHit["scope"],
  q: string,
): RecallHit[] {
  return eps.flatMap((e) => {
    const haystack = e.summary + (e.outcome ? ` ${e.outcome}` : "");
    const overlap = keywordScore(q, haystack);
    if (overlap <= 0) return [];
    // Combine keyword overlap (0..1) with salience (0..1). Equal weight.
    const score = 0.5 * overlap + 0.5 * e.salience;
    return [
      {
        source: "episode" as const,
        scope,
        id: e.id,
        text: e.outcome ? `${e.summary} — ${e.outcome}` : e.summary,
        score,
        createdAt: e.createdAt,
      },
    ];
  });
}

/**
 * Token-level overlap score, 0..1. Identical to the heuristic in
 * `memory-index.ts` so the two recall paths feel similar to the model.
 */
function keywordScore(query: string, content: string): number {
  const words = (s: string) =>
    new Set(
      s
        .toLowerCase()
        .split(/\W+/)
        .filter((w) => w.length > 2),
    );
  const queryWords = words(query);
  const contentWords = words(content);
  if (queryWords.size === 0) return 0;
  let matches = 0;
  for (const w of queryWords) if (contentWords.has(w)) matches++;
  return matches / queryWords.size;
}

function formatHits(hits: ReadonlyArray<RecallHit>, query: string): string {
  if (hits.length === 0) return `No memories matched "${query}".`;
  return hits
    .map((h, i) => `${i + 1}. [${h.scope}/${h.source}] (${h.score.toFixed(2)}) ${h.text}`)
    .join("\n");
}

function shortId(id: string): string {
  return id.length > 8 ? id.slice(0, 8) : id;
}
