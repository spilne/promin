// ---------------------------------------------------------------------------
// Tools for the model to interact with the layered `MemoryStore`.
//
// Three operations the model needs:
//   - set         → append a fact at thread or resource scope
//   - setWorking  → overwrite the markdown scratchpad at thread or resource
//   - recall      → search facts + episodes by keyword
//
// Reads beyond `recall` happen automatically via `MemoryStore.resolveContext`
// at prompt build — facts, working memory, and (opt-in) episodes are
// already in the prompt's memory reference-data block every turn. `recall` is for the cases that
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
import { tool, type AgentTool } from "../tool.ts";
import {
  MAX_FACT_CHARS,
  MAX_WORKING_MEMORY_CHARS,
  MemoryLimitError,
  type EpisodeListParams,
  type EpisodicRecord,
  type Fact,
  type MemoryStore,
  type Relation,
  type ScopedKey,
  type ThreadKey,
} from "../memory/types.ts";
import type { EmbeddingProvider } from "../embedding-provider.ts";
import { memoizeEmbedder, rankCandidates, tokenize } from "../memory/scoring.ts";
import {
  MAX_ENTITY_CHARS,
  MAX_PREDICATE_CHARS,
  MAX_RELATIONS_PER_RESOURCE,
  relationKey,
  relationNeighborhood,
} from "../memory/relations.ts";
import { fenceReferenceData, oneLine } from "../reference-data.ts";
import { THREAD_AGENT_METADATA_KEY } from "../agent/types.ts";

export interface LayeredMemoryToolConfig {
  readonly store: MemoryStore;
  readonly namespaceId: string;
  /** Optional — when absent, `"resource"` scope is rejected at runtime. */
  readonly resourceId?: string;
  readonly threadId: string;
  /**
   * Agent stamped as the owner (`THREAD_AGENT_METADATA_KEY`) of a thread
   * this tool has to create. Omitted: the thread is created unstamped.
   */
  readonly agentId?: string;
  /**
   * Default sources searched by `recall` when the model does not specify.
   * Default: `["fact", "episode"]`. Messages are not searchable: recent ones
   * are already in context.
   */
  readonly defaultRecallSources?: ReadonlyArray<RecallSourceName>;
  /**
   * Adds semantic matching to `recall`: query and candidates are embedded
   * and ranked by cosine similarity next to BM25. Without it, or when it
   * throws, recall is lexical only.
   */
  readonly embedder?: EmbeddingProvider;
  /**
   * Called when the embedder throws during `recall`, which then ranks
   * lexically. Use it to log or count embedding failures.
   */
  readonly onEmbedderError?: (error: unknown) => void;
  /**
   * Half-life of the recency term in `recall` ranking, in ms.
   * Default: `DEFAULT_RECALL_RECENCY_HALF_LIFE_MS` (90 days).
   */
  readonly recencyHalfLifeMs?: number;
  /** Clock, epoch ms. Default `Date.now`. */
  readonly now?: () => number;
}

/** Default `LayeredMemoryToolConfig.recencyHalfLifeMs`. */
export const DEFAULT_RECALL_RECENCY_HALF_LIFE_MS = 90 * 24 * 60 * 60 * 1000;

/** How far from a named entity `recall` follows relations, and how many it returns. */
const RECALL_GRAPH_HOPS = 2;
const RECALL_GRAPH_LIMIT = 12;

const ModelScope = z.enum(["thread", "resource"]);
const RecallScope = z.enum(["thread", "resource", "namespace"]);
const RecallSource = z.enum(["fact", "episode"]);
type RecallSourceName = z.infer<typeof RecallSource>;

const SET_DESCRIPTION =
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
  "When a saved fact changes (the user moved, switched tools), save the new " +
  "one with `supersedes` set to the old fact's id from `recall`, so the old " +
  "one stops being shown but stays as history. Facts a person wrote cannot be " +
  "replaced this way.\n\n" +
  "DO NOT use this for current state, in-progress reasoning, or anything " +
  "that will change soon — that's what `setWorking` is for. Don't repeat " +
  "facts that are already saved.";

const LINK_DESCRIPTION =
  "Record how two things relate, for this user across all conversations: " +
  "subject, predicate, object.\n" +
  "  - 'Anton' / 'works at' / 'Acme'\n" +
  "  - 'Acme' / 'is based in' / 'Berlin'\n" +
  "Use it for people, places, projects and organisations that come up " +
  "again; `recall` then follows the links, so asking about Anton also " +
  "surfaces where Acme is. Names match ignoring case. Use `set` for " +
  "anything that is not a relation between two named things, and `unlink` " +
  "(with an id from `recall`) to remove one you saved that is wrong.";

const SET_WORKING_DESCRIPTION =
  "Replace the markdown scratchpad with current state. Use this for " +
  "things that change frequently and aren't worth saving as durable " +
  "facts:\n" +
  "  - 'currently debugging the login flow, blocked on token refresh'\n" +
  "  - 'next step: write the schema migration, then the rollback test'\n" +
  "  - 'open questions: do we need pgvector or is FTS enough?'\n\n" +
  "The scratchpad is shown to you as memory every turn, so " +
  "keep it SHORT (a few lines) and CURRENT. Each call REPLACES the " +
  "previous scratchpad — it's not append. To remember something " +
  "permanently, use `set` instead.";

const RECALL_DESCRIPTION =
  "Search durable memory for things you don't already see in your " +
  "context. Use this when the user references something from a past " +
  "conversation:\n" +
  "  - 'remember that thing about my partner's allergies?'\n" +
  "  - 'what did we decide about the database last month?'\n" +
  "  - 'didn't we already talk about this somewhere?'\n\n" +
  "Returns the top matching facts and past-session summaries " +
  "(episodes) ranked by relevance + salience. Don't use this for " +
  "things already shown in your memory block — those are " +
  "already visible to you.";

/**
 * Input of the `memory` tool: one branch per command, discriminated by
 * `command`. The JSON-schema converter flattens it into a single object
 * with a required `command` enum, which is what provider tool APIs accept.
 */
export const LayeredMemoryToolInputSchema = z.discriminatedUnion("command", [
  z.object({
    command: z.literal("set"),
    scope: ModelScope.default("thread").describe(
      "thread = this conversation only. resource = this user, all conversations.",
    ),
    text: z
      .string()
      .trim()
      .min(1)
      .max(MAX_FACT_CHARS)
      .describe(`The fact, in one line (at most ${MAX_FACT_CHARS} characters).`),
    supersedes: z
      .string()
      .min(1)
      .optional()
      .describe(
        "Id (from a `recall` result) of a fact you saved earlier that this one replaces because " +
          "it changed or was wrong. Only facts the model saved can be replaced.",
      ),
    confidence: z
      .number()
      .min(0)
      .max(1)
      .optional()
      .describe(
        "How sure you are, 0 to 1: lower for a guess or inference, higher for something the " +
          "user said outright. Shakier facts rank lower in `recall`.",
      ),
  }),
  z.object({
    command: z.literal("link"),
    subject: z
      .string()
      .trim()
      .min(1)
      .max(MAX_ENTITY_CHARS)
      .describe("The entity the relation starts from, e.g. 'Anton'."),
    predicate: z
      .string()
      .trim()
      .min(1)
      .max(MAX_PREDICATE_CHARS)
      .describe("How they are related, a short verb phrase, e.g. 'works at'."),
    object: z
      .string()
      .trim()
      .min(1)
      .max(MAX_ENTITY_CHARS)
      .describe("The entity it points to, e.g. 'Acme'."),
    confidence: z
      .number()
      .min(0)
      .max(1)
      .optional()
      .describe("How sure you are, 0 to 1. Defaults to the usual for a model-written note."),
  }),
  z.object({
    command: z.literal("unlink"),
    id: z
      .string()
      .min(1)
      .describe("Id of a relation you saved earlier, from a `recall` result's Related section."),
  }),
  z.object({
    command: z.literal("setWorking"),
    scope: ModelScope.default("thread").describe(
      "thread = this conversation. resource = persists across this user's conversations.",
    ),
    markdown: z
      .string()
      .max(MAX_WORKING_MEMORY_CHARS)
      .describe(
        `New scratchpad content (at most ${MAX_WORKING_MEMORY_CHARS} characters). Replaces whatever was there.`,
      ),
  }),
  z.object({
    command: z.literal("recall"),
    query: z.string().min(1).describe("Natural-language search query."),
    scopes: z
      .array(RecallScope)
      .optional()
      .describe("Layers to search. Default: thread + resource."),
    sources: z.array(RecallSource).optional().describe("Source types. Default: fact + episode."),
    limit: z.number().int().min(1).max(20).default(5),
  }),
]);

export type LayeredMemoryToolInput = z.infer<typeof LayeredMemoryToolInputSchema>;

/**
 * `memory.set` / `memory.setWorking` / `memory.recall` bundled into one tool
 * so agents use a single tool slot. Pass the result into your tool map:
 *
 *   const memory = createLayeredMemoryTool({
 *     store, namespaceId, resourceId, threadId,
 *   });
 *   agentAction({ tools: { memory }, ... });
 */
export function createLayeredMemoryTool(
  config: LayeredMemoryToolConfig,
): AgentTool<LayeredMemoryToolInput, string> {
  const threadKey: ThreadKey = {
    namespaceId: config.namespaceId,
    resourceId: config.resourceId,
    threadId: config.threadId,
  };
  const resourceKey: ScopedKey | null = config.resourceId
    ? { namespaceId: config.namespaceId, resourceId: config.resourceId }
    : null;
  const defaultSources = config.defaultRecallSources ?? ["fact", "episode"];
  // One cache per tool set: recalls over the same memories re-embed nothing.
  const embedder = config.embedder && memoizeEmbedder({ embedder: config.embedder });

  return tool({
    name: "memory",
    description:
      "Read and write durable agent memory. Pick the right command for the kind " +
      "of information:\n" +
      "  - `set`: a small, lasting fact about the user or this conversation\n" +
      "  - `setWorking`: current state / focus / scratch — overwrites in place\n" +
      "  - `link`: how two named things relate (person works at company)\n" +
      "  - `recall`: search past facts and session summaries that aren't already " +
      "in your context.\n\n" +
      "Commands:\n" +
      `  set: ${SET_DESCRIPTION}\n` +
      `  link: ${LINK_DESCRIPTION}\n` +
      `  setWorking: ${SET_WORKING_DESCRIPTION}\n` +
      `  recall: ${RECALL_DESCRIPTION}`,
    parameters: LayeredMemoryToolInputSchema,
    execute: async (input) => {
      switch (input.command) {
        case "set": {
          if (input.scope === "resource") {
            if (!resourceKey) {
              return "Cannot write resource-scope memory: no resourceId is bound to this task.";
            }
            const replaced = resolveSupersedes({
              facts: await config.store.listResourceFacts(resourceKey),
              id: input.supersedes,
            });
            if (replaced.refusal) return replaced.refusal;
            const f = await config.store.appendResourceFact(resourceKey, {
              text: input.text,
              source: "model",
              ...(input.confidence !== undefined && { confidence: input.confidence }),
              ...(replaced.id && { supersedes: replaced.id }),
            });
            return `Saved to resource memory (id: ${f.id}).${replaced.note}`;
          }
          const replaced = resolveSupersedes({
            facts: await config.store.listThreadFacts(threadKey),
            id: input.supersedes,
          });
          if (replaced.refusal) return replaced.refusal;
          const f = await config.store.appendThreadFact(threadKey, {
            text: input.text,
            source: "model",
            ...(input.confidence !== undefined && { confidence: input.confidence }),
            ...(replaced.id && { supersedes: replaced.id }),
          });
          return `Saved to thread memory (id: ${f.id}).${replaced.note}`;
        }
        case "link": {
          if (!resourceKey) {
            return "Cannot write relations: no resourceId is bound to this task.";
          }
          const identity = relationKey(input);
          const already = (await config.store.listResourceRelations(resourceKey)).find(
            (r) => relationKey(r) === identity,
          );
          let r: Relation;
          try {
            r = await config.store.appendResourceRelation(
              resourceKey,
              {
                subject: input.subject,
                predicate: input.predicate,
                object: input.object,
                source: "model",
                ...(input.confidence !== undefined && { confidence: input.confidence }),
              },
              { maxRelations: MAX_RELATIONS_PER_RESOURCE },
            );
          } catch (error) {
            if (error instanceof MemoryLimitError) {
              return `Nothing saved: this user already has ${MAX_RELATIONS_PER_RESOURCE} relations. Use \`unlink\` on stale ones first.`;
            }
            throw error;
          }
          return already ? `Already linked (id: ${r.id}).` : `Linked (id: ${r.id}).`;
        }
        case "unlink": {
          if (!resourceKey) {
            return "Cannot remove relations: no resourceId is bound to this task.";
          }
          const target = (await config.store.listResourceRelations(resourceKey)).find(
            (r) => r.id === input.id,
          );
          if (!target) {
            return `Nothing removed: no relation with id ${input.id}. Use \`recall\` to find the id.`;
          }
          if (target.source !== "model") {
            return "Nothing removed: that relation was written by a person, not by you.";
          }
          await config.store.deleteResourceRelation(resourceKey, target.id);
          return "Removed the relation.";
        }
        case "setWorking": {
          if (input.scope === "resource") {
            if (!resourceKey) {
              return "Cannot write resource-scope working memory: no resourceId is bound.";
            }
            await config.store.upsertResource(resourceKey, { workingMemory: input.markdown });
            return "Updated resource working memory.";
          }
          // Auto-create the thread if it doesn't exist yet — symmetric with
          // `appendMessages` / `appendThreadFact` which also auto-create.
          if (!(await config.store.getThread(threadKey))) {
            await config.store.createThread(
              threadKey,
              config.agentId !== undefined
                ? { metadata: { [THREAD_AGENT_METADATA_KEY]: config.agentId } }
                : {},
            );
          }
          await config.store.setThreadWorking(threadKey, input.markdown);
          return "Updated thread working memory.";
        }
        case "recall": {
          const scopes = input.scopes ?? defaultSearchScopes(resourceKey != null);
          const hits = await recallSearch({
            store: config.store,
            now: (config.now ?? Date.now)(),
            recencyHalfLifeMs: config.recencyHalfLifeMs ?? DEFAULT_RECALL_RECENCY_HALF_LIFE_MS,
            ...(embedder && { embedder }),
            ...(config.onEmbedderError && { onEmbedderError: config.onEmbedderError }),
            namespaceId: config.namespaceId,
            resourceKey,
            threadKey,
            query: input.query,
            scopes,
            sources: input.sources ?? [...defaultSources],
            limit: input.limit,
          });
          const related =
            resourceKey && scopes.includes("resource")
              ? relationNeighborhood({
                  relations: await config.store.listResourceRelations(resourceKey),
                  query: input.query,
                  hops: RECALL_GRAPH_HOPS,
                  limit: RECALL_GRAPH_LIMIT,
                })
              : [];
          return formatHits({ hits, related, query: input.query });
        }
      }
    },
  });
}

/**
 * Check a `supersedes` id against the scope's current facts. The model may
 * only retire facts it wrote itself: retiring what an operator or user
 * wrote on the model's say-so would let injected text erase them.
 */
function resolveSupersedes(params: {
  readonly facts: ReadonlyArray<Fact>;
  readonly id: string | undefined;
}): { id?: string; note: string; refusal?: string } {
  const { facts, id } = params;
  if (id === undefined) return { note: "" };
  const target = facts.find((f) => f.id === id);
  if (!target) {
    return {
      note: "",
      refusal: `Nothing saved: no current fact with id ${id} in that scope. Use \`recall\` to find the id, or omit \`supersedes\`.`,
    };
  }
  if (target.source !== "model") {
    return {
      note: "",
      refusal: `Nothing saved: fact ${id} was written by a ${target.source} and cannot be replaced by the model.`,
    };
  }
  return { id: target.id, note: ` Replaced fact ${target.id}.` };
}

function defaultSearchScopes(hasResource: boolean): Array<"thread" | "resource" | "namespace"> {
  return hasResource ? ["thread", "resource"] : ["thread"];
}

/** One ranked `recall` match. */
interface MemoryHit {
  readonly source: "fact" | "episode";
  /** Provenance of a fact hit. */
  readonly factSource?: Fact["source"];
  readonly scope: "namespace" | "resource" | "thread";
  readonly id: string;
  readonly text: string;
  readonly score: number;
  /** When the fact was last written, or the episode happened; epoch ms. */
  readonly at: number;
}

/**
 * Episodes `recall` ranks per scope: the best by decayed salience, plus as
 * many that mention a query word, so a long history stays cheap.
 */
const MAX_RECALL_EPISODES_PER_SCOPE = 200;

/** Query words used to find matching episodes beyond the top ones. */
const MAX_RECALL_QUERY_TERMS = 16;

interface RecallSearchInput {
  readonly store: MemoryStore;
  readonly namespaceId: string;
  readonly resourceKey: ScopedKey | null;
  readonly threadKey: ThreadKey;
  readonly query: string;
  readonly scopes: ReadonlyArray<"thread" | "resource" | "namespace">;
  readonly sources: ReadonlyArray<RecallSourceName>;
  readonly limit: number;
  readonly now: number;
  readonly recencyHalfLifeMs: number;
  readonly embedder?: EmbeddingProvider;
  readonly onEmbedderError?: (error: unknown) => void;
}

/** Hybrid recall: gather every candidate in scope, then rank them as one corpus. */
async function recallSearch(input: RecallSearchInput): Promise<MemoryHit[]> {
  const wantFacts = input.sources.includes("fact");
  const wantEpisodes = input.sources.includes("episode");
  const candidates: Array<Omit<MemoryHit, "score"> & { importance: number }> = [];
  const episodeParams = {
    order: "decayedSalienceDesc",
    halfLifeMs: input.recencyHalfLifeMs,
    now: input.now,
    limit: MAX_RECALL_EPISODES_PER_SCOPE,
  } as const;
  // The best episodes by decayed salience, plus every episode that mentions a
  // query word (also capped): an exact match outside the top is still found.
  const queryTerms = [...new Set(tokenize(input.query))].slice(0, MAX_RECALL_QUERY_TERMS);
  const loadEpisodes = async (
    list: (params: EpisodeListParams) => Promise<EpisodicRecord[]>,
  ): Promise<EpisodicRecord[]> => {
    const top = await list(episodeParams);
    if (queryTerms.length === 0) return top;
    const seen = new Set(top.map((e) => e.id));
    const matching = await list({ ...episodeParams, anyTerms: queryTerms });
    return [...top, ...matching.filter((e) => !seen.has(e.id))];
  };

  for (const scope of input.scopes) {
    let facts: Fact[] = [];
    let episodes: EpisodicRecord[] = [];
    if (scope === "namespace") {
      if (wantFacts) facts = await input.store.listNamespaceFacts(input.namespaceId);
      if (wantEpisodes)
        episodes = await loadEpisodes((p) =>
          input.store.listNamespaceEpisodes(input.namespaceId, p),
        );
    } else if (scope === "resource" && input.resourceKey) {
      const resourceKey = input.resourceKey;
      if (wantFacts) facts = await input.store.listResourceFacts(resourceKey);
      if (wantEpisodes)
        episodes = await loadEpisodes((p) => input.store.listResourceEpisodes(resourceKey, p));
    } else if (scope === "thread") {
      if (wantFacts) facts = await input.store.listThreadFacts(input.threadKey);
      if (wantEpisodes)
        episodes = await loadEpisodes((p) => input.store.listThreadEpisodes(input.threadKey, p));
    }
    for (const f of facts) {
      candidates.push({
        source: "fact",
        factSource: f.source,
        scope,
        id: f.id,
        text: f.text,
        at: f.updatedAt,
        importance: f.confidence,
      });
    }
    for (const e of episodes) {
      candidates.push({
        source: "episode",
        scope,
        id: e.id,
        text: e.outcome ? `${e.summary} — ${e.outcome}` : e.summary,
        at: e.occurredAt,
        importance: e.salience,
      });
    }
  }

  const ranked = await rankCandidates({
    query: input.query,
    candidates: candidates.map((c) => ({
      text: c.text,
      importance: c.importance,
      at: c.at,
    })),
    now: input.now,
    recencyHalfLifeMs: input.recencyHalfLifeMs,
    ...(input.embedder && { embedder: input.embedder }),
    ...(input.onEmbedderError && { onEmbedderError: input.onEmbedderError }),
  });
  return ranked.slice(0, input.limit).map(({ index, score }) => {
    const { importance: _importance, ...hit } = candidates[index]!;
    return { ...hit, score };
  });
}

/** Recalled memories are stored, untrusted text — returned as fenced reference data. */
function formatHits(params: {
  readonly hits: ReadonlyArray<MemoryHit>;
  readonly related: ReadonlyArray<Relation>;
  readonly query: string;
}): string {
  const { hits, related, query } = params;
  if (hits.length === 0 && related.length === 0) {
    return `No memories matched "${oneLine(query)}".`;
  }
  const sections: Array<{ title: string; body: string }> = [];
  if (hits.length > 0) {
    sections.push({
      title: "Recalled memories",
      body: hits
        .map((h, i) => {
          const tag = h.factSource
            ? `${h.scope}/${h.source}, ${h.factSource}`
            : `${h.scope}/${h.source}`;
          return `${i + 1}. [${tag}, id=${h.id}] (${h.score.toFixed(2)}) ${oneLine(h.text)}`;
        })
        .join("\n"),
    });
  }
  if (related.length > 0) {
    sections.push({
      title: "Related entities",
      body: related
        .map(
          (r) =>
            `- [${r.source}, id=${r.id}] ${oneLine(r.subject)} —${oneLine(r.predicate)}→ ${oneLine(r.object)}`,
        )
        .join("\n"),
    });
  }
  return fenceReferenceData({ source: "memory-recall", sections })!;
}
