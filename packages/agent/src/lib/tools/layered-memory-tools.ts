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
  type EpisodicRecord,
  type Fact,
  type MemoryStore,
  type ScopedKey,
  type ThreadKey,
} from "../memory/types.ts";
import type { EmbeddingProvider } from "../embedding-provider.ts";
import { rankCandidates } from "../memory/scoring.ts";
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
   * Default: `["fact", "episode"]` — messages are not searched by default
   * since recent ones are already in context.
   */
  readonly defaultRecallSources?: ReadonlyArray<"fact" | "episode" | "message">;
  /**
   * Adds semantic matching to `recall`: query and candidates are embedded
   * and ranked by cosine similarity next to BM25. Without it, or when it
   * throws, recall is lexical only.
   */
  readonly embedder?: EmbeddingProvider;
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

const ModelScope = z.enum(["thread", "resource"]);
const RecallScope = z.enum(["thread", "resource", "namespace"]);
const RecallSource = z.enum(["fact", "episode", "message"]);

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

  return tool({
    name: "memory",
    description:
      "Read and write durable agent memory. Pick the right command for the kind " +
      "of information:\n" +
      "  - `set`: a small, lasting fact about the user or this conversation\n" +
      "  - `setWorking`: current state / focus / scratch — overwrites in place\n" +
      "  - `recall`: search past facts and session summaries that aren't already " +
      "in your context.\n\n" +
      "Commands:\n" +
      `  set: ${SET_DESCRIPTION}\n` +
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
            return `Saved to resource memory (id: ${shortId(f.id)}).${replaced.note}`;
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
          return `Saved to thread memory (id: ${shortId(f.id)}).${replaced.note}`;
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
          const hits = await recallSearch({
            store: config.store,
            now: (config.now ?? Date.now)(),
            recencyHalfLifeMs: config.recencyHalfLifeMs ?? DEFAULT_RECALL_RECENCY_HALF_LIFE_MS,
            ...(config.embedder && { embedder: config.embedder }),
            namespaceId: config.namespaceId,
            resourceKey,
            threadKey,
            query: input.query,
            scopes: input.scopes ?? defaultSearchScopes(resourceKey != null),
            sources: input.sources ?? [...defaultSources],
            limit: input.limit,
          });
          return formatHits(hits, input.query);
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
  return { id: target.id, note: ` Replaced fact ${shortId(target.id)}.` };
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
  readonly createdAt: number;
}

interface RecallSearchInput {
  readonly store: MemoryStore;
  readonly namespaceId: string;
  readonly resourceKey: ScopedKey | null;
  readonly threadKey: ThreadKey;
  readonly query: string;
  readonly scopes: ReadonlyArray<"thread" | "resource" | "namespace">;
  readonly sources: ReadonlyArray<"fact" | "episode" | "message">;
  readonly limit: number;
  readonly now: number;
  readonly recencyHalfLifeMs: number;
  readonly embedder?: EmbeddingProvider;
}

/** Hybrid recall: gather every candidate in scope, then rank them as one corpus. */
async function recallSearch(input: RecallSearchInput): Promise<MemoryHit[]> {
  const wantFacts = input.sources.includes("fact");
  const wantEpisodes = input.sources.includes("episode");
  const candidates: Array<Omit<MemoryHit, "score"> & { importance: number }> = [];

  for (const scope of input.scopes) {
    let facts: Fact[] = [];
    let episodes: EpisodicRecord[] = [];
    if (scope === "namespace") {
      if (wantFacts) facts = await input.store.listNamespaceFacts(input.namespaceId);
      if (wantEpisodes) episodes = await input.store.listNamespaceEpisodes(input.namespaceId);
    } else if (scope === "resource" && input.resourceKey) {
      if (wantFacts) facts = await input.store.listResourceFacts(input.resourceKey);
      if (wantEpisodes) episodes = await input.store.listResourceEpisodes(input.resourceKey);
    } else if (scope === "thread") {
      if (wantFacts) facts = await input.store.listThreadFacts(input.threadKey);
      if (wantEpisodes) episodes = await input.store.listThreadEpisodes(input.threadKey);
    }
    for (const f of facts) {
      candidates.push({
        source: "fact",
        factSource: f.source,
        scope,
        id: f.id,
        text: f.text,
        createdAt: f.updatedAt,
        importance: f.confidence,
      });
    }
    for (const e of episodes) {
      candidates.push({
        source: "episode",
        scope,
        id: e.id,
        text: e.outcome ? `${e.summary} — ${e.outcome}` : e.summary,
        createdAt: e.occurredAt,
        importance: e.salience,
      });
    }
  }

  const ranked = await rankCandidates({
    query: input.query,
    candidates: candidates.map((c) => ({
      text: c.text,
      importance: c.importance,
      at: c.createdAt,
    })),
    now: input.now,
    recencyHalfLifeMs: input.recencyHalfLifeMs,
    ...(input.embedder && { embedder: input.embedder }),
  });
  return ranked.slice(0, input.limit).map(({ index, score }) => {
    const { importance: _importance, ...hit } = candidates[index]!;
    return { ...hit, score };
  });
}

/** Recalled memories are stored, untrusted text — returned as fenced reference data. */
function formatHits(hits: ReadonlyArray<MemoryHit>, query: string): string {
  if (hits.length === 0) return `No memories matched "${oneLine(query)}".`;
  const body = hits
    .map((h, i) => {
      const tag = h.factSource
        ? `${h.scope}/${h.source}, ${h.factSource}`
        : `${h.scope}/${h.source}`;
      return `${i + 1}. [${tag}, id=${h.id}] (${h.score.toFixed(2)}) ${oneLine(h.text)}`;
    })
    .join("\n");
  return fenceReferenceData({
    source: "memory-recall",
    sections: [{ title: "Recalled memories", body }],
  })!;
}

function shortId(id: string): string {
  return id.length > 8 ? id.slice(0, 8) : id;
}
