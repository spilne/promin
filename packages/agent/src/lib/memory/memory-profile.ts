// ---------------------------------------------------------------------------
// Memory profile — one typed description of how an agent's memory behaves,
// instead of options spread over the agent, the `memory` tool, the
// consolidator and the context budget:
//
//   const profile = memoryProfile({ store, preset: "full" })
//     .recall({ embedder, recencyHalfLifeMs })
//     .consolidate({ distillInstructions: "Keep project names." })
//     .context({ maxEpisodeTokens: 3_000 })
//     .build();
//   new LocalAgent({ agent, runner, memoryProfile: profile });
//
// The builder is immutable (each step returns a new builder) and `build()`
// rejects combinations that cannot work, rather than letting them fail later.
// ---------------------------------------------------------------------------

import type { LayeredMemoryToolConfig } from "../tools/layered-memory-tools.ts";
import type { DefaultConsolidatorConfig } from "./consolidator.ts";
import {
  composeMemoryStore,
  type RelationStore,
  type RuleProposalStore,
} from "./compose-memory-store.ts";
import type { MemoryStore, TokenBudget } from "./types.ts";

/** How much the memory does on its own. Each preset is a starting point for the builder. */
export type MemoryPreset = "minimal" | "standard" | "full";

/** Sources `recall` searches, as `LayeredMemoryToolConfig.defaultRecallSources`. */
type RecallSources = NonNullable<LayeredMemoryToolConfig["defaultRecallSources"]>;

/** How the `memory` tool's `recall` searches and ranks. */
export interface RecallOptions {
  /** Where `recall` looks when the model names no source. Default: facts and episodes. */
  readonly sources?: RecallSources;
  /** Adds semantic matching next to BM25. */
  readonly embedder?: LayeredMemoryToolConfig["embedder"];
  /** Half-life of the recency term, in ms. Positive. */
  readonly recencyHalfLifeMs?: number;
  /** Called when the embedder throws; `recall` then ranks lexically. */
  readonly onEmbedderError?: (error: unknown) => void;
}

/**
 * What the consolidator does: every `DefaultConsolidatorConfig` option except
 * the ones the agent supplies itself (store, LLM, retry, limiter, clock, telemetry).
 */
export type ConsolidationOptions = Omit<
  DefaultConsolidatorConfig,
  | "store"
  | "llm"
  | "retry"
  | "rateLimiter"
  | "clock"
  | "metrics"
  | "costs"
  | "llmProvider"
  | "llmModel"
  | "metricLabels"
>;

/** What reaches each turn's prompt. `maxMessageTokens` defaults to 16 000. */
export type ContextOptions = Omit<TokenBudget, "maxMessageTokens"> & {
  readonly maxMessageTokens?: number;
};

/** A built, validated memory configuration. Pass it as `LocalAgentConfig.memoryProfile`. */
export interface MemoryProfile {
  readonly store: MemoryStore;
  readonly recall: RecallOptions;
  /** `false` turns the consolidator off: `compactThread` and `distillThread` then throw. */
  readonly consolidation: ConsolidationOptions | false;
  readonly context: TokenBudget;
}

const DEFAULT_MAX_MESSAGE_TOKENS = 16_000;
const DAY_MS = 24 * 60 * 60 * 1000;

interface ProfileDraft {
  readonly store: MemoryStore;
  readonly relations?: RelationStore;
  readonly ruleProposals?: RuleProposalStore;
  readonly recall: RecallOptions;
  readonly consolidation: ConsolidationOptions | false;
  readonly context: ContextOptions;
}

/**
 * - `minimal`: the model keeps facts and notes; the consolidator only
 *   summarizes (no reconciling, graph or proposals); no episodes in the prompt.
 * - `standard`: adds retiring contradicted facts, the entity graph, and
 *   episodes in the prompt (2 000 tokens, 90-day half-life).
 * - `full`: adds rule proposals from repeated user corrections.
 */
function presetDraft(params: { readonly store: MemoryStore; readonly preset: MemoryPreset }) {
  const { store, preset } = params;
  const consolidation: ConsolidationOptions =
    preset === "minimal"
      ? { reconcileFacts: false, extractRelations: false, ruleProposals: false }
      : {
          reconcileFacts: true,
          extractRelations: true,
          ruleProposals: preset === "full",
        };
  const context: ContextOptions =
    preset === "minimal"
      ? { maxEpisodeTokens: 0 }
      : { maxEpisodeTokens: 2_000, episodeHalfLifeMs: 90 * DAY_MS };
  return { store, recall: {}, consolidation, context } satisfies ProfileDraft;
}

/** Start a memory profile over `store`. `preset` defaults to `"standard"`. */
export function memoryProfile(params: {
  readonly store: MemoryStore;
  readonly preset?: MemoryPreset;
}): MemoryProfileBuilder {
  return new MemoryProfileBuilder(
    presetDraft({ store: params.store, preset: params.preset ?? "standard" }),
  );
}

/** Immutable builder behind `memoryProfile`. Each method returns a new builder. */
export class MemoryProfileBuilder {
  constructor(private readonly draft: ProfileDraft) {}

  /**
   * Keep relations (the entity graph) in `store` instead of the main store,
   * e.g. a graph database. Nothing links the two backends: deleting a
   * resource from the main store does not remove its relations here.
   */
  relationsIn(store: RelationStore): MemoryProfileBuilder {
    return new MemoryProfileBuilder({ ...this.draft, relations: store });
  }

  /** Keep rule proposals in `store` instead of the main store. Same caveat as `relationsIn`. */
  ruleProposalsIn(store: RuleProposalStore): MemoryProfileBuilder {
    return new MemoryProfileBuilder({ ...this.draft, ruleProposals: store });
  }

  /** Merge `options` into how `recall` searches and ranks. */
  recall(options: RecallOptions): MemoryProfileBuilder {
    return new MemoryProfileBuilder({
      ...this.draft,
      recall: { ...this.draft.recall, ...options },
    });
  }

  /**
   * Merge `options` into the consolidator's, switching it back on if it was
   * off. `false` turns it off, dropping every consolidation option.
   */
  consolidate(options: ConsolidationOptions | false): MemoryProfileBuilder {
    const consolidation =
      options === false
        ? false
        : { ...(this.draft.consolidation === false ? {} : this.draft.consolidation), ...options };
    return new MemoryProfileBuilder({ ...this.draft, consolidation });
  }

  /** Merge `options` into what reaches each turn's prompt. */
  context(options: ContextOptions): MemoryProfileBuilder {
    return new MemoryProfileBuilder({
      ...this.draft,
      context: { ...this.draft.context, ...options },
    });
  }

  /** Validate and freeze the profile. Throws on a combination that cannot work. */
  build(): MemoryProfile {
    const { recall, consolidation, context } = this.draft;
    const store = composeMemoryStore({
      base: this.draft.store,
      ...(this.draft.relations && { relations: this.draft.relations }),
      ...(this.draft.ruleProposals && { ruleProposals: this.draft.ruleProposals }),
    });
    assertPositive({ name: "recall.recencyHalfLifeMs", value: recall.recencyHalfLifeMs });
    assertPositive({ name: "context.episodeHalfLifeMs", value: context.episodeHalfLifeMs });
    assertPositive({ name: "context.maxMessageTokens", value: context.maxMessageTokens });
    if (recall.sources?.length === 0) {
      throw new Error("Memory profile: recall.sources must name at least one source.");
    }
    if (consolidation !== false) assertConsolidationOptions(consolidation);
    return {
      store,
      recall,
      consolidation,
      context: {
        ...context,
        maxMessageTokens: context.maxMessageTokens ?? DEFAULT_MAX_MESSAGE_TOKENS,
      },
    };
  }
}

function assertPositive(params: { readonly name: string; readonly value: number | undefined }) {
  const { name, value } = params;
  if (value !== undefined && !(value > 0)) {
    throw new Error(`Memory profile: ${name} must be a positive number, got ${value}.`);
  }
}

function assertConsolidationOptions(options: ConsolidationOptions): void {
  if (options.distillPrompt !== undefined && options.extractRelations === true) {
    // A replaced distill prompt owns the reply format, so relations and proposals come back empty.
    throw new Error(
      "Memory profile: `distillPrompt` replaces the reply format, so `extractRelations` would " +
        "record nothing. Use `distillInstructions`, or turn the option off.",
    );
  }
  if (options.distillPrompt !== undefined && options.ruleProposals === true) {
    throw new Error(
      "Memory profile: `distillPrompt` replaces the reply format, so `ruleProposals` would " +
        "record nothing. Use `distillInstructions`, or turn the option off.",
    );
  }
}
