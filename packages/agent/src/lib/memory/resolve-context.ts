// ---------------------------------------------------------------------------
// `resolveContext` — pure cascade builder.
//
// Takes pre-loaded layer rows + facts + messages + a token budget and
// returns the prompt-ready `ResolvedContext`. No I/O. Storage backends
// load the rows however they like; this function decides what gets
// stitched into the system prompt and in what order.
//
// Cascade (deepest layer wins; severance via `inheritFromParent`):
//
//   namespace.staticRules
//   namespace.facts                    (numbered, dated)
//   namespace.workingMemory
//   ── PROMPT_CACHE_BOUNDARY ──
//   resource.staticRules
//   resource.facts                     (numbered, dated)
//   resource.workingMemory
//   resource.episodes        [L2]      (opt-in via budget.maxEpisodeTokens)
//   thread.facts
//   thread.workingMemory
//
// Inheritance rules:
//   - thread.inheritFromParent === false  →  skip namespace AND resource
//                                            (and resource episodes)
//   - resource.inheritFromParent === false →  skip namespace
//
// Trimming policy:
//   - messages: drop oldest first to fit `budget.maxMessageTokens`
//   - episodes: drop lowest-salience first (tail of salience-sorted
//               input) to fit `budget.maxEpisodeTokens`
//   - static layers (rules, facts, working memory) are never trimmed —
//     they're small and they're what makes the agent know who it is
// ---------------------------------------------------------------------------

import type { Message } from "../message.ts";
import {
  PROMPT_CACHE_BOUNDARY,
  type EpisodicRecord,
  type Fact,
  type NamespaceRow,
  type ResolvedContext,
  type ResourceRow,
  type StoredMessage,
  type ThreadRow,
  type TokenBudget,
} from "./types.ts";

/** Inputs to `resolveContext`. Storage backends load these as they see fit. */
export interface ResolveContextInput {
  readonly namespace?: NamespaceRow | null;
  readonly namespaceFacts?: ReadonlyArray<Fact>;
  readonly resource?: ResourceRow | null;
  readonly resourceFacts?: ReadonlyArray<Fact>;
  /**
   * Resource-scope episodes (L2) — pre-sorted by the caller. Rendered
   * only when `budget.maxEpisodeTokens` is set; otherwise ignored.
   * Per-thread and per-namespace episodes are not auto-rendered; they
   * are storage-only unless a `SemanticRecall` capability surfaces them.
   */
  readonly resourceEpisodes?: ReadonlyArray<EpisodicRecord>;
  /** Thread row — required, since `resolveContext` is keyed on a thread. */
  readonly thread: ThreadRow;
  readonly threadFacts?: ReadonlyArray<Fact>;
  readonly messages: ReadonlyArray<StoredMessage>;
  readonly budget: TokenBudget;
}

/** Default token estimator: chars/4 (rough heuristic, good enough for trim decisions). */
function defaultEstimate(m: StoredMessage | Message): number {
  const len = typeof m.content === "string" ? m.content.length : 0;
  return Math.ceil(len / 4);
}

/** Default episode token estimator: chars/4 over `summary` + outcome. */
function defaultEstimateEpisode(e: EpisodicRecord): number {
  const len = e.summary.length + (e.outcome?.length ?? 0);
  return Math.ceil(len / 4);
}

/**
 * Render an episode block. One block per episode, salience-tagged,
 * dated. Caller provides episodes in display order; this only renders.
 */
function renderEpisodes(episodes: ReadonlyArray<EpisodicRecord>, header: string): string {
  if (episodes.length === 0) return "";
  const lines = episodes.map((e, idx) => {
    const date = new Date(e.occurredAt).toISOString().slice(0, 10);
    const sal = e.salience.toFixed(2);
    const outcome = e.outcome ? `\n   Outcome: ${e.outcome}` : "";
    return `${idx + 1}. [${date}, salience=${sal}] ${e.summary}${outcome}`;
  });
  return `## ${header}\n${lines.join("\n")}`;
}

/** Render a numbered, dated fact list. Returns `""` when no facts. */
function renderFacts(facts: ReadonlyArray<Fact> | undefined, header: string): string {
  if (!facts || facts.length === 0) return "";
  const lines = facts.map((f, idx) => {
    const date = new Date(f.createdAt).toISOString().slice(0, 10);
    return `${idx + 1}. [${date}] ${f.text}`;
  });
  return `## ${header}\n${lines.join("\n")}`;
}

/** Render a markdown section. Returns `""` when content is null/empty. */
function renderSection(content: string | null | undefined, header: string): string {
  if (!content || content.trim().length === 0) return "";
  return `## ${header}\n${content.trim()}`;
}

/**
 * Build the prompt-ready cascade view from pre-loaded layer rows.
 *
 * Pure — no I/O. Implementations of `MemoryStore.resolveContext`
 * delegate here after loading the rows from their backing store.
 */
export function resolveContext(input: ResolveContextInput): ResolvedContext {
  const {
    namespace,
    namespaceFacts,
    resource,
    resourceFacts,
    resourceEpisodes,
    thread,
    threadFacts,
    budget,
  } = input;

  // Inheritance gates.
  const includeResource = thread.inheritFromParent;
  const includeNamespace = includeResource && (resource ? resource.inheritFromParent : true);

  // Build the system prompt sections, in cascade order.
  const sections: string[] = [];

  if (includeNamespace && namespace) {
    sections.push(
      renderSection(namespace.staticRules, "Namespace Rules"),
      renderFacts(namespaceFacts, "Namespace Facts"),
      renderSection(namespace.workingMemory, "Namespace Working Memory"),
    );
  }

  // Cache boundary always emitted between stable (namespace) and
  // volatile (resource + thread) layers — even when one side is empty,
  // so providers can split deterministically.
  sections.push(PROMPT_CACHE_BOUNDARY);

  if (includeResource && resource) {
    sections.push(
      renderSection(resource.staticRules, "Resource Rules"),
      renderFacts(resourceFacts, "Resource Facts"),
      renderSection(resource.workingMemory, "Resource Working Memory"),
    );
  }

  // Episodic recall — only rendered when a budget is set. Trimmed by
  // dropping the lowest-salience entries first (input is expected to
  // be salience-sorted; we drop from the tail).
  if (includeResource && (budget.maxEpisodeTokens ?? 0) > 0 && resourceEpisodes?.length) {
    const trimmed = trimEpisodes(resourceEpisodes, budget);
    if (trimmed.length > 0) {
      sections.push(renderEpisodes(trimmed, "Recent Episodes"));
    }
  }

  sections.push(
    renderFacts(threadFacts, "Thread Facts"),
    renderSection(thread.workingMemory, "Thread Working Memory"),
  );

  const systemPrompt = sections.filter((s) => s.length > 0).join("\n\n");

  // Trim messages from the oldest end until total token estimate fits.
  const messages = trimMessages(input.messages, budget);

  return { systemPrompt, messages };
}

/**
 * Drop lowest-salience episodes (from the tail of a salience-sorted
 * input) until the total estimate fits the episode budget.
 */
function trimEpisodes(
  episodes: ReadonlyArray<EpisodicRecord>,
  budget: TokenBudget,
): EpisodicRecord[] {
  const cap = budget.maxEpisodeTokens ?? 0;
  if (cap <= 0) return [];
  const estimate = budget.estimateEpisode ?? defaultEstimateEpisode;
  const remaining = episodes.slice();
  let total = 0;
  for (const e of remaining) total += estimate(e);
  while (remaining.length > 0 && total > cap) {
    const dropped = remaining.pop()!;
    total -= estimate(dropped);
  }
  return remaining;
}

/**
 * Drop messages from the oldest end until the total estimate fits the
 * budget. Returns a new array; the input is not mutated.
 */
function trimMessages(msgs: ReadonlyArray<StoredMessage>, budget: TokenBudget): StoredMessage[] {
  const estimate = budget.estimate ?? defaultEstimate;
  let total = 0;
  for (const m of msgs) total += estimate(m);
  if (total <= budget.maxMessageTokens) return msgs.slice();

  // Trim from the front (oldest) until we fit.
  const remaining = msgs.slice();
  while (remaining.length > 0 && total > budget.maxMessageTokens) {
    const dropped = remaining.shift()!;
    total -= estimate(dropped);
  }
  return remaining;
}
