// ---------------------------------------------------------------------------
// `resolveContext` — pure cascade builder.
//
// Takes pre-loaded layer rows + facts + messages + a token budget and
// returns the prompt-ready `ResolvedContext`. No I/O. Storage backends
// load the rows however they like; this function decides what gets
// stitched into the prompt and in what order.
//
// The cascade is split by trust. Operator-authored rules are instructions;
// everything the model, tools or end users can write is reference data:
//
//   instructions (system block)      reference (one fenced data block)
//   ───────────────────────────      ─────────────────────────────────
//   namespace.staticRules            namespace.facts         (numbered, dated, sourced)
//   resource.staticRules             namespace.workingMemory
//                                    resource.facts
//                                    resource.workingMemory
//                                    resource.episodes  [L2] (opt-in via budget.maxEpisodeTokens)
//                                    thread compaction  [L2] (latest; replaces the
//                                                             messages it folded)
//                                    thread.facts
//                                    thread.workingMemory
//
// Inheritance rules (severance via `inheritFromParent`):
//   - thread.inheritFromParent === false  →  skip namespace AND resource
//                                            (and resource episodes)
//   - resource.inheritFromParent === false →  skip namespace
//
// Trimming policy:
//   - messages: those folded by the compaction are left out; of the rest,
//               drop whole turns oldest first to fit `budget.maxMessageTokens`
//   - episodes: drop lowest-salience first (tail of salience-sorted
//               input) to fit `budget.maxEpisodeTokens`
//   - staticRules: each scope cut to `MAX_STATIC_RULES_CHARS`
//   - facts and working memory are not trimmed here — their write paths
//     cap them (`MAX_FACT_CHARS`, `MAX_WORKING_MEMORY_CHARS`, the
//     consolidator's fact-count cap)
// ---------------------------------------------------------------------------

import type { Message } from "../message.ts";
import { fenceReferenceData, oneLine, type ReferenceSection } from "../reference-data.ts";
import {
  MAX_STATIC_RULES_CHARS,
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
   * Per-thread and per-namespace episodes are not auto-rendered; the
   * memory tool's `recall` command searches them on demand.
   */
  readonly resourceEpisodes?: ReadonlyArray<EpisodicRecord>;
  /** Thread row — required, since `resolveContext` is keyed on a thread. */
  readonly thread: ThreadRow;
  readonly threadFacts?: ReadonlyArray<Fact>;
  /**
   * The thread's latest compaction episode. Its summary is rendered as
   * reference data, and messages up to its `sourceMessageRange.toSeq`
   * are left out of `messages` — the summary stands in for them.
   */
  readonly compaction?: EpisodicRecord | null;
  readonly messages: ReadonlyArray<StoredMessage>;
  readonly budget: TokenBudget;
}

/** Default token estimator: chars/4 (rough heuristic, good enough for trim decisions). */
export function defaultEstimate(m: StoredMessage | Message): number {
  const len = typeof m.content === "string" ? m.content.length : 0;
  return Math.ceil(len / 4);
}

/** Default episode token estimator: chars/4 over `summary` + outcome. */
function defaultEstimateEpisode(e: EpisodicRecord): number {
  const len = e.summary.length + (e.outcome?.length ?? 0);
  return Math.ceil(len / 4);
}

/** One line per episode, salience-tagged, dated. Caller provides display order. */
function episodesBody(episodes: ReadonlyArray<EpisodicRecord>): string {
  return episodes
    .map((e, idx) => {
      const date = new Date(e.occurredAt).toISOString().slice(0, 10);
      const sal = e.salience.toFixed(2);
      const outcome = e.outcome ? `\n   Outcome: ${oneLine(e.outcome)}` : "";
      return `${idx + 1}. [${date}, salience=${sal}] ${oneLine(e.summary)}${outcome}`;
    })
    .join("\n");
}

/** Numbered fact list, each dated and tagged with its provenance. */
function factsBody(facts: ReadonlyArray<Fact> | undefined): string {
  if (!facts || facts.length === 0) return "";
  return facts
    .map((f, idx) => {
      const date = new Date(f.createdAt).toISOString().slice(0, 10);
      return `${idx + 1}. [${date}, ${f.source}] ${oneLine(f.text)}`;
    })
    .join("\n");
}

/** Render one scope's rules as an instruction section. `""` when empty. */
function renderRules(content: string | null | undefined, header: string): string {
  const rules = capStaticRules(content);
  return rules ? `## ${header}\n${rules}` : "";
}

/**
 * Trim `staticRules` and cut it to `MAX_STATIC_RULES_CHARS`, noting how
 * much was dropped. Returns `""` for null / blank rules.
 */
export function capStaticRules(content: string | null | undefined): string {
  const rules = content?.trim() ?? "";
  if (rules.length <= MAX_STATIC_RULES_CHARS) return rules;
  const dropped = rules.length - MAX_STATIC_RULES_CHARS;
  return `${rules.slice(0, MAX_STATIC_RULES_CHARS)}\n[… ${dropped} characters of rules truncated]`;
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
    compaction,
    budget,
  } = input;

  // Inheritance gates.
  const includeResource = thread.inheritFromParent;
  const includeNamespace = includeResource && (resource ? resource.inheritFromParent : true);

  const rules: string[] = [];
  const sections: ReferenceSection[] = [];

  if (includeNamespace && namespace) {
    rules.push(renderRules(namespace.staticRules, "Namespace Rules"));
    sections.push(
      { title: "Namespace Facts", body: factsBody(namespaceFacts) },
      { title: "Namespace Working Memory", body: namespace.workingMemory ?? "" },
    );
  }

  if (includeResource && resource) {
    rules.push(renderRules(resource.staticRules, "Resource Rules"));
    sections.push(
      { title: "Resource Facts", body: factsBody(resourceFacts) },
      { title: "Resource Working Memory", body: resource.workingMemory ?? "" },
    );
  }

  // Episodic recall — only rendered when a budget is set. Trimmed by
  // dropping the lowest-salience entries first (input is expected to
  // be salience-sorted; we drop from the tail).
  if (includeResource && (budget.maxEpisodeTokens ?? 0) > 0 && resourceEpisodes?.length) {
    const trimmed = trimEpisodes(resourceEpisodes, budget);
    if (trimmed.length > 0) {
      sections.push({ title: "Recent Episodes", body: episodesBody(trimmed) });
    }
  }

  const foldedThrough = compaction?.sourceMessageRange?.toSeq ?? 0;
  if (compaction) {
    sections.push({
      title: `Summary of this thread's earlier messages (through message ${foldedThrough})`,
      body: compaction.summary,
    });
  }
  sections.push(
    { title: "Thread Facts", body: factsBody(threadFacts) },
    { title: "Thread Working Memory", body: thread.workingMemory ?? "" },
  );

  const instructions = rules.filter((r) => r.length > 0).join("\n\n");
  const reference = fenceReferenceData({ source: "memory", sections }) ?? "";

  // Messages the compaction folded are represented by its summary; trim
  // the rest from the oldest end until the token estimate fits.
  const live = input.messages.filter((m) => m.seq > foldedThrough);
  const messages = trimMessages(live, budget);

  return { instructions, reference, messages };
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

  // Trim whole turns from the front (oldest first). A turn spans from a
  // `user` message up to (but not including) the next `user` message.
  // Dropping individual messages would orphan tool_result blocks whose
  // paired tool_use lived in the dropped assistant message, causing a
  // 400 from the Anthropic API.
  const remaining = msgs.slice();
  while (remaining.length > 0 && total > budget.maxMessageTokens) {
    let turnEnd = remaining.length;
    for (let i = 1; i < remaining.length; i++) {
      if (remaining[i]!.role === "user") {
        turnEnd = i;
        break;
      }
    }
    for (let i = 0; i < turnEnd; i++) total -= estimate(remaining[i]!);
    remaining.splice(0, turnEnd);
  }
  return remaining;
}
