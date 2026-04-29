// ---------------------------------------------------------------------------
// `Consolidator` — turns raw L1 thread messages into L2 episodic records.
//
// Two paths the writer supports today:
//
//   compactThread(key)   — synchronous mid-thread roll-up. Summarises the
//                          oldest portion of a thread into a ThreadEpisode,
//                          freeing message budget without losing the gist.
//                          Pairs with the agent loop's existing `compact()`
//                          trigger.
//
//   distillThread(key)   — end-of-thread / "sleep" pass. Produces ONE
//                          ResourceEpisode summarising the thread for
//                          cross-thread recall. This is the one that powers
//                          "what did this user and I work on last week?"
//                          via `resolveContext`'s episode-injection budget.
//
//   distillResource(key) — sweep all closed threads for a resource. Default
//                          implementation calls `distillThread` per thread;
//                          custom implementations can do batched LLM calls.
//
// Plug in your own: anywhere a Zorya / agent-host API takes a Consolidator,
// you can pass any object satisfying this interface. The framework calls
// the methods; the implementation chooses how to summarise.
//
// `DefaultConsolidator` ships an LLM-driven reference implementation: it
// prompts the model with the messages, parses a small JSON envelope back
// out, and writes the resulting episode (optionally with an embedding).
// Heuristics are deliberately simple — start there, layer salience tweaks
// only when retrieval quality regresses.
// ---------------------------------------------------------------------------

import type { LLMProvider } from "../llm-provider.ts";
import type { EmbeddingProvider } from "../memory-index.ts";
import type { Message } from "../message.ts";
import type { EpisodicRecord, MemoryStore, ScopedKey, StoredMessage, ThreadKey } from "./types.ts";

// ---------------------------------------------------------------------------
// Public interface
// ---------------------------------------------------------------------------

export interface Consolidator {
  /**
   * Summarise the oldest `messages.length - keepRecent` messages of a
   * thread into a single `ThreadEpisode`. The agent loop typically calls
   * this when message count exceeds a budget; the resulting episode is
   * picked up by `resolveContext` so the gist survives the trim.
   *
   * Returns the written episode. Throws when the thread has fewer than
   * `keepRecent` messages (nothing to compact).
   */
  compactThread(key: ThreadKey, opts?: CompactThreadOptions): Promise<EpisodicRecord>;

  /**
   * Produce ONE `ResourceEpisode` summarising a thread, persisted at
   * resource scope so future threads under the same `(namespaceId,
   * resourceId)` can pick it up via `resolveContext`'s episode budget.
   *
   * Idempotency: by default, the implementation should treat a thread
   * with an existing `sourceThreadId`-tagged episode as already distilled
   * and refuse to write a duplicate. Override via `force: true`.
   */
  distillThread(key: ThreadKey, opts?: DistillThreadOptions): Promise<EpisodicRecord>;

  /**
   * Sweep every thread under a resource (optionally filtered by
   * `since`) and run `distillThread` on each. Default implementations
   * dedupe by `sourceThreadId` so the same closed thread isn't re-
   * distilled on every nightly run.
   */
  distillResource(key: ScopedKey, opts?: DistillResourceOptions): Promise<EpisodicRecord[]>;
}

export interface CompactThreadOptions {
  /** Leave this many newest messages unsummarised. Default 10. */
  readonly keepRecent?: number;
}

export interface DistillThreadOptions {
  /** Re-distill even when an episode already exists for this thread. Default false. */
  readonly force?: boolean;
}

export interface DistillResourceOptions {
  /** Only consider threads with `lastActiveAt >= since`. */
  readonly since?: Date;
  /** Skip threads with fewer than this many messages. Default 2. */
  readonly minMessages?: number;
  /** Pass `force: true` through to each `distillThread` call. */
  readonly force?: boolean;
}

/**
 * Inputs the salience function sees. `summary` is the LLM-emitted text
 * (so heuristics can grep it); the rest is structural.
 */
export interface ConsolidationSignals {
  readonly messageCount: number;
  readonly toolCallCount: number;
  readonly userMessageCount: number;
  readonly hasFailures: boolean;
  /** Optional 0..1 importance the model self-assessed during distillation. */
  readonly modelSalience?: number;
  readonly summary: string;
}

// ---------------------------------------------------------------------------
// DefaultConsolidator — LLM-driven reference impl
// ---------------------------------------------------------------------------

/**
 * How to represent non-interactive trigger turns (scheduled, webhook,
 * agent-delegated) in the transcript sent to the distillation LLM.
 *
 * - `"include"` — emit as `[scheduled-trigger] <task>` followed by normal
 *   assistant / tool lines. Useful when you want the full conversation
 *   verbatim.
 * - `"reframe"` *(default)* — skip the trigger user message; emit assistant
 *   lines in the turn as `[scheduled-result: <task>] <text>` and tool calls /
 *   results as `[scheduled-tool-call]` / `[scheduled-tool-result]`. The
 *   summarizer sees "what the agent did" without attributing it to the user.
 * - `"skip"` — drop the entire trigger turn (user + assistant + tools). Good
 *   when scheduled runs are high-frequency background noise that would dilute
 *   the episode.
 */
export type TriggerTurnHandling = "include" | "reframe" | "skip";

export interface DefaultConsolidatorConfig {
  readonly store: MemoryStore;
  /** Used for the distillation prompt. Often a cheaper model than the chat LLM. */
  readonly llm: LLMProvider;
  /** Optional — when set, every written episode carries an embedding. */
  readonly embeddings?: EmbeddingProvider;
  /** Override the system prompt sent to the distillation LLM. */
  readonly distillPrompt?: string;
  /** Override the salience score derivation. Default uses heuristics + modelSalience. */
  readonly salienceFn?: (signals: ConsolidationSignals) => number;
  /** Default messages-to-keep when `compactThread` is called without opts. Default 10. */
  readonly defaultKeepRecent?: number;
  /** Min messages a thread must have before `distillThread` runs. Default 2. */
  readonly minDistillMessages?: number;
  /**
   * Cap on resource-scope facts per `(namespaceId, resourceId)`. When
   * `distillThread` writes new facts, the oldest are evicted until the
   * row count is at or below this number. Unset means unbounded —
   * facts grow forever, which works for short-lived sessions but bloats
   * the resolveContext prompt over time. A small cap (10–20) is
   * usually right: if a fact matters across sessions it'll get
   * re-extracted when the model encounters it again.
   *
   * Eviction is oldest-first by `createdAt`. `appendResourceFact`
   * results from the current call are never evicted in the same call.
   */
  readonly maxResourceFacts?: number;
  /**
   * How trigger turns (scheduled, webhook, agent-delegated) appear in the
   * compaction / distillation transcript. Default: `"reframe"`.
   */
  readonly triggerTurnHandling?: TriggerTurnHandling;
}

const DEFAULT_DISTILL_PROMPT = `You are a memory consolidation assistant. You will be shown a chat thread between a user and an agent. Produce a JSON envelope summarising the conversation in a way that would help the SAME user pick up later in a NEW thread.

Output STRICT JSON, no prose around it, matching:

{
  "summary": "2-3 line markdown gist of what happened",
  "outcome": "one-line resolution / status, or null if unresolved",
  "salience": 0.0,
  "facts": ["one-line atomic fact about the user worth remembering long-term", ...]
}

Rules:
  - "summary": brief, focuses on the WHAT, not the verbatim transcript.
  - "outcome": null when nothing is decided/finished.
  - "salience": float in [0,1]. Higher = more worth recalling later. Use ~0.3 for chitchat, ~0.7 for substantive work, ~0.9 for explicit user preferences / corrections / strong personal facts.
  - "facts": durable claims about THIS user (name, role, location, persistent preferences, allergies, etc.). Empty list when none. Don't include ephemeral context.

Do not invent facts. If a turn was inconclusive, return a low salience and an empty facts list.`;

export class DefaultConsolidator implements Consolidator {
  private readonly store: MemoryStore;
  private readonly llm: LLMProvider;
  private readonly embeddings?: EmbeddingProvider;
  private readonly distillPrompt: string;
  private readonly salienceFn: (signals: ConsolidationSignals) => number;
  private readonly defaultKeepRecent: number;
  private readonly minDistillMessages: number;
  private readonly maxResourceFacts?: number;
  private readonly triggerTurnHandling: TriggerTurnHandling;

  constructor(config: DefaultConsolidatorConfig) {
    this.store = config.store;
    this.llm = config.llm;
    this.embeddings = config.embeddings;
    this.distillPrompt = config.distillPrompt ?? DEFAULT_DISTILL_PROMPT;
    this.salienceFn = config.salienceFn ?? defaultSalience;
    this.defaultKeepRecent = config.defaultKeepRecent ?? 10;
    this.minDistillMessages = config.minDistillMessages ?? 2;
    if (config.maxResourceFacts !== undefined) this.maxResourceFacts = config.maxResourceFacts;
    this.triggerTurnHandling = config.triggerTurnHandling ?? "reframe";
  }

  async compactThread(key: ThreadKey, opts: CompactThreadOptions = {}): Promise<EpisodicRecord> {
    const keepRecent = opts.keepRecent ?? this.defaultKeepRecent;
    const messages = await this.store.getMessages(key, { order: "asc" });
    if (messages.length <= keepRecent) {
      throw new Error(
        `compactThread: thread has ${messages.length} messages, ≤ keepRecent=${keepRecent}; nothing to compact`,
      );
    }
    const toCompact = messages.slice(0, messages.length - keepRecent);
    const distilled = await this.distill(toCompact);
    const fromSeq = toCompact[0]!.seq;
    const toSeq = toCompact[toCompact.length - 1]!.seq;
    return this.store.appendThreadEpisode(key, {
      summary: distilled.summary,
      outcome: distilled.outcome,
      salience: distilled.salience,
      embedding: distilled.embedding,
      sourceThreadId: key.threadId,
      sourceMessageRange: { fromSeq, toSeq },
      metadata: { kind: "compact" },
    });
  }

  async distillThread(key: ThreadKey, opts: DistillThreadOptions = {}): Promise<EpisodicRecord> {
    if (!key.resourceId) {
      throw new Error("distillThread: resourceId is required (resource-scope episode target)");
    }
    const resourceKey: ScopedKey = { namespaceId: key.namespaceId, resourceId: key.resourceId };

    if (!opts.force) {
      const existing = await this.store.listResourceEpisodes(resourceKey);
      const dup = existing.find((e) => e.sourceThreadId === key.threadId);
      if (dup) return dup;
    }

    const messages = await this.store.getMessages(key, { order: "asc" });
    if (messages.length < this.minDistillMessages) {
      throw new Error(
        `distillThread: thread has ${messages.length} messages, < minDistillMessages=${this.minDistillMessages}`,
      );
    }
    const distilled = await this.distill(messages);

    // Persist any explicit personal facts as resource-scope facts so the
    // hot-path injection picks them up immediately. Episodes also get
    // injected (when budget allows) but facts are smaller and always-on.
    //
    // Dedup against existing facts — re-distilling a thread (or distilling
    // after the model already wrote the same fact via the memory tool)
    // would otherwise produce visible duplicates in resolveContext output.
    // Compare on a normalized form (lowercase, trimmed, trailing
    // punctuation stripped) so "User's name is Anton" and "User's name is
    // Anton." don't both make it through.
    if (distilled.facts.length > 0) {
      const existing = await this.store.listResourceFacts(resourceKey);
      const seen = new Set(existing.map((f) => normalizeFact(f.text)));
      let appended = 0;
      for (const text of distilled.facts) {
        const norm = normalizeFact(text);
        if (norm.length === 0 || seen.has(norm)) continue;
        await this.store.appendResourceFact(resourceKey, text);
        seen.add(norm);
        appended += 1;
      }
      // Apply retention cap after appending. Re-list so the order is the
      // store's authoritative view (createdAt asc per the contract). Drop
      // the oldest until we're under the cap. Newly appended facts will
      // be at the end of the list — they survive eviction in the same
      // pass, which is what we want (current turn's signal beats stale
      // history).
      if (this.maxResourceFacts !== undefined && appended > 0) {
        const all = await this.store.listResourceFacts(resourceKey);
        const overflow = all.length - this.maxResourceFacts;
        if (overflow > 0) {
          const toEvict = all.slice(0, overflow);
          for (const f of toEvict) {
            await this.store.deleteResourceFact(resourceKey, f.id);
          }
        }
      }
    }

    const fromSeq = messages[0]!.seq;
    const toSeq = messages[messages.length - 1]!.seq;
    return this.store.appendResourceEpisode(resourceKey, {
      summary: distilled.summary,
      outcome: distilled.outcome,
      salience: distilled.salience,
      embedding: distilled.embedding,
      sourceThreadId: key.threadId,
      sourceMessageRange: { fromSeq, toSeq },
      metadata: { kind: "distill", factCount: distilled.facts.length },
    });
  }

  async distillResource(
    key: ScopedKey,
    opts: DistillResourceOptions = {},
  ): Promise<EpisodicRecord[]> {
    const minMessages = opts.minMessages ?? this.minDistillMessages;
    const sinceMs = opts.since ? opts.since.getTime() : 0;
    const threads = await this.store.listThreads({
      namespaceId: key.namespaceId,
      resourceId: key.resourceId,
      limit: 1000,
    });
    const out: EpisodicRecord[] = [];
    for (const t of threads) {
      if (t.lastActiveAt < sinceMs) continue;
      if (t.messageCount < minMessages) continue;
      try {
        const ep = await this.distillThread(
          {
            namespaceId: t.namespaceId,
            resourceId: t.resourceId ?? key.resourceId,
            threadId: t.threadId,
          },
          { force: opts.force ?? false },
        );
        out.push(ep);
      } catch {
        // One failing thread shouldn't kill the whole sweep.
      }
    }
    return out;
  }

  // ---- Internal: prompt + parse + signals --------------------------------

  private async distill(messages: ReadonlyArray<StoredMessage>): Promise<{
    summary: string;
    outcome: string | null;
    salience: number;
    facts: string[];
    embedding?: number[];
  }> {
    const transcript = formatTranscript(messages, this.triggerTurnHandling);
    const llmMessages: Message[] = [
      { role: "system", content: this.distillPrompt },
      { role: "user", content: transcript },
    ];
    const response = await this.llm.chat({ messages: llmMessages });
    const parsed = parseEnvelope(response.content ?? "");
    const signals: ConsolidationSignals = {
      messageCount: messages.length,
      toolCallCount: messages.filter(
        (m) => m.role === "assistant" && (m.toolCalls ?? []).length > 0,
      ).length,
      userMessageCount: messages.filter(
        (m) => m.role === "user" && !(m.metadata?.source as { kind?: string } | undefined)?.kind,
      ).length,
      hasFailures: messages.some((m) => m.role === "tool" && /error|failed/i.test(m.content)),
      modelSalience: parsed.salience,
      summary: parsed.summary,
    };
    const salience = clamp01(this.salienceFn(signals));
    const embedding = this.embeddings ? await this.embeddings.embed(parsed.summary) : undefined;
    return {
      summary: parsed.summary,
      outcome: parsed.outcome,
      salience,
      facts: parsed.facts,
      embedding,
    };
  }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

interface ParsedEnvelope {
  summary: string;
  outcome: string | null;
  salience: number;
  facts: string[];
}

function parseEnvelope(raw: string): ParsedEnvelope {
  // Tolerate a fenced ```json ``` block or extra whitespace around the payload.
  const trimmed = raw
    .trim()
    .replace(/^```(?:json)?\s*/i, "")
    .replace(/\s*```$/i, "");
  let json: Record<string, unknown>;
  try {
    json = JSON.parse(trimmed) as Record<string, unknown>;
  } catch {
    // LLM didn't follow the envelope — fall back to a low-salience stub
    // so the writer still produces something rather than failing the
    // distillation entirely.
    return {
      summary: trimmed.slice(0, 500) || "(no summary)",
      outcome: null,
      salience: 0.1,
      facts: [],
    };
  }
  const summary =
    typeof json["summary"] === "string" ? (json["summary"] as string) : "(no summary)";
  const outcome = typeof json["outcome"] === "string" ? (json["outcome"] as string) : null;
  const salience = typeof json["salience"] === "number" ? clamp01(json["salience"] as number) : 0.5;
  const facts = Array.isArray(json["facts"])
    ? (json["facts"] as unknown[]).filter((s): s is string => typeof s === "string" && s.length > 0)
    : [];
  return { summary, outcome, salience, facts };
}

function formatTranscript(
  messages: ReadonlyArray<StoredMessage>,
  triggerHandling: TriggerTurnHandling = "reframe",
): string {
  const lines: string[] = [];
  let inTriggerTurn = false;
  let triggerTask = "";

  for (const m of messages) {
    if (m.role === "system") continue;

    if (m.role === "user") {
      const srcKind = (m.metadata?.source as { kind?: string } | undefined)?.kind;
      if (srcKind && srcKind !== "user") {
        const bareTask = m.content.replace(/^\[[^\]]+\]\s*/, "");
        if (triggerHandling === "include") {
          lines.push(`[${srcKind}-trigger] ${bareTask}`);
          inTriggerTurn = false;
        } else {
          inTriggerTurn = true;
          triggerTask = bareTask;
        }
      } else {
        inTriggerTurn = false;
        lines.push(`[user] ${m.content}`);
      }
      continue;
    }

    if (m.role === "assistant") {
      const text = (m.content ?? "").trim();
      const toolCalls = m.toolCalls ?? [];
      if (inTriggerTurn) {
        if (triggerHandling === "skip") continue;
        if (text) lines.push(`[scheduled-result: ${triggerTask}] ${text}`);
        for (const t of toolCalls) {
          lines.push(`[scheduled-tool-call] ${t.name}(${JSON.stringify(t.input)})`);
        }
      } else {
        if (text) lines.push(`[assistant] ${text}`);
        for (const t of toolCalls) {
          lines.push(`[assistant tool-call] ${t.name}(${JSON.stringify(t.input)})`);
        }
      }
      continue;
    }

    if (m.role === "tool") {
      if (inTriggerTurn) {
        if (triggerHandling === "skip") continue;
        lines.push(`[scheduled-tool-result] ${m.content.slice(0, 240)}`);
      } else {
        lines.push(`[tool-result] ${m.content.slice(0, 240)}`);
      }
      continue;
    }
  }
  return lines.join("\n");
}

function defaultSalience(signals: ConsolidationSignals): number {
  // Trust the model's self-assessed salience when present; lightly nudge
  // it based on structural signals so heuristics still bias the score
  // when the model returns a generic value.
  const base = signals.modelSalience ?? 0.5;
  let adj = base;
  if (signals.toolCallCount > 0) adj += 0.05;
  if (signals.hasFailures) adj += 0.05;
  if (signals.userMessageCount >= 4) adj += 0.05;
  if (signals.messageCount < 3) adj -= 0.2;
  return clamp01(adj);
}

function clamp01(n: number): number {
  if (Number.isNaN(n)) return 0;
  if (n < 0) return 0;
  if (n > 1) return 1;
  return n;
}

function normalizeFact(text: string): string {
  return (
    text
      .toLowerCase()
      .trim()
      // Drop possessive markers FIRST so "user's name" → "user name"
      // (matching the same fact phrased without the possessive).
      // Order matters — generic apostrophe-stripping below would turn
      // "user's" into "users" and break the match against "user".
      .replace(/['`’‘]s\b/g, "")
      // Strip stray apostrophes / smart quotes left over from above.
      .replace(/['`’‘]/g, "")
      // Strip trailing punctuation so "Anton" / "Anton." dedupe.
      .replace(/[.!?]+$/, "")
      // Collapse internal whitespace.
      .replace(/\s+/g, " ")
  );
}
