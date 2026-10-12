// ---------------------------------------------------------------------------
// Auto-compaction and auto-distillation of a `LocalAgent` thread: after each
// thread turn, read the persisted thread, decide whether the configured
// rule fires, and run the consolidator in the background or blocking.
// ---------------------------------------------------------------------------

import type { WallClock } from "@promin/workflow";
import type { Consolidator } from "../memory/consolidator.ts";
import type { MemoryStore, ThreadKey } from "../memory/types.ts";

export interface AutoCompactConfig {
  /**
   * Built-in count gate: fire when the count of UNCOMPACTED messages —
   * those with seq higher than the most recent compaction episode's
   * toSeq — exceeds this number. Tracks "since last compact" so the
   * trigger doesn't re-fire every turn once the thread is past the
   * threshold.
   */
  readonly messageThreshold?: number;
  /**
   * Built-in token gate: fire when uncompacted-message tokens exceed
   * this number. Tokens are estimated as `chars/4` over `content`,
   * matching the `resolveContext` default estimator. More accurate
   * than `messageThreshold` when message lengths vary widely (one
   * giant tool result can blow context even at low message counts).
   *
   * Often more ergonomic to express via `contextLimit` + `compressAt`
   * below — that way you don't have to recompute the threshold when
   * you swap chat models.
   */
  readonly tokenThreshold?: number;
  /**
   * Model context window in tokens. When combined with `compressAt`,
   * the effective token gate becomes `contextLimit * compressAt`.
   *
   * Examples: 200_000 for current Anthropic models. Wins over
   * `tokenThreshold` when both are set.
   */
  readonly contextLimit?: number;
  /**
   * Fraction of `contextLimit` at which to compact. Default 0.70 —
   * leaves the model 30% headroom for the assistant's reply.
   * Ignored when `contextLimit` isn't set.
   */
  readonly compressAt?: number;
  /**
   * Custom predicate. Mirrors `RetryPolicy.when` from `@promin/workflow`:
   * receives a signals envelope and returns `true` to fire on this
   * turn. When set, REPLACES both built-in thresholds. Use this for
   * compound rules.
   *
   * Examples:
   *
   *     // count + cooldown — don't compact more than once a minute
   *     when: ({ uncompactedCount, lastCompactedAt }) =>
   *       uncompactedCount > 30 &&
   *       Date.now() - lastCompactedAt > 60_000
   *
   *     // token gate with thread-id allowlist
   *     when: ({ uncompactedTokens, threadKey }) =>
   *       uncompactedTokens > 8_000 &&
   *       threadKey.threadId.startsWith("lr-")
   */
  readonly when?: (signals: AutoCompactSignals) => boolean;
  /** Newest messages to leave unsummarised. Default 10. */
  readonly keepRecent?: number;
  /**
   * Execution mode:
   *   - `"background"` (default) — fire-and-forget AFTER the turn
   *     resolves. The caller never waits for the compact LLM call.
   *     Errors are logged but not surfaced.
   *   - `"blocking"` — await the compact before the turn resolves.
   *     Caller pays the compactionLlm round-trip on every triggered
   *     turn. Use only when a downstream caller expects the episode
   *     to exist before moving on.
   */
  readonly mode?: "background" | "blocking";
}

/** State the auto-compact predicate sees. */
export interface AutoCompactSignals {
  /** Total persisted message count for this thread. */
  readonly totalCount: number;
  /** Count of messages with seq > most recent compact episode's toSeq. */
  readonly uncompactedCount: number;
  /** Estimated token count over all persisted messages (chars/4). */
  readonly totalTokens: number;
  /** Estimated token count over uncompacted messages (chars/4). */
  readonly uncompactedTokens: number;
  /** `createdAt` of the most recent compact episode (0 if none). */
  readonly lastCompactedAt: number;
  readonly threadKey: ThreadKey;
}

export interface AutoDistillConfig {
  /**
   * Built-in message-count gate: fire when the thread's persisted
   * message count reaches this number. Cheap to compute — best for
   * simple "distill once after N messages" patterns.
   *
   * Common values: 6 (after a couple of turns, conversation has
   * enough substance), 20 (only substantial threads).
   *
   * Multiple gates compose with OR semantics — fires when ANY of
   * `messageThreshold`, `tokenThreshold`, `intervalMs` trips.
   */
  readonly messageThreshold?: number;
  /**
   * Built-in token-count gate: fire when the SUM of estimated tokens
   * across NEW messages (since last distill) exceeds this value.
   * Tokens estimated as `chars/4`, matching autoCompact's heuristic.
   *
   * More cost-aware than `messageThreshold` — three short messages
   * vs three giant ones cost very different amounts to distill.
   * Common values: 4_000 (roughly one Haiku context's worth of new
   * material), 16_000 (substantial accumulated content).
   */
  readonly tokenThreshold?: number;
  /**
   * Built-in time-since-last-distill gate (milliseconds): fire when
   * the elapsed time since the last distill episode exceeds this.
   * Distinct from message/token gates — useful for low-volume
   * threads where 6 messages might span a week, OR for periodic
   * re-distillation patterns regardless of message volume.
   *
   * Combine with `force: true` to re-distill on every interval tick.
   */
  readonly intervalMs?: number;
  /**
   * Custom predicate. When set, REPLACES the built-in gates entirely
   * — the predicate has full control. Use for compound rules:
   *
   *     when: ({ turnsSinceLastDistill, lastUserMessage }) =>
   *       turnsSinceLastDistill >= 10 ||
   *       /^(thanks|bye|goodbye|see ya)\b/i.test(lastUserMessage ?? ""),
   */
  readonly when?: (signals: AutoDistillSignals) => boolean;
  /**
   * Re-distill even when an episode for this thread already exists.
   * Default `false` (idempotent — first qualifying turn writes one
   * episode; subsequent turns are no-ops). Set `true` for periodic
   * re-distillation patterns (works well with `intervalMs` or
   * `tokenThreshold` past initial fire).
   */
  readonly force?: boolean;
  /**
   * Execution mode (same semantics as `AutoCompactConfig.mode`):
   *   - `"background"` (default) — fire-and-forget after the turn resolves
   *   - `"blocking"` — await before resolving the turn
   */
  readonly mode?: "background" | "blocking";
}

/** State the auto-distill predicate sees. */
export interface AutoDistillSignals {
  /** Total persisted message count for this thread. */
  readonly totalCount: number;
  /**
   * Number of NEW messages since the most recent
   * resource-scope episode whose `sourceThreadId` is this thread.
   * Equals `totalCount` when never distilled.
   */
  readonly turnsSinceLastDistill: number;
  /**
   * Estimated token count (chars/4) summed across the NEW messages
   * since the most recent distill episode. Equals total-thread-tokens
   * when never distilled.
   */
  readonly tokensSinceLastDistill: number;
  /**
   * Milliseconds elapsed since the most recent distill episode.
   * `Infinity` when never distilled (so `intervalMs` always trips).
   */
  readonly msSinceLastDistill: number;
  /**
   * `createdAt` of the most recent distill episode for this thread
   * (0 if none).
   */
  readonly lastDistilledAt: number;
  /**
   * Trimmed string content of the thread's most recent user message,
   * or `undefined` when the latest message isn't a user turn.
   * Useful for goodbye-detection heuristics.
   */
  readonly lastUserMessage?: string;
  readonly threadKey: ThreadKey;
}

/**
 * Auto-compaction trigger. Reads the persisted state, computes signals
 * (uncompacted-since-last-compact counts in messages and tokens), checks
 * whether the configured rule fires, and dispatches the compactThread
 * call in either background or blocking mode.
 *
 * Writes a thread episode only; `resolveContext` does not render
 * per-thread episodes into the prompt (the memory tool's `recall`
 * searches them on demand).
 */
export async function maybeAutoCompact(params: {
  readonly config: AutoCompactConfig;
  readonly memory: MemoryStore;
  readonly key: ThreadKey;
  readonly consolidator: () => Consolidator;
}): Promise<void> {
  const { config: cfg, memory, key } = params;
  if (
    cfg.messageThreshold === undefined &&
    cfg.tokenThreshold === undefined &&
    cfg.contextLimit === undefined &&
    !cfg.when
  ) {
    return; // nothing to gate on
  }

  const messages = await memory.getMessages(key, { order: "asc" });
  const episodes = await memory.listThreadEpisodes(key, { order: "createdDesc" }).catch(() => []);
  const lastCompact = episodes.find(
    (e) => (e.metadata as { kind?: unknown } | null)?.kind === "compact",
  );
  const lastCompactedSeq = lastCompact?.sourceMessageRange?.toSeq ?? 0;
  const lastCompactedAt = lastCompact?.createdAt ?? 0;

  const uncompacted = messages.filter((m) => m.seq > lastCompactedSeq);
  const totalTokens = messages.reduce((s, m) => s + estimateTokens(m), 0);
  const uncompactedTokens = uncompacted.reduce((s, m) => s + estimateTokens(m), 0);

  const signals: AutoCompactSignals = {
    totalCount: messages.length,
    uncompactedCount: uncompacted.length,
    totalTokens,
    uncompactedTokens,
    lastCompactedAt,
    threadKey: key,
  };

  // Effective token gate: contextLimit * compressAt wins when set,
  // else tokenThreshold.
  const effectiveTokenThreshold =
    cfg.contextLimit !== undefined
      ? cfg.contextLimit * (cfg.compressAt ?? 0.7)
      : cfg.tokenThreshold;

  const fire = cfg.when
    ? cfg.when(signals)
    : (cfg.messageThreshold !== undefined && uncompacted.length > cfg.messageThreshold) ||
      (effectiveTokenThreshold !== undefined && uncompactedTokens > effectiveTokenThreshold);
  if (!fire) return;

  const run = params
    .consolidator()
    .compactThread(key, { keepRecent: cfg.keepRecent ?? 10 })
    .catch((err) => {
      // Don't propagate — auto-compact is best-effort. Log so operators
      // can spot a misconfigured consolidator (missing key, malformed
      // LLM output, etc.) without breaking the user-visible turn.
      console.warn(
        `[LocalAgent] autoCompact failed for thread ${key.threadId}:`,
        err instanceof Error ? err.message : err,
      );
    });

  if ((cfg.mode ?? "background") === "blocking") {
    await run;
  }
  // else: fire-and-forget; the run's promise carries no value the
  // turn cares about, errors are already logged above.
}

/**
 * Auto-distillation trigger. Symmetric with `maybeAutoCompact`: computes
 * signals (totalCount, turnsSinceLastDistill, lastDistilledAt,
 * lastUserMessage), runs the configured rule, dispatches to the
 * Consolidator. Idempotency is on the consolidator side — a second fire
 * on the same thread without `force: true` returns the existing episode
 * without re-running the LLM.
 */
export async function maybeAutoDistill(params: {
  readonly config: AutoDistillConfig;
  readonly memory: MemoryStore;
  readonly key: ThreadKey;
  readonly consolidator: () => Consolidator;
  readonly clock: WallClock;
}): Promise<void> {
  const { config: cfg, memory, key } = params;
  const hasGate =
    cfg.messageThreshold !== undefined ||
    cfg.tokenThreshold !== undefined ||
    cfg.intervalMs !== undefined ||
    cfg.when !== undefined;
  if (!hasGate) return;
  // Distillation writes a ResourceEpisode — only meaningful when a
  // resourceId is bound. Without one, silently skip.
  if (!key.resourceId) return;

  const messages = await memory.getMessages(key, { order: "asc" });
  const resourceEpisodes = await memory
    .listResourceEpisodes(
      { namespaceId: key.namespaceId, resourceId: key.resourceId },
      { order: "createdDesc" },
    )
    .catch(() => []);
  const lastDistill = resourceEpisodes.find(
    (e) =>
      e.sourceThreadId === key.threadId &&
      (e.metadata as { kind?: unknown } | null)?.kind === "distill",
  );
  const lastDistilledSeq = lastDistill?.sourceMessageRange?.toSeq ?? 0;
  const lastDistilledAt = lastDistill?.createdAt ?? 0;
  const newMessages = messages.filter((m) => m.seq > lastDistilledSeq);
  const turnsSinceLastDistill = newMessages.length;
  const tokensSinceLastDistill = newMessages.reduce((s, m) => s + estimateTokens(m), 0);
  const now = params.clock.currentTimeMs();
  const msSinceLastDistill = lastDistilledAt ? now - lastDistilledAt : Number.POSITIVE_INFINITY;

  // Walk back from the end to find the most recent USER message —
  // after the turn's commit the assistant turn (and any tool messages)
  // are at the tail; the user's input sits before them.
  let lastUserMessage: string | undefined;
  for (let i = messages.length - 1; i >= 0; i--) {
    const m = messages[i]!;
    if (m.role === "user" && typeof m.content === "string") {
      lastUserMessage = m.content.trim();
      break;
    }
  }

  const signals: AutoDistillSignals = {
    totalCount: messages.length,
    turnsSinceLastDistill,
    tokensSinceLastDistill,
    msSinceLastDistill,
    lastDistilledAt,
    lastUserMessage,
    threadKey: key,
  };

  // Custom predicate REPLACES built-in gates entirely. Otherwise OR
  // across whichever built-in gates are configured: any one tripping
  // is enough to fire.
  const fire = cfg.when
    ? cfg.when(signals)
    : (cfg.messageThreshold !== undefined && messages.length >= cfg.messageThreshold) ||
      (cfg.tokenThreshold !== undefined && tokensSinceLastDistill >= cfg.tokenThreshold) ||
      (cfg.intervalMs !== undefined && msSinceLastDistill >= cfg.intervalMs);
  if (!fire) return;

  const run = params
    .consolidator()
    .distillThread(key, { force: cfg.force ?? false })
    .catch((err) => {
      // Rate-limit hits are by design (host-configured cost cap),
      // not failures worth a warn. Swallow silently.
      if (
        err &&
        typeof err === "object" &&
        (err as { name?: string }).name === "ConsolidatorRateLimitError"
      ) {
        return;
      }
      console.warn(
        `[LocalAgent] autoDistill failed for thread ${key.threadId}:`,
        err instanceof Error ? err.message : err,
      );
    });

  if ((cfg.mode ?? "background") === "blocking") {
    await run;
  }
}

/**
 * chars/4 token estimator — same heuristic resolveContext's default
 * uses. Good enough for trigger decisions; an over-estimate only
 * consolidates slightly early.
 */
function estimateTokens(m: { content?: string | null }): number {
  const len = typeof m.content === "string" ? m.content.length : 0;
  return Math.ceil(len / 4);
}
