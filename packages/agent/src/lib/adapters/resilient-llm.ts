// ---------------------------------------------------------------------------
// `resilientLLM` — LLM wrapper that recovers from transient stream
// errors and downshifts to a fallback model on quota / context overflow.
//
// The existing `fallbackLLM` chains providers on any failure — too
// coarse: it can't distinguish "we should keep retrying the primary"
// from "primary is rate-limited, swap to backup" from "the prompt is
// too long, only a bigger-window model will help".
//
// `resilientLLM` adds:
//   - configurable retry count + backoff for transient errors
//   - explicit policy for partial-stream errors (retry replays from
//     scratch — risky because the caller may have already consumed
//     deltas; default off for that reason)
//   - quota / context-overflow detection → switch to a fallback
//     provider once and replay the call
//   - metric hooks so the wrapper's recovery is visible
//
// Replay-safety
// -------------
// `chat()` (non-stream) is trivially replay-safe — the caller doesn't
// see partial state, so we can retry transparently as many times as
// the policy allows.
//
// `chatStream()` is harder. Two cases:
//
//   A. Error before any chunk emitted. Caller has seen nothing yet,
//      so retrying is invisible — same shape as chat() retry.
//
//   B. Error after partial chunks. The caller has already received
//      some deltas. Retrying from scratch means the caller will see
//      duplicate text — for a chat UI that's user-visible. We default
//      to surfacing the error in this case; opt-in to `retryAfterPartial`
//      if your consumer can handle replay (e.g. it buffers deltas and
//      renders only on completion).
// ---------------------------------------------------------------------------

import type { LLMChatParams, LLMProvider, LLMResponse, LLMStreamChunk } from "../llm-provider.ts";
import type { AgentMetrics } from "../metrics/types.ts";
import { classifyLLMError, type LLMErrorClass } from "./llm-error-classification.ts";

export interface ResilientLLMPolicy {
  /**
   * Max retries for transient errors (network, 5xx, overloaded).
   * Default: 3. Set to 0 to disable.
   */
  readonly transientRetries?: number;
  /**
   * Max retries for rate_limit errors. Defaults to `transientRetries`.
   * Often set higher because rate-limits are usually time-bounded.
   */
  readonly rateLimitRetries?: number;
  /**
   * Backoff in ms before attempt N (1-indexed). Default: exponential
   * 200, 400, 800, 1600 capped at 30s, plus 0–25% jitter.
   */
  readonly backoffMs?: (attempt: number) => number;
  /**
   * Allow replaying a stream from scratch after partial chunks were
   * already emitted. Default: false. Enable only when the caller
   * buffers deltas and won't render duplicates.
   */
  readonly retryAfterPartial?: boolean;
  /**
   * Fallback provider used on rate_limit / context_overflow when the
   * primary's retries are exhausted. The wrapper switches once and
   * replays the call against the fallback — does NOT chain further
   * fallbacks. Stack `resilientLLM` instances if you want multi-tier.
   */
  readonly fallback?: LLMProvider;
  /**
   * Optional telemetry. Emits `llm.retries` (counter, labeled by
   * reason: transient | rate_limit | context_overflow | partial) and
   * `llm.fallback.switches` (counter) so retry behaviour is visible
   * via the same metrics surface as the rest of the runtime.
   */
  readonly metrics?: AgentMetrics;
  readonly metricLabels?: Readonly<Record<string, string>>;
}

const DEFAULT_TRANSIENT_RETRIES = 3;
const MAX_BACKOFF_MS = 30_000;

function defaultBackoff(attempt: number): number {
  const base = Math.min(200 * 2 ** (attempt - 1), MAX_BACKOFF_MS);
  // 0–25% jitter — spreads thundering-herd retries.
  return base + Math.random() * (base * 0.25);
}

function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    if (signal?.aborted) {
      reject(new Error("aborted"));
      return;
    }
    const t = setTimeout(resolve, ms);
    signal?.addEventListener(
      "abort",
      () => {
        clearTimeout(t);
        reject(new Error("aborted"));
      },
      { once: true },
    );
  });
}

/**
 * Wrap an LLMProvider with retry + downshift behaviour. Call-shape is
 * identical to the inner provider — a drop-in replacement that adds
 * resilience.
 */
export function resilientLLM(inner: LLMProvider, policy: ResilientLLMPolicy = {}): LLMProvider {
  const transientRetries = policy.transientRetries ?? DEFAULT_TRANSIENT_RETRIES;
  const rateLimitRetries = policy.rateLimitRetries ?? transientRetries;
  const backoff = policy.backoffMs ?? defaultBackoff;

  function recordRetry(reason: LLMErrorClass | "partial"): void {
    if (!policy.metrics) return;
    policy.metrics.counter("llm.retries").inc(1, { reason, ...policy.metricLabels });
  }

  function recordFallback(reason: LLMErrorClass): void {
    if (!policy.metrics) return;
    policy.metrics.counter("llm.fallback.switches").inc(1, { reason, ...policy.metricLabels });
  }

  return {
    async chat(params: LLMChatParams): Promise<LLMResponse> {
      // Per-class attempt counters — a sustained rate-limit shouldn't
      // burn the transient budget and vice versa.
      const attempts: Record<"transient" | "rate_limit", number> = {
        transient: 0,
        rate_limit: 0,
      };
      while (true) {
        try {
          return await inner.chat(params);
        } catch (err) {
          const cls = classifyLLMError(err);

          if (cls === "permanent") throw err;

          if (cls === "context_overflow") {
            // Retrying the same model won't help — only a bigger-window
            // fallback can.
            if (policy.fallback) {
              recordFallback(cls);
              return await policy.fallback.chat(params);
            }
            throw err;
          }

          const limit = cls === "rate_limit" ? rateLimitRetries : transientRetries;
          attempts[cls] += 1;
          if (attempts[cls] > limit) {
            // Retry budget for this class exhausted. Fall back on rate-
            // limit (different model often has independent quota);
            // transient burns suggest a real outage that fallback can't fix.
            if (cls === "rate_limit" && policy.fallback) {
              recordFallback(cls);
              return await policy.fallback.chat(params);
            }
            throw err;
          }

          recordRetry(cls);
          await sleep(backoff(attempts[cls]), params.signal);
        }
      }
    },

    chatStream(params: LLMChatParams): AsyncIterable<LLMStreamChunk> {
      // Top-level stream wrapper — handles retries on the OUTER iterator.
      // The inner stream may be torn down + replayed from scratch on a
      // recoverable error, hidden from the caller.
      return resilientStream(inner, params, policy, {
        transientRetries,
        rateLimitRetries,
        backoff,
        recordRetry,
        recordFallback,
      });
    },
  };
}

interface StreamCtx {
  readonly transientRetries: number;
  readonly rateLimitRetries: number;
  readonly backoff: (attempt: number) => number;
  readonly recordRetry: (reason: LLMErrorClass | "partial") => void;
  readonly recordFallback: (reason: LLMErrorClass) => void;
}

async function* resilientStream(
  inner: LLMProvider,
  params: LLMChatParams,
  policy: ResilientLLMPolicy,
  ctx: StreamCtx,
): AsyncIterable<LLMStreamChunk> {
  const attempts: Record<"transient" | "rate_limit", number> = {
    transient: 0,
    rate_limit: 0,
  };
  let activeProvider: LLMProvider = inner;
  // Tracks whether any chunk has been yielded to the caller. After the
  // first chunk leaves this generator, retrying replays from scratch —
  // only allowed if `retryAfterPartial`.
  while (true) {
    let yieldedAny = false;
    try {
      const stream = await openStream(activeProvider, params);
      for await (const chunk of stream) {
        yieldedAny = true;
        yield chunk;
      }
      return;
    } catch (err) {
      const cls = classifyLLMError(err);

      if (cls === "permanent") throw err;

      if (cls === "context_overflow") {
        if (yieldedAny && !policy.retryAfterPartial) throw err;
        if (policy.fallback) {
          ctx.recordFallback(cls);
          activeProvider = policy.fallback;
          if (yieldedAny) ctx.recordRetry("partial");
          continue;
        }
        throw err;
      }

      if (yieldedAny && !policy.retryAfterPartial) {
        // Caller has already received deltas; replaying would cause
        // duplicate output. Surface the error so the caller decides.
        throw err;
      }
      if (yieldedAny) ctx.recordRetry("partial");

      const limit = cls === "rate_limit" ? ctx.rateLimitRetries : ctx.transientRetries;
      attempts[cls] += 1;
      if (attempts[cls] > limit) {
        if (cls === "rate_limit" && policy.fallback) {
          ctx.recordFallback(cls);
          activeProvider = policy.fallback;
          attempts.transient = 0;
          attempts.rate_limit = 0; // fresh budget on the fallback
          continue;
        }
        throw err;
      }

      ctx.recordRetry(cls);
      await sleep(ctx.backoff(attempts[cls]), params.signal);
    }
  }
}

async function openStream(
  provider: LLMProvider,
  params: LLMChatParams,
): Promise<AsyncIterable<LLMStreamChunk>> {
  if (provider.chatStream) {
    return provider.chatStream(params);
  }
  // Fall back to chat() and synthesize a single-chunk stream so
  // resilientLLM works against providers that only implement chat.
  const r = await provider.chat(params);
  async function* once(): AsyncIterable<LLMStreamChunk> {
    if (r.content) yield { delta: r.content };
    yield {
      delta: "",
      finishReason: r.finishReason,
      ...(r.toolCalls && { toolCalls: r.toolCalls }),
      ...(r.usage && { usage: r.usage }),
      ...(r.thinkingBlocks && { thinkingBlocks: r.thinkingBlocks }),
    };
  }
  return once();
}
