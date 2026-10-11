// ---------------------------------------------------------------------------
// `resilientLLM` — LLM wrapper that recovers from transient stream
// errors and fails over along a chain of fallback providers.
//
// It distinguishes "keep retrying the primary" from "primary is
// rate-limited, swap to a backup" from "the prompt is too long, only a
// bigger-window model will help":
//   - configurable retry count + backoff for transient errors
//   - explicit policy for partial-stream errors (retry replays from
//     scratch — risky because the caller may have already consumed
//     deltas; default off for that reason)
//   - a fallback chain, entered on the error classes in `fallbackOn`
//     (quota / context overflow by default; every class for a plain
//     "try each provider in order" chain)
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
import { streamChat } from "./stream-chat.ts";

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
   * Fallback providers, tried in order. The call moves to the next one
   * when the active provider fails with an error class listed in
   * `fallbackOn` — immediately for `context_overflow` / `permanent`,
   * after its retry budget is spent for `transient` / `rate_limit`.
   * Each fallback starts with a fresh retry budget. A single provider
   * is a one-element chain.
   */
  readonly fallback?: LLMProvider | ReadonlyArray<LLMProvider>;
  /**
   * Error classes that fail over to the next fallback provider.
   * Default: `["rate_limit", "context_overflow"]` — a different model
   * often has independent quota or a bigger window, while transient
   * burns suggest an outage a fallback can't fix. Pass every class
   * (`ALL_LLM_ERROR_CLASSES`) for a plain "try each provider in order"
   * chain.
   */
  readonly fallbackOn?: ReadonlyArray<LLMErrorClass>;
  /**
   * Optional telemetry. Emits `llm.retries` (counter, labeled by
   * reason: transient | rate_limit | context_overflow | partial) and
   * `llm.fallback.switches` (counter) so retry behaviour is visible
   * via the same metrics surface as the rest of the runtime.
   */
  readonly metrics?: AgentMetrics;
  readonly metricLabels?: Readonly<Record<string, string>>;
}

/** Every `LLMErrorClass` — pass as `fallbackOn` to fail over on any error. */
export const ALL_LLM_ERROR_CLASSES: ReadonlyArray<LLMErrorClass> = [
  "transient",
  "rate_limit",
  "context_overflow",
  "permanent",
];

const DEFAULT_FALLBACK_ON: ReadonlyArray<LLMErrorClass> = ["rate_limit", "context_overflow"];
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
 * Wrap an LLMProvider with retry + fallback behaviour. Call-shape is
 * identical to the inner provider — a drop-in replacement that adds
 * resilience.
 *
 * @example
 * ```ts
 * // Retry the primary, fail over to a backup on quota / context overflow.
 * const llm = resilientLLM(primary, { fallback: backup });
 *
 * // Plain fallback chain: try each provider once, in order, on any error.
 * const chain = resilientLLM(primary, {
 *   transientRetries: 0,
 *   rateLimitRetries: 0,
 *   fallback: [backup, lastResort],
 *   fallbackOn: ALL_LLM_ERROR_CLASSES,
 * });
 * ```
 */
export function resilientLLM(inner: LLMProvider, policy: ResilientLLMPolicy = {}): LLMProvider {
  const transientRetries = policy.transientRetries ?? DEFAULT_TRANSIENT_RETRIES;
  const rateLimitRetries = policy.rateLimitRetries ?? transientRetries;
  const backoff = policy.backoffMs ?? defaultBackoff;
  const fallbackOn = new Set(policy.fallbackOn ?? DEFAULT_FALLBACK_ON);
  const fallbacks: ReadonlyArray<LLMProvider> =
    policy.fallback === undefined
      ? []
      : "chat" in policy.fallback
        ? [policy.fallback]
        : policy.fallback;
  const chain: ReadonlyArray<LLMProvider> = [inner, ...fallbacks];

  function recordRetry(reason: LLMErrorClass | "partial"): void {
    if (!policy.metrics) return;
    policy.metrics.counter("llm.retries").inc(1, { reason, ...policy.metricLabels });
  }

  function recordFallback(reason: LLMErrorClass): void {
    if (!policy.metrics) return;
    policy.metrics.counter("llm.fallback.switches").inc(1, { reason, ...policy.metricLabels });
  }

  /**
   * One failure's outcome on the provider at `chain[index]`. Per-class
   * attempt counters, so a sustained rate-limit doesn't burn the
   * transient budget and vice versa.
   */
  function decide(args: {
    err: unknown;
    attempts: Record<"transient" | "rate_limit", number>;
    index: number;
    signal: AbortSignal | undefined;
  }):
    | { kind: "retry"; cls: "transient" | "rate_limit" }
    | { kind: "fallback" }
    | { kind: "throw" } {
    if (args.signal?.aborted) return { kind: "throw" };
    const cls = classifyLLMError(args.err);
    if (cls === "transient" || cls === "rate_limit") {
      const limit = cls === "rate_limit" ? rateLimitRetries : transientRetries;
      args.attempts[cls] += 1;
      if (args.attempts[cls] <= limit) return { kind: "retry", cls };
    }
    if (fallbackOn.has(cls) && args.index + 1 < chain.length) {
      recordFallback(cls);
      return { kind: "fallback" };
    }
    return { kind: "throw" };
  }

  return {
    async chat(params: LLMChatParams): Promise<LLMResponse> {
      let index = 0;
      let attempts = { transient: 0, rate_limit: 0 };
      while (true) {
        try {
          return await chain[index]!.chat(params);
        } catch (err) {
          const next = decide({ err, attempts, index, signal: params.signal });
          if (next.kind === "throw") throw err;
          if (next.kind === "fallback") {
            index += 1;
            attempts = { transient: 0, rate_limit: 0 };
            continue;
          }
          recordRetry(next.cls);
          await sleep(backoff(attempts[next.cls]), params.signal);
        }
      }
    },

    async *chatStream(params: LLMChatParams): AsyncIterable<LLMStreamChunk> {
      // Retries happen on the OUTER iterator: the inner stream may be torn
      // down + replayed from scratch on a recoverable error, hidden from
      // the caller as long as nothing was yielded yet.
      let index = 0;
      let attempts = { transient: 0, rate_limit: 0 };
      while (true) {
        let yieldedAny = false;
        try {
          for await (const chunk of streamChat({ provider: chain[index]!, params })) {
            yieldedAny = true;
            yield chunk;
          }
          return;
        } catch (err) {
          // Caller has already received deltas; replaying would cause
          // duplicate output. Surface the error so the caller decides.
          if (yieldedAny && !policy.retryAfterPartial) throw err;
          const next = decide({ err, attempts, index, signal: params.signal });
          if (next.kind === "throw") throw err;
          if (yieldedAny) recordRetry("partial");
          if (next.kind === "fallback") {
            index += 1;
            attempts = { transient: 0, rate_limit: 0 };
            continue;
          }
          recordRetry(next.cls);
          await sleep(backoff(attempts[next.cls]), params.signal);
        }
      }
    },
  };
}
