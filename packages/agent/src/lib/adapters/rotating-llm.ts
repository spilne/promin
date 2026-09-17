import type {
  LLMProvider,
  LLMChatParams,
  LLMResponse,
  LLMStreamChunk,
  RateLimitHint,
} from "../llm-provider.ts";
import { classifyLLMError } from "./llm-error-classification.ts";
import { InMemoryCapacityStore, type CapacityStore } from "./capacity-store.ts";

export type RotatingLLMStrategy = "round-robin" | "least-remaining";

export interface RotatingLLMOptions {
  /**
   * Slot selection policy. Default: `"least-remaining"` — picks the slot
   * with the highest known `remainingTokens` from the most recent
   * response. When no slot has reported a hint yet, falls through to
   * round-robin so cold-start traffic still distributes evenly.
   */
  strategy?: RotatingLLMStrategy;

  /**
   * Backing store for cross-instance cooldown coordination. Default:
   * `InMemoryCapacityStore` (single-process). Pass a Redis / Postgres
   * implementation to share exhaustion across replicas.
   */
  capacityStore?: CapacityStore;

  /**
   * Threshold (in tokens) at which a slot is treated as exhausted before
   * the next 429 fires. When a response's `rateLimitHint.remainingTokens`
   * drops below this AND `resetsAt` is set, the slot is marked exhausted
   * in the capacity store. Set to `0` to disable proactive cooldown
   * (only react to actual 429s). Default: `1000`.
   */
  exhaustionTokenThreshold?: number;

  /**
   * Override the slot id derived for each provider. Defaults to the
   * provider's index in `slots`. Override when multiple processes
   * register the same key set under different array orderings — the id
   * is the cross-process correlation key, so it has to match.
   */
  slotId?: (provider: LLMProvider, index: number) => string;
}

interface SlotState {
  readonly id: string;
  readonly provider: LLMProvider;
  remainingTokens?: number;
  remainingRequests?: number;
  resetsAt?: number;
  /**
   * Bumped whenever the slot is selected for a call. Drives round-robin
   * fallback when no rate-limit hints are available.
   */
  selections: number;
}

const DEFAULT_EXHAUSTION_THRESHOLD = 1_000;

/**
 * Distribute load across N providers (typically multiple API keys for the
 * same model) and transparently failover on 429. Implements `LLMProvider`
 * so it drops in anywhere a single provider would.
 *
 * Selection: when slots report `rateLimitHint.remainingTokens`, picks the
 * one with the most headroom. Otherwise round-robins on a per-instance
 * counter. Exhausted slots (cooldown stored in `CapacityStore`) are
 * skipped until their `resetsAt` window passes; a shared store
 * propagates that exhaustion across replicas so a 429 hit by one process
 * doesn't have to be hit again by every other process.
 *
 * Failover: on 429 (classified by `classifyLLMError`), the slot is
 * marked exhausted in the store and the call retries against the next
 * available slot. When every slot is exhausted, the original error is
 * re-thrown so the caller sees an actionable failure instead of looping
 * forever.
 *
 * @example
 * ```ts
 * const llm = rotatingLLM([
 *   anthropic("claude-sonnet-4-6", { apiKey: keyA }),
 *   anthropic("claude-sonnet-4-6", { apiKey: keyB }),
 *   anthropic("claude-sonnet-4-6", { apiKey: keyC }),
 * ]);
 * ```
 */
export function rotatingLLM(slots: LLMProvider[], opts: RotatingLLMOptions = {}): LLMProvider {
  if (slots.length === 0) {
    throw new Error("rotatingLLM: at least one slot is required");
  }

  const capacityStore = opts.capacityStore ?? new InMemoryCapacityStore();
  const exhaustionThreshold = opts.exhaustionTokenThreshold ?? DEFAULT_EXHAUSTION_THRESHOLD;
  const strategy = opts.strategy ?? "least-remaining";
  const slotIdFn = opts.slotId ?? ((_, i) => `slot-${i}`);

  const slotStates: SlotState[] = slots.map((provider, i) => ({
    id: slotIdFn(provider, i),
    provider,
    selections: 0,
  }));

  // Round-robin cursor — used when no slot has rate-limit hints, or as a
  // tiebreaker when remainingTokens is unknown across all candidates.
  let rrCursor = 0;

  /**
   * Pick the next slot to call. Returns `null` when every slot is in
   * cooldown — caller surfaces the most recent error as the failure.
   * `excluded` lets callers loop through every slot exactly once during
   * a single failover cycle (a 429-ed slot shouldn't be re-selected on
   * the same call).
   */
  async function pickSlot(excluded: Set<string>): Promise<SlotState | null> {
    const exhausted = await capacityStore.getExhausted();
    const now = Date.now();
    const eligible = slotStates.filter((s) => {
      if (excluded.has(s.id)) return false;
      const cooldown = exhausted.get(s.id);
      if (cooldown !== undefined && cooldown > now) return false;
      // Local cooldown: the slot reported a near-exhaustion hint that
      // hasn't been written to the store yet (proactive skip lives only
      // here — if we couldn't write to the store we still want to honour
      // the hint locally).
      if (s.resetsAt !== undefined && s.resetsAt > now && hasNoBudget(s)) {
        return false;
      }
      return true;
    });
    if (eligible.length === 0) return null;

    if (strategy === "least-remaining") {
      const withHints = eligible.filter((s) => s.remainingTokens !== undefined);
      if (withHints.length > 0) {
        // Highest remainingTokens wins. When a hint is missing, the slot
        // didn't make it into withHints so it's already excluded —
        // pure-cold-start traffic keeps round-robining below until at
        // least one slot has a hint to compare against.
        return withHints.reduce((best, s) =>
          (s.remainingTokens ?? 0) > (best.remainingTokens ?? 0) ? s : best,
        );
      }
    }

    // Round-robin path: walk eligible from the cursor so we don't keep
    // hitting slot 0 when slot 1 is also free.
    const idx = rrCursor++ % eligible.length;
    return eligible[idx]!;
  }

  function applyHint(state: SlotState, hint: RateLimitHint | undefined): void {
    if (!hint) return;
    if (hint.remainingTokens !== undefined) state.remainingTokens = hint.remainingTokens;
    if (hint.remainingRequests !== undefined) state.remainingRequests = hint.remainingRequests;
    if (hint.resetsAt !== undefined) state.resetsAt = hint.resetsAt;
    // Proactive cooldown: cheap when threshold==0, otherwise mark before
    // the next 429 fires so peers in other processes also skip.
    if (
      exhaustionThreshold > 0 &&
      hint.remainingTokens !== undefined &&
      hint.remainingTokens < exhaustionThreshold &&
      hint.resetsAt !== undefined
    ) {
      void capacityStore.markExhausted(state.id, hint.resetsAt);
    }
  }

  /**
   * Parse `Retry-After` either from the error's enriched `headers` /
   * `retryAfter` properties (when the adapter exposes them) or from the
   * error message as a last resort. Returns a unix-ms timestamp by which
   * the caller should not retry. Falls back to `now + 60s` when nothing
   * is parseable so the slot still cools down briefly.
   */
  function deriveCooldownFromError(err: unknown): number {
    const now = Date.now();
    if (err && typeof err === "object") {
      const e = err as {
        retryAfter?: unknown;
        headers?: { get?: (k: string) => string | null } | Record<string, string>;
      };
      if (typeof e.retryAfter === "number" && Number.isFinite(e.retryAfter)) {
        return now + e.retryAfter * 1000;
      }
      if (e.headers) {
        const get = (k: string): string | null => {
          if (typeof (e.headers as { get?: unknown }).get === "function") {
            return (e.headers as { get: (k: string) => string | null }).get(k);
          }
          return ((e.headers as Record<string, string>)[k] ?? null) as string | null;
        };
        const ra = get("retry-after") ?? get("Retry-After");
        if (ra) {
          const n = Number.parseInt(ra, 10);
          if (Number.isFinite(n)) return now + n * 1000;
          const t = Date.parse(ra);
          if (Number.isFinite(t)) return t;
        }
      }
      const message = err instanceof Error ? err.message : "";
      const match = /retry[- ]after[: ]+(\d+)/i.exec(message);
      if (match?.[1]) return now + Number.parseInt(match[1], 10) * 1000;
    }
    return now + 60_000;
  }

  async function chat(params: LLMChatParams): Promise<LLMResponse> {
    const tried = new Set<string>();
    let lastErr: unknown;
    while (tried.size < slotStates.length) {
      const slot = await pickSlot(tried);
      if (!slot) break;
      tried.add(slot.id);
      slot.selections++;
      try {
        const response = await slot.provider.chat(params);
        applyHint(slot, response.rateLimitHint);
        return response;
      } catch (err) {
        lastErr = err;
        if (classifyLLMError(err) === "rate_limit") {
          await capacityStore.markExhausted(slot.id, deriveCooldownFromError(err));
          continue;
        }
        // Any other error class is the slot saying "this call won't
        // succeed regardless of slot" — surface it without burning the
        // remaining quota looking for a different answer.
        throw err;
      }
    }
    throw lastErr ?? new Error("rotatingLLM: all slots exhausted");
  }

  async function* providerStream(
    provider: LLMProvider,
    params: LLMChatParams,
  ): AsyncIterable<LLMStreamChunk> {
    if (provider.chatStream) {
      yield* provider.chatStream(params);
      return;
    }
    const response = await provider.chat(params);
    if (response.content) yield { delta: response.content };
    yield {
      delta: "",
      finishReason: response.finishReason,
      toolCalls: response.toolCalls,
      thinkingBlocks: response.thinkingBlocks,
      usage: response.usage,
      ...(response.rateLimitHint ? { rateLimitHint: response.rateLimitHint } : {}),
    };
  }

  async function* chatStream(params: LLMChatParams): AsyncIterable<LLMStreamChunk> {
    const tried = new Set<string>();
    let lastErr: unknown;
    while (tried.size < slotStates.length) {
      const slot = await pickSlot(tried);
      if (!slot) break;
      tried.add(slot.id);
      slot.selections++;
      // 429 mid-stream is rare in practice — providers usually fail
      // pre-stream when the rate limit is hit. We only fail over BEFORE
      // the first chunk; once the model started streaming, the caller
      // already saw partial output and a switch would interleave two
      // partial responses (same constraint as fallbackLLM). After-first-
      // chunk failures rethrow.
      let yielded = false;
      try {
        for await (const chunk of providerStream(slot.provider, params)) {
          yielded = true;
          if (chunk.finishReason && chunk.rateLimitHint) {
            applyHint(slot, chunk.rateLimitHint);
          }
          yield chunk;
        }
        return;
      } catch (err) {
        lastErr = err;
        if (yielded) throw err;
        if (classifyLLMError(err) === "rate_limit") {
          await capacityStore.markExhausted(slot.id, deriveCooldownFromError(err));
          continue;
        }
        throw err;
      }
    }
    throw lastErr ?? new Error("rotatingLLM: all slots exhausted");
  }

  return { chat, chatStream };
}

function hasNoBudget(state: SlotState): boolean {
  if (state.remainingTokens !== undefined && state.remainingTokens <= 0) return true;
  if (state.remainingRequests !== undefined && state.remainingRequests <= 0) return true;
  return false;
}
