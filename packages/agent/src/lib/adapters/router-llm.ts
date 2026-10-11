import type { LLMProvider, LLMChatParams, LLMResponse, LLMStreamChunk } from "../llm-provider.ts";
import { streamChat } from "./stream-chat.ts";

export interface LLMRoute {
  when: (params: LLMChatParams) => boolean;
  use: LLMProvider;
}

function resolve(routes: ReadonlyArray<LLMRoute>, params: LLMChatParams): LLMProvider {
  for (const route of routes) {
    if (route.when(params)) return route.use;
  }
  return routes[routes.length - 1]!.use;
}

/**
 * Route each LLM call to the first `LLMRoute` whose `when` predicate returns true.
 * Falls back to the last route's provider if none match — make the last route a
 * catch-all (e.g. `when: () => true`) to guarantee a match.
 *
 * @example
 * ```ts
 * const llm = routerLLM([
 *   { when: (p) => (p.messages.length ?? 0) > 50, use: claude },  // long context → Claude
 *   { when: () => true,                            use: gpt4 },    // default → GPT-4
 * ]);
 * ```
 */
export function routerLLM(routes: ReadonlyArray<LLMRoute>): LLMProvider {
  return {
    chat(params: LLMChatParams): Promise<LLMResponse> {
      return resolve(routes, params).chat(params);
    },

    chatStream(params: LLMChatParams): AsyncIterable<LLMStreamChunk> {
      return streamChat({ provider: resolve(routes, params), params });
    },
  };
}

/**
 * `true` when the last non-system message is a tool result — the step is
 * about to synthesize what a tool just returned, typically a short,
 * predictable reply. Steps that precede a tool call involve open-ended
 * reasoning instead.
 */
export function isToolResultStep(params: LLMChatParams): boolean {
  for (let i = params.messages.length - 1; i >= 0; i--) {
    const msg = params.messages[i]!;
    if (msg.role !== "system") return msg.role === "tool";
  }
  return false;
}

export interface TwoSpeedLLMConfig {
  fast: LLMProvider;
  capable: LLMProvider;
  /**
   * Return `true` to route the call to `fast`, `false` for `capable`.
   * Default: {@link isToolResultStep}.
   */
  when?: (params: LLMChatParams) => boolean;
}

/**
 * Cost-optimized {@link routerLLM} preset: `fast` for synthesis steps
 * (by default, the step right after a tool result), `capable` for
 * everything else. Equivalent to
 * `routerLLM([{ when: isToolResultStep, use: fast }, { when: () => true, use: capable }])`.
 *
 * @example
 * ```ts
 * const llm = twoSpeedLLM({
 *   capable: anthropic("claude-opus-4-7", { apiKey }),
 *   fast:    anthropic("claude-haiku-4-5-20251001", { apiKey }),
 * });
 * ```
 */
export function twoSpeedLLM(config: TwoSpeedLLMConfig): LLMProvider {
  return routerLLM([
    { when: config.when ?? isToolResultStep, use: config.fast },
    { when: () => true, use: config.capable },
  ]);
}
