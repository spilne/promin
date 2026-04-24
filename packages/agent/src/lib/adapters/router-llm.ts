import type { LLMProvider, LLMChatParams, LLMResponse, LLMStreamChunk } from "../llm-provider.ts";

export interface LLMRoute {
  when: (params: LLMChatParams) => boolean;
  use: LLMProvider;
}

function resolve(routes: LLMRoute[], params: LLMChatParams): LLMProvider {
  for (const route of routes) {
    if (route.when(params)) return route.use;
  }
  return routes[routes.length - 1].use;
}

async function* responseToStream(response: LLMResponse): AsyncIterable<LLMStreamChunk> {
  if (response.content) {
    yield { delta: response.content };
  }
  yield {
    delta: "",
    finishReason: response.finishReason,
    toolCalls: response.toolCalls,
    usage: response.usage,
  };
}

async function* chatViaChat(
  provider: LLMProvider,
  params: LLMChatParams,
): AsyncIterable<LLMStreamChunk> {
  const response = await provider.chat(params);
  yield* responseToStream(response);
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
export function routerLLM(routes: LLMRoute[]): LLMProvider {
  return {
    chat(params: LLMChatParams): Promise<LLMResponse> {
      return resolve(routes, params).chat(params);
    },

    chatStream(params: LLMChatParams): AsyncIterable<LLMStreamChunk> {
      const provider = resolve(routes, params);
      if (provider.chatStream) {
        return provider.chatStream(params);
      }
      return chatViaChat(provider, params);
    },
  };
}
