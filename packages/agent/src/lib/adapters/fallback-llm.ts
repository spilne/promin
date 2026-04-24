import type { LLMProvider, LLMChatParams, LLMResponse, LLMStreamChunk } from "../llm-provider.ts";

async function* providerStream(
  provider: LLMProvider,
  params: LLMChatParams,
): AsyncIterable<LLMStreamChunk> {
  if (provider.chatStream) {
    yield* provider.chatStream(params);
  } else {
    const response = await provider.chat(params);
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
}

/**
 * Try each provider in order, returning the first successful response.
 *
 * On `chat()`, failures are caught and the next provider is tried cleanly.
 * On `chatStream()`, if a provider throws mid-stream, already-yielded chunks are
 * lost and the next provider restarts from the beginning — callers that cannot
 * tolerate duplicate output should prefer `chat()`.
 *
 * @example
 * ```ts
 * const llm = fallbackLLM([primaryProvider, backupProvider]);
 * ```
 */
export function fallbackLLM(providers: LLMProvider[]): LLMProvider {
  return {
    async chat(params: LLMChatParams): Promise<LLMResponse> {
      const errors: string[] = [];
      for (const provider of providers) {
        try {
          return await provider.chat(params);
        } catch (err) {
          errors.push(err instanceof Error ? err.message : String(err));
        }
      }
      throw new Error(`All providers failed: ${errors.join("; ")}`);
    },

    async *chatStream(params: LLMChatParams): AsyncIterable<LLMStreamChunk> {
      // NOTE: If a provider starts yielding chunks and then throws mid-stream,
      // the caller has already received those chunks. Generators cannot un-yield,
      // so the caller sees a partial stream. The next provider (if any) will then
      // stream from the beginning, meaning the caller receives two interleaved
      // partial responses. Callers that cannot tolerate this should use chat()
      // instead, which retries cleanly on the full response.
      const errors: string[] = [];
      for (const provider of providers) {
        try {
          yield* providerStream(provider, params);
          return;
        } catch (err) {
          errors.push(err instanceof Error ? err.message : String(err));
        }
      }
      throw new Error(`All providers failed: ${errors.join("; ")}`);
    },
  };
}
