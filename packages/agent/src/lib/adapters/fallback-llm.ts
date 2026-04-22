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
