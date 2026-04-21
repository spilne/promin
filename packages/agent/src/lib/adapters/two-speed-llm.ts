import type { LLMProvider, LLMChatParams, LLMResponse, LLMStreamChunk } from "../llm-provider.ts";

export interface TwoSpeedLLMConfig {
  fast: LLMProvider;
  capable: LLMProvider;
}

function selectProvider(config: TwoSpeedLLMConfig, params: LLMChatParams): LLMProvider {
  const messages = params.messages;
  for (let i = messages.length - 1; i >= 0; i--) {
    const msg = messages[i];
    if (msg.role !== "system") {
      return msg.role === "tool" ? config.fast : config.capable;
    }
  }
  return config.capable;
}

async function* chatViaChat(provider: LLMProvider, params: LLMChatParams): AsyncIterable<LLMStreamChunk> {
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

export function twoSpeedLLM(config: TwoSpeedLLMConfig): LLMProvider {
  return {
    chat(params: LLMChatParams): Promise<LLMResponse> {
      return selectProvider(config, params).chat(params);
    },

    chatStream(params: LLMChatParams): AsyncIterable<LLMStreamChunk> {
      const provider = selectProvider(config, params);
      if (provider.chatStream) {
        return provider.chatStream(params);
      }
      return chatViaChat(provider, params);
    },
  };
}
