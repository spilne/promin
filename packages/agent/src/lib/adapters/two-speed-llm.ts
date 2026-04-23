import type { LLMProvider, LLMChatParams, LLMResponse, LLMStreamChunk } from "../llm-provider.ts";

export interface TwoSpeedLLMConfig {
  fast: LLMProvider;
  capable: LLMProvider;
  /**
   * Override the default routing heuristic. Return `true` to route to `fast`,
   * `false` to route to `capable`.
   *
   * Default heuristic: route to `fast` when the last non-system message is a
   * tool result. The reasoning is that the model is about to synthesize what it
   * just read — typically a short, predictable reply — so the fast model is
   * sufficient. Steps that precede a tool call involve open-ended reasoning and
   * go to `capable`.
   */
  when?: (params: LLMChatParams) => boolean;
}

function selectProvider(config: TwoSpeedLLMConfig, params: LLMChatParams): LLMProvider {
  if (config.when) return config.when(params) ? config.fast : config.capable;
  // Default: last non-system message is a tool result → fast model.
  // Tool-result steps are typically short synthesis, not open-ended reasoning.
  const messages = params.messages;
  for (let i = messages.length - 1; i >= 0; i--) {
    const msg = messages[i];
    if (msg.role !== "system") {
      return msg.role === "tool" ? config.fast : config.capable;
    }
  }
  return config.capable;
}

async function* chatViaChat(
  provider: LLMProvider,
  params: LLMChatParams,
): AsyncIterable<LLMStreamChunk> {
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
