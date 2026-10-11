import type { LLMChatParams, LLMProvider, LLMResponse, LLMStreamChunk } from "../llm-provider.ts";

/**
 * Replay a complete `chat()` response as a stream: one delta with the
 * text (when there is any), then the final chunk carrying everything else
 * the response had — finish reason, tool calls, thinking blocks, usage and
 * rate-limit hint.
 */
export async function* responseToStream(response: LLMResponse): AsyncIterable<LLMStreamChunk> {
  if (response.content) yield { delta: response.content };
  yield {
    delta: "",
    finishReason: response.finishReason,
    ...(response.toolCalls && { toolCalls: response.toolCalls }),
    ...(response.thinkingBlocks && { thinkingBlocks: response.thinkingBlocks }),
    ...(response.usage && { usage: response.usage }),
    ...(response.rateLimitHint && { rateLimitHint: response.rateLimitHint }),
  };
}

/**
 * Stream a call through `provider`: its own `chatStream` when it has one,
 * otherwise its `chat()` response replayed by {@link responseToStream}.
 * Lets provider wrappers stream over providers that only implement `chat`.
 */
export async function* streamChat(args: {
  provider: LLMProvider;
  params: LLMChatParams;
}): AsyncIterable<LLMStreamChunk> {
  const { provider, params } = args;
  if (provider.chatStream) {
    yield* provider.chatStream(params);
    return;
  }
  yield* responseToStream(await provider.chat(params));
}
