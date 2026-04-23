import type { Message, ToolCall, ThinkingBlock } from "./message.ts";

export interface LLMToolDefinition {
  name: string;
  description: string;
  parameters: Record<string, unknown>;
}

export interface LLMChatParams {
  messages: Message[];
  tools?: LLMToolDefinition[];
  temperature?: number;
  maxTokens?: number;
  signal?: AbortSignal;
  /**
   * When set, enables extended thinking. The value is the token budget for the
   * thinking phase. Requires a model that supports extended thinking (Claude 3.7+).
   * Forces temperature=1 in the Anthropic adapter.
   */
  thinkingBudgetTokens?: number;
}

export interface LLMUsage {
  inputTokens: number;
  outputTokens: number;
  cacheReadTokens?: number;
  cacheWriteTokens?: number;
}

export type LLMFinishReason = "stop" | "tool_calls" | "length" | "error";

export interface LLMResponse {
  content: string | null;
  toolCalls?: ToolCall[];
  finishReason: LLMFinishReason;
  usage?: LLMUsage;
  /** Thinking blocks produced by extended thinking, if enabled. */
  thinkingBlocks?: ThinkingBlock[];
}

export interface LLMStreamChunk {
  /** Partial text delta. Empty on the final chunk or when the step is a pure tool call. */
  delta: string;
  /** Partial thinking delta. Only present on chunks that carry extended-thinking text. */
  thinkingDelta?: string;
  /** Assembled tool calls. Only present on the final chunk. */
  toolCalls?: ToolCall[];
  /** Complete thinking blocks. Only present on the final chunk when extended thinking was used. */
  thinkingBlocks?: ThinkingBlock[];
  /** Set on the final chunk. */
  finishReason?: LLMFinishReason;
  usage?: LLMUsage;
}

export interface LLMProvider {
  chat(params: LLMChatParams): Promise<LLMResponse>;
  /**
   * Optional streaming variant. When present, agentLoop.stream() uses it to push
   * token deltas to the caller in real time. Falls back to chat() when absent.
   * Yields text deltas followed by a final chunk carrying finishReason + toolCalls.
   */
  chatStream?(params: LLMChatParams): AsyncIterable<LLMStreamChunk>;
}
