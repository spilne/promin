import type { Message, ToolCall } from "./message.ts";

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
}

export interface LLMUsage {
  inputTokens: number;
  outputTokens: number;
}

export type LLMFinishReason = "stop" | "tool_calls" | "length" | "error";

export interface LLMResponse {
  content: string | null;
  toolCalls?: ToolCall[];
  finishReason: LLMFinishReason;
  usage?: LLMUsage;
}

export interface LLMStreamChunk {
  /** Partial text delta. Empty on the final chunk or when the step is a pure tool call. */
  delta: string;
  /** Assembled tool calls. Only present on the final chunk. */
  toolCalls?: ToolCall[];
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
