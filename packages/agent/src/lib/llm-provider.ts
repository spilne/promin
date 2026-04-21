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

export interface LLMProvider {
  chat(params: LLMChatParams): Promise<LLMResponse>;
}
