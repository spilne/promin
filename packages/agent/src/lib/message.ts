export interface SystemMessage {
  role: "system";
  content: string;
}

export interface UserMessage {
  role: "user";
  content: string;
  /** Serialized source info stamped by the agent runtime for non-interactive triggers. */
  metadata?: Readonly<Record<string, unknown>>;
}

export interface ToolCall {
  id: string;
  name: string;
  input: unknown;
}

export interface ThinkingBlock {
  thinking: string;
  /** Anthropic-issued signature required to replay thinking in subsequent turns. */
  signature: string;
}

export interface AssistantMessage {
  role: "assistant";
  content: string | null;
  toolCalls?: ToolCall[];
  /** Extended-thinking blocks emitted before the text response. Must be replayed verbatim on subsequent turns. */
  thinkingBlocks?: ThinkingBlock[];
}

export interface ToolResultMessage {
  role: "tool";
  toolCallId: string;
  content: string;
}

export type Message = SystemMessage | UserMessage | AssistantMessage | ToolResultMessage;
