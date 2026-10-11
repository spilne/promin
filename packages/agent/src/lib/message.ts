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
  /**
   * Set by the adapter when the model's tool arguments could not be parsed
   * (malformed or truncated JSON); `input` is then `{}`. The tool is not
   * run: the agent returns this message to the model as the tool result.
   */
  inputError?: string;
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
  /**
   * `true` when `content` reports a failure instead of the tool's output:
   * invalid input, a thrown error, a timeout, a cancellation, a rejected
   * approval, an unknown tool or an outcome lost to a crash. Adapters that
   * support it forward the flag to the model (Anthropic `is_error`);
   * consumers branch on it instead of sniffing the content text.
   */
  isError?: boolean;
  /**
   * Structured side-channel data, persisted with the message but NOT
   * shown to the LLM (only `content` reaches the model). A tool emits it
   * via `AgentTool.toResultMetadata`. Used by `callAgent` to carry the
   * sub-agent run's trace for the cross-thread graph view.
   */
  metadata?: Readonly<Record<string, unknown>>;
}

export type Message = SystemMessage | UserMessage | AssistantMessage | ToolResultMessage;
