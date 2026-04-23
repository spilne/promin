import type { RateLimiter } from "@promin/core";
import type { AgentTool } from "./tool.ts";
import type { ToolCall, ToolResultMessage, Message } from "./message.ts";
import type {
  LLMProvider,
  LLMChatParams,
  LLMResponse,
  LLMFinishReason,
  LLMUsage,
  LLMToolDefinition,
} from "./llm-provider.ts";
import type { ThinkingBlock } from "./message.ts";
import type { ProcessorsConfig, ProcessorContext } from "./processors.ts";

export function formatToolError(err: unknown): string {
  if (
    err instanceof Error &&
    "issues" in err &&
    Array.isArray((err as { issues: unknown }).issues)
  ) {
    const issues = (err as { issues: Array<{ path: unknown[]; message: string }> }).issues;
    return issues
      .map((i) => `${i.path.length ? i.path.join(".") : "(root)"}: ${i.message}`)
      .join("; ");
  }
  return err instanceof Error ? err.message : String(err);
}

// Hard cap on any single tool result — prevents a runaway listing or read
// from blowing up the LLM context window.
const MAX_TOOL_CONTENT_CHARS = 200_000;

// biome-ignore lint/suspicious/noExplicitAny: tool registry uses runtime Zod validation
export async function executeToolCall(
  call: ToolCall,
  toolDef: AgentTool<any, any>,
  onResult?: (call: ToolCall, output: unknown) => void,
): Promise<ToolResultMessage> {
  let parsed: unknown;
  try {
    parsed = toolDef.parameters.parse(call.input);
  } catch (err) {
    const received = JSON.stringify(call.input ?? null);
    return {
      role: "tool",
      toolCallId: call.id,
      content: `Invalid input: ${formatToolError(err)}\nReceived: ${received}`,
    };
  }
  try {
    // biome-ignore lint/suspicious/noExplicitAny: Zod validates input at runtime
    const output = await toolDef.execute(parsed as any);
    onResult?.(call, output);
    let content = toolDef.toModelOutput
      ? toolDef.toModelOutput(output)
      : typeof output === "string"
        ? output
        : JSON.stringify(output);
    if (content.length > MAX_TOOL_CONTENT_CHARS) {
      content =
        content.slice(0, MAX_TOOL_CONTENT_CHARS) +
        `\n\n[Output truncated: ${content.length.toLocaleString()} chars total. Use a more specific query to avoid truncation.]`;
    }
    return { role: "tool", toolCallId: call.id, content };
  } catch (err) {
    return {
      role: "tool",
      toolCallId: call.id,
      content: `Tool execution failed: ${formatToolError(err)}`,
    };
  }
}

// ---- shared LLM call helper ----

export interface RunLlmCallParams {
  llm: LLMProvider;
  messages: Message[];
  tools?: LLMToolDefinition[];
  rateLimiter?: RateLimiter;
  processors?: ProcessorsConfig;
  processorCtx: ProcessorContext;
  /** Called with each text delta when the LLM streams. When omitted, falls back to non-streaming. */
  onChunk?: (delta: string) => void;
  /** Called with each thinking delta when the LLM streams extended thinking. */
  onThinking?: (delta: string) => void;
  /** Token budget for extended thinking. Forwarded to the LLM as-is. */
  thinkingBudgetTokens?: number;
  signal?: AbortSignal;
}

/**
 * Execute a single LLM call with optional streaming, rate limiting, and processor pipeline.
 * Handles the beforeLLM → (streaming|non-streaming) → afterLLM lifecycle.
 * Not journaled — callers wrap this in ctx.activity.
 */
export async function runLlmCall({
  llm,
  messages,
  tools,
  rateLimiter,
  processors,
  processorCtx,
  onChunk,
  onThinking,
  thinkingBudgetTokens,
  signal,
}: RunLlmCallParams): Promise<LLMResponse> {
  const processedMessages = processors?.beforeLLM
    ? await processors.beforeLLM(messages, processorCtx)
    : messages;

  const chatParams: LLMChatParams = {
    messages: processedMessages,
    tools,
    signal,
    thinkingBudgetTokens,
  };

  let raw: LLMResponse;
  try {
    if (onChunk && llm.chatStream) {
      const startStream = () => Promise.resolve(llm.chatStream!(chatParams));
      const stream = await (rateLimiter ? rateLimiter.withLimitAsync(startStream) : startStream());
      let content = "";
      let finishReason: LLMFinishReason = "stop";
      let usage: LLMUsage | undefined;
      const toolCalls: ToolCall[] = [];
      const thinkingBlocks: ThinkingBlock[] = [];
      for await (const chunk of stream) {
        if (chunk.thinkingDelta) {
          onThinking?.(chunk.thinkingDelta);
        }
        if (chunk.delta) {
          content += chunk.delta;
          onChunk(chunk.delta);
        }
        if (chunk.thinkingBlocks) thinkingBlocks.push(...chunk.thinkingBlocks);
        if (chunk.toolCalls) toolCalls.push(...chunk.toolCalls);
        if (chunk.finishReason) finishReason = chunk.finishReason;
        if (chunk.usage) usage = chunk.usage;
      }
      raw = {
        content: content || null,
        toolCalls: toolCalls.length > 0 ? toolCalls : undefined,
        finishReason,
        usage,
        thinkingBlocks: thinkingBlocks.length > 0 ? thinkingBlocks : undefined,
      };
    } else {
      const call = () => llm.chat(chatParams);
      raw = await (rateLimiter ? rateLimiter.withLimitAsync(call) : call());
      // Push full content so stream() callers get something even when chatStream is unavailable.
      if (onChunk && raw.content) onChunk(raw.content);
    }
  } catch (err) {
    // Treat abort as a clean stop so conversation history stays consistent.
    if (signal?.aborted || (err instanceof Error && err.name === "AbortError")) {
      return { content: null, toolCalls: undefined, finishReason: "stop" };
    }
    throw err;
  }

  return processors?.afterLLM ? await processors.afterLLM(raw, processorCtx) : raw;
}
