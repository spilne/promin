import type {
  LLMProvider,
  LLMChatParams,
  LLMResponse,
  LLMStreamChunk,
  LLMFinishReason,
} from "../llm-provider.ts";
import type { Message, ToolCall } from "../message.ts";

interface AnthropicContentBlock {
  type: string;
  text?: string;
  id?: string;
  tool_use_id?: string;
  name?: string;
  input?: unknown;
  content?: string;
}

interface AnthropicResponse {
  content: AnthropicContentBlock[];
  stop_reason: string;
  usage: {
    input_tokens: number;
    output_tokens: number;
    cache_read_input_tokens?: number;
    cache_creation_input_tokens?: number;
  };
}

interface AnthropicMessage {
  role: "user" | "assistant";
  content: string | AnthropicContentBlock[];
}

// SSE event types for streaming
interface SSEMessageStart {
  type: "message_start";
  message: {
    usage: {
      input_tokens: number;
      cache_read_input_tokens?: number;
      cache_creation_input_tokens?: number;
    };
  };
}
interface SSEContentBlockStart {
  type: "content_block_start";
  index: number;
  content_block: { type: "text" } | { type: "tool_use"; id: string; name: string };
}
interface SSEContentBlockDelta {
  type: "content_block_delta";
  index: number;
  delta: { type: "text_delta"; text: string } | { type: "input_json_delta"; partial_json: string };
}
interface SSEMessageDelta {
  type: "message_delta";
  delta: { stop_reason: string };
  usage: { output_tokens: number };
}
type SSEEvent =
  | SSEMessageStart
  | SSEContentBlockStart
  | SSEContentBlockDelta
  | SSEMessageDelta
  | { type: string };

export interface AnthropicOptions {
  apiKey?: string;
  baseUrl?: string;
  maxTokens?: number;
  defaultHeaders?: Record<string, string>;
}

export function anthropic(model: string, options: AnthropicOptions = {}): LLMProvider {
  const apiKey = options.apiKey ?? process.env["ANTHROPIC_API_KEY"];
  const baseUrl = options.baseUrl ?? "https://api.anthropic.com";

  function buildBody(params: LLMChatParams, stream?: boolean): Record<string, unknown> {
    const systemText = extractSystem(params.messages);
    return {
      model,
      max_tokens: params.maxTokens ?? options.maxTokens ?? 4096,
      messages: toAnthropicMessages(params.messages),
      // Cache the system prompt — same every turn, saves re-processing on each call.
      ...(systemText
        ? { system: [{ type: "text", text: systemText, cache_control: { type: "ephemeral" } }] }
        : {}),
      ...(params.temperature !== undefined ? { temperature: params.temperature } : {}),
      ...(params.tools && params.tools.length > 0
        ? {
            tools: params.tools.map((t, i) => ({
              name: t.name,
              description: t.description,
              input_schema: t.parameters,
              // Cache after the last tool so the entire system+tools prefix is cached.
              ...(i === (params.tools?.length ?? 0) - 1
                ? { cache_control: { type: "ephemeral" } }
                : {}),
            })),
          }
        : {}),
      ...(stream ? { stream: true } : {}),
    };
  }

  function headers(): Record<string, string> {
    return {
      "content-type": "application/json",
      "x-api-key": apiKey ?? "",
      "anthropic-version": "2023-06-01",
      "anthropic-beta": "prompt-caching-2024-07-31",
      ...options.defaultHeaders,
    };
  }

  return {
    async chat(params: LLMChatParams): Promise<LLMResponse> {
      const resp = await fetch(`${baseUrl}/v1/messages`, {
        method: "POST",
        headers: headers(),
        body: JSON.stringify(buildBody(params)),
        signal: params.signal,
      });

      if (!resp.ok) {
        const text = await resp.text();
        throw new Error(`Anthropic API error ${resp.status}: ${text}`);
      }

      const data = (await resp.json()) as AnthropicResponse;
      return parseAnthropicResponse(data);
    },

    async *chatStream(params: LLMChatParams): AsyncIterable<LLMStreamChunk> {
      const resp = await fetch(`${baseUrl}/v1/messages`, {
        method: "POST",
        headers: headers(),
        body: JSON.stringify(buildBody(params, true)),
        signal: params.signal,
      });

      if (!resp.ok) {
        const text = await resp.text();
        throw new Error(`Anthropic API error ${resp.status}: ${text}`);
      }

      // Accumulate per-block state keyed by block index
      const textBlocks = new Map<number, string>();
      const toolBlocks = new Map<number, { id: string; name: string; inputJson: string }>();
      let inputTokens = 0;
      let outputTokens = 0;
      let cacheReadTokens = 0;
      let cacheWriteTokens = 0;
      let finishReason: LLMFinishReason = "stop";

      for await (const event of parseSSE(resp.body!)) {
        if (event.type === "message_start") {
          const u = (event as SSEMessageStart).message.usage;
          inputTokens = u.input_tokens;
          cacheReadTokens = u.cache_read_input_tokens ?? 0;
          cacheWriteTokens = u.cache_creation_input_tokens ?? 0;
        } else if (event.type === "content_block_start") {
          const e = event as SSEContentBlockStart;
          if (e.content_block.type === "text") {
            textBlocks.set(e.index, "");
          } else if (e.content_block.type === "tool_use") {
            toolBlocks.set(e.index, {
              id: e.content_block.id,
              name: e.content_block.name,
              inputJson: "",
            });
          }
        } else if (event.type === "content_block_delta") {
          const e = event as SSEContentBlockDelta;
          if (e.delta.type === "text_delta") {
            textBlocks.set(e.index, (textBlocks.get(e.index) ?? "") + e.delta.text);
            yield { delta: e.delta.text };
          } else if (e.delta.type === "input_json_delta") {
            const block = toolBlocks.get(e.index);
            if (block) block.inputJson += e.delta.partial_json;
          }
        } else if (event.type === "message_delta") {
          const e = event as SSEMessageDelta;
          outputTokens = e.usage.output_tokens;
          finishReason =
            e.delta.stop_reason === "end_turn"
              ? "stop"
              : e.delta.stop_reason === "tool_use"
                ? "tool_calls"
                : e.delta.stop_reason === "max_tokens"
                  ? "length"
                  : "stop";
        }
      }

      const toolCalls: ToolCall[] = [...toolBlocks.values()].map(({ id, name, inputJson }) => {
        let input: unknown;
        try {
          input = JSON.parse(inputJson || "{}");
        } catch {
          // Truncated stream (e.g. max_tokens hit mid-JSON) — fall back to empty object so
          // Zod validation in executeToolCall returns a clean "Invalid input" to the agent
          // rather than crashing the workflow with a permanent journal failure.
          input = {};
        }
        return { id, name, input };
      });

      yield {
        delta: "",
        finishReason,
        toolCalls: toolCalls.length > 0 ? toolCalls : undefined,
        usage: { inputTokens, outputTokens, cacheReadTokens, cacheWriteTokens },
      };
    },
  };
}

async function* parseSSE(body: ReadableStream<Uint8Array>): AsyncIterable<SSEEvent> {
  const reader = body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      const parts = buffer.split("\n\n");
      buffer = parts.pop() ?? "";
      for (const part of parts) {
        for (const line of part.split("\n")) {
          if (line.startsWith("data: ")) {
            const data = line.slice(6);
            if (data === "[DONE]") return;
            try {
              yield JSON.parse(data) as SSEEvent;
            } catch {}
          }
        }
      }
    }
  } finally {
    reader.releaseLock();
  }
}

function extractSystem(messages: Message[]): string | undefined {
  const sys = messages.find((m) => m.role === "system");
  return sys ? sys.content : undefined;
}

function toAnthropicMessages(messages: Message[]): AnthropicMessage[] {
  const result: AnthropicMessage[] = [];

  for (const msg of messages) {
    if (msg.role === "system") continue;

    if (msg.role === "user") {
      result.push({ role: "user", content: msg.content });
      continue;
    }

    if (msg.role === "assistant") {
      const content: AnthropicContentBlock[] = [];
      if (msg.content) content.push({ type: "text", text: msg.content });
      for (const tc of msg.toolCalls ?? []) {
        content.push({ type: "tool_use", id: tc.id, name: tc.name, input: tc.input });
      }
      result.push({ role: "assistant", content });
      continue;
    }

    if (msg.role === "tool") {
      const last = result[result.length - 1];
      const toolResult = {
        type: "tool_result",
        tool_use_id: msg.toolCallId,
        content: msg.content,
      };

      if (last?.role === "user" && Array.isArray(last.content)) {
        (last.content as AnthropicContentBlock[]).push(toolResult);
      } else {
        result.push({ role: "user", content: [toolResult] });
      }
    }
  }

  return result;
}

function parseAnthropicResponse(data: AnthropicResponse): LLMResponse {
  let content: string | null = null;
  const toolCalls: ToolCall[] = [];

  for (const block of data.content) {
    if (block.type === "text" && block.text) {
      content = block.text;
    } else if (block.type === "tool_use" && block.id && block.name) {
      toolCalls.push({ id: block.id, name: block.name, input: block.input });
    }
  }

  const finishReason =
    data.stop_reason === "end_turn"
      ? "stop"
      : data.stop_reason === "tool_use"
        ? "tool_calls"
        : data.stop_reason === "max_tokens"
          ? "length"
          : "stop";

  return {
    content,
    toolCalls: toolCalls.length > 0 ? toolCalls : undefined,
    finishReason,
    usage: {
      inputTokens: data.usage.input_tokens,
      outputTokens: data.usage.output_tokens,
      cacheReadTokens: data.usage.cache_read_input_tokens ?? 0,
      cacheWriteTokens: data.usage.cache_creation_input_tokens ?? 0,
    },
  };
}
