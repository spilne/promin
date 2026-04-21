import type { LLMProvider, LLMChatParams, LLMResponse } from "../llm-provider.ts";
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
  usage: { input_tokens: number; output_tokens: number };
}

interface AnthropicMessage {
  role: "user" | "assistant";
  content: string | AnthropicContentBlock[];
}

export interface AnthropicOptions {
  apiKey?: string;
  baseUrl?: string;
  maxTokens?: number;
  defaultHeaders?: Record<string, string>;
}

export function anthropic(model: string, options: AnthropicOptions = {}): LLMProvider {
  const apiKey = options.apiKey ?? process.env["ANTHROPIC_API_KEY"];
  const baseUrl = options.baseUrl ?? "https://api.anthropic.com";

  return {
    async chat(params: LLMChatParams): Promise<LLMResponse> {
      const system = extractSystem(params.messages);
      const messages = toAnthropicMessages(params.messages);

      const body: Record<string, unknown> = {
        model,
        max_tokens: params.maxTokens ?? options.maxTokens ?? 4096,
        messages,
        ...(system ? { system } : {}),
        ...(params.temperature !== undefined ? { temperature: params.temperature } : {}),
        ...(params.tools && params.tools.length > 0
          ? {
              tools: params.tools.map((t) => ({
                name: t.name,
                description: t.description,
                input_schema: t.parameters,
              })),
            }
          : {}),
      };

      const resp = await fetch(`${baseUrl}/v1/messages`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "x-api-key": apiKey ?? "",
          "anthropic-version": "2023-06-01",
          ...options.defaultHeaders,
        },
        body: JSON.stringify(body),
        signal: params.signal,
      });

      if (!resp.ok) {
        const text = await resp.text();
        throw new Error(`Anthropic API error ${resp.status}: ${text}`);
      }

      const data = (await resp.json()) as AnthropicResponse;
      return parseAnthropicResponse(data);
    },
  };
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
    },
  };
}
