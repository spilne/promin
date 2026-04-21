import type { LLMProvider, LLMChatParams, LLMResponse } from "../llm-provider.ts";
import type { Message, ToolCall } from "../message.ts";

interface OpenAIMessage {
  role: "system" | "user" | "assistant" | "tool";
  content?: string | null;
  tool_calls?: OpenAIToolCall[];
  tool_call_id?: string;
}

interface OpenAIToolCall {
  id: string;
  type: "function";
  function: { name: string; arguments: string };
}

interface OpenAIResponse {
  choices: Array<{
    message: {
      content: string | null;
      tool_calls?: OpenAIToolCall[];
    };
    finish_reason: string;
  }>;
  usage: { prompt_tokens: number; completion_tokens: number };
}

export interface OpenAIOptions {
  apiKey?: string;
  baseUrl?: string;
  defaultHeaders?: Record<string, string>;
}

export function openai(model: string, options: OpenAIOptions = {}): LLMProvider {
  const apiKey = options.apiKey ?? process.env["OPENAI_API_KEY"];
  const baseUrl = options.baseUrl ?? "https://api.openai.com";

  return {
    async chat(params: LLMChatParams): Promise<LLMResponse> {
      const messages = toOpenAIMessages(params.messages);

      const body: Record<string, unknown> = {
        model,
        messages,
        ...(params.temperature !== undefined ? { temperature: params.temperature } : {}),
        ...(params.maxTokens !== undefined ? { max_tokens: params.maxTokens } : {}),
        ...(params.tools && params.tools.length > 0
          ? {
              tools: params.tools.map((t) => ({
                type: "function",
                function: {
                  name: t.name,
                  description: t.description,
                  parameters: t.parameters,
                },
              })),
              tool_choice: "auto",
            }
          : {}),
      };

      const resp = await fetch(`${baseUrl}/v1/chat/completions`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${apiKey ?? ""}`,
          ...options.defaultHeaders,
        },
        body: JSON.stringify(body),
        signal: params.signal,
      });

      if (!resp.ok) {
        const text = await resp.text();
        throw new Error(`OpenAI API error ${resp.status}: ${text}`);
      }

      const data = (await resp.json()) as OpenAIResponse;
      return parseOpenAIResponse(data);
    },
  };
}

function toOpenAIMessages(messages: Message[]): OpenAIMessage[] {
  return messages.map((msg) => {
    if (msg.role === "system") return { role: "system", content: msg.content };
    if (msg.role === "user") return { role: "user", content: msg.content };
    if (msg.role === "tool") {
      return { role: "tool", tool_call_id: msg.toolCallId, content: msg.content };
    }
    const openaiMsg: OpenAIMessage = {
      role: "assistant",
      content: msg.content,
    };
    if (msg.toolCalls && msg.toolCalls.length > 0) {
      openaiMsg.tool_calls = msg.toolCalls.map((tc) => ({
        id: tc.id,
        type: "function",
        function: { name: tc.name, arguments: JSON.stringify(tc.input) },
      }));
    }
    return openaiMsg;
  });
}

function parseOpenAIResponse(data: OpenAIResponse): LLMResponse {
  const choice = data.choices[0];
  if (!choice) throw new Error("OpenAI returned no choices");

  const toolCalls: ToolCall[] = (choice.message.tool_calls ?? []).map((tc) => ({
    id: tc.id,
    name: tc.function.name,
    input: JSON.parse(tc.function.arguments) as unknown,
  }));

  const finishReason =
    choice.finish_reason === "stop"
      ? "stop"
      : choice.finish_reason === "tool_calls"
        ? "tool_calls"
        : choice.finish_reason === "length"
          ? "length"
          : "stop";

  return {
    content: choice.message.content,
    toolCalls: toolCalls.length > 0 ? toolCalls : undefined,
    finishReason,
    usage: {
      inputTokens: data.usage.prompt_tokens,
      outputTokens: data.usage.completion_tokens,
    },
  };
}
