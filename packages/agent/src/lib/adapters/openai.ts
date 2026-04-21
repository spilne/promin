import type { LLMProvider, LLMChatParams, LLMResponse, LLMStreamChunk, LLMFinishReason } from "../llm-provider.ts";
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

  function buildBody(params: LLMChatParams, stream?: boolean): Record<string, unknown> {
    return {
      model,
      messages: toOpenAIMessages(params.messages),
      ...(params.temperature !== undefined ? { temperature: params.temperature } : {}),
      ...(params.maxTokens !== undefined ? { max_tokens: params.maxTokens } : {}),
      ...(params.tools && params.tools.length > 0
        ? {
            tools: params.tools.map((t) => ({
              type: "function",
              function: { name: t.name, description: t.description, parameters: t.parameters },
            })),
            tool_choice: "auto",
          }
        : {}),
      ...(stream ? { stream: true, stream_options: { include_usage: true } } : {}),
    };
  }

  function headers(): Record<string, string> {
    return {
      "content-type": "application/json",
      authorization: `Bearer ${apiKey ?? ""}`,
      ...options.defaultHeaders,
    };
  }

  return {
    async chat(params: LLMChatParams): Promise<LLMResponse> {
      const resp = await fetch(`${baseUrl}/v1/chat/completions`, {
        method: "POST",
        headers: headers(),
        body: JSON.stringify(buildBody(params)),
        signal: params.signal,
      });

      if (!resp.ok) {
        const text = await resp.text();
        throw new Error(`OpenAI API error ${resp.status}: ${text}`);
      }

      const data = (await resp.json()) as OpenAIResponse;
      return parseOpenAIResponse(data);
    },

    async *chatStream(params: LLMChatParams): AsyncIterable<LLMStreamChunk> {
      const resp = await fetch(`${baseUrl}/v1/chat/completions`, {
        method: "POST",
        headers: headers(),
        body: JSON.stringify(buildBody(params, true)),
        signal: params.signal,
      });

      if (!resp.ok) {
        const text = await resp.text();
        throw new Error(`OpenAI API error ${resp.status}: ${text}`);
      }

      // Accumulate tool call deltas keyed by index
      const toolAcc = new Map<number, { id: string; name: string; argsJson: string }>();
      let finishReason: LLMFinishReason = "stop";
      let inputTokens = 0;
      let outputTokens = 0;

      for await (const chunk of parseSSE(resp.body!)) {
        const choice = (chunk.choices as Array<{
          delta: {
            content?: string | null;
            tool_calls?: Array<{
              index: number;
              id?: string;
              function?: { name?: string; arguments?: string };
            }>;
          };
          finish_reason?: string | null;
        }>)?.[0];

        if (!choice) {
          // usage-only chunk (stream_options: include_usage)
          const usage = chunk.usage as { prompt_tokens?: number; completion_tokens?: number } | undefined;
          if (usage) {
            inputTokens = usage.prompt_tokens ?? 0;
            outputTokens = usage.completion_tokens ?? 0;
          }
          continue;
        }

        if (choice.finish_reason) {
          finishReason =
            choice.finish_reason === "stop"
              ? "stop"
              : choice.finish_reason === "tool_calls"
                ? "tool_calls"
                : choice.finish_reason === "length"
                  ? "length"
                  : "stop";
        }

        const { delta } = choice;

        if (delta.content) {
          yield { delta: delta.content };
        }

        for (const tc of delta.tool_calls ?? []) {
          const acc = toolAcc.get(tc.index);
          if (!acc) {
            toolAcc.set(tc.index, {
              id: tc.id ?? "",
              name: tc.function?.name ?? "",
              argsJson: tc.function?.arguments ?? "",
            });
          } else {
            acc.argsJson += tc.function?.arguments ?? "";
          }
        }
      }

      const toolCalls: ToolCall[] = [...toolAcc.values()].map(({ id, name, argsJson }) => ({
        id,
        name,
        input: JSON.parse(argsJson || "{}") as unknown,
      }));

      yield {
        delta: "",
        finishReason,
        toolCalls: toolCalls.length > 0 ? toolCalls : undefined,
        usage: { inputTokens, outputTokens },
      };
    },
  };
}

async function* parseSSE(body: ReadableStream<Uint8Array>): AsyncIterable<Record<string, unknown>> {
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
              yield JSON.parse(data) as Record<string, unknown>;
            } catch {}
          }
        }
      }
    }
  } finally {
    reader.releaseLock();
  }
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
