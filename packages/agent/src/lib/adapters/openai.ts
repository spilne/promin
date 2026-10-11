import type {
  LLMProvider,
  LLMChatParams,
  LLMResponse,
  LLMStreamChunk,
  LLMFinishReason,
  LLMToolChoice,
} from "../llm-provider.ts";
import type { Message, ToolCall } from "../message.ts";
import {
  LLMHttpError,
  LLMStreamInterruptedError,
  httpErrorFromResponse,
  parseToolArguments,
  resolveHttpOptions,
  startLLMRequest,
  type LLMHttpOptions,
} from "./llm-http.ts";

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

export interface OpenAIOptions extends LLMHttpOptions {
  apiKey?: string;
  /**
   * API root including the version segment; requests go to
   * `${baseUrl}/chat/completions`. Default: `https://api.openai.com/v1`.
   */
  baseUrl?: string;
  defaultHeaders?: Record<string, string>;
  /** Provider name on errors and metrics. Default: `"openai"`. */
  providerName?: string;
}

/**
 * Create an `LLMProvider` for the OpenAI Chat Completions API, or any
 * server compatible with it (`baseUrl`).
 *
 * Failures throw `LLMHttpError` (HTTP errors and streamed error chunks),
 * `LLMTimeoutError` (after `timeoutMs`) or `LLMStreamInterruptedError`
 * (stream ended without a finish reason). Tool arguments that are not
 * valid JSON become a `ToolCall.inputError` the model sees as the tool
 * result.
 */
export function openai(model: string, options: OpenAIOptions = {}): LLMProvider {
  const apiKey = options.apiKey ?? process.env["OPENAI_API_KEY"];
  const baseUrl = (options.baseUrl ?? "https://api.openai.com/v1").replace(/\/$/, "");
  const provider = options.providerName ?? "openai";
  const http = resolveHttpOptions(options);

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
            tool_choice: toOpenAIToolChoice(params.toolChoice ?? "auto"),
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
      const request = startLLMRequest({
        provider,
        signal: params.signal,
        timeoutMs: http.timeoutMs,
        clock: http.clock,
      });
      try {
        const resp = await http.fetch(`${baseUrl}/chat/completions`, {
          method: "POST",
          headers: headers(),
          body: JSON.stringify(buildBody(params)),
          signal: request.signal,
        });
        if (!resp.ok) {
          throw await httpErrorFromResponse({ provider, response: resp, clock: http.clock });
        }
        const data = (await resp.json()) as OpenAIResponse;
        return parseOpenAIResponse({ data, provider });
      } catch (err) {
        throw request.mapError(err);
      } finally {
        request.dispose();
      }
    },

    async *chatStream(params: LLMChatParams): AsyncIterable<LLMStreamChunk> {
      const request = startLLMRequest({
        provider,
        signal: params.signal,
        timeoutMs: http.timeoutMs,
        clock: http.clock,
      });
      try {
        yield* streamCompletions(params, request.signal);
      } catch (err) {
        throw request.mapError(err);
      } finally {
        request.dispose();
      }
    },
  };

  async function* streamCompletions(
    params: LLMChatParams,
    signal: AbortSignal,
  ): AsyncIterable<LLMStreamChunk> {
    const resp = await http.fetch(`${baseUrl}/chat/completions`, {
      method: "POST",
      headers: headers(),
      body: JSON.stringify(buildBody(params, true)),
      signal,
    });

    if (!resp.ok) {
      throw await httpErrorFromResponse({ provider, response: resp, clock: http.clock });
    }

    // Accumulate tool call deltas keyed by index
    const toolAcc = new Map<number, { id: string; name: string; argsJson: string }>();
    let finishReason: LLMFinishReason | undefined;
    let inputTokens = 0;
    let outputTokens = 0;

    for await (const chunk of parseSSE(resp.body!)) {
      if (chunk.error) {
        // Compatible servers report a failure after the 200 as an error chunk.
        throw new LLMHttpError({
          status: streamErrorStatus(chunk.error),
          provider,
          body: JSON.stringify(chunk),
        });
      }
      const choice = (
        chunk.choices as Array<{
          delta: {
            content?: string | null;
            tool_calls?: Array<{
              index: number;
              id?: string;
              function?: { name?: string; arguments?: string };
            }>;
          };
          finish_reason?: string | null;
        }>
      )?.[0];

      if (!choice) {
        // usage-only chunk (stream_options: include_usage)
        const usage = chunk.usage as
          | { prompt_tokens?: number; completion_tokens?: number }
          | undefined;
        if (usage) {
          inputTokens = usage.prompt_tokens ?? 0;
          outputTokens = usage.completion_tokens ?? 0;
        }
        continue;
      }

      if (choice.finish_reason) finishReason = mapFinishReason(choice.finish_reason);

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
          // Some OpenAI-compatible servers send the id or name on a later
          // delta than the first one of the call.
          if (!acc.id && tc.id) acc.id = tc.id;
          if (!acc.name && tc.function?.name) acc.name = tc.function.name;
          acc.argsJson += tc.function?.arguments ?? "";
        }
      }
    }

    if (finishReason === undefined) {
      throw new LLMStreamInterruptedError({
        provider,
        detail: "the stream ended without a finish_reason",
      });
    }

    const truncated = finishReason === "length";
    const toolCalls: ToolCall[] = [...toolAcc.values()].map(({ id, name, argsJson }) => ({
      id,
      name,
      ...parseToolArguments({ json: argsJson, truncated }),
    }));

    yield {
      delta: "",
      finishReason,
      toolCalls: toolCalls.length > 0 ? toolCalls : undefined,
      usage: { inputTokens, outputTokens },
    };
  }
}

function toOpenAIToolChoice(choice: LLMToolChoice): unknown {
  if (typeof choice === "string") return choice;
  return { type: "function", function: { name: choice.name } };
}

function mapFinishReason(reason: string): LLMFinishReason {
  if (reason === "tool_calls") return "tool_calls";
  if (reason === "length") return "length";
  return "stop";
}

/** HTTP status for a streamed error chunk: its numeric `code`, else 500. */
function streamErrorStatus(error: unknown): number {
  const code = (error as { code?: unknown } | null)?.code;
  const n = typeof code === "number" ? code : Number(code);
  return Number.isInteger(n) && n >= 400 && n < 600 ? n : 500;
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
  const result: OpenAIMessage[] = [];
  for (const msg of messages) {
    if (msg.role === "system") {
      result.push({ role: "system", content: msg.content });
    } else if (msg.role === "user") {
      result.push({ role: "user", content: msg.content });
    } else if (msg.role === "tool") {
      result.push({ role: "tool", tool_call_id: msg.toolCallId, content: msg.content });
    } else {
      // An assistant turn without text or tool calls (an aborted or empty
      // reply) is rejected by the API; leave it out of the request.
      const text = msg.content?.trim() ? msg.content : null;
      if (text === null && !msg.toolCalls?.length) continue;
      const openaiMsg: OpenAIMessage = { role: "assistant", content: text };
      if (msg.toolCalls && msg.toolCalls.length > 0) {
        openaiMsg.tool_calls = msg.toolCalls.map((tc) => ({
          id: tc.id,
          type: "function",
          function: { name: tc.name, arguments: JSON.stringify(tc.input) },
        }));
      }
      result.push(openaiMsg);
    }
  }
  return result;
}

function parseOpenAIResponse(args: { data: OpenAIResponse; provider: string }): LLMResponse {
  const { data, provider } = args;
  const choice = data.choices[0];
  if (!choice) throw new Error(`${provider} returned no choices`);

  const finishReason = mapFinishReason(choice.finish_reason);
  const toolCalls: ToolCall[] = (choice.message.tool_calls ?? []).map((tc) => ({
    id: tc.id,
    name: tc.function.name,
    ...parseToolArguments({ json: tc.function.arguments, truncated: finishReason === "length" }),
  }));

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
