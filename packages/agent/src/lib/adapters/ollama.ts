import type {
  LLMProvider,
  LLMChatParams,
  LLMResponse,
  LLMStreamChunk,
  LLMFinishReason,
} from "../llm-provider.ts";
import type { Message, ToolCall } from "../message.ts";
import {
  LLMHttpError,
  httpErrorFromResponse,
  parseToolArguments,
  resolveHttpOptions,
  startLLMRequest,
  type LLMHttpOptions,
} from "./llm-http.ts";

const PROVIDER = "ollama";

// ---- wire types ----

interface OllamaMessage {
  role: "system" | "user" | "assistant" | "tool";
  content?: string | null;
  tool_calls?: Array<{ function: { name: string; arguments: unknown } }>;
  tool_call_id?: string;
}

interface OllamaChunk {
  message: OllamaMessage;
  done: boolean;
  done_reason?: string;
  prompt_eval_count?: number;
  eval_count?: number;
  error?: unknown;
}

// ---- public API ----

export interface OllamaOptions extends LLMHttpOptions {
  model: string;
  /** Default: http://localhost:11434 */
  baseURL?: string;
  temperature?: number;
  maxTokens?: number;
  /**
   * Context window size in tokens. Critical knob: many recent models
   * (e.g. llama3.2:3b) ship with a 131K default that allocates a huge
   * KV cache (>20GB) and effectively hangs the API on machines without
   * the memory to back it. 4K–8K is plenty for chat and tool calling;
   * crank it up only if you actually feed long histories. Defaults to
   * 8192 — sane for chat, won't accidentally OOM.
   */
  numCtx?: number;
  defaultHeaders?: Record<string, string>;
}

export function ollama(options: OllamaOptions): LLMProvider {
  const baseURL = (options.baseURL ?? "http://localhost:11434").replace(/\/$/, "");
  const http = resolveHttpOptions(options);

  function buildBody(params: LLMChatParams, stream: boolean): Record<string, unknown> {
    const ollamaOptions: Record<string, unknown> = {};
    const temperature = params.temperature ?? options.temperature;
    if (temperature !== undefined) ollamaOptions.temperature = temperature;
    const maxTokens = params.maxTokens ?? options.maxTokens;
    if (maxTokens !== undefined) ollamaOptions.num_predict = maxTokens;
    // num_ctx defaults to 8192 — see OllamaOptions.numCtx for rationale.
    ollamaOptions.num_ctx = options.numCtx ?? 8192;
    const tools = ollamaTools(params);

    return {
      model: options.model,
      messages: toOllamaMessages(params.messages),
      stream,
      ...(tools.length > 0
        ? {
            tools: tools.map((t) => ({
              type: "function",
              function: { name: t.name, description: t.description, parameters: t.parameters },
            })),
          }
        : {}),
      ...(Object.keys(ollamaOptions).length > 0 ? { options: ollamaOptions } : {}),
    };
  }

  function requestHeaders(): Record<string, string> {
    return { "content-type": "application/json", ...options.defaultHeaders };
  }

  return {
    async chat(params: LLMChatParams): Promise<LLMResponse> {
      const request = startLLMRequest({
        provider: PROVIDER,
        signal: params.signal,
        timeoutMs: http.timeoutMs,
        clock: http.clock,
      });
      try {
        const resp = await http.fetch(`${baseURL}/api/chat`, {
          method: "POST",
          headers: requestHeaders(),
          body: JSON.stringify(buildBody(params, false)),
          signal: request.signal,
        });
        if (!resp.ok) {
          throw await httpErrorFromResponse({
            provider: PROVIDER,
            response: resp,
            clock: http.clock,
          });
        }
        const data = (await resp.json()) as OllamaChunk;
        return ollamaChunkToResponse(data);
      } catch (err) {
        throw request.mapError(err);
      } finally {
        request.dispose();
      }
    },

    async *chatStream(params: LLMChatParams): AsyncIterable<LLMStreamChunk> {
      const request = startLLMRequest({
        provider: PROVIDER,
        signal: params.signal,
        timeoutMs: http.timeoutMs,
        clock: http.clock,
      });
      try {
        yield* streamChat(params, request.signal);
      } catch (err) {
        throw request.mapError(err);
      } finally {
        request.dispose();
      }
    },
  };

  async function* streamChat(
    params: LLMChatParams,
    signal: AbortSignal,
  ): AsyncIterable<LLMStreamChunk> {
    const resp = await http.fetch(`${baseURL}/api/chat`, {
      method: "POST",
      headers: requestHeaders(),
      body: JSON.stringify(buildBody(params, true)),
      signal,
    });
    if (!resp.ok) {
      throw await httpErrorFromResponse({ provider: PROVIDER, response: resp, clock: http.clock });
    }

    const rawToolCalls: Array<{ function: { name: string; arguments: unknown } }> = [];
    let finishReason: LLMFinishReason = "stop";
    let usage: LLMResponse["usage"] | undefined;

    for await (const chunk of parseNDJSON(resp.body!)) {
      if (chunk.error !== undefined) {
        // Ollama reports a failure after the 200 as an `{"error": ...}` line.
        throw new LLMHttpError({ status: 500, provider: PROVIDER, body: JSON.stringify(chunk) });
      }
      if (chunk.message?.tool_calls) {
        rawToolCalls.push(...chunk.message.tool_calls);
      }
      if (!chunk.done) {
        const content = chunk.message?.content;
        if (content) yield { delta: content };
      } else {
        if (chunk.prompt_eval_count !== undefined) {
          usage = { inputTokens: chunk.prompt_eval_count, outputTokens: chunk.eval_count ?? 0 };
        }
        finishReason = rawToolCalls.length > 0 ? "tool_calls" : mapFinishReason(chunk.done_reason);
      }
    }

    const toolCalls = assembleToolCalls(rawToolCalls);
    yield {
      delta: "",
      finishReason,
      toolCalls: toolCalls.length > 0 ? toolCalls : undefined,
      usage,
    };
  }
}

// ---- helpers ----

function ollamaChunkToResponse(chunk: OllamaChunk): LLMResponse {
  const toolCalls = assembleToolCalls(chunk.message.tool_calls ?? []);
  const finishReason = toolCalls.length > 0 ? "tool_calls" : mapFinishReason(chunk.done_reason);
  return {
    content: chunk.message.content ?? null,
    toolCalls: toolCalls.length > 0 ? toolCalls : undefined,
    finishReason,
    usage:
      chunk.prompt_eval_count !== undefined
        ? { inputTokens: chunk.prompt_eval_count, outputTokens: chunk.eval_count ?? 0 }
        : undefined,
  };
}

function assembleToolCalls(
  calls: Array<{ function: { name: string; arguments: unknown } }>,
): ToolCall[] {
  return calls.map((c, i) => ({
    // Ollama returns no call ids. Generate one unique per call: the agent
    // engine keys approvals (`approve:<id>`) and tool results on the id,
    // so an index-based id would repeat across the steps of a turn.
    id: `call_${i}_${crypto.randomUUID()}`,
    name: c.function.name,
    ...(typeof c.function.arguments === "string"
      ? parseToolArguments({ json: c.function.arguments })
      : { input: c.function.arguments }),
  }));
}

function mapFinishReason(reason?: string): LLMFinishReason {
  if (reason === "length") return "length";
  return "stop";
}

function toOllamaMessages(messages: Message[]): OllamaMessage[] {
  const result: OllamaMessage[] = [];
  for (const msg of messages) {
    if (msg.role === "system") {
      result.push({ role: "system", content: msg.content });
    } else if (msg.role === "user") {
      result.push({ role: "user", content: msg.content });
    } else if (msg.role === "tool") {
      result.push({ role: "tool", tool_call_id: msg.toolCallId, content: msg.content });
    } else {
      // Leave out an assistant turn with neither text nor tool calls (an
      // aborted or empty reply).
      const text = msg.content?.trim() ? msg.content : null;
      if (text === null && !msg.toolCalls?.length) continue;
      const out: OllamaMessage = { role: "assistant", content: text ?? "" };
      if (msg.toolCalls && msg.toolCalls.length > 0) {
        out.tool_calls = msg.toolCalls.map((tc) => ({
          function: { name: tc.name, arguments: tc.input },
        }));
      }
      result.push(out);
    }
  }
  return result;
}

/**
 * Ollama has no tool_choice: a forced tool is approximated by sending only
 * that tool, `"none"` by sending no tools.
 */
function ollamaTools(params: LLMChatParams): NonNullable<LLMChatParams["tools"]> {
  const tools = params.tools ?? [];
  const choice = params.toolChoice;
  if (choice === "none") return [];
  if (choice !== undefined && typeof choice === "object") {
    return tools.filter((t) => t.name === choice.name);
  }
  return tools;
}

async function* parseNDJSON(body: ReadableStream<Uint8Array>): AsyncIterable<OllamaChunk> {
  const reader = body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split("\n");
      buffer = lines.pop() ?? "";
      for (const line of lines) {
        const trimmed = line.trim();
        if (!trimmed) continue;
        try {
          yield JSON.parse(trimmed) as OllamaChunk;
        } catch {}
      }
    }
    const remaining = buffer.trim();
    if (remaining) {
      try {
        yield JSON.parse(remaining) as OllamaChunk;
      } catch {}
    }
  } finally {
    reader.releaseLock();
  }
}
