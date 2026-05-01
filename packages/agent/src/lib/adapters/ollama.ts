import type {
  LLMProvider,
  LLMChatParams,
  LLMResponse,
  LLMStreamChunk,
  LLMFinishReason,
} from "../llm-provider.ts";
import type { Message, ToolCall } from "../message.ts";

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
}

// ---- public API ----

export interface OllamaOptions {
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

  function buildBody(params: LLMChatParams, stream: boolean): Record<string, unknown> {
    const ollamaOptions: Record<string, unknown> = {};
    const temperature = params.temperature ?? options.temperature;
    if (temperature !== undefined) ollamaOptions.temperature = temperature;
    const maxTokens = params.maxTokens ?? options.maxTokens;
    if (maxTokens !== undefined) ollamaOptions.num_predict = maxTokens;
    // num_ctx defaults to 8192 — see OllamaOptions.numCtx for rationale.
    ollamaOptions.num_ctx = options.numCtx ?? 8192;

    return {
      model: options.model,
      messages: toOllamaMessages(params.messages),
      stream,
      ...(params.tools && params.tools.length > 0
        ? {
            tools: params.tools.map((t) => ({
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
      const resp = await fetch(`${baseURL}/api/chat`, {
        method: "POST",
        headers: requestHeaders(),
        body: JSON.stringify(buildBody(params, false)),
        signal: params.signal,
      });
      if (!resp.ok) {
        const text = await resp.text();
        throw new Error(`Ollama API error ${resp.status}: ${text}`);
      }
      const data = (await resp.json()) as OllamaChunk;
      return ollamaChunkToResponse(data);
    },

    async *chatStream(params: LLMChatParams): AsyncIterable<LLMStreamChunk> {
      const resp = await fetch(`${baseURL}/api/chat`, {
        method: "POST",
        headers: requestHeaders(),
        body: JSON.stringify(buildBody(params, true)),
        signal: params.signal,
      });
      if (!resp.ok) {
        const text = await resp.text();
        throw new Error(`Ollama API error ${resp.status}: ${text}`);
      }

      const rawToolCalls: Array<{ function: { name: string; arguments: unknown } }> = [];
      let finishReason: LLMFinishReason = "stop";
      let usage: LLMResponse["usage"] | undefined;

      for await (const chunk of parseNDJSON(resp.body!)) {
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
          finishReason =
            rawToolCalls.length > 0 ? "tool_calls" : mapFinishReason(chunk.done_reason);
        }
      }

      const toolCalls = assembleToolCalls(rawToolCalls);
      yield {
        delta: "",
        finishReason,
        toolCalls: toolCalls.length > 0 ? toolCalls : undefined,
        usage,
      };
    },
  };
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
    // Ollama does not return call IDs — generate a stable index-based one
    id: `call_${i}`,
    name: c.function.name,
    input:
      typeof c.function.arguments === "string"
        ? (JSON.parse(c.function.arguments) as unknown)
        : c.function.arguments,
  }));
}

function mapFinishReason(reason?: string): LLMFinishReason {
  if (reason === "length") return "length";
  return "stop";
}

function toOllamaMessages(messages: Message[]): OllamaMessage[] {
  return messages.map((msg): OllamaMessage => {
    if (msg.role === "system") return { role: "system", content: msg.content };
    if (msg.role === "user") return { role: "user", content: msg.content };
    if (msg.role === "tool") {
      return { role: "tool", tool_call_id: msg.toolCallId, content: msg.content };
    }
    // assistant
    const out: OllamaMessage = { role: "assistant", content: msg.content };
    if (msg.toolCalls && msg.toolCalls.length > 0) {
      out.tool_calls = msg.toolCalls.map((tc) => ({
        function: { name: tc.name, arguments: tc.input },
      }));
    }
    return out;
  });
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
