import type {
  LLMProvider,
  LLMChatParams,
  LLMResponse,
  LLMStreamChunk,
  LLMFinishReason,
  RateLimitHint,
} from "../llm-provider.ts";
import type { Message, ToolCall, ThinkingBlock } from "../message.ts";

interface AnthropicContentBlock {
  type: string;
  text?: string;
  thinking?: string;
  signature?: string;
  id?: string;
  tool_use_id?: string;
  name?: string;
  input?: unknown;
  content?: string;
  cache_control?: { type: "ephemeral" };
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
  content_block:
    | { type: "text" }
    | { type: "thinking" }
    | { type: "tool_use"; id: string; name: string };
}
interface SSEContentBlockDelta {
  type: "content_block_delta";
  index: number;
  delta:
    | { type: "text_delta"; text: string }
    | { type: "thinking_delta"; thinking: string }
    | { type: "signature_delta"; signature: string }
    | { type: "input_json_delta"; partial_json: string };
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

/**
 * Create an `LLMProvider` backed by the Anthropic Messages API.
 *
 * Supports both `chat` (single request/response) and `chatStream` (SSE token stream).
 * Automatically enables prompt caching (`anthropic-beta: prompt-caching-2024-07-31`) —
 * the last system block and the last tool definition are marked `cache_control: ephemeral`
 * so the combined system + tools prefix is cached across turns.
 * Extended thinking is enabled when `params.thinkingBudgetTokens` is set.
 *
 * @example
 * ```ts
 * const llm = anthropic("claude-sonnet-4-6", { apiKey: process.env.ANTHROPIC_API_KEY });
 * const { content } = await llm.chat({ messages: [{ role: "user", content: "Hello" }] });
 * ```
 */
export function anthropic(model: string, options: AnthropicOptions = {}): LLMProvider {
  const apiKey = options.apiKey ?? process.env["ANTHROPIC_API_KEY"];
  const baseUrl = options.baseUrl ?? "https://api.anthropic.com";

  function buildBody(params: LLMChatParams, stream?: boolean): Record<string, unknown> {
    const systemBlocks = extractSystemBlocks(params.messages);
    const thinking = params.thinkingBudgetTokens
      ? { type: "enabled", budget_tokens: params.thinkingBudgetTokens }
      : undefined;
    return {
      model,
      max_tokens: params.maxTokens ?? options.maxTokens ?? 4096,
      messages: toAnthropicMessages(params.messages),
      // All system messages are collected into blocks. The last block gets
      // cache_control so the entire system prefix (including memory injections,
      // tool descriptions, etc.) is cached as one unit.
      ...(systemBlocks.length > 0 ? { system: systemBlocks } : {}),
      // Extended thinking requires temperature=1.
      temperature: thinking ? 1 : (params.temperature ?? undefined),
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
      ...(thinking ? { thinking } : {}),
      ...(stream ? { stream: true } : {}),
    };
  }

  function headers(params?: LLMChatParams): Record<string, string> {
    const betaFeatures = ["prompt-caching-2024-07-31"];
    if (params?.thinkingBudgetTokens) betaFeatures.push("interleaved-thinking-2025-05-14");
    return {
      "content-type": "application/json",
      "x-api-key": apiKey ?? "",
      "anthropic-version": "2023-06-01",
      "anthropic-beta": betaFeatures.join(","),
      ...options.defaultHeaders,
    };
  }

  return {
    async chat(params: LLMChatParams): Promise<LLMResponse> {
      const resp = await fetch(`${baseUrl}/v1/messages`, {
        method: "POST",
        headers: headers(params),
        body: JSON.stringify(buildBody(params)),
        signal: params.signal,
      });

      if (!resp.ok) {
        const text = await resp.text();
        throw new Error(`Anthropic API error ${resp.status}: ${text}`);
      }

      const data = (await resp.json()) as AnthropicResponse;
      const parsed = parseAnthropicResponse(data);
      const rateLimitHint = parseRateLimitHeaders(resp.headers);
      return rateLimitHint ? { ...parsed, rateLimitHint } : parsed;
    },

    async *chatStream(params: LLMChatParams): AsyncIterable<LLMStreamChunk> {
      const resp = await fetch(`${baseUrl}/v1/messages`, {
        method: "POST",
        headers: headers(params),
        body: JSON.stringify(buildBody(params, true)),
        signal: params.signal,
      });

      if (!resp.ok) {
        const text = await resp.text();
        throw new Error(`Anthropic API error ${resp.status}: ${text}`);
      }

      // Stream headers arrive on the response itself, before the first
      // SSE event. Capture once so the final chunk can carry the hint.
      const rateLimitHint = parseRateLimitHeaders(resp.headers);

      // Accumulate per-block state keyed by block index
      const textBlocks = new Map<number, string>();
      const toolBlocks = new Map<number, { id: string; name: string; inputJson: string }>();
      const thinkingBlocks = new Map<number, { thinking: string; signature: string }>();
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
          } else if (e.content_block.type === "thinking") {
            thinkingBlocks.set(e.index, { thinking: "", signature: "" });
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
          } else if (e.delta.type === "thinking_delta") {
            const block = thinkingBlocks.get(e.index);
            if (block) block.thinking += e.delta.thinking;
            yield { delta: "", thinkingDelta: e.delta.thinking };
          } else if (e.delta.type === "signature_delta") {
            const block = thinkingBlocks.get(e.index);
            if (block) block.signature = e.delta.signature;
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

      const completedThinkingBlocks: ThinkingBlock[] = [...thinkingBlocks.values()].filter(
        (b) => b.signature,
      );

      yield {
        delta: "",
        finishReason,
        toolCalls: toolCalls.length > 0 ? toolCalls : undefined,
        thinkingBlocks: completedThinkingBlocks.length > 0 ? completedThinkingBlocks : undefined,
        usage: { inputTokens, outputTokens, cacheReadTokens, cacheWriteTokens },
        ...(rateLimitHint ? { rateLimitHint } : {}),
      };
    },
  };
}

/**
 * Read Anthropic's rate-limit headers off a Response and project them onto
 * the protocol-level `RateLimitHint`. Returns `undefined` when no headers
 * are present (e.g. test fakes, gateways that strip them) so consumers
 * fall back to round-robin / blind retry without spurious zero hints.
 *
 * Header names per Anthropic's public API docs:
 *   - x-ratelimit-remaining-tokens     — input-token budget remaining
 *   - x-ratelimit-remaining-requests   — request budget remaining
 *   - x-ratelimit-reset-tokens         — ISO-8601 reset timestamp (token bucket)
 *
 * The token reset is the most operationally useful timestamp. Falls back
 * to the requests reset header when the token reset is missing.
 */
function parseRateLimitHeaders(headers: Headers): RateLimitHint | undefined {
  const remainingTokens = parseIntHeader(headers.get("x-ratelimit-remaining-tokens"));
  const remainingRequests = parseIntHeader(headers.get("x-ratelimit-remaining-requests"));
  const resetsAt = parseDateHeader(
    headers.get("x-ratelimit-reset-tokens") ?? headers.get("x-ratelimit-reset-requests"),
  );
  if (remainingTokens === undefined && remainingRequests === undefined && resetsAt === undefined) {
    return undefined;
  }
  return {
    ...(remainingTokens !== undefined && { remainingTokens }),
    ...(remainingRequests !== undefined && { remainingRequests }),
    ...(resetsAt !== undefined && { resetsAt }),
  };
}

function parseIntHeader(value: string | null): number | undefined {
  if (!value) return undefined;
  const n = Number.parseInt(value, 10);
  return Number.isFinite(n) ? n : undefined;
}

function parseDateHeader(value: string | null): number | undefined {
  if (!value) return undefined;
  const t = Date.parse(value);
  return Number.isFinite(t) ? t : undefined;
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

/**
 * Collect system messages into Anthropic system blocks.
 *
 * Caching strategy: place a `cache_control` breakpoint on BOTH the first
 * and the last system block (when there are multiple). With two markers,
 * Anthropic caches two prefixes — one ending at the first block (typically
 * the always-stable agent persona) and one ending at the last block
 * (the full prefix including volatile cascade content). When the cascade
 * changes mid-conversation, we still hit cache on the persona prefix
 * instead of paying full token price.
 *
 * One block? Just mark it once. Anthropic's 4-breakpoint limit easily
 * accommodates persona + cascade + optional namespace/resource splits.
 *
 * Previously this used `.find()` which silently dropped all system
 * messages after the first one — fixed; now all blocks survive.
 */
function extractSystemBlocks(messages: Message[]): AnthropicContentBlock[] {
  const systemMessages = messages.filter((m) => m.role === "system");
  if (systemMessages.length === 0) return [];
  const last = systemMessages.length - 1;

  return systemMessages.map((m, i) => {
    const cacheBoundary =
      i === last || (systemMessages.length > 1 && i === 0)
        ? { cache_control: { type: "ephemeral" as const } }
        : {};
    return {
      type: "text" as const,
      text: m.content,
      ...cacheBoundary,
    };
  });
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
      // Thinking blocks must precede text/tool blocks and be replayed verbatim.
      for (const tb of msg.thinkingBlocks ?? []) {
        content.push({ type: "thinking", thinking: tb.thinking, signature: tb.signature });
      }
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
  const thinkingBlocks: ThinkingBlock[] = [];

  for (const block of data.content) {
    if (block.type === "thinking" && block.thinking && block.signature) {
      thinkingBlocks.push({ thinking: block.thinking, signature: block.signature });
    } else if (block.type === "text" && block.text) {
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
    thinkingBlocks: thinkingBlocks.length > 0 ? thinkingBlocks : undefined,
    finishReason,
    usage: {
      inputTokens: data.usage.input_tokens,
      outputTokens: data.usage.output_tokens,
      cacheReadTokens: data.usage.cache_read_input_tokens ?? 0,
      cacheWriteTokens: data.usage.cache_creation_input_tokens ?? 0,
    },
  };
}
