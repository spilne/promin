import { SystemWallClock, type WallClock } from "@promin/workflow";
import type { AgentTool, ToolExecuteContext, ToolScope } from "./tool.ts";
import type { ToolAuditLogger } from "./tool-audit/types.ts";
import type { ToolCall, ToolResultMessage, Message } from "./message.ts";
import type {
  LLMProvider,
  LLMChatParams,
  LLMResponse,
  LLMFinishReason,
  LLMUsage,
  LLMToolChoice,
  LLMToolDefinition,
} from "./llm-provider.ts";
import type { ThinkingBlock } from "./message.ts";
import type { ProcessorsConfig, ProcessorContext } from "./processors.ts";
import { recordChat } from "./metrics/instrument.ts";
import type { ToolRegistry } from "./tool-registry.ts";
import { LLMHttpError } from "./adapters/llm-http.ts";
import { classifyLLMError, type LLMErrorClass } from "./adapters/llm-error-classification.ts";

// ---- message utilities ----

export function systemMsgs(messages: Message[]): Message[] {
  return messages.filter((m) => m.role === "system");
}

export function nonSystemMsgs(messages: Message[]): Message[] {
  return messages.filter((m) => m.role !== "system");
}

// ---- tool resolution ----

export function resolveTools(config: {
  toolRegistry?: ToolRegistry;
  tools?: Record<string, AgentTool<any, any>>;
}): Record<string, AgentTool<any, any>> {
  // Reject ambiguous config: specifying both sources silently shadowed the
  // inline `tools` map before, which turned "why isn't my tool firing?"
  // into a debugging session. Force the caller to pick one source.
  if (config.toolRegistry && config.tools) {
    throw new Error(
      "Agent config cannot specify both 'toolRegistry' and 'tools'. " +
        "Pick one source: either register all tools in the registry, " +
        "or pass them all inline via 'tools'.",
    );
  }
  return config.toolRegistry?.getTools() ?? config.tools ?? {};
}

// ---- agent error surfacing ----

/**
 * `true` when the error is a `WorkflowSuspendedError`. Suspension is a
 * normal control signal; callers typically swallow it instead of propagating.
 */
export function isWorkflowSuspension(error: unknown): boolean {
  if (!error || typeof error !== "object") return false;
  return (error as { _tag?: string })._tag === "WorkflowSuspendedError";
}

/**
 * Classification produced by `surfaceAgentError`.
 * - `suspended`: WorkflowSuspendedError — the body is waiting on a signal
 *   or sleep. Not a failure.
 * - `user-error`: thrown by user code, a tool, a hook, or
 *   `TerminalError` — won't succeed on retry.
 * - `infra-error`: framework / transient failures
 *   (`RetryableError`, a thrown non-Error value, etc.).
 */
export type SurfacedAgentErrorKind = "suspended" | "user-error" | "infra-error";

export interface SurfacedAgentError {
  readonly kind: SurfacedAgentErrorKind;
  /** Always an Error instance with a meaningful `.message`. */
  readonly error: Error;
}

function toError(err: unknown): Error {
  if (err instanceof Error) return err;
  if (typeof err === "string") return new Error(err);
  try {
    return new Error(JSON.stringify(err));
  } catch {
    return new Error(String(err));
  }
}

/**
 * Classify an error from any agent primitive (agentLoop / agentAction).
 * The workflow runner rejects with the original thrown value, so this only
 * tags the kind — call sites can branch (suspend vs surface to user vs
 * infra-retry) without re-implementing the same `instanceof` chain.
 *
 * Preserves tagged-error metadata: a `TerminalError` keeps its `_tag`. The
 * returned `error.name` matches the original constructor when one is present.
 */
export function surfaceAgentError(err: unknown): SurfacedAgentError {
  if (isWorkflowSuspension(err)) return { kind: "suspended", error: toError(err) };
  const tag = (err as { _tag?: string } | null | undefined)?._tag;
  if (tag === "TerminalError") return { kind: "user-error", error: toError(err) };
  if (tag === "RetryableError") return { kind: "infra-error", error: toError(err) };
  if (err instanceof Error) return { kind: "user-error", error: err };
  return { kind: "infra-error", error: toError(err) };
}

// ---- tool error formatting ----

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

// ---- tool execution ----

// Hard cap on any single tool result — prevents a runaway listing or read
// from blowing up the LLM context window.
const MAX_TOOL_CONTENT_CHARS = 200_000;

const TOOL_CANCELLED_MESSAGE = "Tool cancelled: the turn was cancelled before the tool finished.";

export interface ExecuteToolCallOptions {
  readonly call: ToolCall;
  readonly tool: AgentTool<any, any>;
  /** Caller scope surfaced on `ctx.scope` (namespace, resource, thread, agent). */
  readonly scope?: ToolScope;
  /** Audit sink surfaced on `ctx.toolAuditLogger` for elevated tools. */
  readonly toolAuditLogger?: ToolAuditLogger;
  /** Turn cancellation. Aborting it aborts `ctx.signal` and fails the call. */
  readonly signal?: AbortSignal;
  /** Time limit for this call. Unset or `0`: none. */
  readonly timeoutMs?: number;
  /** Time source for the timeout. Default: `SystemWallClock`. */
  readonly clock?: WallClock;
  /** Progress sink; becomes `ctx.writer`. */
  readonly onProgress?: (payload: unknown) => void;
  /** Called with the raw output when the tool succeeds. */
  readonly onResult?: (call: ToolCall, output: unknown) => void;
}

/**
 * Validate a tool call's input, run the tool and turn the outcome into the
 * tool-result message the model sees. Never throws: invalid input, a
 * thrown error, a timeout and a cancellation all come back as a result
 * with `isError: true`.
 *
 * The tool gets a `ToolExecuteContext` carrying `toolCallId`, `signal`,
 * `timeoutMs`, `scope`, `toolAuditLogger` and (with `onProgress`) a
 * `writer`. The engine stops waiting when `signal` aborts or the timeout
 * fires, whether or not the tool honors the signal.
 */
export async function executeToolCall(options: ExecuteToolCallOptions): Promise<ToolResultMessage> {
  const { call, tool, signal } = options;
  const errorResult = (content: string): ToolResultMessage => ({
    role: "tool",
    toolCallId: call.id,
    content,
    isError: true,
  });
  if (call.inputError !== undefined) return errorResult(`Invalid input: ${call.inputError}`);
  let parsed: unknown;
  try {
    parsed = tool.parameters.parse(call.input);
  } catch (err) {
    const received = JSON.stringify(call.input ?? null);
    return errorResult(`Invalid input: ${formatToolError(err)}\nReceived: ${received}`);
  }
  if (signal?.aborted) return errorResult(TOOL_CANCELLED_MESSAGE);

  const clock = options.clock ?? SystemWallClock;
  const timeoutMs =
    options.timeoutMs !== undefined && options.timeoutMs > 0 ? options.timeoutMs : 0;
  const controller = new AbortController();
  const onParentAbort = () => controller.abort(signal?.reason);
  signal?.addEventListener("abort", onParentAbort, { once: true });
  let timedOut = false;
  const timer =
    timeoutMs > 0
      ? clock.setTimeout(() => {
          timedOut = true;
          controller.abort(new Error(`tool timed out after ${timeoutMs}ms`));
        }, timeoutMs)
      : undefined;
  const aborted = new Promise<never>((_, reject) => {
    controller.signal.addEventListener("abort", () => reject(controller.signal.reason), {
      once: true,
    });
  });
  aborted.catch(() => {});

  const ctx: ToolExecuteContext = {
    toolCallId: call.id,
    signal: controller.signal,
    ...(timeoutMs > 0 && { timeoutMs }),
    ...(options.onProgress && { writer: { write: options.onProgress } }),
    ...(options.scope && { scope: options.scope }),
    ...(options.toolAuditLogger && { toolAuditLogger: options.toolAuditLogger }),
  };
  try {
    const running = Promise.resolve().then(() => tool.execute(parsed as any, ctx));
    // The tool may outlive a timeout / cancel; its late rejection is moot.
    running.catch(() => {});
    const output = await Promise.race([running, aborted]);
    options.onResult?.(call, output);
    let content = tool.toModelOutput
      ? tool.toModelOutput(output)
      : typeof output === "string"
        ? output
        : JSON.stringify(output);
    if (content.length > MAX_TOOL_CONTENT_CHARS) {
      content =
        content.slice(0, MAX_TOOL_CONTENT_CHARS) +
        `\n\n[Output truncated: ${content.length.toLocaleString()} chars total. Use a more specific query to avoid truncation.]`;
    }
    // Structured side-channel metadata — persisted with the message but
    // never shown to the LLM. `callAgent` uses it to carry the sub-run
    // trace; most tools declare no `toResultMetadata` and emit none.
    const metadata = tool.toResultMetadata?.(output);
    return { role: "tool", toolCallId: call.id, content, ...(metadata && { metadata }) };
  } catch (err) {
    if (timedOut) return errorResult(`Tool timed out after ${timeoutMs}ms.`);
    if (controller.signal.aborted) return errorResult(TOOL_CANCELLED_MESSAGE);
    return errorResult(`Tool execution failed: ${formatToolError(err)}`);
  } finally {
    timer?.clear();
    signal?.removeEventListener("abort", onParentAbort);
  }
}

// ---- shared LLM call helper ----

/**
 * Gate for outbound LLM calls. Any limiter whose `withLimitAsync` runs (or
 * rejects) the wrapped call fits — e.g. `SqliteRateLimiter`.
 */
export interface RateLimiter {
  withLimitAsync<T>(fn: () => Promise<T>): Promise<T>;
}

export interface RunLlmCallParams {
  llm: LLMProvider;
  messages: Message[];
  tools?: LLMToolDefinition[];
  /** Tool-use constraint, forwarded as-is. */
  toolChoice?: LLMToolChoice;
  rateLimiter?: RateLimiter;
  processors?: ProcessorsConfig;
  processorCtx: ProcessorContext;
  /** Called with each text delta when the LLM streams. When omitted, falls back to non-streaming. */
  onChunk?: (delta: string) => void;
  /** Called with each thinking delta when the LLM streams extended thinking. */
  onThinking?: (delta: string) => void;
  /** Token budget for extended thinking. Forwarded to the LLM as-is. */
  thinkingBudgetTokens?: number;
  /** Output token cap. Forwarded to the LLM as-is. */
  maxTokens?: number;
  signal?: AbortSignal;
  /** Time source for the latency metric. Default: `SystemWallClock`. */
  clock?: WallClock;
  /**
   * Optional telemetry sink. When set, records llm.calls / llm.tokens.*
   * / llm.latency.ms / llm.cost.usd per call. Defaults to no-op.
   */
  metrics?: import("./metrics/types.ts").AgentMetrics;
  /** Per-model USD rates. Required for llm.cost.usd to be emitted. */
  costs?: import("./metrics/types.ts").ModelCostRegistry;
  /** Provider id for metric labels (e.g. "anthropic"). Required when `metrics` is set. */
  provider?: string;
  /** Model id for metric labels (e.g. "claude-sonnet-4-6"). Required when `metrics` is set. */
  model?: string;
  /** Extra labels appended to every metric (e.g. agent id). */
  metricLabels?: Readonly<Record<string, string>>;
}

/** `runLlmCall`'s result: the response, flagged when `signal` cut it short. */
export interface RunLlmCallResult extends LLMResponse {
  /**
   * `true` when the call was aborted through `signal`. The response then
   * holds the text streamed so far and no tool calls.
   */
  aborted?: true;
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
  toolChoice,
  rateLimiter,
  processors,
  processorCtx,
  onChunk,
  onThinking,
  thinkingBudgetTokens,
  maxTokens,
  signal,
  clock = SystemWallClock,
  metrics,
  costs,
  provider,
  model,
  metricLabels,
}: RunLlmCallParams): Promise<RunLlmCallResult> {
  const callStart = clock.currentTimeMs();
  const processedMessages = processors?.beforeLLM
    ? await processors.beforeLLM(messages, processorCtx)
    : messages;

  const chatParams: LLMChatParams = {
    messages: processedMessages,
    tools,
    ...(toolChoice !== undefined && { toolChoice }),
    signal,
    thinkingBudgetTokens,
    ...(maxTokens !== undefined && { maxTokens }),
  };

  let raw: LLMResponse;
  // Text streamed so far — kept as the answer when the call is aborted mid-stream.
  let content = "";
  try {
    if (onChunk && llm.chatStream) {
      const startStream = () => Promise.resolve(llm.chatStream!(chatParams));
      const stream = await (rateLimiter ? rateLimiter.withLimitAsync(startStream) : startStream());
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
    // Treat abort as a clean stop so conversation history stays consistent:
    // keep the text already streamed to the caller, drop half-built tool
    // calls. `content` stays null when nothing was streamed, so no empty
    // assistant text reaches the next request.
    if (signal?.aborted || (err instanceof Error && err.name === "AbortError")) {
      return {
        content: content.trim() ? content : null,
        toolCalls: undefined,
        finishReason: "stop",
        aborted: true,
      };
    }
    throw err;
  }

  const finalResponse = processors?.afterLLM ? await processors.afterLLM(raw, processorCtx) : raw;

  // Per-call instrumentation. Skip when metrics is unset, or when the
  // host didn't bother to label the call (provider/model unknown — we
  // don't want unlabeled metrics polluting Prometheus dashboards).
  if (metrics && provider && model) {
    recordChat({
      metrics,
      ...(costs && { costs }),
      provider,
      model,
      response: finalResponse,
      durationMs: clock.currentTimeMs() - callStart,
      ...(metricLabels && { extraLabels: metricLabels }),
    });
  }

  return finalResponse;
}

// ---- LLM retry ----

/**
 * Retry policy for an agent's LLM calls. Failures are classified with
 * `classifyLLMError`: `transient` and `rate_limit` are retried with
 * exponential backoff, or after the server's `Retry-After` when it sent
 * one; `context_overflow` and `permanent` fail at once, as does a
 * cancelled call.
 */
export interface LlmRetryOptions {
  /** Retries after the first attempt. Default 3. */
  readonly maxRetries?: number;
  /** First backoff delay in ms; doubles per retry. Default 1_000. */
  readonly baseDelayMs?: number;
  /** Cap on one backoff delay in ms. Default 30_000. */
  readonly maxDelayMs?: number;
  /**
   * Longest server-requested wait (`LLMHttpError.retryAfterMs`) honored,
   * in ms. A longer one fails the call instead of parking the turn.
   * Default 60_000.
   */
  readonly maxRetryAfterMs?: number;
}

/** What a retry looks like to `onRetry` observers. */
export interface LlmRetryInfo {
  /** Retry number, 1 for the first retry. */
  readonly attempt: number;
  readonly delayMs: number;
  readonly errorClass: LLMErrorClass;
  readonly error: unknown;
}

const LLM_RETRY_DEFAULTS = {
  maxRetries: 3,
  baseDelayMs: 1_000,
  maxDelayMs: 30_000,
  maxRetryAfterMs: 60_000,
} as const;

/**
 * Run `run` and retry it per `retry` (see `LlmRetryOptions`). Waits run on
 * `clock`; an aborted `signal` ends a wait at once and stops retrying
 * (returning `onAborted()` when given).
 * `retry: false` runs once.
 */
export async function callLlmWithRetry<T>(params: {
  readonly run: () => Promise<T>;
  readonly retry: LlmRetryOptions | false;
  readonly clock: WallClock;
  readonly signal?: AbortSignal;
  readonly onRetry?: (info: LlmRetryInfo) => void;
  /**
   * Result to return when `signal` aborts during a backoff wait — the
   * cancel wins over the error being retried. Without it the error is
   * rethrown.
   */
  readonly onAborted?: () => T;
}): Promise<T> {
  const { run, retry, clock, signal } = params;
  for (let attempt = 0; ; attempt++) {
    try {
      return await run();
    } catch (error) {
      if (retry === false || signal?.aborted) throw error;
      const delayMs = llmRetryDelayMs({ error, attempt, retry });
      if (delayMs === undefined) throw error;
      params.onRetry?.({
        attempt: attempt + 1,
        delayMs,
        errorClass: classifyLLMError(error),
        error,
      });
      await waitOn({ clock, ms: delayMs, signal });
      if (signal?.aborted) {
        if (params.onAborted) return params.onAborted();
        throw error;
      }
    }
  }
}

/** Delay before retrying after `error`, or `undefined` to give up. */
function llmRetryDelayMs(params: {
  error: unknown;
  attempt: number;
  retry: LlmRetryOptions;
}): number | undefined {
  const { error, attempt, retry } = params;
  const maxRetries = retry.maxRetries ?? LLM_RETRY_DEFAULTS.maxRetries;
  if (attempt >= maxRetries) return undefined;
  const cls = classifyLLMError(error);
  if (cls !== "transient" && cls !== "rate_limit") return undefined;
  const retryAfterMs = error instanceof LLMHttpError ? error.retryAfterMs : undefined;
  if (retryAfterMs !== undefined) {
    const cap = retry.maxRetryAfterMs ?? LLM_RETRY_DEFAULTS.maxRetryAfterMs;
    return retryAfterMs <= cap ? retryAfterMs : undefined;
  }
  const base = retry.baseDelayMs ?? LLM_RETRY_DEFAULTS.baseDelayMs;
  const max = retry.maxDelayMs ?? LLM_RETRY_DEFAULTS.maxDelayMs;
  return Math.min(base * 2 ** attempt, max);
}

function waitOn(params: { clock: WallClock; ms: number; signal?: AbortSignal }): Promise<void> {
  const { clock, ms, signal } = params;
  return new Promise<void>((resolve) => {
    if (signal?.aborted || ms <= 0) {
      resolve();
      return;
    }
    const onAbort = () => {
      handle.clear();
      resolve();
    };
    const handle = clock.setTimeout(() => {
      signal?.removeEventListener("abort", onAbort);
      resolve();
    }, ms);
    signal?.addEventListener("abort", onAbort, { once: true });
  });
}

// ---- thinking redaction ----

/**
 * Strip extended-thinking blocks from assistant messages older than the
 * most recent user turn. Thinking is per-turn ephemeral state — useful
 * to the model while it picked the prior step, dead weight on the next
 * turn. Cuts input-token cost for any extended-thinking-enabled agent
 * (typically 10–30% on long sessions) without affecting reasoning
 * quality.
 *
 * Boundary: walk back from the end, find the most recent user message,
 * drop `thinkingBlocks` from every assistant message older than it.
 * The current turn's thinking is preserved so the model can see its
 * own reasoning while it plans the next step within the turn.
 *
 * Pure — returns a new array. Messages without thinkingBlocks are
 * shared by reference (no allocation when nothing to redact).
 */
export function redactPriorThinkingBlocks(messages: ReadonlyArray<Message>): Message[] {
  let mostRecentUserIdx = -1;
  for (let i = messages.length - 1; i >= 0; i--) {
    if (messages[i]!.role === "user") {
      mostRecentUserIdx = i;
      break;
    }
  }
  // No user message yet (e.g. system-only) → nothing to redact.
  if (mostRecentUserIdx <= 0) return [...messages];

  return messages.map((m, i) => {
    if (i >= mostRecentUserIdx) return m;
    if (m.role !== "assistant") return m;
    if (!m.thinkingBlocks || m.thinkingBlocks.length === 0) return m;
    const { thinkingBlocks: _, ...rest } = m;
    return rest as Message;
  });
}
