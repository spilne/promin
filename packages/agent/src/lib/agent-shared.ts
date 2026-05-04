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
import { recordChat } from "./metrics/instrument.ts";
import type { ToolRegistry } from "./tool-registry.ts";
import type { MemoryIndex, MemoryScope } from "./memory-index.ts";

// ---- message utilities ----

export function systemMsgs(messages: Message[]): Message[] {
  return messages.filter((m) => m.role === "system");
}

export function nonSystemMsgs(messages: Message[]): Message[] {
  return messages.filter((m) => m.role !== "system");
}

// ---- tool resolution ----

export interface MemoryInjectionConfig {
  store: MemoryIndex;
  scope?: MemoryScope;
  injectLimit?: number;
  searchQuery?: string;
}

// biome-ignore lint/suspicious/noExplicitAny: tool registry uses runtime Zod validation
export function resolveTools(config: {
  toolRegistry?: ToolRegistry;
  tools?: Record<string, AgentTool<any, any>>;
  // biome-ignore lint/suspicious/noExplicitAny: tool registry uses runtime Zod validation
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

// ---- memory injection ----

export async function searchRelevantMemories(
  memory: MemoryInjectionConfig,
  defaultQuery: string,
): Promise<Message[]> {
  const limit = memory.injectLimit ?? 5;
  if (limit <= 0) return [];
  const results = await memory.store.search(
    memory.searchQuery ?? defaultQuery,
    limit,
    memory.scope,
  );
  if (results.length === 0) return [];
  const block = results.map((r) => `- ${r.content}`).join("\n");
  return [{ role: "system" as const, content: `Relevant context from memory:\n${block}` }];
}

// ---- agent error surfacing ----

// Effect wraps thrown errors inside journaled steps in a FiberFailure.
// WorkflowSuspendedError is a normal signal — not a real failure. Validated
// against Effect 3.x; the cause symbol is a stable public API.
const FIBER_FAILURE_CAUSE = Symbol.for("effect/Runtime/FiberFailure/Cause");

type FiberFailureCause =
  | { _tag: "Die"; defect: unknown }
  | { _tag: "Fail"; error: unknown }
  | { _tag: string };

function getFiberFailureCause(error: unknown): FiberFailureCause | undefined {
  if (!error || typeof error !== "object") return undefined;
  return (error as Record<symbol, FiberFailureCause | undefined>)[FIBER_FAILURE_CAUSE];
}

/**
 * Unwrap an Effect FiberFailure to the underlying thrown error. Returns the
 * input unchanged when there's no FiberFailure cause.
 */
export function unwrapFiberFailure(error: unknown): unknown {
  const cause = getFiberFailureCause(error);
  if (cause?._tag === "Die") return (cause as { defect: unknown }).defect;
  if (cause?._tag === "Fail") return (cause as { error: unknown }).error;
  return error;
}

/**
 * `true` when the error is a `WorkflowSuspendedError` — either as a direct
 * tagged error or wrapped in an Effect FiberFailure defect. Suspension is a
 * normal control signal; callers typically swallow it instead of propagating.
 */
export function isWorkflowSuspension(error: unknown): boolean {
  if (!error || typeof error !== "object") return false;
  if ((error as { _tag?: string })._tag === "WorkflowSuspendedError") return true;
  const cause = getFiberFailureCause(error);
  return (
    cause?._tag === "Die" &&
    (cause as { defect?: { _tag?: string } }).defect?._tag === "WorkflowSuspendedError"
  );
}

/**
 * Classification produced by `surfaceAgentError`.
 * - `suspended`: WorkflowSuspendedError — the body is waiting on a signal
 *   or sleep. Not a failure.
 * - `step-limit`: MaxStepsError from `agentAction` (the step loop ran out
 *   without a final answer).
 * - `user-error`: thrown by user code, a tool, a hook, or
 *   `TerminalError` — won't succeed on retry.
 * - `infra-error`: framework / transient failures
 *   (`RetryableError`, FiberFailure with a non-Error defect, etc.).
 */
export type SurfacedAgentErrorKind = "suspended" | "step-limit" | "user-error" | "infra-error";

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
 * Classify and unwrap an error from any agent primitive (agentLoop /
 * agentAction / agentTown). Strips Effect FiberFailure wrappers so callers
 * see the original thrown value, and tags the kind so call sites can
 * branch (suspend vs surface to user vs infra-retry) without re-implementing
 * the same `instanceof` chain.
 *
 * Preserves tagged-error metadata: a `TerminalError` keeps its `_tag`, the
 * underlying `MaxStepsError` keeps its `maxSteps`. The returned `error.name`
 * matches the original constructor when one is present.
 */
export function surfaceAgentError(err: unknown): SurfacedAgentError {
  if (isWorkflowSuspension(err)) return { kind: "suspended", error: toError(err) };
  const unwrapped = unwrapFiberFailure(err);
  const tag = (unwrapped as { _tag?: string } | null | undefined)?._tag;
  if (tag === "MaxStepsError") return { kind: "step-limit", error: toError(unwrapped) };
  if (tag === "TerminalError") return { kind: "user-error", error: toError(unwrapped) };
  if (tag === "RetryableError") return { kind: "infra-error", error: toError(unwrapped) };
  if (unwrapped instanceof Error) return { kind: "user-error", error: unwrapped };
  return { kind: "infra-error", error: toError(unwrapped) };
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

// Hard cap on any single tool result — prevents a runaway listing or read
// from blowing up the LLM context window.
const MAX_TOOL_CONTENT_CHARS = 200_000;

// biome-ignore lint/suspicious/noExplicitAny: tool registry uses runtime Zod validation
export async function executeToolCall(
  call: ToolCall,
  toolDef: AgentTool<any, any>,
  onResult?: (call: ToolCall, output: unknown) => void,
  /**
   * Optional progress sink. When passed, executeToolCall builds a
   * ToolWriter that forwards each `write(payload)` call to this
   * callback. The agent loop wires the callback to emit a
   * `tool.progress` SessionEvent labeled with turn / step / toolCallId.
   */
  onProgress?: (payload: unknown) => void,
  /**
   * Optional caller scope (namespace, resource, thread, agentId).
   * Populated by the agent runtime per-call so scope-aware tools
   * (durable scheduler, secrets, audit) can act on behalf of the
   * live caller. Surfaced on `ctx.scope`.
   */
  scope?: import("./tool.ts").ToolScope,
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
    const ctx =
      onProgress || scope
        ? {
            ...(onProgress && { writer: { write: onProgress } }),
            ...(scope && { scope }),
          }
        : undefined;
    const output = await toolDef.execute(parsed as any, ctx);
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
  metrics,
  costs,
  provider,
  model,
  metricLabels,
}: RunLlmCallParams): Promise<LLMResponse> {
  const callStart = metrics ? Date.now() : 0;
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
      durationMs: Date.now() - callStart,
      ...(metricLabels && { extraLabels: metricLabels }),
    });
  }

  return finalResponse;
}
