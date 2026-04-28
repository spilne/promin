// ---------------------------------------------------------------------------
// `recordChat` — single instrumentation point for an LLM call. Called
// from `runLlmCall` once the response is in hand. Centralized so adding
// a new counter or label is a one-file change instead of a sweep across
// every adapter.
//
// The metric names mirror what ai_coach exposes after a year of
// production tuning. Anything renamed here breaks dashboards.
//
// Names:
//   llm.tokens.input          counter, labeled { provider, model }
//   llm.tokens.output         counter, labeled { provider, model }
//   llm.tokens.cache.read     counter, labeled { provider, model }
//   llm.tokens.cache.write    counter, labeled { provider, model }
//   llm.calls                 counter, labeled { provider, model, finish_reason }
//   llm.latency.ms            histogram, labeled { provider, model }
//   llm.cost.usd              counter, labeled { provider, model }
// ---------------------------------------------------------------------------

import type { LLMResponse, LLMUsage } from "../llm-provider.ts";
import { computeCallCostUsd, type AgentMetrics, type ModelCostRegistry } from "./types.ts";

export interface RecordChatArgs {
  readonly metrics: AgentMetrics;
  readonly costs?: ModelCostRegistry;
  readonly provider: string;
  readonly model: string;
  readonly response: LLMResponse;
  /** Wall-clock duration of the LLM call. */
  readonly durationMs: number;
  /** Extra labels to attach to every metric (e.g. agent id, mode). */
  readonly extraLabels?: Readonly<Record<string, string>>;
}

export function recordChat(args: RecordChatArgs): void {
  const labels = {
    provider: args.provider,
    model: args.model,
    ...args.extraLabels,
  };

  args.metrics
    .counter("llm.calls")
    .inc(1, { ...labels, finish_reason: args.response.finishReason ?? "unknown" });
  args.metrics.histogram("llm.latency.ms").observe(args.durationMs, labels);

  if (args.response.usage) {
    recordTokens(args.metrics, args.response.usage, labels);

    if (args.costs) {
      const rates = args.costs.rates(args.provider, args.model);
      const usd = computeCallCostUsd(args.response.usage, rates);
      if (usd !== undefined) {
        args.metrics.counter("llm.cost.usd").inc(usd, labels);
      }
    }
  }
}

function recordTokens(
  metrics: AgentMetrics,
  usage: LLMUsage,
  labels: Readonly<Record<string, string>>,
): void {
  metrics.counter("llm.tokens.input").inc(usage.inputTokens, labels);
  metrics.counter("llm.tokens.output").inc(usage.outputTokens, labels);
  if (usage.cacheReadTokens !== undefined && usage.cacheReadTokens > 0) {
    metrics.counter("llm.tokens.cache.read").inc(usage.cacheReadTokens, labels);
  }
  if (usage.cacheWriteTokens !== undefined && usage.cacheWriteTokens > 0) {
    metrics.counter("llm.tokens.cache.write").inc(usage.cacheWriteTokens, labels);
  }
}

// ---------------------------------------------------------------------------
// Tool instrumentation — same shape, called from the tool execution
// path. Tool errors don't always crash the turn (the model recovers),
// so we count them separately from successful calls.
// ---------------------------------------------------------------------------

export interface RecordToolArgs {
  readonly metrics: AgentMetrics;
  readonly tool: string;
  readonly durationMs: number;
  readonly ok: boolean;
  readonly extraLabels?: Readonly<Record<string, string>>;
}

export function recordTool(args: RecordToolArgs): void {
  const labels = { tool: args.tool, ...args.extraLabels };
  args.metrics.counter("tool.calls").inc(1, labels);
  if (!args.ok) {
    args.metrics.counter("tool.errors").inc(1, labels);
  }
  args.metrics.histogram("tool.latency.ms").observe(args.durationMs, labels);
}

// ---------------------------------------------------------------------------
// Approval instrumentation. ai_coach tracks consent decisions per-skill;
// we keep it generic — the host attaches outcome labels.
// ---------------------------------------------------------------------------

export function recordApproval(
  metrics: AgentMetrics,
  outcome: "requested" | "approved" | "rejected" | "timeout",
  extraLabels: Readonly<Record<string, string>> = {},
): void {
  if (outcome === "requested") {
    metrics.counter("approval.requests").inc(1, extraLabels);
    return;
  }
  metrics.counter("approval.decisions").inc(1, { ...extraLabels, outcome });
}
