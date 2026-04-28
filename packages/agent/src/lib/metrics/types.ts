// ---------------------------------------------------------------------------
// AgentMetrics — pluggable telemetry sink for the agent runtime.
//
// Why an interface instead of a hardcoded backend
// -----------------------------------------------
// The agent ships in three deployment shapes that need different
// observability:
//
//   1. Server / multi-pod: scrape Prometheus. Wants real counters +
//      histograms + labels. Buy with `PrometheusAgentMetrics` adapter.
//   2. Hosted SaaS: pipe to OpenTelemetry / Datadog / Honeycomb. Wants
//      the same shape, different sink.
//   3. Embedded / edge: no metrics, no allocations, no overhead. Wants
//      a real no-op so call sites stay clean and the cost is zero.
//
// One hardcoded backend forces every consumer to either pay for the
// dependency they don't want or fork the code. The interface lets each
// host wire whatever it has.
//
// What the interface tracks
// -------------------------
// Per-call instrumentation is centralized in `recordChat` (below) so
// adding a new label or counter is a one-file change. Default counters
// match what ai_coach exposes after a year of production tuning:
//
//   - llm.tokens.input / output / cache.read / cache.write — labeled by
//     provider, model. Cache visibility is the main observability gap
//     vs raw token counts (a prompt-cache hit should show up as cheaper
//     than a fresh call, not as zero tokens).
//   - llm.latency.ms — histogram, labeled by provider, model.
//   - llm.cost.usd — counter, labeled by provider, model. Computed from
//     the token counts × per-model cost rates from `ModelCostRegistry`.
//   - tool.calls / tool.errors — labeled by tool name. Tool errors
//     don't always crash the turn (the model recovers via retry); the
//     counter makes them visible regardless.
//   - approval.requests / approval.decisions — labeled by outcome.
//
// Cost rates are configured per-model. Hardcoded prices go stale fast,
// so the registry is loaded by the host (typically from a YAML or env
// at boot) and threaded into the metrics adapter.
// ---------------------------------------------------------------------------

import type { LLMUsage } from "../llm-provider.ts";

/** Label set carried alongside every metric increment. */
export type MetricLabels = Readonly<Record<string, string | number>>;

/**
 * Counter — monotonically increasing scalar. Use for events you want
 * to count (calls, errors, decisions) and for monetary totals like
 * cost-in-USD where the absolute trend matters more than rate of
 * change.
 */
export interface Counter {
  inc(value?: number, labels?: MetricLabels): void;
}

/**
 * Histogram — distribution of observed values. Use for latency,
 * payload sizes, cost-per-call. Implementations choose buckets.
 */
export interface Histogram {
  observe(value: number, labels?: MetricLabels): void;
}

/**
 * Pluggable telemetry sink. The agent runtime consults `metric`s by
 * name; the host wires an implementation that returns real Counter /
 * Histogram instances (Prometheus, OTel) or no-ops.
 */
export interface AgentMetrics {
  counter(name: string): Counter;
  histogram(name: string): Histogram;
}

// ---------------------------------------------------------------------------
// No-op metrics — the safe default. Every method is a fast no-op so
// consumers can leave instrumentation in place without paying when
// metrics aren't configured.
// ---------------------------------------------------------------------------

const NOOP_COUNTER: Counter = { inc: () => {} };
const NOOP_HISTOGRAM: Histogram = { observe: () => {} };

export const NoopAgentMetrics: AgentMetrics = {
  counter: () => NOOP_COUNTER,
  histogram: () => NOOP_HISTOGRAM,
};

// ---------------------------------------------------------------------------
// In-memory metrics — useful for tests and the dashboard demo. Keeps
// every observation in arrays under the metric name. Not suitable for
// production (no aggregation, unbounded growth).
// ---------------------------------------------------------------------------

interface InMemoryCounterRow {
  total: number;
  samples: Array<{ value: number; labels: MetricLabels }>;
}

interface InMemoryHistogramRow {
  count: number;
  sum: number;
  samples: Array<{ value: number; labels: MetricLabels }>;
}

export class InMemoryAgentMetrics implements AgentMetrics {
  private readonly counters = new Map<string, InMemoryCounterRow>();
  private readonly histograms = new Map<string, InMemoryHistogramRow>();

  counter(name: string): Counter {
    let row = this.counters.get(name);
    if (!row) {
      row = { total: 0, samples: [] };
      this.counters.set(name, row);
    }
    return {
      inc: (value = 1, labels = {}) => {
        row!.total += value;
        row!.samples.push({ value, labels });
      },
    };
  }

  histogram(name: string): Histogram {
    let row = this.histograms.get(name);
    if (!row) {
      row = { count: 0, sum: 0, samples: [] };
      this.histograms.set(name, row);
    }
    return {
      observe: (value, labels = {}) => {
        row!.count += 1;
        row!.sum += value;
        row!.samples.push({ value, labels });
      },
    };
  }

  /** Total observations for a counter, summed across all label sets. */
  counterTotal(name: string): number {
    return this.counters.get(name)?.total ?? 0;
  }

  /** Sum of histogram observations across all label sets. */
  histogramSum(name: string): number {
    return this.histograms.get(name)?.sum ?? 0;
  }

  /** Snapshot of every counter sample (label inspection in tests). */
  counterSamples(name: string): ReadonlyArray<{ value: number; labels: MetricLabels }> {
    return this.counters.get(name)?.samples ?? [];
  }

  /** Snapshot of every histogram sample. */
  histogramSamples(name: string): ReadonlyArray<{ value: number; labels: MetricLabels }> {
    return this.histograms.get(name)?.samples ?? [];
  }

  reset(): void {
    this.counters.clear();
    this.histograms.clear();
  }
}

// ---------------------------------------------------------------------------
// Cost calculation — per-model rates registry.
// ---------------------------------------------------------------------------

/**
 * Per-million-token rates for a model. All four fields are optional;
 * undefined means "no cost recorded for this dimension".
 */
export interface ModelCostRates {
  /** USD per million input tokens (non-cached). */
  readonly inputPerMillion?: number;
  /** USD per million output tokens. */
  readonly outputPerMillion?: number;
  /** USD per million cache-read tokens (Anthropic-style prompt caching). */
  readonly cacheReadPerMillion?: number;
  /** USD per million cache-write tokens. */
  readonly cacheWritePerMillion?: number;
}

/**
 * Registry of cost rates keyed by `${provider}/${modelId}`. Hosts load
 * this from config — hardcoded prices go stale fast.
 *
 * Lookup misses return undefined; cost calculation falls through to
 * "unknown" (no metric emitted, no fake number recorded).
 */
export interface ModelCostRegistry {
  rates(provider: string, modelId: string): ModelCostRates | undefined;
}

/**
 * Compose a registry from a static map. Use for testing or when costs
 * are known at boot.
 *
 * @example
 *   const registry = staticCostRegistry({
 *     "anthropic/claude-sonnet-4-6": {
 *       inputPerMillion: 3.00,
 *       outputPerMillion: 15.00,
 *       cacheReadPerMillion: 0.30,
 *       cacheWritePerMillion: 3.75,
 *     },
 *   });
 */
export function staticCostRegistry(
  table: Readonly<Record<string, ModelCostRates>>,
): ModelCostRegistry {
  return {
    rates: (provider, modelId) => table[`${provider}/${modelId}`],
  };
}

/**
 * Compute total USD spent on a chat call from token usage + per-model
 * rates. Returns undefined when the model isn't in the registry — never
 * fabricates a number.
 */
export function computeCallCostUsd(
  usage: LLMUsage,
  rates: ModelCostRates | undefined,
): number | undefined {
  if (!rates) return undefined;
  let total = 0;
  if (rates.inputPerMillion !== undefined) {
    total += (usage.inputTokens * rates.inputPerMillion) / 1_000_000;
  }
  if (rates.outputPerMillion !== undefined) {
    total += (usage.outputTokens * rates.outputPerMillion) / 1_000_000;
  }
  if (rates.cacheReadPerMillion !== undefined && usage.cacheReadTokens !== undefined) {
    total += (usage.cacheReadTokens * rates.cacheReadPerMillion) / 1_000_000;
  }
  if (rates.cacheWritePerMillion !== undefined && usage.cacheWriteTokens !== undefined) {
    total += (usage.cacheWriteTokens * rates.cacheWritePerMillion) / 1_000_000;
  }
  return total;
}
