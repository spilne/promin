// ---------------------------------------------------------------------------
// budget — score a run against latency / cost / token ceilings.
//
// Reads `EvalOutput.metrics`. The score is the fraction of configured
// limits the run stayed within; a limit whose metric is unavailable (e.g.
// `maxCostUsd` with no cost recorded) is skipped rather than failed.
// ---------------------------------------------------------------------------

import type { Scorer } from "../types.ts";

export interface BudgetConfig {
  /** Ceiling on `metrics.latencyMs`. */
  readonly maxLatencyMs?: number;
  /** Ceiling on `metrics.costUsd` — skipped when cost is not recorded. */
  readonly maxCostUsd?: number;
  /** Ceiling on `inputTokens + outputTokens` — skipped when tokens are absent. */
  readonly maxTotalTokens?: number;
  /** Scorer id — default `"budget"`. */
  readonly id?: string;
  readonly threshold?: number;
  readonly required?: boolean;
}

/** Build a scorer that judges a run against resource ceilings. */
export function budget(config: BudgetConfig): Scorer {
  const id = config.id ?? "budget";
  return {
    id,
    threshold: config.threshold ?? 1,
    ...(config.required !== undefined && { required: config.required }),
    async score({ output }) {
      const { metrics } = output;
      let total = 0;
      let met = 0;
      const breaches: string[] = [];
      const check = (ok: boolean, breach: string): void => {
        total += 1;
        if (ok) met += 1;
        else breaches.push(breach);
      };

      if (config.maxLatencyMs !== undefined) {
        check(
          metrics.latencyMs <= config.maxLatencyMs,
          `latency ${metrics.latencyMs}ms > ${config.maxLatencyMs}ms`,
        );
      }
      if (config.maxCostUsd !== undefined && metrics.costUsd !== undefined) {
        check(
          metrics.costUsd <= config.maxCostUsd,
          `cost ${metrics.costUsd} > ${config.maxCostUsd}`,
        );
      }
      if (
        config.maxTotalTokens !== undefined &&
        metrics.inputTokens !== undefined &&
        metrics.outputTokens !== undefined
      ) {
        const tokens = metrics.inputTokens + metrics.outputTokens;
        check(tokens <= config.maxTotalTokens, `${tokens} tokens > ${config.maxTotalTokens}`);
      }

      if (total === 0) {
        return { scorerId: id, value: 1, reason: "no budget limits applied to the run's metrics" };
      }
      const value = met / total;
      return {
        scorerId: id,
        value,
        ...(value < 1 && { reason: `over budget: ${breaches.join("; ")}` }),
      };
    },
  };
}
