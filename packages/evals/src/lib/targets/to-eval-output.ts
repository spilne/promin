// ---------------------------------------------------------------------------
// toEvalOutput — adapt an AgentRunOutput into an EvalOutput.
//
// Shared by `recipeTarget` and (later) live scoring: it builds the run
// trace via `buildAgentTrace`, computes cost from token usage + per-model
// rates, and surfaces a finished-in-error run as `EvalOutput.error`.
// ---------------------------------------------------------------------------

import { buildAgentTrace, computeCallCostUsd } from "@promin/agent";
import type { AgentRunOutput, ModelCostRates } from "@promin/agent";
import type { EvalMetrics, EvalOutput } from "../types.ts";

export interface ToEvalOutputOpts {
  /** Wall-clock duration of the run, measured by the caller. */
  readonly latencyMs: number;
  /** Per-model cost rates — when supplied, `metrics.costUsd` is computed. */
  readonly rates?: ModelCostRates;
}

/** Map a completed `AgentRunOutput` to the eval framework's `EvalOutput`. */
export async function toEvalOutput(
  run: AgentRunOutput,
  opts: ToEvalOutputOpts,
): Promise<EvalOutput> {
  const [text, structured, usage, messages, finishReason] = await Promise.all([
    run.text,
    run.output,
    run.usage,
    run.messages,
    run.finishReason,
  ]);

  const costUsd = computeCallCostUsd(usage, opts.rates);
  const metrics: EvalMetrics = {
    latencyMs: opts.latencyMs,
    inputTokens: usage.inputTokens,
    outputTokens: usage.outputTokens,
    ...(costUsd !== undefined && { costUsd }),
  };

  // Structured output wins when the agent declared an outputSchema; the
  // final answer text is the fallback.
  return {
    output: structured !== undefined ? structured : text,
    trace: buildAgentTrace(messages),
    metrics,
    ...(finishReason === "error" && { error: "agent run finished with reason: error" }),
  };
}
