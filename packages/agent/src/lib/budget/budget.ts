// ---------------------------------------------------------------------------
// Token and cost budgets — the limits a turn, a delegation call tree, a
// thread or a namespace may spend, and the arithmetic over them.
//
// A `TurnBudget` caps what one turn (with everything it delegates to)
// spends; a `BudgetUsage` is what was spent. Usage of an LLM call is
// measured from the provider's reported usage, priced with the engine's
// `ModelCostRegistry`, and journaled with the call, so every budget
// decision is a pure function of journaled values and replays the same.
// ---------------------------------------------------------------------------

import { z } from "zod";
import type { LLMUsage } from "../llm-provider.ts";
import { computeCallCostUsd, type ModelCostRates } from "../metrics/types.ts";

const TokenCount = z.number().int().nonnegative();

/**
 * Limits on what a turn may spend. Every field is optional; an unset one
 * is unlimited. Tokens add up over every LLM call of the turn (each call
 * re-sends its prompt, so input tokens grow with the conversation):
 *
 * - `maxInputTokens` — prompt tokens, cache reads and writes included.
 * - `maxOutputTokens` — completion tokens.
 * - `maxTotalTokens` — input plus output.
 * - `maxCostUsd` — priced with the engine's `costs` registry; a turn
 *   under a cost limit must be able to price its model.
 * - `maxLlmCalls` — think steps plus compaction summaries.
 * - `maxToolCalls` — tool calls the turn runs (sub-agent calls included).
 */
export const TurnBudgetSchema = z
  .object({
    maxInputTokens: TokenCount.optional(),
    maxOutputTokens: TokenCount.optional(),
    maxTotalTokens: TokenCount.optional(),
    maxCostUsd: z.number().nonnegative().finite().optional(),
    maxLlmCalls: TokenCount.optional(),
    maxToolCalls: TokenCount.optional(),
  })
  .strict();

export type TurnBudget = z.infer<typeof TurnBudgetSchema>;

/** One limit of a `TurnBudget`. */
export type BudgetDimension = keyof TurnBudget;

const DIMENSIONS: ReadonlyArray<BudgetDimension> = [
  "maxInputTokens",
  "maxOutputTokens",
  "maxTotalTokens",
  "maxCostUsd",
  "maxLlmCalls",
  "maxToolCalls",
];

/** What was spent: by one LLM call, one turn, a thread or a namespace window. */
export interface BudgetUsage {
  /** Prompt tokens, excluding cache reads and writes (as providers report them). */
  readonly inputTokens: number;
  readonly outputTokens: number;
  readonly cacheReadTokens: number;
  readonly cacheWriteTokens: number;
  /** USD, priced per call; 0 for calls whose model has no rates. */
  readonly costUsd: number;
  readonly llmCalls: number;
  readonly toolCalls: number;
}

export const EMPTY_USAGE: BudgetUsage = {
  inputTokens: 0,
  outputTokens: 0,
  cacheReadTokens: 0,
  cacheWriteTokens: 0,
  costUsd: 0,
  llmCalls: 0,
  toolCalls: 0,
};

/** Validate a budget read from untrusted JSON (a run input, a recipe). */
export function parseTurnBudget(value: unknown): TurnBudget {
  const parsed = TurnBudgetSchema.safeParse(value);
  if (!parsed.success) {
    throw new Error(`invalid budget: ${parsed.error.issues.map((i) => i.message).join("; ")}`);
  }
  return parsed.data;
}

/** Validate usage read from untrusted JSON (a child's result); anything malformed counts as zero. */
export function usageOf(value: unknown): BudgetUsage {
  const raw = (value ?? {}) as Partial<Record<keyof BudgetUsage, unknown>>;
  const num = (v: unknown): number =>
    typeof v === "number" && Number.isFinite(v) && v > 0 ? v : 0;
  return {
    inputTokens: num(raw.inputTokens),
    outputTokens: num(raw.outputTokens),
    cacheReadTokens: num(raw.cacheReadTokens),
    cacheWriteTokens: num(raw.cacheWriteTokens),
    costUsd: num(raw.costUsd),
    llmCalls: num(raw.llmCalls),
    toolCalls: num(raw.toolCalls),
  };
}

export function addUsage(a: BudgetUsage, b: BudgetUsage): BudgetUsage {
  return {
    inputTokens: a.inputTokens + b.inputTokens,
    outputTokens: a.outputTokens + b.outputTokens,
    cacheReadTokens: a.cacheReadTokens + b.cacheReadTokens,
    cacheWriteTokens: a.cacheWriteTokens + b.cacheWriteTokens,
    costUsd: a.costUsd + b.costUsd,
    llmCalls: a.llmCalls + b.llmCalls,
    toolCalls: a.toolCalls + b.toolCalls,
  };
}

/** The usage of one LLM call, priced with `rates` (unpriced: cost 0). */
export function llmCallUsage(params: {
  readonly usage: LLMUsage | undefined;
  readonly rates: ModelCostRates | undefined;
}): BudgetUsage {
  const { usage, rates } = params;
  return {
    inputTokens: usage?.inputTokens ?? 0,
    outputTokens: usage?.outputTokens ?? 0,
    cacheReadTokens: usage?.cacheReadTokens ?? 0,
    cacheWriteTokens: usage?.cacheWriteTokens ?? 0,
    costUsd: usage ? (computeCallCostUsd(usage, rates) ?? 0) : 0,
    llmCalls: 1,
    toolCalls: 0,
  };
}

/** How much of `dimension` `usage` spent. */
export function spentOn(params: {
  readonly usage: BudgetUsage;
  readonly dimension: BudgetDimension;
}): number {
  const { usage } = params;
  const input = usage.inputTokens + usage.cacheReadTokens + usage.cacheWriteTokens;
  switch (params.dimension) {
    case "maxInputTokens":
      return input;
    case "maxOutputTokens":
      return usage.outputTokens;
    case "maxTotalTokens":
      return input + usage.outputTokens;
    case "maxCostUsd":
      return usage.costUsd;
    case "maxLlmCalls":
      return usage.llmCalls;
    case "maxToolCalls":
      return usage.toolCalls;
  }
}

/** A limit `usage` went past. */
export interface BudgetOverrun {
  readonly dimension: BudgetDimension;
  readonly limit: number;
  readonly used: number;
}

/**
 * The first limit of `budget` that `usage` is over (`used > limit`), or
 * with `exhausted`, the first one it has used up (`used >= limit`): no
 * room left to start anything.
 */
export function budgetOverrun(params: {
  readonly budget: TurnBudget | undefined;
  readonly usage: BudgetUsage;
  readonly exhausted?: boolean;
}): BudgetOverrun | undefined {
  const { budget, usage } = params;
  if (!budget) return undefined;
  for (const dimension of DIMENSIONS) {
    const limit = budget[dimension];
    if (limit === undefined) continue;
    const used = spentOn({ usage, dimension });
    if (params.exhausted ? used >= limit : used > limit) return { dimension, limit, used };
  }
  return undefined;
}

/** What is left of `budget` after `usage`: each set limit minus its spend, floored at 0. */
export function remainingBudget(params: {
  readonly budget: TurnBudget;
  readonly usage: BudgetUsage;
}): TurnBudget {
  const out: Partial<Record<BudgetDimension, number>> = {};
  for (const dimension of DIMENSIONS) {
    const limit = params.budget[dimension];
    if (limit === undefined) continue;
    out[dimension] = Math.max(0, limit - spentOn({ usage: params.usage, dimension }));
  }
  return out;
}

/** `budget` with `taken` set aside, limit by limit, floored at 0. */
export function subtractBudget(params: {
  readonly budget: TurnBudget;
  readonly taken: TurnBudget;
}): TurnBudget {
  const out: Partial<Record<BudgetDimension, number>> = {};
  for (const dimension of DIMENSIONS) {
    const limit = params.budget[dimension];
    if (limit === undefined) continue;
    out[dimension] = Math.max(0, limit - (params.taken[dimension] ?? 0));
  }
  return out;
}

/**
 * The most a turn under `budget` can have spent, as usage — what a turn
 * whose real spend was lost (it failed without a result) is charged.
 */
export function budgetAsUsage(budget: TurnBudget): BudgetUsage {
  const input = budget.maxInputTokens ?? 0;
  const output = budget.maxOutputTokens ?? 0;
  const total = budget.maxTotalTokens;
  return {
    ...EMPTY_USAGE,
    // `maxTotalTokens` bounds input + output; charged as input when no
    // finer limit is set.
    inputTokens: total !== undefined && input + output === 0 ? total : input,
    outputTokens: output,
    costUsd: budget.maxCostUsd ?? 0,
    llmCalls: budget.maxLlmCalls ?? 0,
    toolCalls: budget.maxToolCalls ?? 0,
  };
}

/** The tighter of two budgets, limit by limit; undefined when both are. */
export function tightenBudget(
  a: TurnBudget | undefined,
  b: TurnBudget | undefined,
): TurnBudget | undefined {
  if (!a) return b;
  if (!b) return a;
  const out: Partial<Record<BudgetDimension, number>> = {};
  for (const dimension of DIMENSIONS) {
    const x = a[dimension];
    const y = b[dimension];
    const limit = x === undefined ? y : y === undefined ? x : Math.min(x, y);
    if (limit !== undefined) out[dimension] = limit;
  }
  return out;
}

/**
 * One of `parts` equal shares of `budget`: token and call limits rounded
 * down, so the shares never add up to more than the whole.
 */
export function shareOfBudget(params: {
  readonly budget: TurnBudget;
  readonly parts: number;
}): TurnBudget {
  const parts = Math.max(1, Math.floor(params.parts));
  const out: Partial<Record<BudgetDimension, number>> = {};
  for (const dimension of DIMENSIONS) {
    const limit = params.budget[dimension];
    if (limit === undefined) continue;
    out[dimension] = dimension === "maxCostUsd" ? limit / parts : Math.floor(limit / parts);
  }
  return out;
}

/** Whether `budget` sets any limit. */
export function hasLimits(budget: TurnBudget | undefined): budget is TurnBudget {
  return budget !== undefined && DIMENSIONS.some((d) => budget[d] !== undefined);
}

/**
 * Smallest token allowance worth an LLM call: a call that could only
 * produce fewer output tokens than this is not made (the turn ends with
 * `budget_exceeded`), and a thread or namespace with less than this left
 * refuses new turns.
 */
export const DEFAULT_MIN_CALL_TOKENS = 256;

/**
 * The first token limit of `remaining` (input, output, total) below `min`,
 * or a call limit at 0: what is left can't hold a useful LLM call.
 */
export function tooLittleLeft(params: {
  readonly remaining: TurnBudget;
  readonly budget: TurnBudget;
  readonly usage: BudgetUsage;
  readonly min: number;
}): BudgetOverrun | undefined {
  const { remaining, budget, usage, min } = params;
  for (const dimension of ["maxInputTokens", "maxOutputTokens", "maxTotalTokens"] as const) {
    const left = remaining[dimension];
    if (left !== undefined && left < min) {
      return { dimension, limit: budget[dimension]!, used: spentOn({ usage, dimension }) };
    }
  }
  if (remaining.maxLlmCalls === 0) {
    return { dimension: "maxLlmCalls", limit: budget.maxLlmCalls!, used: usage.llmCalls };
  }
  return undefined;
}

/** Whether the next LLM call fits, and the output cap it must be sent with. */
export interface CallPlan {
  /** The limit the call can't fit under; null when it fits. */
  readonly overrun: BudgetOverrun | null;
  /**
   * The call's output-token cap: the configured `maxTokens`, lowered to what
   * the output, total and (priced) cost limits leave. Null: no cap.
   */
  readonly maxTokens: number | null;
}

/**
 * Plan the next LLM call under `budget`: refuse it when the spend so far
 * plus its prompt is over a limit or the output it could still produce is
 * below `minOutputTokens`; otherwise cap its output to what is left instead
 * of reserving the full `maxTokens`.
 */
export function planBudgetedCall(params: {
  readonly budget: TurnBudget;
  readonly spent: BudgetUsage;
  /** Estimated prompt tokens of the call. */
  readonly promptTokens: number;
  /** LLM calls the step makes (2 with a compaction summary). */
  readonly llmCalls: number;
  readonly rates: ModelCostRates | undefined;
  readonly maxTokens: number | undefined;
  readonly minOutputTokens: number;
}): CallPlan {
  const { budget, spent, rates, minOutputTokens } = params;
  const configured = params.maxTokens ?? null;
  const already = budgetOverrun({ budget, usage: spent });
  if (already) return { overrun: already, maxTokens: null };
  const projected = addUsage(spent, {
    ...llmCallUsage({ usage: { inputTokens: params.promptTokens, outputTokens: 0 }, rates }),
    llmCalls: params.llmCalls,
  });
  const promptOverrun = budgetOverrun({ budget, usage: projected });
  if (promptOverrun) return { overrun: promptOverrun, maxTokens: null };

  // The output the call may still produce, per limit.
  let allowance = Number.POSITIVE_INFINITY;
  let bound: BudgetDimension | undefined;
  const lower = (dimension: BudgetDimension, value: number): void => {
    if (value < allowance) {
      allowance = value;
      bound = dimension;
    }
  };
  if (budget.maxOutputTokens !== undefined) {
    lower("maxOutputTokens", budget.maxOutputTokens - projected.outputTokens);
  }
  if (budget.maxTotalTokens !== undefined) {
    lower(
      "maxTotalTokens",
      budget.maxTotalTokens - spentOn({ usage: projected, dimension: "maxTotalTokens" }),
    );
  }
  const outputRate = rates?.outputPerMillion;
  if (budget.maxCostUsd !== undefined && outputRate !== undefined && outputRate > 0) {
    lower(
      "maxCostUsd",
      Math.floor(((budget.maxCostUsd - projected.costUsd) * 1_000_000) / outputRate),
    );
  }
  if (bound === undefined) return { overrun: null, maxTokens: configured };
  if (allowance < minOutputTokens) {
    const limit = budget[bound]!;
    const outputCost = bound === "maxCostUsd" ? (minOutputTokens * outputRate!) / 1_000_000 : 0;
    const used =
      bound === "maxCostUsd"
        ? projected.costUsd + outputCost
        : spentOn({ usage: projected, dimension: bound }) + minOutputTokens;
    return { overrun: { dimension: bound, limit, used }, maxTokens: null };
  }
  const cap = Math.floor(allowance);
  return { overrun: null, maxTokens: configured === null ? cap : Math.min(configured, cap) };
}

/**
 * A cost budget whose model has no price: the turn can't measure its
 * spend, so it doesn't start.
 */
export class ModelUnpricedError extends Error {
  readonly code = "model_unpriced";
  constructor(
    readonly provider: string | undefined,
    readonly model: string | undefined,
  ) {
    super(
      `the turn has a cost budget (maxCostUsd) but its model has no price ` +
        `(${provider ?? "?"}/${model ?? "?"}): set \`costs\` with rates for ` +
        "`llmProvider` / `llmModel`",
    );
  }
}

/** Which budget refused a turn. */
export type BudgetScope = "thread" | "namespace";

/**
 * A turn refused before it started: the thread's cumulative budget or the
 * namespace's rolling-window budget is used up.
 */
export class BudgetExceededError extends Error {
  readonly code = "budget_exceeded";
  readonly scope: BudgetScope;
  readonly dimension: BudgetDimension;
  readonly limit: number;
  readonly used: number;
  constructor(params: { readonly scope: BudgetScope } & BudgetOverrun) {
    super(
      `${params.scope} budget exceeded: ${params.dimension} ${formatAmount(params)} of ` +
        `${params.limit} used`,
    );
    this.scope = params.scope;
    this.dimension = params.dimension;
    this.limit = params.limit;
    this.used = params.used;
  }
}

function formatAmount(overrun: BudgetOverrun): string {
  return overrun.dimension === "maxCostUsd" ? overrun.used.toFixed(4) : String(overrun.used);
}
