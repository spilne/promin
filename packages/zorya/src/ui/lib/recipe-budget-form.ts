// ---------------------------------------------------------------------------
// recipe-budget-form — the agent edit-drawer's "Turn budget" fields
// (`backend.budget`) as form state, and the spend-vs-limit meters the chat
// header shows from the usage routes.
//
// Fields are strings so a blank one (no limit) stays distinct from "0".
// ---------------------------------------------------------------------------

import type { BudgetDimension, BudgetUsage, TurnBudget } from "@promin/agent/browser";

export type BudgetForm = Record<BudgetDimension, string>;

/** The fields in display order, with their labels. */
export const BUDGET_FIELDS: ReadonlyArray<{ key: BudgetDimension; label: string }> = [
  { key: "maxTotalTokens", label: "Max total tokens" },
  { key: "maxInputTokens", label: "Max input tokens" },
  { key: "maxOutputTokens", label: "Max output tokens" },
  { key: "maxCostUsd", label: "Max cost (USD)" },
  { key: "maxLlmCalls", label: "Max LLM calls" },
  { key: "maxToolCalls", label: "Max tool calls" },
];

export function initBudgetForm(budget: TurnBudget | undefined): BudgetForm {
  const form = {} as BudgetForm;
  for (const { key } of BUDGET_FIELDS) {
    const value = budget?.[key];
    form[key] = value === undefined ? "" : String(value);
  }
  return form;
}

/**
 * The recipe's `budget` from the form: undefined when every field is
 * blank, an error naming the first field that is not a valid limit.
 */
export function buildBudget(
  form: BudgetForm,
): { budget: TurnBudget | undefined } | { error: string } {
  const budget: Partial<Record<BudgetDimension, number>> = {};
  for (const { key, label } of BUDGET_FIELDS) {
    const raw = form[key].trim();
    if (!raw) continue;
    const value = Number(raw);
    const whole = key !== "maxCostUsd";
    if (!Number.isFinite(value) || value < 0 || (whole && !Number.isInteger(value))) {
      return {
        error: `${label}: expected ${whole ? "a whole number" : "a number"} of 0 or more`,
      };
    }
    budget[key] = value;
  }
  return { budget: Object.keys(budget).length > 0 ? budget : undefined };
}

/** What a budget dimension has spent, from a `BudgetUsage`. */
export function spentOn(usage: BudgetUsage, dimension: BudgetDimension): number {
  const input = usage.inputTokens + usage.cacheReadTokens + usage.cacheWriteTokens;
  switch (dimension) {
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

export interface BudgetMeter {
  readonly label: string;
  readonly used: number;
  readonly limit: number;
  /** used / limit, capped at 1. */
  readonly ratio: number;
  readonly cost: boolean;
}

/** One meter per limit that is set, fullest first. */
export function budgetMeters(params: {
  readonly spent: BudgetUsage;
  readonly limits?: TurnBudget | undefined;
}): BudgetMeter[] {
  const { spent, limits } = params;
  if (!limits) return [];
  const meters: BudgetMeter[] = [];
  for (const { key, label } of BUDGET_FIELDS) {
    const limit = limits[key];
    if (limit === undefined) continue;
    const used = spentOn(spent, key);
    meters.push({
      label,
      used,
      limit,
      ratio: limit === 0 ? 1 : Math.min(1, used / limit),
      cost: key === "maxCostUsd",
    });
  }
  return meters.sort((a, b) => b.ratio - a.ratio);
}

/** A meter's amount as text: dollars to the cent, counts compacted (12.3k). */
export function formatBudgetAmount(value: number, cost: boolean): string {
  if (cost) return `$${value.toFixed(2)}`;
  if (value >= 1_000_000) return `${(value / 1_000_000).toFixed(1)}M`;
  if (value >= 1_000) return `${(value / 1_000).toFixed(1)}k`;
  return String(value);
}
