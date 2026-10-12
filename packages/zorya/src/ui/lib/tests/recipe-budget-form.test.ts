import { describe, expect, it } from "bun:test";
import {
  budgetMeters,
  buildBudget,
  formatBudgetAmount,
  initBudgetForm,
} from "../recipe-budget-form.ts";

const ZERO = {
  inputTokens: 0,
  outputTokens: 0,
  cacheReadTokens: 0,
  cacheWriteTokens: 0,
  costUsd: 0,
  llmCalls: 0,
  toolCalls: 0,
};

describe("recipe budget form", () => {
  it("round-trips a budget and omits blank fields", () => {
    const form = initBudgetForm({ maxTotalTokens: 5_000, maxCostUsd: 0.5 });
    expect(form.maxTotalTokens).toBe("5000");
    expect(form.maxLlmCalls).toBe("");
    expect(buildBudget(form)).toEqual({ budget: { maxTotalTokens: 5_000, maxCostUsd: 0.5 } });
    expect(buildBudget(initBudgetForm(undefined))).toEqual({ budget: undefined });
  });

  it("refuses negative, fractional counts and non-numbers", () => {
    expect(buildBudget({ ...initBudgetForm(undefined), maxLlmCalls: "1.5" })).toHaveProperty(
      "error",
    );
    expect(buildBudget({ ...initBudgetForm(undefined), maxCostUsd: "-1" })).toHaveProperty("error");
    expect(buildBudget({ ...initBudgetForm(undefined), maxTotalTokens: "lots" })).toHaveProperty(
      "error",
    );
    expect(buildBudget({ ...initBudgetForm(undefined), maxCostUsd: "0.25" })).toEqual({
      budget: { maxCostUsd: 0.25 },
    });
  });
});

describe("budget meters", () => {
  it("shows each set limit against its spend, fullest first", () => {
    const meters = budgetMeters({
      spent: { ...ZERO, inputTokens: 800, cacheReadTokens: 100, outputTokens: 100, costUsd: 0.1 },
      limits: { maxTotalTokens: 2_000, maxCostUsd: 0.2, maxLlmCalls: 0 },
    });
    expect(meters.map((m) => [m.label, m.used, m.limit, m.ratio])).toEqual([
      ["Max LLM calls", 0, 0, 1],
      ["Max total tokens", 1_000, 2_000, 0.5],
      ["Max cost (USD)", 0.1, 0.2, 0.5],
    ]);
    expect(budgetMeters({ spent: ZERO, limits: undefined })).toEqual([]);
  });

  it("formats amounts", () => {
    expect(formatBudgetAmount(0.123, true)).toBe("$0.12");
    expect(formatBudgetAmount(12_345, false)).toBe("12.3k");
    expect(formatBudgetAmount(2_500_000, false)).toBe("2.5M");
    expect(formatBudgetAmount(42, false)).toBe("42");
  });
});
