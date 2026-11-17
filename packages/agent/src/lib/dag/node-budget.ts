import { shareOfBudget, type BudgetDimension, type TurnBudget } from "../budget/budget.ts";
import type { DagNode } from "./types.ts";

const DIMENSIONS: ReadonlyArray<BudgetDimension> = [
  "maxInputTokens",
  "maxOutputTokens",
  "maxTotalTokens",
  "maxCostUsd",
  "maxLlmCalls",
  "maxToolCalls",
];

/** How many equal parts of the free budget a node not yet started stands for. */
export function waitingWeight(node: DagNode): number {
  return node.kind === "transform" ? 0 : 1;
}

/** The share of a node starting now. */
export function startShare(params: {
  readonly free: TurnBudget;
  readonly weight: number;
  readonly othersWaiting: number;
}): TurnBudget {
  const weight = Math.max(0, Math.floor(params.weight));
  const perPart = shareOfBudget({
    budget: params.free,
    parts: weight + Math.max(0, Math.floor(params.othersWaiting)),
  });
  const out: Partial<Record<BudgetDimension, number>> = {};
  for (const dimension of DIMENSIONS) {
    const limit = perPart[dimension];
    if (limit !== undefined) out[dimension] = limit * weight;
  }
  return out;
}

/** One of `parts` equal shares of a node's share. */
export function splitShare(params: {
  readonly share: TurnBudget;
  readonly parts: number;
}): TurnBudget {
  return shareOfBudget({ budget: params.share, parts: params.parts });
}

/** The first limit that leaves no room to start. */
export function emptyDimension(share: TurnBudget): BudgetDimension | undefined {
  return DIMENSIONS.find((dimension) => share[dimension] === 0);
}
