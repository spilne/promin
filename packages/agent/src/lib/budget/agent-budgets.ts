// ---------------------------------------------------------------------------
// Thread and namespace budgets — limits on what a conversation, and all of
// a tenant's turns over a rolling window, may spend.
//
// Enforced at turn start: a turn whose thread or namespace has used up a
// limit — or has less left than one useful LLM call — is refused
// (`BudgetExceededError`); an admitted turn gets what is
// left as its turn budget's ceiling (`AgentInput.budget`), so it ends with
// `budget_exceeded` instead of spending past it. Its spend is recorded in
// the `UsageLedger` when it commits, once per turn workflow id.
//
// Concurrent turns of one namespace are each admitted against the spend
// recorded so far and each capped by what was left at their start, so
// together they can overshoot a namespace window by at most what the
// turns in flight were allowed; the ledger's records themselves are
// atomic, so no turn's spend is lost or counted twice.
// ---------------------------------------------------------------------------

import {
  budgetOverrun,
  BudgetExceededError,
  DEFAULT_MIN_CALL_TOKENS,
  tooLittleLeft,
  hasLimits,
  remainingBudget,
  tightenBudget,
  type BudgetUsage,
  type TurnBudget,
} from "./budget.ts";
import type { UsageLedger } from "./usage-ledger.ts";

/** A rolling-window limit on what one namespace's turns spend. */
export interface NamespaceBudget {
  readonly limits: TurnBudget;
  /** Window length in ms (e.g. `86_400_000` for a day). */
  readonly windowMs: number;
}

export interface AgentBudgets {
  /** Where turns' spend is recorded and summed. */
  readonly ledger: UsageLedger;
  /** Cumulative limit on what one thread's turns spend. */
  readonly thread?: TurnBudget;
  /** Rolling-window limit per namespace: one for all, or picked per namespace. */
  readonly namespace?:
    | NamespaceBudget
    | ((namespaceId: string) => NamespaceBudget | undefined | Promise<NamespaceBudget | undefined>);
  /**
   * A thread or namespace with fewer tokens than this left (input, output
   * or total) refuses new turns, like one that used its budget up. Keep it
   * at the agents' `minCallTokens` (both default `DEFAULT_MIN_CALL_TOKENS`),
   * so an admitted turn can always make its first call.
   */
  readonly minCallTokens?: number;
}

/** A budget's limits next to what was spent against them. */
export interface BudgetStanding {
  readonly limits: TurnBudget;
  readonly spent: BudgetUsage;
}

/** Thread and namespace standings, for the limits that are set. */
export interface BudgetStatus {
  readonly thread?: BudgetStanding;
  readonly namespace?: BudgetStanding & { readonly windowMs: number };
}

async function namespaceBudget(params: {
  readonly budgets: AgentBudgets;
  readonly namespaceId: string;
}): Promise<NamespaceBudget | undefined> {
  const { namespace } = params.budgets;
  const resolved =
    typeof namespace === "function" ? await namespace(params.namespaceId) : namespace;
  return resolved && hasLimits(resolved.limits) ? resolved : undefined;
}

/** Where a thread (or, without `threadId`, a namespace) stands against its budgets. */
export async function budgetStatus(params: {
  readonly budgets: AgentBudgets;
  readonly namespaceId: string;
  readonly threadId?: string;
}): Promise<BudgetStatus> {
  const { budgets, namespaceId, threadId } = params;
  const ns = await namespaceBudget({ budgets, namespaceId });
  const [threadSpent, namespaceSpent] = await Promise.all([
    threadId !== undefined && hasLimits(budgets.thread)
      ? budgets.ledger.total({ namespaceId, threadId })
      : undefined,
    ns ? budgets.ledger.total({ namespaceId, windowMs: ns.windowMs }) : undefined,
  ]);
  return {
    ...(threadSpent &&
      budgets.thread && { thread: { limits: budgets.thread, spent: threadSpent } }),
    ...(namespaceSpent &&
      ns && { namespace: { limits: ns.limits, windowMs: ns.windowMs, spent: namespaceSpent } }),
  };
}

/**
 * Admit a turn: throw `BudgetExceededError` when the thread's or the
 * namespace's budget is used up, else answer the turn's ceiling — what is
 * left of both (undefined when neither is set).
 */
export async function admitTurn(params: {
  readonly budgets: AgentBudgets;
  readonly namespaceId: string;
  readonly threadId?: string;
}): Promise<TurnBudget | undefined> {
  const min = params.budgets.minCallTokens ?? DEFAULT_MIN_CALL_TOKENS;
  const status = await budgetStatus(params);
  let ceiling: TurnBudget | undefined;
  for (const scope of ["thread", "namespace"] as const) {
    const standing = status[scope];
    if (!standing) continue;
    const remaining = remainingBudget({ budget: standing.limits, usage: standing.spent });
    // Used up, or too little left for one useful LLM call: a turn admitted
    // with less would only end at once with `budget_exceeded`.
    const overrun =
      budgetOverrun({ budget: standing.limits, usage: standing.spent, exhausted: true }) ??
      tooLittleLeft({ remaining, budget: standing.limits, usage: standing.spent, min });
    if (overrun) throw new BudgetExceededError({ scope, ...overrun });
    ceiling = tightenBudget(ceiling, remaining);
  }
  return ceiling;
}
