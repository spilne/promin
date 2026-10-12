// ---------------------------------------------------------------------------
// Budgets on the agent routes: what a thread and a namespace have spent
// against their budgets, and the 429 a turn gets when one is used up.
// ---------------------------------------------------------------------------

import {
  budgetStatus,
  BudgetExceededError,
  ModelUnpricedError,
  type AgentBudgets,
} from "@promin/agent";
import { json, jsonError } from "../../router.ts";
import type {
  AgentGatewayDeps,
  BudgetExceededResponse,
  NamespaceUsageResponse,
  ThreadUsageResponse,
} from "./agent-gateway-deps.ts";
import { openThreadFromQuery } from "./agent-scope.ts";

/**
 * The answer for a turn refused by its budget — 429 `budget_exceeded` for a
 * used-up budget, 400 `model_unpriced` for a cost budget on a model with no
 * price (a configuration error) — or null for any other error.
 */
export function budgetRefusal(err: unknown): Response | null {
  if (err instanceof ModelUnpricedError) return jsonError(400, "model_unpriced", err.message);
  if (!(err instanceof BudgetExceededError)) return null;
  const body: BudgetExceededResponse = {
    error: "budget_exceeded",
    message: err.message,
    scope: err.scope,
    dimension: err.dimension,
    limit: err.limit,
    used: err.used,
  };
  return json(429, body);
}

/**
 * `GET /api/agents/:id/threads/:threadId/usage` — the thread's spend (all
 * its turns) against its cumulative budget, and its namespace's spend in
 * the window against the namespace budget. Scope from the query, as for
 * the thread's other reads.
 */
export function getThreadUsage(deps: AgentGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const opened = await openThreadFromQuery({ deps, req, params });
    if (opened instanceof Response) return opened;
    const threadId = opened.thread.id;
    const budgets = deps.budgets;
    if (!budgets) return json(200, { threadId, enabled: false } satisfies ThreadUsageResponse);
    const namespaceId = opened.scoped.namespaceId;
    const [status, threadSpent] = await Promise.all([
      budgetStatus({ budgets, namespaceId, threadId }),
      budgets.ledger.total({ namespaceId, threadId }),
    ]);
    const response: ThreadUsageResponse = {
      threadId,
      enabled: true,
      thread: { spent: threadSpent, ...(budgets.thread && { limits: budgets.thread }) },
      ...(status.namespace && { namespace: status.namespace }),
    };
    return json(200, response);
  };
}

/**
 * `GET /api/agents/_usage?namespaceId=` — the namespace's spend in its
 * window against its budget. The caller must be allowed the namespace
 * (`authorizeScope`).
 */
export function getNamespaceUsage(deps: AgentGatewayDeps) {
  return async (req: Request): Promise<Response> => {
    const namespaceId = new URL(req.url).searchParams.get("namespaceId");
    if (!namespaceId) return jsonError(400, "missing_namespaceId");
    if (deps.authorizeScope) {
      const allowed = await deps.authorizeScope({ req, scope: { namespaceId } });
      if (!allowed) return jsonError(403, "scope_forbidden");
    }
    const budgets: AgentBudgets | undefined = deps.budgets;
    if (!budgets) {
      return json(200, { namespaceId, enabled: false } satisfies NamespaceUsageResponse);
    }
    const status = await budgetStatus({ budgets, namespaceId });
    const response: NamespaceUsageResponse = {
      namespaceId,
      enabled: true,
      ...(status.namespace && { namespace: status.namespace }),
    };
    return json(200, response);
  };
}
