// ---------------------------------------------------------------------------
// ThreadBudgetBadge — the chat header's spend indicator: the thread's (and
// its namespace's) spend against their budgets, from
// GET /api/agents/:id/threads/:threadId/usage. Shows the fullest limit;
// the tooltip lists every one. Hidden when the server has no budgets.
// ---------------------------------------------------------------------------

import { useEffect, useState } from "preact/hooks";
import { api } from "../../api/client.ts";
import type { ThreadUsageResponse } from "../../../server/routes/agents/agent-gateway-deps.ts";
import {
  budgetMeters,
  formatBudgetAmount,
  spentOn,
  type BudgetMeter,
} from "../../lib/recipe-budget-form.ts";

export function ThreadBudgetBadge(props: {
  agentId: string;
  threadId: string;
  tenant: { namespaceId: string; resourceId: string };
  /** Changes when the thread has new messages: the spend is read again. */
  refreshKey: number;
}) {
  const { agentId, threadId, tenant, refreshKey } = props;
  const [usage, setUsage] = useState<ThreadUsageResponse | null>(null);
  useEffect(() => {
    let cancelled = false;
    api
      .getAgentThreadUsage(agentId, threadId, tenant)
      .then((u) => {
        if (!cancelled) setUsage(u);
      })
      // A brand-new thread answers 404: nothing spent yet.
      .catch(() => {});
    return () => {
      cancelled = true;
    };
  }, [agentId, threadId, tenant.namespaceId, tenant.resourceId, refreshKey]);

  if (!usage?.enabled) return null;
  const meters: Array<BudgetMeter & { scope: string }> = [
    ...(usage.thread ? budgetMeters(usage.thread) : []).map((m) => ({ ...m, scope: "thread" })),
    ...(usage.namespace ? budgetMeters(usage.namespace) : []).map((m) => ({
      ...m,
      scope: "namespace",
    })),
  ].sort((a, b) => b.ratio - a.ratio);
  const top = meters[0];
  const lines = meters.map(
    (m) =>
      `${m.scope} · ${m.label}: ${formatBudgetAmount(m.used, m.cost)} / ${formatBudgetAmount(m.limit, m.cost)}`,
  );

  if (!top) {
    // Accounted, but no limit set: show what the thread spent.
    const spent = usage.thread ? spentOn(usage.thread.spent, "maxTotalTokens") : 0;
    return (
      <span class="badge badge-ghost badge-sm font-mono" title="Tokens this thread has spent">
        {formatBudgetAmount(spent, false)} tok
      </span>
    );
  }
  const tone = top.ratio >= 1 ? "badge-error" : top.ratio >= 0.8 ? "badge-warning" : "badge-ghost";
  return (
    <span
      class={`badge badge-sm font-mono gap-1 ${tone}`}
      title={lines.join("\n")}
      data-testid="thread-budget"
    >
      <span>{top.scope === "thread" ? "budget" : "ns budget"}</span>
      <span>
        {formatBudgetAmount(top.used, top.cost)} / {formatBudgetAmount(top.limit, top.cost)}
      </span>
    </span>
  );
}
