// ---------------------------------------------------------------------------
// ApprovalList — operator inbox for tool calls waiting on `approve:<id>`.
// Read-only for now (resolving decisions still goes through the agent
// loop / session API). Each row is one suspended workflow; clicking
// "View run" drills into RunDetail where the operator can inspect the
// suspending step in context.
// ---------------------------------------------------------------------------

import { useFetch } from "../../hooks/use-fetch.ts";
import { useNamespace } from "../../hooks/use-namespace.ts";
import { api } from "../../api/client.ts";
import { Page } from "../ui/page.tsx";
import { SkeletonRows } from "../ui/skeleton.tsx";
import { formatRelative } from "../../lib/format.ts";

interface Props {
  onOpenRun: (workflowId: string) => void;
}

export function ApprovalList({ onOpenRun }: Props) {
  const [namespace] = useNamespace();
  const { data, loading, error, refresh } = useFetch(
    () => api.listApprovals({ namespace: namespace || undefined }),
    [namespace],
    15_000,
  );

  const approvals = data?.approvals ?? [];

  return (
    <Page>
      <div class="flex items-end justify-between flex-wrap gap-2 mb-4">
        <div>
          <h2 class="text-xl font-semibold">Approvals</h2>
          <p class="text-xs text-base-content/50">
            Tool calls suspended awaiting approval. Decisions still flow through the agent
            loop&apos;s signal channel — this view is the inbox.
          </p>
        </div>
        <button class="btn btn-sm btn-ghost gap-1" onClick={() => refresh()}>
          <span>↻</span>
          Refresh
        </button>
      </div>

      {error && (
        <div class="alert alert-error text-sm mb-3">
          <span>{error.message}</span>
        </div>
      )}

      <div class="rounded-box border border-base-content/10 overflow-x-auto">
        <table class="table table-sm">
          <thead>
            <tr>
              <th>Tool</th>
              <th>Workflow</th>
              <th>Namespace</th>
              <th>Suspended</th>
              <th class="text-right">Action</th>
            </tr>
          </thead>
          <tbody>
            {loading && !data && <SkeletonRows rows={5} cols={5} />}
            {!loading && approvals.length === 0 && (
              <tr>
                <td colSpan={5} class="text-center text-sm text-base-content/50 py-8">
                  No pending approvals.
                </td>
              </tr>
            )}
            {approvals.map((a) => (
              <tr key={a.workflowId + a.toolCallId} class="hover">
                <td>
                  <div class="font-mono text-xs">
                    {a.toolName ?? <span class="text-base-content/40">unknown</span>}
                  </div>
                  {a.toolInput !== undefined && a.toolInput !== null && (
                    <details class="text-[11px] text-base-content/60 mt-1 max-w-md">
                      <summary class="cursor-pointer">input</summary>
                      <pre class="mt-1 whitespace-pre-wrap break-all bg-base-200 rounded p-1">
                        {safeStringify(a.toolInput)}
                      </pre>
                    </details>
                  )}
                </td>
                <td>
                  <div class="text-sm">{a.workflowName}</div>
                  <div class="font-mono text-[11px] text-base-content/50">{a.workflowId}</div>
                </td>
                <td class="text-sm">
                  {a.namespace ?? <span class="text-base-content/40">—</span>}
                </td>
                <td class="text-sm">
                  {a.suspendedAt ? (
                    formatRelative(a.suspendedAt)
                  ) : (
                    <span class="text-base-content/40">—</span>
                  )}
                </td>
                <td class="text-right">
                  <button
                    class="btn btn-xs btn-ghost"
                    onClick={() => onOpenRun(a.workflowId)}
                    title="View run"
                  >
                    View run →
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </Page>
  );
}

function safeStringify(value: unknown): string {
  try {
    return JSON.stringify(value, null, 2);
  } catch {
    return String(value);
  }
}
