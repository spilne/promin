// ---------------------------------------------------------------------------
// SignalList — operator inbox for workflows suspended on external signals.
//
// Lists every workflow in `waiting_for_signal` status: agent-tool
// approvals (`approve:<callId>`) get their tool name + input surfaced
// when the agent loop wrote it; any other custom-named signal a workflow
// is waiting for shows up here too. Read-only — decisions still flow
// through the workflow run's Signals tab (or the agent loop's resume).
// ---------------------------------------------------------------------------

import { api } from "../../api/client.ts";
import { useFetch } from "../../hooks/use-fetch.ts";
import { useNamespace } from "../../hooks/use-namespace.ts";
import { formatRelative } from "../../lib/format.ts";
import { Page } from "../ui/page.tsx";
import { SkeletonRows } from "../ui/skeleton.tsx";

interface Props {
  onOpenRun: (workflowId: string) => void;
}

export function SignalList({ onOpenRun }: Props) {
  const [namespace] = useNamespace();
  const { data, loading, error, refresh } = useFetch(
    () => api.listSignals({ namespace: namespace || undefined }),
    [namespace],
    15_000,
  );

  const signals = data?.signals ?? [];

  return (
    <Page>
      <div class="flex items-end justify-between flex-wrap gap-2 mb-4">
        <div>
          <h2 class="text-xl font-semibold">Signals</h2>
          <p class="text-xs text-base-content/50">
            Workflows currently suspended on an external signal — tool-call approvals (
            <code class="font-mono">approve:&lt;callId&gt;</code>) and any custom-named signal a
            workflow is waiting for. Decisions still flow through{" "}
            <code class="font-mono">deliverSignal</code> (or the agent loop's resume); this view is
            the inbox.
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
              <th>Signal</th>
              <th>Workflow</th>
              <th>Namespace</th>
              <th>Suspended</th>
              <th class="text-right">Action</th>
            </tr>
          </thead>
          <tbody>
            {loading && !data && <SkeletonRows rows={5} cols={5} />}
            {!loading && signals.length === 0 && (
              <tr>
                <td colSpan={5} class="text-center text-sm text-base-content/50 py-8">
                  No pending signals.
                </td>
              </tr>
            )}
            {signals.map((s) => (
              <tr key={s.workflowId + s.signalName} class="hover">
                <td>
                  <div class="font-mono text-xs">{s.signalName}</div>
                  {s.toolName !== undefined && (
                    <div class="text-[11px] text-base-content/60 mt-1">
                      tool: <span class="font-mono">{s.toolName}</span>
                    </div>
                  )}
                  {s.toolInput !== undefined && s.toolInput !== null && (
                    <details class="text-[11px] text-base-content/60 mt-1 max-w-md">
                      <summary class="cursor-pointer">input</summary>
                      <pre class="mt-1 whitespace-pre-wrap break-all bg-base-200 rounded p-1">
                        {safeStringify(s.toolInput)}
                      </pre>
                    </details>
                  )}
                </td>
                <td>
                  <div class="text-sm">{s.workflowName}</div>
                  <div class="font-mono text-[11px] text-base-content/50">{s.workflowId}</div>
                </td>
                <td class="text-sm">
                  {s.namespace ?? <span class="text-base-content/40">—</span>}
                </td>
                <td class="text-sm">
                  {s.suspendedAt ? (
                    formatRelative(s.suspendedAt)
                  ) : (
                    <span class="text-base-content/40">—</span>
                  )}
                </td>
                <td class="text-right">
                  <button
                    class="btn btn-xs btn-ghost"
                    onClick={() => onOpenRun(s.workflowId)}
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
