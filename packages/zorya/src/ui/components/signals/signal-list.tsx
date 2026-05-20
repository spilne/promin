// ---------------------------------------------------------------------------
// SignalList — operator inbox for workflows suspended on external signals.
//
// Lists every workflow in `waiting_for_signal` status: agent-tool
// approvals (`approve:<callId>`) get their tool name + input surfaced
// when the agent loop wrote it; any other custom-named signal a workflow
// is waiting for shows up here too.
//
// Inline actions:
//   `approve:` signals — Approve / Reject buttons deliver
//                        `{approved: true|false, by: "operator"}` via
//                        POST /api/runs/:id/signal.
//   custom-named       — payload shape is unknown, so the row shows
//                        "View run →" only; deliver from the run's
//                        Signals tab.
// ---------------------------------------------------------------------------

import { useState } from "preact/hooks";
import { api } from "../../api/client.ts";
import { useFetch } from "../../hooks/use-fetch.ts";
import { useNamespace } from "../../hooks/use-namespace.ts";
import { toast } from "../../lib/dialogs.ts";
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
  const [busyKey, setBusyKey] = useState<string | null>(null);

  const signals = data?.signals ?? [];

  async function decide(workflowId: string, signalName: string, approved: boolean): Promise<void> {
    const key = workflowId + signalName;
    if (busyKey !== null) return;
    setBusyKey(key);
    try {
      await api.sendSignal(workflowId, signalName, { approved, by: "operator" });
      toast(`Signal ${approved ? "approved" : "rejected"} — workflow will resume.`, {
        variant: "success",
      });
      refresh();
    } catch (err) {
      toast(`Signal delivery failed: ${err instanceof Error ? err.message : String(err)}`, {
        variant: "error",
      });
    } finally {
      setBusyKey(null);
    }
  }

  return (
    <Page>
      <div class="flex items-end justify-between flex-wrap gap-2 mb-4">
        <div>
          <h2 class="text-xl font-semibold">Signals</h2>
          <p class="text-xs text-base-content/50">
            Workflows currently suspended on an external signal — tool-call approvals (
            <code class="font-mono">approve:&lt;callId&gt;</code>) and any custom-named signal a
            workflow is waiting for. Approve / Reject deliver directly; custom signals open the
            run's Signals tab.
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
            {signals.map((s) => {
              const key = s.workflowId + s.signalName;
              const isApproval = s.signalName.startsWith("approve:");
              const busy = busyKey === key;
              return (
                <tr key={key} class="hover">
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
                    <div class="inline-flex gap-1">
                      {isApproval && (
                        <>
                          <button
                            type="button"
                            class="btn btn-xs btn-success"
                            onClick={() => void decide(s.workflowId, s.signalName, true)}
                            disabled={busy}
                            title="Deliver { approved: true } to this workflow"
                          >
                            {busy ? "…" : "Approve"}
                          </button>
                          <button
                            type="button"
                            class="btn btn-xs btn-error btn-outline"
                            onClick={() => void decide(s.workflowId, s.signalName, false)}
                            disabled={busy}
                            title="Deliver { approved: false } to this workflow"
                          >
                            {busy ? "…" : "Reject"}
                          </button>
                        </>
                      )}
                      <button
                        type="button"
                        class="btn btn-xs btn-ghost"
                        onClick={() => onOpenRun(s.workflowId)}
                        title={
                          isApproval
                            ? "View run"
                            : "Custom signal — open the run's Signals tab to deliver a payload"
                        }
                      >
                        View run →
                      </button>
                    </div>
                  </td>
                </tr>
              );
            })}
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
