// ---------------------------------------------------------------------------
// SignalList — operator inbox for workflows suspended on external signals.
//
// Lists every workflow in `waiting_for_signal` status: agent-tool
// approvals (`approve:<callId>`) get their tool name + input surfaced
// when the agent loop wrote it; any other custom-named signal a workflow
// is waiting for shows up here too.
//
// Inline actions per row:
//   `Deliver…` — always available. Opens an inline JSON-payload editor
//                and posts the parsed payload via api.sendSignal. The
//                workflow-agnostic primitive.
//   Approve / Reject — shortcut buttons for signals tagged
//                      `isApproval: true` by the server (i.e. names
//                      matching the `approve:<callId>` convention).
//                      They deliver `{approved, by:"operator"}` in one
//                      click without needing the editor.
//   `View run →` — opens the workflow run detail.
//
// The `isApproval` discriminator comes from the SignalDto — computed
// server-side via `parseApprovalSignal` so the wire-format prefix
// string never crosses the bundle boundary.
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
  // Which row's inline Deliver editor is open + its draft state.
  const [deliveringKey, setDeliveringKey] = useState<string | null>(null);
  const [draftPayload, setDraftPayload] = useState("");
  const [draftError, setDraftError] = useState<string | null>(null);

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

  function openDeliver(key: string): void {
    setDeliveringKey(key);
    setDraftPayload("");
    setDraftError(null);
  }

  function cancelDeliver(): void {
    setDeliveringKey(null);
    setDraftPayload("");
    setDraftError(null);
  }

  async function submitDeliver(workflowId: string, signalName: string): Promise<void> {
    const key = workflowId + signalName;
    let parsed: unknown;
    if (draftPayload.trim() === "") {
      parsed = null;
    } else {
      try {
        parsed = JSON.parse(draftPayload);
      } catch (err) {
        setDraftError(`Invalid JSON: ${err instanceof Error ? err.message : String(err)}`);
        return;
      }
    }
    if (busyKey !== null) return;
    setBusyKey(key);
    try {
      await api.sendSignal(workflowId, signalName, parsed);
      toast(`Signal "${signalName}" delivered.`, { variant: "success" });
      cancelDeliver();
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
            workflow is waiting for. Approve / Reject are shortcuts for approval-shaped signals;
            <code class="font-mono">Deliver…</code> works for any signal with any JSON payload.
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
              const busy = busyKey === key;
              const editorOpen = deliveringKey === key;
              const exampleHint = s.isApproval ? '{"approved": true, "by": "operator"}' : "{}";
              return (
                <>
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
                        {s.isApproval && (
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
                          onClick={() => (editorOpen ? cancelDeliver() : openDeliver(key))}
                          disabled={busy}
                          title="Deliver a custom JSON payload to this signal"
                        >
                          {editorOpen ? "Close" : "Deliver…"}
                        </button>
                        <button
                          type="button"
                          class="btn btn-xs btn-ghost"
                          onClick={() => onOpenRun(s.workflowId)}
                          title="View the workflow run"
                        >
                          View run →
                        </button>
                      </div>
                    </td>
                  </tr>
                  {editorOpen && (
                    <tr class="bg-base-200/60">
                      <td colSpan={5}>
                        <div class="p-3 space-y-2">
                          <div class="text-xs text-base-content/60">
                            Deliver to <span class="font-mono">{s.signalName}</span> — payload must
                            be valid JSON. Blank means <code>null</code>.
                          </div>
                          <textarea
                            class="textarea textarea-bordered textarea-sm w-full font-mono text-xs"
                            rows={4}
                            value={draftPayload}
                            placeholder={exampleHint}
                            onInput={(e) => {
                              setDraftPayload((e.target as HTMLTextAreaElement).value);
                              if (draftError !== null) setDraftError(null);
                            }}
                          />
                          {draftError !== null && (
                            <div class="alert alert-error text-xs">{draftError}</div>
                          )}
                          <div class="flex justify-end gap-2">
                            <button
                              type="button"
                              class="btn btn-xs btn-ghost"
                              onClick={cancelDeliver}
                              disabled={busy}
                            >
                              Cancel
                            </button>
                            <button
                              type="button"
                              class="btn btn-xs btn-primary"
                              onClick={() => void submitDeliver(s.workflowId, s.signalName)}
                              disabled={busy}
                            >
                              {busy ? "Delivering…" : "Deliver"}
                            </button>
                          </div>
                        </div>
                      </td>
                    </tr>
                  )}
                </>
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
