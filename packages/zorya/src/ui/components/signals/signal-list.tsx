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

import { useEffect, useState } from "preact/hooks";
import { api } from "../../api/client.ts";
import { useFetch } from "../../hooks/use-fetch.ts";
import { useNamespace } from "../../hooks/use-namespace.ts";
import { toast } from "../../lib/dialogs.ts";
import { formatRelative } from "../../lib/format.ts";
import type { MintTokenResponse } from "../../../server/routes/signal-tokens.ts";
import { Page, PageHeader } from "../ui/page.tsx";
import { SkeletonRows } from "../ui/skeleton.tsx";
import { SchemaForm } from "./schema-form.tsx";

interface ShareCtx {
  readonly workflowId: string;
  readonly signalName: string;
  readonly isApproval: boolean;
}

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
  // Which row's inline Deliver editor is open + its draft state. `draftValue`
  // holds the structured value when the row has a schema (SchemaForm path);
  // `draftPayload` holds the raw JSON when no schema is present (fallback
  // textarea). `draftError` is set when JSON parsing fails or the server
  // returns a validation error.
  const [deliveringKey, setDeliveringKey] = useState<string | null>(null);
  const [draftPayload, setDraftPayload] = useState("");
  const [draftValue, setDraftValue] = useState<unknown>(undefined);
  const [draftError, setDraftError] = useState<string | null>(null);
  // Share-link modal — non-null when a token has been minted for one row.
  const [shareToken, setShareToken] = useState<MintTokenResponse | null>(null);
  const [shareCtx, setShareCtx] = useState<ShareCtx | null>(null);
  const [sharingKey, setSharingKey] = useState<string | null>(null);

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

  function openDeliver(key: string, hasSchema: boolean): void {
    setDeliveringKey(key);
    setDraftPayload("");
    // Schema-driven forms start with an empty object (object schemas) so
    // first-keystroke renders correctly; JSON fallback starts with empty
    // string. The renderer fills in fields from `draftValue` on each render.
    setDraftValue(hasSchema ? {} : undefined);
    setDraftError(null);
  }

  function cancelDeliver(): void {
    setDeliveringKey(null);
    setDraftPayload("");
    setDraftValue(undefined);
    setDraftError(null);
  }

  async function shareLink(ctx: ShareCtx): Promise<void> {
    const key = ctx.workflowId + ctx.signalName;
    if (sharingKey !== null) return;
    setSharingKey(key);
    try {
      const token = await api.mintSignalToken(ctx.workflowId, ctx.signalName);
      setShareToken(token);
      setShareCtx(ctx);
    } catch (err) {
      toast(`Could not mint share link: ${err instanceof Error ? err.message : String(err)}`, {
        variant: "error",
      });
    } finally {
      setSharingKey(null);
    }
  }

  function closeShare(): void {
    setShareToken(null);
    setShareCtx(null);
  }

  async function submitDeliver(
    workflowId: string,
    signalName: string,
    hasSchema: boolean,
  ): Promise<void> {
    const key = workflowId + signalName;
    let parsed: unknown;
    if (hasSchema) {
      // Schema-driven form — the SchemaForm component already keeps
      // `draftValue` parsed; no string parsing needed.
      parsed = draftValue;
    } else if (draftPayload.trim() === "") {
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
      // Surface server-side schema_mismatch inline (under the editor) so
      // the user can fix and resubmit without losing their draft.
      const msg = err instanceof Error ? err.message : String(err);
      if (msg.includes("schema_mismatch")) {
        setDraftError(msg);
      }
      toast(`Signal delivery failed: ${msg}`, {
        variant: "error",
      });
    } finally {
      setBusyKey(null);
    }
  }

  return (
    <Page>
      <PageHeader
        title="Signals"
        eyebrow="Human-in-the-loop"
        description={
          <>
            Workflows currently suspended on an external signal — tool-call approvals (
            <code class="font-mono">approve:&lt;callId&gt;</code>) and any custom-named signal a
            workflow is waiting for. Approve / Reject are shortcuts for approval-shaped signals;
            <code class="font-mono">Deliver…</code> works for any signal with any JSON payload.
          </>
        }
        actions={
          <button class="btn btn-sm btn-ghost gap-1" onClick={() => refresh()}>
            <span>↻</span>
            Refresh
          </button>
        }
      />

      {error && (
        <div class="alert alert-error text-sm mb-3">
          <span>{error.message}</span>
        </div>
      )}

      {shareToken && shareCtx && (
        <ShareLinkModal
          token={shareToken}
          ctx={shareCtx}
          onMintAgain={() => void shareLink(shareCtx)}
          onClose={closeShare}
          busy={sharingKey !== null}
        />
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
                          onClick={() =>
                            editorOpen
                              ? cancelDeliver()
                              : openDeliver(key, s.jsonSchema !== undefined)
                          }
                          disabled={busy}
                          title={
                            s.jsonSchema !== undefined
                              ? "Deliver a typed payload (schema-driven form)"
                              : "Deliver a custom JSON payload to this signal"
                          }
                        >
                          {editorOpen ? "Close" : "Deliver…"}
                        </button>
                        <button
                          type="button"
                          class="btn btn-xs btn-ghost"
                          onClick={() =>
                            void shareLink({
                              workflowId: s.workflowId,
                              signalName: s.signalName,
                              isApproval: s.isApproval,
                            })
                          }
                          disabled={sharingKey !== null}
                          title="Mint a bearer-token URL anyone can use to deliver this signal — for Slack / email / webhook flows"
                        >
                          {sharingKey === key ? "…" : "Share link"}
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
                            Deliver to <span class="font-mono">{s.signalName}</span>
                            {s.jsonSchema === undefined && (
                              <>
                                {" "}
                                — payload must be valid JSON. Blank means <code>null</code>.
                              </>
                            )}
                          </div>
                          {s.jsonSchema !== undefined ? (
                            <SchemaForm
                              schema={s.jsonSchema}
                              value={draftValue}
                              onChange={(next) => {
                                setDraftValue(next);
                                if (draftError !== null) setDraftError(null);
                              }}
                              error={draftError}
                            />
                          ) : (
                            <>
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
                            </>
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
                              onClick={() =>
                                void submitDeliver(
                                  s.workflowId,
                                  s.signalName,
                                  s.jsonSchema !== undefined,
                                )
                              }
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

function ShareLinkModal({
  token,
  ctx,
  onMintAgain,
  onClose,
  busy,
}: {
  token: MintTokenResponse;
  ctx: ShareCtx;
  onMintAgain: () => void;
  onClose: () => void;
  busy: boolean;
}) {
  // Tick once a minute so the "Expires in …" relative time refreshes.
  const [, setTick] = useState(0);
  useEffect(() => {
    const id = setInterval(() => setTick((n) => n + 1), 60_000);
    return () => clearInterval(id);
  }, []);

  useEffect(() => {
    const onKey = (e: KeyboardEvent): void => {
      if (e.key === "Escape") onClose();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onClose]);

  const expiresAt = new Date(token.expiresAt);
  const expired = expiresAt.getTime() <= Date.now();
  const url = token.url ?? `${location.origin}/api/signal-tokens/${token.tokenId}/complete`;
  const sharePageUrl = `${location.origin}/#/share/${token.tokenId}.${token.bearer}`;
  const examplePayload = ctx.isApproval
    ? `{"approved": true, "by": "external"}`
    : `<YOUR_JSON_PAYLOAD>`;
  const curl =
    `curl -X POST "${url}" \\\n` +
    `  -H "Authorization: Bearer ${token.bearer}" \\\n` +
    `  -H "content-type: application/json" \\\n` +
    `  -d '{"value": ${examplePayload}}'`;

  const copy = async (text: string, label: string): Promise<void> => {
    try {
      await navigator.clipboard.writeText(text);
      toast(`${label} copied.`, { variant: "success" });
    } catch {
      toast(`Could not copy ${label}.`, { variant: "error" });
    }
  };

  return (
    <>
      <div class="fixed inset-0 bg-black/40 z-30 anim-backdrop-in" onClick={onClose} aria-hidden />
      <div
        class="fixed inset-x-4 top-12 mx-auto max-w-2xl bg-base-100 rounded-lg shadow-2xl z-40 p-5 space-y-4"
        role="dialog"
        aria-label="Share signal link"
      >
        <div class="flex items-start justify-between gap-2">
          <div class="min-w-0">
            <div class="text-xs text-base-content/50 uppercase tracking-wider">Share link</div>
            <div class="font-mono text-sm truncate">{ctx.signalName}</div>
            <div class="text-[10px] text-base-content/40 mt-0.5">
              Anyone with this URL + bearer can deliver the signal.{" "}
              {expired ? (
                <span class="text-error">Expired.</span>
              ) : (
                <>
                  Expires {formatRelative(expiresAt.toISOString())} ({expiresAt.toISOString()}).
                </>
              )}{" "}
              {token.isCached && <span class="text-warning">Reused an existing token.</span>}
            </div>
          </div>
          <button class="btn btn-sm btn-ghost" onClick={onClose} title="Close (Esc)">
            ✕
          </button>
        </div>

        <label class="block space-y-1">
          <span class="text-xs text-base-content/60">
            Browser share link — opens an Approve / Reject page for the recipient
          </span>
          <div class="flex gap-1">
            <input
              readonly
              class="input input-sm input-bordered w-full font-mono text-xs"
              value={sharePageUrl}
            />
            <button
              type="button"
              class="btn btn-sm btn-primary"
              onClick={() => void copy(sharePageUrl, "Share link")}
            >
              Copy
            </button>
          </div>
        </label>

        <label class="block space-y-1">
          <span class="text-xs text-base-content/60">Complete endpoint</span>
          <div class="flex gap-1">
            <input
              readonly
              class="input input-sm input-bordered w-full font-mono text-xs"
              value={url}
            />
            <button
              type="button"
              class="btn btn-sm btn-ghost"
              onClick={() => void copy(url, "URL")}
            >
              Copy
            </button>
          </div>
          {token.url === null && (
            <div class="text-[10px] text-warning">
              Server has no <code>publicBaseUrl</code> configured; URL was built from{" "}
              <code>location.origin</code>. Set <code>publicBaseUrl</code> in production so the link
              works from outside the dashboard.
            </div>
          )}
        </label>

        <label class="block space-y-1">
          <span class="text-xs text-base-content/60">Bearer token</span>
          <div class="flex gap-1">
            <input
              readonly
              type="password"
              class="input input-sm input-bordered w-full font-mono text-xs"
              value={token.bearer}
            />
            <button
              type="button"
              class="btn btn-sm btn-ghost"
              onClick={() => void copy(token.bearer, "Bearer")}
            >
              Copy
            </button>
          </div>
        </label>

        <label class="block space-y-1">
          <span class="text-xs text-base-content/60">One-line curl</span>
          <pre class="text-[11px] font-mono bg-base-200 rounded p-2 overflow-x-auto whitespace-pre">
            {curl}
          </pre>
          <button
            type="button"
            class="btn btn-sm btn-primary"
            onClick={() => void copy(curl, "curl command")}
          >
            Copy curl
          </button>
        </label>

        <div class="flex justify-between gap-2 pt-2 border-t border-base-300">
          <button
            type="button"
            class="btn btn-sm btn-ghost"
            onClick={onMintAgain}
            disabled={busy}
            title="Mint a fresh token — replaces this one in the modal (the prior token stays valid until it expires)"
          >
            {busy ? "Minting…" : "Mint another"}
          </button>
          <button type="button" class="btn btn-sm btn-ghost" onClick={onClose}>
            Done
          </button>
        </div>
      </div>
    </>
  );
}
