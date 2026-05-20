// ---------------------------------------------------------------------------
// SharedSignalPage — public form page reached via a Share-link URL.
//
// URL: /#/share/<tokenId>.<bearer> — the combined credential the
// Share-link modal hands the operator to paste into Slack / email / wherever.
//
// Flow:
//   1. Parse tokenId + bearer from the URL path.
//   2. Fetch describe (bearer-authed) to get workflow name, signal name,
//      isApproval, expiry, and whether the workflow is still pending.
//   3. Render Approve / Reject (isApproval) or a JSON editor (custom).
//   4. POST to the existing complete endpoint via api.completeSignalToken.
//
// Phase 1 reuses the dashboard sidebar layout — pixel-polish ("standalone
// page, no sidebar") is tracked as polish in the parent bead. Functionally
// the page works end-to-end without dashboard auth (the complete + describe
// endpoints are bearer-authed only).
// ---------------------------------------------------------------------------

import { useEffect, useState } from "preact/hooks";
import { api } from "../../api/client.ts";
import type { DescribeSignalTokenResponse } from "../../../server/routes/signal-tokens.ts";

interface Props {
  tokenId: string;
  bearer: string;
}

type Phase = "loading" | "ready" | "delivered" | "error";

export function SharedSignalPage({ tokenId, bearer }: Props) {
  const [phase, setPhase] = useState<Phase>("loading");
  const [meta, setMeta] = useState<DescribeSignalTokenResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [draftPayload, setDraftPayload] = useState("");
  const [draftError, setDraftError] = useState<string | null>(null);
  const [resolvedAs, setResolvedAs] = useState<string | null>(null);

  useEffect(() => {
    if (!tokenId || !bearer) {
      setError("Invalid share link — missing token or bearer.");
      setPhase("error");
      return;
    }
    api
      .describeSignalToken(tokenId, bearer)
      .then((m) => {
        setMeta(m);
        if (m.completedAt) {
          setResolvedAs("previously");
          setPhase("delivered");
        } else {
          setPhase("ready");
        }
      })
      .catch((err: unknown) => {
        setError(err instanceof Error ? err.message : String(err));
        setPhase("error");
      });
  }, [tokenId, bearer]);

  async function deliver(value: unknown): Promise<void> {
    if (busy) return;
    setBusy(true);
    setError(null);
    try {
      const result = await api.completeSignalToken(tokenId, bearer, value);
      setResolvedAs(result.alreadyCompleted ? "previously" : "just now");
      setPhase("delivered");
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setBusy(false);
    }
  }

  function deliverCustom(): void {
    if (draftPayload.trim() === "") {
      void deliver(null);
      return;
    }
    try {
      const parsed: unknown = JSON.parse(draftPayload);
      setDraftError(null);
      void deliver(parsed);
    } catch (err) {
      setDraftError(`Invalid JSON: ${err instanceof Error ? err.message : String(err)}`);
    }
  }

  return (
    <div class="max-w-2xl mx-auto p-6">
      <div class="mb-4">
        <h1 class="text-2xl font-semibold">Signal delivery</h1>
        <p class="text-xs text-base-content/50 mt-1">
          A workflow is waiting for you to deliver a signal. Once you submit, the workflow resumes
          and this link becomes single-use (re-submitting returns the original outcome).
        </p>
      </div>

      {phase === "loading" && <div class="text-sm text-base-content/60">Loading…</div>}

      {phase === "error" && (
        <div class="alert alert-error text-sm">
          <span>{error ?? "Could not load this share link."}</span>
        </div>
      )}

      {phase === "ready" && meta && (
        <div class="card bg-base-100 shadow p-4 space-y-4">
          <div>
            <div class="text-xs text-base-content/50 uppercase tracking-wider">Workflow</div>
            <div class="font-mono text-sm">{meta.workflowName}</div>
            <div class="font-mono text-[11px] text-base-content/40 mt-0.5">{meta.workflowId}</div>
          </div>
          <div>
            <div class="text-xs text-base-content/50 uppercase tracking-wider">Signal</div>
            <div class="font-mono text-sm">{meta.signalName}</div>
          </div>

          {!meta.pending && (
            <div class="alert alert-warning text-xs">
              This workflow already resumed through another delivery. Submitting won't change the
              outcome (the server returns the original value).
            </div>
          )}

          {meta.isApproval ? (
            <div class="flex gap-2">
              <button
                type="button"
                class="btn btn-success flex-1"
                onClick={() => void deliver({ approved: true, by: "external" })}
                disabled={busy}
              >
                {busy ? "…" : "Approve"}
              </button>
              <button
                type="button"
                class="btn btn-error btn-outline flex-1"
                onClick={() => void deliver({ approved: false, by: "external" })}
                disabled={busy}
              >
                {busy ? "…" : "Reject"}
              </button>
            </div>
          ) : (
            <div class="space-y-2">
              <label class="text-xs text-base-content/60">
                JSON payload — blank means <code>null</code>
              </label>
              <textarea
                class="textarea textarea-bordered w-full font-mono text-xs"
                rows={5}
                value={draftPayload}
                onInput={(e) => {
                  setDraftPayload((e.target as HTMLTextAreaElement).value);
                  if (draftError !== null) setDraftError(null);
                }}
                placeholder="{}"
              />
              {draftError !== null && <div class="alert alert-error text-xs">{draftError}</div>}
              <button type="button" class="btn btn-primary" onClick={deliverCustom} disabled={busy}>
                {busy ? "Delivering…" : "Deliver"}
              </button>
            </div>
          )}

          <div class="text-[10px] text-base-content/40">
            Expires {new Date(meta.expiresAt).toLocaleString()}.
          </div>

          {error !== null && <div class="alert alert-error text-xs">{error}</div>}
        </div>
      )}

      {phase === "delivered" && meta && (
        <div class="card bg-base-100 shadow p-4 space-y-2">
          <div class="text-success font-medium">Delivered {resolvedAs ?? ""}.</div>
          <div class="text-xs text-base-content/60">
            Workflow <span class="font-mono">{meta.workflowName}</span> has been notified. You can
            close this page.
          </div>
        </div>
      )}
    </div>
  );
}
