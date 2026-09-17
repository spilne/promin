// ---------------------------------------------------------------------------
// AgentSecretsPanel — set / rotate / delete an agent's BYOK secret.
//
// Manages the recipe's `model.credentialRef` secret in SecretsStorage via
// the /api/secrets CRUD routes. The list endpoint returns key NAMES only —
// values never leave the server — so the panel shows "set / not set"
// status, never the value itself.
//
// Scope defaults to the current tenant namespace so a SaaS tenant's key
// stays isolated; a global option covers single-tenant hosts.
// ---------------------------------------------------------------------------

import { useEffect, useState } from "preact/hooks";
import { secretsApi, type SecretScopeWire } from "../../api/client.ts";
import { useFetch } from "../../hooks/use-fetch.ts";
import type { RegisteredAgent } from "../../../server/routes/agents.ts";

interface Props {
  source: RegisteredAgent;
  /** Current tenant namespace — supplied secrets default to its scope. */
  namespaceId?: string;
  onClose: () => void;
  /**
   * When true, render only the form body (no backdrop, no positioning, no
   * own header). The host owns the surrounding chrome — e.g. a tab inside
   * the agent's ⚙ Manage drawer.
   */
  embed?: boolean;
}

export function AgentSecretsPanel({ source, namespaceId, onClose, embed }: Props) {
  const credentialRef =
    source.backend.type === "local" ? source.backend.model.credentialRef : undefined;

  const [scopeKind, setScopeKind] = useState<"namespace" | "global">(
    namespaceId ? "namespace" : "global",
  );
  const [value, setValue] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);

  const scope: SecretScopeWire =
    scopeKind === "namespace" && namespaceId
      ? { kind: "namespace", namespaceId }
      : { kind: "global" };

  // List key NAMES at the scope — never values. Re-keyed on scope change.
  const {
    data,
    loading,
    error: listError,
    refresh,
  } = useFetch(() => secretsApi.list(scope), [scopeKind], 0);
  const isSet = credentialRef !== undefined && (data?.keys ?? []).includes(credentialRef);

  useEffect(() => {
    // Skip the global Esc handler in embed mode — the host drawer owns the
    // close key, and intercepting it here would close the whole drawer
    // while the user is typing into the password field.
    if (embed) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape" && !busy) onClose();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onClose, busy, embed]);

  async function save(): Promise<void> {
    if (!credentialRef || value.length === 0 || busy) return;
    setBusy(true);
    setError(null);
    setNotice(null);
    try {
      await secretsApi.create({ scope, key: credentialRef, value });
      setValue("");
      setNotice(`Saved “${credentialRef}” at ${scopeLabel(scope)}.`);
      refresh();
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setBusy(false);
    }
  }

  async function remove(): Promise<void> {
    if (!credentialRef || busy) return;
    setBusy(true);
    setError(null);
    setNotice(null);
    try {
      await secretsApi.delete(scope, credentialRef);
      setNotice(`Removed “${credentialRef}” from ${scopeLabel(scope)}.`);
      refresh();
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setBusy(false);
    }
  }

  const body =
    credentialRef === undefined ? (
      <div class="text-xs text-base-content/60 leading-relaxed">
        This agent declares no <code class="font-mono">model.credentialRef</code> — it runs on the
        host's pooled key. Set a credential reference in <strong>Edit</strong> to enable BYOK, then
        return here to store the key.
      </div>
    ) : (
      <>
        <label class="block space-y-1">
          <span class="text-xs text-base-content/60">Scope</span>
          {namespaceId ? (
            <select
              class="select select-sm select-bordered w-full"
              value={scopeKind}
              onChange={(e) =>
                setScopeKind((e.target as HTMLSelectElement).value as "namespace" | "global")
              }
            >
              <option value="namespace">
                This namespace ({namespaceId}) — isolated to your tenant
              </option>
              <option value="global">Global — shared across all tenants</option>
            </select>
          ) : (
            <div class="text-xs text-base-content/50">Global scope.</div>
          )}
        </label>

        <div class="flex items-center gap-2">
          <span class="font-mono text-sm">{credentialRef}</span>
          {loading ? (
            <span class="badge badge-sm badge-ghost">checking…</span>
          ) : isSet ? (
            <span class="badge badge-sm badge-success">set</span>
          ) : (
            <span class="badge badge-sm badge-warning">not set</span>
          )}
        </div>

        <label class="block space-y-1">
          <span class="text-xs text-base-content/60">{isSet ? "Replace value" : "Set value"}</span>
          <input
            type="password"
            class="input input-sm input-bordered w-full font-mono"
            placeholder="paste the key — stored encrypted, never displayed"
            value={value}
            onInput={(e) => setValue((e.target as HTMLInputElement).value)}
          />
        </label>

        {listError && <div class="alert alert-error text-xs">{listError.message}</div>}
        {error && <div class="alert alert-error text-xs">{error}</div>}
        {notice && <div class="alert alert-success text-xs">{notice}</div>}

        <div class="flex justify-end gap-2">
          {isSet && (
            <button
              class="btn btn-sm btn-ghost text-error"
              onClick={() => void remove()}
              disabled={busy}
            >
              Remove
            </button>
          )}
          <button
            class="btn btn-sm btn-primary"
            onClick={() => void save()}
            disabled={busy || value.length === 0}
          >
            {busy ? "Saving…" : isSet ? "Replace" : "Save"}
          </button>
        </div>
      </>
    );

  if (embed) {
    return <div class="space-y-4">{body}</div>;
  }

  return (
    <>
      <div
        class="fixed inset-0 bg-black/40 z-30 anim-backdrop-in"
        onClick={() => !busy && onClose()}
        aria-hidden
      />
      <div class="fixed inset-x-4 top-24 mx-auto max-w-md bg-base-100 rounded-lg shadow-2xl z-40 p-5 space-y-4">
        <div class="flex items-start justify-between gap-2">
          <div>
            <div class="text-xs text-base-content/50 uppercase tracking-wider">Secrets</div>
            <div class="font-mono text-sm">{source.id}</div>
          </div>
          <button
            class="btn btn-sm btn-ghost"
            onClick={onClose}
            disabled={busy}
            title="Close (Esc)"
          >
            ✕
          </button>
        </div>
        {body}
      </div>
    </>
  );
}

function scopeLabel(scope: SecretScopeWire): string {
  if (scope.kind === "namespace") return `namespace “${scope.namespaceId}”`;
  if (scope.kind === "resource") return `resource “${scope.resourceId}”`;
  return "global scope";
}
