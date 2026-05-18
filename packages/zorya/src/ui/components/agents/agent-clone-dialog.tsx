// ---------------------------------------------------------------------------
// AgentCloneDialog — fork an existing agent recipe into a new one.
//
// Wraps POST /api/agents/:id/clone: the clone copies the source's backend
// verbatim, so the new recipe starts identical and the operator then
// customizes it in the edit drawer. When the source is a template with
// `requiredSecrets`, the dialog collects those BYOK values up front (the
// clone endpoint rejects the request otherwise).
// ---------------------------------------------------------------------------

import { useEffect, useState } from "preact/hooks";
import { api } from "../../api/client.ts";
import type { RegisteredAgent } from "../../../server/routes/agents.ts";

interface Props {
  /** The recipe being forked. */
  source: RegisteredAgent;
  /**
   * Current tenant namespace. When set, supplied BYOK secrets default to
   * this namespace's scope so a SaaS tenant's key stays isolated rather
   * than landing in the shared global scope.
   */
  namespaceId?: string;
  onClose: () => void;
  /** Called with the new recipe id once the clone succeeds. */
  onCloned: (newId: string) => void;
}

/** Agent ids: letters / digits / `-` / `_`, not starting with `_` (reserved). */
const ID_RE = /^[A-Za-z0-9][A-Za-z0-9_-]*$/;

export function AgentCloneDialog({ source, namespaceId, onClose, onCloned }: Props) {
  const requiredSecrets = source.metadata.requiredSecrets ?? [];
  const [targetId, setTargetId] = useState("");
  const [targetVersion, setTargetVersion] = useState("");
  const [secrets, setSecrets] = useState<Record<string, string>>({});
  // Where supplied secrets land. Default to the tenant's namespace when
  // one is known — global would leak a SaaS tenant's key fleet-wide.
  const [scopeKind, setScopeKind] = useState<"namespace" | "global">(
    namespaceId ? "namespace" : "global",
  );
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape" && !busy) onClose();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onClose, busy]);

  const idValid = ID_RE.test(targetId);
  const secretsComplete = requiredSecrets.every((name) => (secrets[name] ?? "").length > 0);
  const canSubmit = idValid && secretsComplete && !busy;

  async function submit(): Promise<void> {
    if (!canSubmit) return;
    setBusy(true);
    setError(null);
    try {
      const res = await api.cloneAgent(source.id, {
        targetId,
        ...(targetVersion.trim() ? { targetVersion: targetVersion.trim() } : {}),
        ...(requiredSecrets.length > 0
          ? {
              secrets,
              secretsScope:
                scopeKind === "namespace" && namespaceId
                  ? { kind: "namespace", namespaceId }
                  : { kind: "global" },
            }
          : {}),
      });
      onCloned(res.recipe.id);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
      setBusy(false);
    }
  }

  return (
    <>
      <div
        class="fixed inset-0 bg-black/40 z-30 anim-backdrop-in"
        onClick={() => !busy && onClose()}
        aria-hidden
      />
      <div class="fixed inset-x-4 top-24 mx-auto max-w-md bg-base-100 rounded-lg shadow-2xl z-40 p-5 space-y-4">
        <div>
          <div class="text-xs text-base-content/50 uppercase tracking-wider">Clone agent</div>
          <div class="font-mono text-sm">{source.id}</div>
          <p class="text-xs text-base-content/50 mt-1">
            Copies this recipe into a new agent you own. Customize it afterwards in Edit.
          </p>
        </div>

        <label class="block space-y-1">
          <span class="text-xs text-base-content/60">New agent id</span>
          <input
            class="input input-sm input-bordered w-full font-mono"
            value={targetId}
            placeholder="my-support-bot"
            onInput={(e) => setTargetId((e.target as HTMLInputElement).value)}
            // biome-ignore lint/a11y/noAutofocus: dialog's primary field
            autofocus
          />
          {targetId.length > 0 && !idValid && (
            <span class="text-xs text-error">
              Letters, digits, <code>-</code> or <code>_</code>; cannot start with <code>_</code>.
            </span>
          )}
        </label>

        <label class="block space-y-1">
          <span class="text-xs text-base-content/60">Version (optional)</span>
          <input
            class="input input-sm input-bordered w-full font-mono"
            value={targetVersion}
            placeholder="v1"
            onInput={(e) => setTargetVersion((e.target as HTMLInputElement).value)}
          />
        </label>

        {requiredSecrets.length > 0 && (
          <div class="space-y-2">
            <div class="text-xs text-base-content/60">
              This template needs secrets — supply your own (BYOK):
            </div>
            <label class="block space-y-1">
              <span class="text-xs text-base-content/60">Store secrets at</span>
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
                <div class="text-xs text-base-content/50">
                  Global scope (no tenant namespace in context).
                </div>
              )}
            </label>
            {requiredSecrets.map((name) => (
              <label class="block space-y-1" key={name}>
                <span class="text-xs font-mono text-base-content/70">{name}</span>
                <input
                  type="password"
                  class="input input-sm input-bordered w-full font-mono"
                  value={secrets[name] ?? ""}
                  onInput={(e) =>
                    setSecrets((s) => ({ ...s, [name]: (e.target as HTMLInputElement).value }))
                  }
                />
              </label>
            ))}
          </div>
        )}

        {error && <div class="alert alert-error text-xs">{error}</div>}

        <div class="flex justify-end gap-2">
          <button class="btn btn-sm btn-ghost" onClick={onClose} disabled={busy}>
            Cancel
          </button>
          <button
            class="btn btn-sm btn-primary"
            onClick={() => void submit()}
            disabled={!canSubmit}
          >
            {busy ? "Cloning…" : "Clone"}
          </button>
        </div>
      </div>
    </>
  );
}
