// ---------------------------------------------------------------------------
// AgentCloneDialog — fork an existing agent recipe into a new one.
//
// Wraps POST /api/agents/:id/clone: the clone copies the source's backend
// verbatim, so the new recipe starts identical and the operator then
// customizes it in the edit drawer.
// ---------------------------------------------------------------------------

import { useEffect, useState } from "preact/hooks";
import { api } from "../../api/client.ts";
import type { RegisteredAgent } from "../../../server/routes/agents.ts";

interface Props {
  /** The recipe being forked. */
  source: RegisteredAgent;
  onClose: () => void;
  /** Called with the new recipe id once the clone succeeds. */
  onCloned: (newId: string) => void;
  /**
   * When true, render only the form body (no backdrop, no positioning,
   * no own title block). The host owns the chrome — e.g. a tab inside
   * the agent's ⚙ Manage drawer.
   */
  embed?: boolean;
}

/** Agent ids: letters / digits / `-` / `_`, not starting with `_` (reserved). */
const ID_RE = /^[A-Za-z0-9][A-Za-z0-9_-]*$/;

export function AgentCloneDialog({ source, onClose, onCloned, embed }: Props) {
  const [targetId, setTargetId] = useState("");
  const [targetVersion, setTargetVersion] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    // Skip the global Esc handler in embed mode — host drawer owns close.
    if (embed) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape" && !busy) onClose();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onClose, busy, embed]);

  const idValid = ID_RE.test(targetId);
  const canSubmit = idValid && !busy;

  async function submit(): Promise<void> {
    if (!canSubmit) return;
    setBusy(true);
    setError(null);
    try {
      const res = await api.cloneAgent(source.id, {
        targetId,
        ...(targetVersion.trim() ? { targetVersion: targetVersion.trim() } : {}),
      });
      onCloned(res.recipe.id);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
      setBusy(false);
    }
  }

  return (
    <>
      {!embed && (
        <div
          class="fixed inset-0 bg-black/40 z-30 anim-backdrop-in"
          onClick={() => !busy && onClose()}
          aria-hidden
        />
      )}
      <div
        class={
          embed
            ? "space-y-4"
            : "fixed inset-x-4 top-24 mx-auto max-w-md bg-base-100 rounded-lg shadow-2xl z-40 p-5 space-y-4"
        }
      >
        {!embed && (
          <div>
            <div class="text-xs text-base-content/50 uppercase tracking-wider">Clone agent</div>
            <div class="font-mono text-sm">{source.id}</div>
            <p class="text-xs text-base-content/50 mt-1">
              Copies this recipe into a new agent you own. Customize it afterwards in Edit.
            </p>
          </div>
        )}

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
