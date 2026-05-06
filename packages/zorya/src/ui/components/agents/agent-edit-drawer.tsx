// ---------------------------------------------------------------------------
// AgentEditDrawer — operator-facing edit form for the most-iterated
// recipe fields: description, system prompt, capabilities, tags.
//
// Phase 1 cut for promin-khmz. The full Designer (model dropdown,
// tool catalog, "test in chat" panel, version diff, export-as-TS) is
// tracked as Phase 2 follow-ups; this slice ships the highest-value
// edits operators reach for daily — system prompt iteration, tag /
// capability metadata maintenance.
//
// Bigger-shape edits (model swap, tools list, full backend rewrite)
// fall back to PATCH /api/agents/:id with a hand-crafted body via the
// API client until the full form lands.
// ---------------------------------------------------------------------------

import { useEffect, useState } from "preact/hooks";
import { api } from "../../api/client.ts";
import type { RegisteredAgent } from "../../../server/routes/agents.ts";

interface Props {
  agent: RegisteredAgent;
  onClose: () => void;
  onSaved: (updated: RegisteredAgent) => void;
}

export function AgentEditDrawer({ agent, onClose, onSaved }: Props) {
  const [description, setDescription] = useState(agent.metadata.description ?? "");
  const [systemPrompt, setSystemPrompt] = useState(
    agent.backend.type === "local" ? (agent.backend.systemPrompt ?? "") : "",
  );
  const [capabilities, setCapabilities] = useState(agent.metadata.capabilities.join(", "));
  const [tags, setTags] = useState(agent.metadata.tags.join(", "));
  const [enabled, setEnabled] = useState(agent.metadata.enabled !== false);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => e.key === "Escape" && onClose();
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onClose]);

  const onSubmit = async (e: Event) => {
    e.preventDefault();
    setSaving(true);
    setError(null);
    try {
      const updates: Parameters<typeof api.updateAgent>[1] = {
        metadata: {
          description: description.trim() || null,
          capabilities: parseList(capabilities),
          tags: parseList(tags),
          enabled,
        },
      };
      // System prompt only meaningful for local backends.
      if (agent.backend.type === "local") {
        updates.backend = {
          ...agent.backend,
          systemPrompt: systemPrompt.trim() || null,
        };
      }
      const updated = await api.updateAgent(agent.id, updates);
      onSaved(updated);
      onClose();
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setSaving(false);
    }
  };

  return (
    <>
      <div class="fixed inset-0 bg-black/40 z-30 anim-backdrop-in" onClick={onClose} aria-hidden />
      <aside
        class="fixed top-0 right-0 h-screen w-full max-w-2xl bg-base-100 shadow-2xl
               z-40 flex flex-col anim-drawer-in"
        role="dialog"
        aria-label="Edit agent"
      >
        <header class="flex items-start justify-between gap-2 p-4 border-b border-base-300">
          <div class="min-w-0">
            <div class="text-xs text-base-content/50 uppercase tracking-wider">Edit agent</div>
            <div class="font-mono text-sm truncate">{agent.id}</div>
            <div class="text-[10px] text-base-content/40 mt-0.5">
              Updates the latest version in place. Changing model or tools needs a programmatic edit
              until the full Designer ships.
            </div>
          </div>
          <button
            class="btn btn-sm btn-ghost"
            onClick={onClose}
            aria-label="Close"
            title="Close (Esc)"
          >
            ✕
          </button>
        </header>

        <form class="flex-1 overflow-y-auto p-4 space-y-4" onSubmit={onSubmit}>
          <label class="form-control">
            <span class="text-xs text-base-content/60 mb-1 uppercase tracking-wider">
              Description
            </span>
            <input
              class="input input-bordered input-sm"
              placeholder="Short description shown in agent listings"
              value={description}
              onInput={(e) => setDescription((e.target as HTMLInputElement).value)}
            />
          </label>

          {agent.backend.type === "local" && (
            <label class="form-control">
              <span class="text-xs text-base-content/60 mb-1 uppercase tracking-wider">
                System prompt
              </span>
              <textarea
                class="textarea textarea-bordered font-mono text-xs leading-relaxed"
                rows={12}
                placeholder="(empty — agent runs without a static system prompt)"
                value={systemPrompt}
                onInput={(e) => setSystemPrompt((e.target as HTMLTextAreaElement).value)}
              />
              <span class="text-[10px] text-base-content/40 mt-1">
                Memory cascade rules (CLAUDE.md / namespace.staticRules) are layered on top by
                MemoryStore.resolveContext at turn time.
              </span>
            </label>
          )}

          <label class="form-control">
            <span class="text-xs text-base-content/60 mb-1 uppercase tracking-wider">
              Capabilities (comma-separated)
            </span>
            <input
              class="input input-bordered input-sm font-mono"
              placeholder="chat, search, summarize"
              value={capabilities}
              onInput={(e) => setCapabilities((e.target as HTMLInputElement).value)}
            />
          </label>

          <label class="form-control">
            <span class="text-xs text-base-content/60 mb-1 uppercase tracking-wider">
              Tags (comma-separated)
            </span>
            <input
              class="input input-bordered input-sm font-mono"
              placeholder="alpha, internal, stable"
              value={tags}
              onInput={(e) => setTags((e.target as HTMLInputElement).value)}
            />
          </label>

          <label class="form-control">
            <span class="text-xs text-base-content/60 mb-1 uppercase tracking-wider">Status</span>
            <label class="cursor-pointer label justify-start gap-2 px-0 py-1">
              <input
                type="checkbox"
                class="toggle toggle-sm toggle-success"
                checked={enabled}
                onChange={(e) => setEnabled((e.target as HTMLInputElement).checked)}
              />
              <span class="text-sm">
                {enabled ? "Enabled" : "Disabled"}
                <span class="text-[10px] text-base-content/50 ml-1">
                  {enabled
                    ? "— recipe accepts invocations"
                    : "— invocations rejected with 410, threads + history still browsable"}
                </span>
              </span>
            </label>
          </label>

          {error && <div class="alert alert-error text-xs">{error}</div>}

          <div class="flex justify-end gap-2 pt-2">
            <button type="button" class="btn btn-sm btn-ghost" onClick={onClose}>
              Cancel
            </button>
            <button type="submit" class="btn btn-sm btn-primary" disabled={saving}>
              {saving ? "Saving…" : "Save changes"}
            </button>
          </div>
        </form>
      </aside>
    </>
  );
}

function parseList(raw: string): string[] {
  return raw
    .split(",")
    .map((s) => s.trim())
    .filter((s) => s.length > 0);
}
