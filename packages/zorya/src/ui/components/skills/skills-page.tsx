// ---------------------------------------------------------------------------
// SkillsPage — operator UI for the skill registry. Lists registered skills
// and lets operators author custom ones: id, description, whenToUse, a
// markdown body (with live preview), tags, capabilities, enabled.
//
// Skills are instruction blocks an agent loads on demand (loadSkill); the
// agent editor's skill picker attaches them by id. File-scanned skills
// (.ts / SKILL.md) and operator-authored ones share this registry — editing
// a file-scanned skill here republishes the row, but the next scan tick will
// overwrite it from disk, so the form warns when that's the case is left to
// a future polish; for now any skill is editable.
// ---------------------------------------------------------------------------

import { useEffect, useMemo, useState } from "preact/hooks";
import { confirm, toast } from "../../lib/dialogs.ts";
import { useFetch } from "../../hooks/use-fetch.ts";
import { api } from "../../api/client.ts";
import type { RegisteredSkill } from "../../../server/routes/skills.ts";
import { Page } from "../ui/page.tsx";
import { SkeletonRows } from "../ui/skeleton.tsx";
import { Markdown } from "../../lib/markdown.tsx";

type EditorTarget = { mode: "create" } | { mode: "edit"; skill: RegisteredSkill };

export function SkillsPage() {
  const { data, loading, error, refresh } = useFetch(() => api.listSkills(), [], 0);
  const skills = useMemo(() => data?.skills ?? [], [data]);
  const [editor, setEditor] = useState<EditorTarget | null>(null);
  const [query, setQuery] = useState("");

  const filtered = useMemo(() => {
    const q = query.trim().toLowerCase();
    if (!q) return skills;
    return skills.filter(
      (s) =>
        s.id.toLowerCase().includes(q) ||
        s.description.toLowerCase().includes(q) ||
        s.metadata.tags.some((t) => t.toLowerCase().includes(q)),
    );
  }, [skills, query]);

  const onDelete = async (id: string) => {
    if (
      !(await confirm({
        title: `Delete skill "${id}"`,
        message: "Agents that reference it will show a broken-ref warning until updated.",
        variant: "danger",
        confirmLabel: "Delete",
      }))
    )
      return;
    try {
      await api.deleteSkill(id);
      toast(`Deleted skill "${id}"`, { variant: "success" });
      refresh();
    } catch (err) {
      toast(err instanceof Error ? err.message : String(err), { variant: "error" });
    }
  };

  return (
    <Page>
      <div class="flex items-start justify-between gap-4">
        <div>
          <h2 class="text-xl font-semibold">Skills</h2>
          <p class="text-xs text-base-content/50 max-w-2xl">
            Reusable instruction blocks an agent loads on demand. Attach them to an agent in its
            editor; the agent sees each skill's description and pulls the full body into context via{" "}
            <span class="font-mono">loadSkill</span> when a task matches.
          </p>
        </div>
        <button
          type="button"
          class="btn btn-sm btn-primary"
          onClick={() => setEditor({ mode: "create" })}
        >
          New skill
        </button>
      </div>

      <input
        class="input input-bordered input-sm font-mono max-w-md"
        placeholder="Search skills…"
        value={query}
        onInput={(e) => setQuery((e.target as HTMLInputElement).value)}
      />

      {error && <div class="alert alert-error text-xs">{error.message}</div>}

      <div class="card bg-base-100 shadow overflow-hidden">
        <div class="overflow-x-auto">
          <table class="table table-sm">
            <thead>
              <tr class="bg-base-200 text-xs uppercase tracking-wider text-base-content/50">
                <th>Id</th>
                <th>Version</th>
                <th>Description</th>
                <th>Tags</th>
                <th class="text-right">Actions</th>
              </tr>
            </thead>
            <tbody>
              {loading && !data ? (
                <SkeletonRows rows={3} cols={5} />
              ) : filtered.length === 0 ? (
                <tr>
                  <td colSpan={5} class="text-center text-base-content/40 py-6 text-sm">
                    {skills.length === 0
                      ? "No skills registered yet. Create one, or drop a SKILL.md into the scan folder."
                      : "No skills match."}
                  </td>
                </tr>
              ) : (
                filtered.map((s) => (
                  <tr class={`hover ${s.metadata.enabled === false ? "opacity-60" : ""}`}>
                    <td class="font-mono text-xs">{s.id}</td>
                    <td class="font-mono text-xs text-base-content/50">{s.version}</td>
                    <td class="text-xs max-w-md truncate" title={s.description}>
                      {s.description}
                    </td>
                    <td class="flex flex-wrap gap-1 py-2">
                      {s.metadata.enabled === false && (
                        <span class="badge badge-xs badge-error">disabled</span>
                      )}
                      {s.metadata.tags.map((t) => (
                        <span class="badge badge-xs badge-ghost">{t}</span>
                      ))}
                    </td>
                    <td class="text-right whitespace-nowrap">
                      <button
                        type="button"
                        class="btn btn-xs btn-ghost"
                        onClick={() => setEditor({ mode: "edit", skill: s })}
                      >
                        Edit
                      </button>
                      <button
                        type="button"
                        class="btn btn-xs btn-ghost text-error"
                        onClick={() => onDelete(s.id)}
                      >
                        Delete
                      </button>
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </div>

      {editor && (
        <SkillEditor
          target={editor}
          existingIds={skills.map((s) => s.id)}
          onClose={() => setEditor(null)}
          onSaved={() => {
            setEditor(null);
            refresh();
          }}
        />
      )}
    </Page>
  );
}

interface SkillEditorProps {
  target: EditorTarget;
  existingIds: ReadonlyArray<string>;
  onClose: () => void;
  onSaved: () => void;
}

function SkillEditor({ target, existingIds, onClose, onSaved }: SkillEditorProps) {
  const isEdit = target.mode === "edit";
  const seed = target.mode === "edit" ? target.skill : null;
  const [id, setId] = useState(seed?.id ?? "");
  const [description, setDescription] = useState(seed?.description ?? "");
  const [whenToUse, setWhenToUse] = useState(
    // Hide the description-fallback so editing doesn't persist a duplicate.
    seed && seed.whenToUse !== seed.description ? seed.whenToUse : "",
  );
  const [body, setBody] = useState(seed?.body ?? "");
  const [tags, setTags] = useState((seed?.metadata.tags ?? []).join(", "));
  const [capabilities, setCapabilities] = useState((seed?.metadata.capabilities ?? []).join(", "));
  const [enabled, setEnabled] = useState(seed?.metadata.enabled !== false);
  const [preview, setPreview] = useState(false);
  const [saving, setSaving] = useState(false);
  const [err, setErr] = useState<string | null>(null);

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => e.key === "Escape" && onClose();
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onClose]);

  const parseList = (raw: string): string[] =>
    raw
      .split(",")
      .map((s) => s.trim())
      .filter((s) => s.length > 0);

  const idClash = !isEdit && existingIds.includes(id.trim());
  const valid = id.trim().length > 0 && description.trim().length > 0 && body.trim().length > 0;

  const save = async () => {
    setErr(null);
    setSaving(true);
    const metadata = { tags: parseList(tags), capabilities: parseList(capabilities), enabled };
    try {
      if (isEdit) {
        await api.updateSkill(id.trim(), {
          description: description.trim(),
          ...(whenToUse.trim() && { whenToUse: whenToUse.trim() }),
          body,
          metadata,
        });
      } else {
        await api.createSkill({
          id: id.trim(),
          description: description.trim(),
          ...(whenToUse.trim() && { whenToUse: whenToUse.trim() }),
          body,
          metadata,
        });
      }
      toast(`Saved skill "${id.trim()}"`, { variant: "success" });
      onSaved();
    } catch (e) {
      setErr(e instanceof Error ? e.message : String(e));
    } finally {
      setSaving(false);
    }
  };

  return (
    <div class="fixed inset-0 z-40 flex justify-end bg-black/30" onClick={onClose}>
      <div
        class="w-full max-w-2xl h-full bg-base-100 shadow-xl overflow-y-auto p-5 space-y-4"
        onClick={(e) => e.stopPropagation()}
      >
        <div class="flex items-center justify-between">
          <h3 class="text-lg font-semibold">{isEdit ? `Edit "${seed?.id}"` : "New skill"}</h3>
          <button type="button" class="btn btn-sm btn-ghost" onClick={onClose}>
            ✕
          </button>
        </div>

        <label class="form-control">
          <span class="text-xs text-base-content/60 mb-1 uppercase tracking-wider">Id</span>
          <input
            class="input input-bordered input-sm font-mono"
            placeholder="structured-debugging"
            value={id}
            disabled={isEdit}
            onInput={(e) => setId((e.target as HTMLInputElement).value)}
          />
          {idClash && (
            <span class="text-xs text-error mt-1">A skill with this id already exists.</span>
          )}
        </label>

        <label class="form-control">
          <span class="text-xs text-base-content/60 mb-1 uppercase tracking-wider">
            Description
          </span>
          <input
            class="input input-bordered input-sm"
            placeholder="One line shown in the agent's skill catalog."
            value={description}
            onInput={(e) => setDescription((e.target as HTMLInputElement).value)}
          />
        </label>

        <label class="form-control">
          <span class="text-xs text-base-content/60 mb-1 uppercase tracking-wider">
            When to use <span class="normal-case text-base-content/40">(optional)</span>
          </span>
          <input
            class="input input-bordered input-sm"
            placeholder="Defaults to the description when blank."
            value={whenToUse}
            onInput={(e) => setWhenToUse((e.target as HTMLInputElement).value)}
          />
        </label>

        <div class="form-control">
          <div class="flex items-center justify-between mb-1">
            <span class="text-xs text-base-content/60 uppercase tracking-wider">
              Body (markdown)
            </span>
            <button
              type="button"
              class={`btn btn-xs ${preview ? "btn-primary" : "btn-ghost"}`}
              onClick={() => setPreview((v) => !v)}
            >
              {preview ? "Edit" : "Preview"}
            </button>
          </div>
          {preview ? (
            <div class="border border-base-300 rounded p-3 min-h-48 prose prose-sm max-w-none">
              <Markdown text={body || "_(empty)_"} />
            </div>
          ) : (
            <textarea
              class="textarea textarea-bordered font-mono text-xs min-h-48"
              placeholder={"# Skill title\n\nStep-by-step instructions the agent should follow…"}
              value={body}
              onInput={(e) => setBody((e.target as HTMLTextAreaElement).value)}
            />
          )}
        </div>

        <div class="grid grid-cols-2 gap-3">
          <label class="form-control">
            <span class="text-xs text-base-content/60 mb-1 uppercase tracking-wider">
              Tags (comma-separated)
            </span>
            <input
              class="input input-bordered input-sm"
              placeholder="engineering, debugging"
              value={tags}
              onInput={(e) => setTags((e.target as HTMLInputElement).value)}
            />
          </label>
          <label class="form-control">
            <span class="text-xs text-base-content/60 mb-1 uppercase tracking-wider">
              Capabilities (comma-separated)
            </span>
            <input
              class="input input-bordered input-sm"
              placeholder="optional gate"
              value={capabilities}
              onInput={(e) => setCapabilities((e.target as HTMLInputElement).value)}
            />
          </label>
        </div>

        <label class="flex items-center gap-2 cursor-pointer">
          <input
            type="checkbox"
            class="toggle toggle-sm"
            checked={enabled}
            onChange={(e) => setEnabled((e.target as HTMLInputElement).checked)}
          />
          <span class="text-sm">Enabled</span>
          <span class="text-xs text-base-content/40">
            Disabled skills stay registered but are hidden from agent catalogs.
          </span>
        </label>

        {err && <div class="alert alert-error text-xs">{err}</div>}

        <div class="flex justify-end gap-2 pt-2">
          <button type="button" class="btn btn-sm btn-ghost" onClick={onClose}>
            Cancel
          </button>
          <button
            type="button"
            class="btn btn-sm btn-primary"
            disabled={!valid || idClash || saving}
            onClick={save}
          >
            {saving ? "Saving…" : isEdit ? "Save" : "Create"}
          </button>
        </div>
      </div>
    </div>
  );
}
