// ---------------------------------------------------------------------------
// FragmentsPage — operator UI for the prompt-fragment registry. Lists each
// fragment's active version and lets operators author custom ones (markdown
// body with live preview). Saving publishes a new version; the editor's
// Versions panel makes any version active. File-scanned fragments show a
// 📄 badge and disable Save (edit the source file instead). Parallel to
// SkillsPage.
// ---------------------------------------------------------------------------

import { useEffect, useMemo, useState } from "preact/hooks";
import { confirm, toast } from "../../lib/dialogs.ts";
import { useFetch } from "../../hooks/use-fetch.ts";
import { api } from "../../api/client.ts";
import type { RegisteredFragment } from "../../../server/routes/fragments.ts";
import { Page, PageHeader } from "../ui/page.tsx";
import { SkeletonRows } from "../ui/skeleton.tsx";
import { VersionsPanel } from "../ui/versions-panel.tsx";
import { Markdown } from "../../lib/markdown.tsx";

type EditorTarget = { mode: "create" } | { mode: "edit"; fragment: RegisteredFragment };

export function FragmentsPage() {
  const { data, loading, error, refresh } = useFetch(() => api.listFragments(), [], 0);
  const fragments = useMemo(() => data?.fragments ?? [], [data]);
  const { data: sourcesData } = useFetch(() => api.listFragmentSources(), [], 0);
  const fileManaged = useMemo(() => new Set(sourcesData?.fileManaged ?? []), [sourcesData]);
  const [editor, setEditor] = useState<EditorTarget | null>(null);
  const [query, setQuery] = useState("");

  const filtered = useMemo(() => {
    const q = query.trim().toLowerCase();
    if (!q) return fragments;
    return fragments.filter(
      (f) => f.id.toLowerCase().includes(q) || f.content.toLowerCase().includes(q),
    );
  }, [fragments, query]);

  const onDelete = async (id: string) => {
    if (
      !(await confirm({
        title: `Delete fragment "${id}"`,
        message:
          "Removes every version. A fragment still layered by a role or an agent's inline role is refused — remove it there first.",
        variant: "danger",
        confirmLabel: "Delete",
      }))
    )
      return;
    try {
      await api.deleteFragment(id);
      toast(`Deleted fragment "${id}"`, { variant: "success" });
      refresh();
    } catch (err) {
      toast(err instanceof Error ? err.message : String(err), { variant: "error" });
    }
  };

  return (
    <Page>
      <PageHeader
        title="Fragments"
        eyebrow="Agent runtime"
        description={
          <>
            Reusable prompt layers roles compose into their system prompt at resolve time. Always-on
            (concatenated every turn), vs. skills which are load-on-demand. A role layer{" "}
            <span class="font-mono">id</span> follows the active version;{" "}
            <span class="font-mono">id@version</span> pins one. File-scanned fragments (.md) show a
            📄 badge and are read-only here — edit the source file instead.
          </>
        }
        actions={
          <button
            type="button"
            class="btn btn-sm btn-primary"
            onClick={() => setEditor({ mode: "create" })}
          >
            New fragment
          </button>
        }
      />

      <input
        class="input input-bordered input-sm font-mono max-w-md"
        placeholder="Search fragments…"
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
                <th>Preview</th>
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
                    {fragments.length === 0
                      ? "No fragments registered yet. Create one, or drop a .md file into the scan folder."
                      : "No fragments match."}
                  </td>
                </tr>
              ) : (
                filtered.map((f) => (
                  <tr class={`hover ${!f.enabled ? "opacity-60" : ""}`}>
                    <td class="font-mono text-xs">{f.id}</td>
                    <td class="font-mono text-xs text-base-content/50">{f.version}</td>
                    <td class="text-xs max-w-md truncate" title={f.content}>
                      {firstLine(f.content)}
                    </td>
                    <td class="flex flex-wrap gap-1 py-2">
                      {fileManaged.has(f.id) && (
                        <span
                          class="badge badge-xs badge-info gap-1"
                          title="Defined by a file on disk — edit the source file, not here"
                        >
                          📄 file
                        </span>
                      )}
                      {!f.enabled && <span class="badge badge-xs badge-error">disabled</span>}
                    </td>
                    <td class="text-right whitespace-nowrap">
                      <button
                        type="button"
                        class="btn btn-xs btn-ghost"
                        onClick={() => setEditor({ mode: "edit", fragment: f })}
                      >
                        Edit
                      </button>
                      <button
                        type="button"
                        class="btn btn-xs btn-ghost text-error"
                        onClick={() => onDelete(f.id)}
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
        <FragmentEditor
          target={editor}
          existingIds={fragments.map((f) => f.id)}
          fileManaged={editor.mode === "edit" && fileManaged.has(editor.fragment.id)}
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

function firstLine(text: string): string {
  const t = text.trim();
  const nl = t.indexOf("\n");
  return nl === -1 ? t : t.slice(0, nl);
}

interface FragmentEditorProps {
  target: EditorTarget;
  existingIds: ReadonlyArray<string>;
  fileManaged?: boolean;
  onClose: () => void;
  onSaved: () => void;
}

function FragmentEditor({
  target,
  existingIds,
  fileManaged,
  onClose,
  onSaved,
}: FragmentEditorProps) {
  const isEdit = target.mode === "edit";
  const seed = target.mode === "edit" ? target.fragment : null;
  const [id, setId] = useState(seed?.id ?? "");
  const [content, setContent] = useState(seed?.content ?? "");
  const [enabled, setEnabled] = useState(seed?.enabled ?? true);
  const [preview, setPreview] = useState(false);
  const [saving, setSaving] = useState(false);
  const [err, setErr] = useState<string | null>(null);

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => e.key === "Escape" && onClose();
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onClose]);

  const idClash = !isEdit && existingIds.includes(id.trim());
  const valid = id.trim().length > 0 && content.trim().length > 0;

  const save = async () => {
    setErr(null);
    setSaving(true);
    try {
      if (isEdit && seed) {
        // A new version built on the one this editor loaded (a concurrent
        // edit answers 412); unchanged content stores no new version.
        await api.updateFragment(seed.id, {
          expectedVersion: seed.version,
          ...(content !== seed.content && { content }),
          ...(enabled !== seed.enabled && { enabled }),
        });
      } else {
        const created = await api.createFragment({ id: id.trim(), content });
        if (!enabled) {
          await api.updateFragment(created.id, { expectedVersion: created.version, enabled });
        }
      }
      toast(`Saved fragment "${id.trim()}"`, { variant: "success" });
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
          <h3 class="text-lg font-semibold">{isEdit ? `Edit "${seed?.id}"` : "New fragment"}</h3>
          <button type="button" class="btn btn-sm btn-ghost" onClick={onClose}>
            ✕
          </button>
        </div>

        {fileManaged && (
          <div class="alert alert-warning text-xs">
            📄 This fragment is defined by a file on disk. Editing its text is disabled here — edit
            the source file; the next scan publishes it as a new version.
          </div>
        )}

        <label class="form-control">
          <span class="text-xs text-base-content/60 mb-1 uppercase tracking-wider">Id</span>
          <input
            class="input input-bordered input-sm font-mono"
            placeholder="e.g. review-checklist"
            value={id}
            disabled={isEdit}
            onInput={(e) => setId((e.target as HTMLInputElement).value)}
          />
          {idClash && (
            <span class="text-xs text-error mt-1">A fragment with this id already exists.</span>
          )}
        </label>

        <div class="form-control">
          <div class="flex items-center justify-between mb-1">
            <span class="text-xs text-base-content/60 uppercase tracking-wider">
              Content (markdown)
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
              <Markdown text={content || "_(empty)_"} />
            </div>
          ) : (
            <textarea
              class="textarea textarea-bordered font-mono text-xs min-h-48"
              placeholder={"## Title\n\nReusable instruction layer — what should the agent do?"}
              value={content}
              disabled={fileManaged === true}
              onInput={(e) => setContent((e.target as HTMLTextAreaElement).value)}
            />
          )}
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
            A disabled fragment stays registered but is left out of every prompt.
          </span>
        </label>

        {seed && (
          <VersionsPanel
            reloadKey={seed.id}
            load={() => api.listFragmentVersions(seed.id)}
            promote={(p) => api.promoteFragment(seed.id, p)}
            onPromoted={() => onSaved()}
            describe={(v) => firstLine(v.content)}
          />
        )}

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
