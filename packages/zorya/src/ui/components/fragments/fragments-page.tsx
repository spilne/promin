// ---------------------------------------------------------------------------
// FragmentsPage — operator UI for the prompt-fragment registry. Lists
// registered fragments and lets operators author custom ones (markdown body
// with live preview). File-scanned fragments show a 📄 badge and disable
// Save (edit the source file instead). Parallel to SkillsPage.
// ---------------------------------------------------------------------------

import { useEffect, useMemo, useState } from "preact/hooks";
import { confirm, toast } from "../../lib/dialogs.ts";
import { useFetch } from "../../hooks/use-fetch.ts";
import { api } from "../../api/client.ts";
import type { FragmentDto } from "../../../server/routes/fragments.ts";
import { Page } from "../ui/page.tsx";
import { SkeletonRows } from "../ui/skeleton.tsx";
import { Markdown } from "../../lib/markdown.tsx";

type EditorTarget = { mode: "create" } | { mode: "edit"; fragment: FragmentDto };

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
      (f) => f.key.toLowerCase().includes(q) || f.content.toLowerCase().includes(q),
    );
  }, [fragments, query]);

  const onDelete = async (key: string) => {
    if (
      !(await confirm({
        title: `Delete fragment "${key}"`,
        message:
          "Role recipes referencing it will fall back to base only (with a console warning at resolve).",
        variant: "danger",
        confirmLabel: "Delete",
      }))
    )
      return;
    try {
      await api.deleteFragment(key);
      toast(`Deleted fragment "${key}"`, { variant: "success" });
      refresh();
    } catch (err) {
      toast(err instanceof Error ? err.message : String(err), { variant: "error" });
    }
  };

  return (
    <Page>
      <div class="flex items-start justify-between gap-4">
        <div>
          <h2 class="text-xl font-semibold">Fragments</h2>
          <p class="text-xs text-base-content/50 max-w-2xl">
            Reusable prompt layers role recipes compose into their system prompt at resolve time.
            Always-on (concatenated every turn), vs. skills which are load-on-demand. File-scanned
            fragments (.md) show a 📄 badge and are read-only here — edit the source file instead.
          </p>
        </div>
        <button
          type="button"
          class="btn btn-sm btn-primary"
          onClick={() => setEditor({ mode: "create" })}
        >
          New fragment
        </button>
      </div>

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
                <th>Key</th>
                <th>Preview</th>
                <th>Tags</th>
                <th class="text-right">Actions</th>
              </tr>
            </thead>
            <tbody>
              {loading && !data ? (
                <SkeletonRows rows={3} cols={4} />
              ) : filtered.length === 0 ? (
                <tr>
                  <td colSpan={4} class="text-center text-base-content/40 py-6 text-sm">
                    {fragments.length === 0
                      ? "No fragments registered yet. Create one, or drop a .md file into the scan folder."
                      : "No fragments match."}
                  </td>
                </tr>
              ) : (
                filtered.map((f) => (
                  <tr class="hover">
                    <td class="font-mono text-xs">{f.key}</td>
                    <td class="text-xs max-w-md truncate" title={f.content}>
                      {firstLine(f.content)}
                    </td>
                    <td class="flex flex-wrap gap-1 py-2">
                      {fileManaged.has(f.key) && (
                        <span
                          class="badge badge-xs badge-info gap-1"
                          title="Defined by a file on disk — edit the source file, not here"
                        >
                          📄 file
                        </span>
                      )}
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
                        onClick={() => onDelete(f.key)}
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
          existingKeys={fragments.map((f) => f.key)}
          fileManaged={editor.mode === "edit" && fileManaged.has(editor.fragment.key)}
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
  existingKeys: ReadonlyArray<string>;
  fileManaged?: boolean;
  onClose: () => void;
  onSaved: () => void;
}

function FragmentEditor({
  target,
  existingKeys,
  fileManaged,
  onClose,
  onSaved,
}: FragmentEditorProps) {
  const isEdit = target.mode === "edit";
  const seed = target.mode === "edit" ? target.fragment : null;
  const [key, setKey] = useState(seed?.key ?? "");
  const [content, setContent] = useState(seed?.content ?? "");
  const [preview, setPreview] = useState(false);
  const [saving, setSaving] = useState(false);
  const [err, setErr] = useState<string | null>(null);

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => e.key === "Escape" && onClose();
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onClose]);

  const keyClash = !isEdit && existingKeys.includes(key.trim());
  const valid = key.trim().length > 0 && content.trim().length > 0;

  const save = async () => {
    setErr(null);
    setSaving(true);
    try {
      if (isEdit) {
        await api.updateFragment(key.trim(), { content });
      } else {
        await api.createFragment({ key: key.trim(), content });
      }
      toast(`Saved fragment "${key.trim()}"`, { variant: "success" });
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
          <h3 class="text-lg font-semibold">{isEdit ? `Edit "${seed?.key}"` : "New fragment"}</h3>
          <button type="button" class="btn btn-sm btn-ghost" onClick={onClose}>
            ✕
          </button>
        </div>

        {fileManaged && (
          <div class="alert alert-warning text-xs">
            📄 This fragment is defined by a file on disk. Saving is disabled here — edit the source
            file instead, since the next scan would overwrite any change made through the UI.
          </div>
        )}

        <label class="form-control">
          <span class="text-xs text-base-content/60 mb-1 uppercase tracking-wider">Key</span>
          <input
            class="input input-bordered input-sm font-mono"
            placeholder="e.g. review-checklist"
            value={key}
            disabled={isEdit}
            onInput={(e) => setKey((e.target as HTMLInputElement).value)}
          />
          {keyClash && (
            <span class="text-xs text-error mt-1">A fragment with this key already exists.</span>
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
              onInput={(e) => setContent((e.target as HTMLTextAreaElement).value)}
            />
          )}
        </div>

        {err && <div class="alert alert-error text-xs">{err}</div>}

        <div class="flex justify-end gap-2 pt-2">
          <button type="button" class="btn btn-sm btn-ghost" onClick={onClose}>
            Cancel
          </button>
          <button
            type="button"
            class="btn btn-sm btn-primary"
            disabled={!valid || keyClash || saving || fileManaged}
            onClick={save}
          >
            {saving ? "Saving…" : isEdit ? "Save" : "Create"}
          </button>
        </div>
      </div>
    </div>
  );
}
