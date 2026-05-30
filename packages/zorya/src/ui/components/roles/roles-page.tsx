// ---------------------------------------------------------------------------
// RolesPage — CRUD over the role registry. A role is the behavioral bundle an
// agent binds: persona prompt (plain or layered over fragments) + tools +
// skills + capabilities. This page lists registered roles, creates new ones,
// edits them in place (RoleEditDrawer), and deletes them.
// ---------------------------------------------------------------------------

import { useMemo, useState } from "preact/hooks";
import { useFetch } from "../../hooks/use-fetch.ts";
import { api } from "../../api/client.ts";
import type { RegisteredRole } from "../../../server/routes/roles.ts";
import { Page } from "../ui/page.tsx";
import { SkeletonRows } from "../ui/skeleton.tsx";
import { formatRelative } from "../../lib/format.ts";
import { RoleEditDrawer } from "./role-edit-drawer.tsx";

// Seed for "+ New role" — a blank role the create drawer edits into shape.
function blankRole(): RegisteredRole {
  return {
    id: "",
    version: "v1",
    definition: { systemPrompt: null, tools: [] },
    metadata: { description: null, tags: [] },
    createdAt: 0,
    updatedAt: 0,
  };
}

interface RolesPageProps {
  /**
   * Opens an agent detail. No longer used by this page (roles are their own
   * registry now), kept so the app.tsx call site stays compatible.
   */
  onOpenAgent?: (id: string) => void;
}

export function RolesPage(_props: RolesPageProps) {
  const { data, loading, error, refresh } = useFetch(() => api.listRoles(), [], 30_000);
  const [query, setQuery] = useState("");
  // The role currently open in the edit/create drawer, with its mode.
  const [editing, setEditing] = useState<{ role: RegisteredRole; mode: "create" | "edit" } | null>(
    null,
  );
  const [deleting, setDeleting] = useState<RegisteredRole | null>(null);

  const roles = useMemo(() => {
    const list = data?.roles ?? [];
    const q = query.trim().toLowerCase();
    const filtered = !q
      ? list
      : list.filter((r) => {
          const fields = [
            r.id,
            r.metadata.description ?? "",
            ...(r.definition.capabilities ?? []),
            ...r.metadata.tags,
          ];
          return fields.some((f) => f.toLowerCase().includes(q));
        });
    return [...filtered].sort((a, b) => a.id.localeCompare(b.id));
  }, [data, query]);

  return (
    <Page>
      <div class="flex items-start justify-between gap-2">
        <div>
          <h2 class="text-xl font-semibold">Roles</h2>
          <p class="text-xs text-base-content/50 max-w-2xl">
            The behavioral bundle an agent binds — persona prompt (plain or layered over fragments)
            plus tools, skills, and capabilities. Agents bind a role by ref (shared, live) or
            inline.
          </p>
        </div>
        <button
          type="button"
          class="btn btn-sm btn-primary whitespace-nowrap"
          onClick={() => setEditing({ role: blankRole(), mode: "create" })}
        >
          + New role
        </button>
      </div>

      {error && <div class="alert alert-error text-xs">{error.message}</div>}

      <input
        class="input input-bordered input-sm font-mono max-w-md"
        placeholder="Search roles…"
        value={query}
        onInput={(e) => setQuery((e.target as HTMLInputElement).value)}
      />

      <div class="card bg-base-100 shadow">
        <div class="card-body p-0">
          <table class="table table-sm">
            <thead>
              <tr>
                <th>Role</th>
                <th>Description</th>
                <th class="text-right">Tools</th>
                <th>Tags</th>
                <th>Updated</th>
                <th class="text-right">Actions</th>
              </tr>
            </thead>
            <tbody>
              {loading && !data ? (
                <SkeletonRows rows={4} cols={6} />
              ) : roles.length === 0 ? (
                <tr>
                  <td colSpan={6} class="text-center text-base-content/50 py-8">
                    {data && data.roles.length === 0
                      ? "No roles registered yet. Create one with “+ New role”."
                      : query
                        ? `No roles match "${query}".`
                        : "No roles."}
                  </td>
                </tr>
              ) : (
                roles.map((r) => (
                  <tr
                    class="hover cursor-pointer"
                    onClick={() => setEditing({ role: r, mode: "edit" })}
                  >
                    <td class="font-mono text-xs font-semibold">{r.id}</td>
                    <td class="text-xs text-base-content/70 max-w-xs truncate">
                      {r.metadata.description ?? <span class="text-base-content/30">—</span>}
                    </td>
                    <td class="text-right text-xs">{r.definition.tools.length}</td>
                    <td>
                      <div class="flex flex-wrap gap-1">
                        {r.metadata.tags.length === 0 ? (
                          <span class="text-base-content/30 text-xs">—</span>
                        ) : (
                          r.metadata.tags.map((t) => (
                            <span class="badge badge-xs badge-ghost">{t}</span>
                          ))
                        )}
                      </div>
                    </td>
                    <td class="text-xs text-base-content/60 whitespace-nowrap">
                      {r.updatedAt ? formatRelative(new Date(r.updatedAt).toISOString()) : "—"}
                    </td>
                    <td class="text-right">
                      <button
                        type="button"
                        class="btn btn-xs btn-ghost text-error"
                        title="Delete role"
                        onClick={(e) => {
                          e.stopPropagation();
                          setDeleting(r);
                        }}
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

      {editing && (
        <RoleEditDrawer
          role={editing.role}
          mode={editing.mode}
          onClose={() => setEditing(null)}
          onSaved={() => {
            setEditing(null);
            refresh();
          }}
        />
      )}

      {deleting && (
        <DeleteRoleDialog
          role={deleting}
          onClose={() => setDeleting(null)}
          onDeleted={() => {
            setDeleting(null);
            refresh();
          }}
        />
      )}
    </Page>
  );
}

function DeleteRoleDialog({
  role,
  onClose,
  onDeleted,
}: {
  role: RegisteredRole;
  onClose: () => void;
  onDeleted: () => void;
}) {
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const confirm = async () => {
    setBusy(true);
    setError(null);
    try {
      await api.deleteRole(role.id);
      onDeleted();
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
      setBusy(false);
    }
  };

  return (
    <>
      <div class="fixed inset-0 bg-black/40 z-30 anim-backdrop-in" onClick={onClose} aria-hidden />
      <div class="fixed inset-0 z-40 flex items-center justify-center p-4" role="dialog">
        <div class="card bg-base-100 shadow-2xl max-w-md w-full anim-drawer-in">
          <div class="card-body p-4 space-y-3">
            <h3 class="font-semibold">Delete role?</h3>
            <p class="text-sm text-base-content/70">
              Delete <span class="font-mono">{role.id}</span> (all versions). Agents binding it by
              ref will fail to resolve until rebound. This can't be undone.
            </p>
            {error && <div class="alert alert-error text-xs">{error}</div>}
            <div class="flex justify-end gap-2">
              <button type="button" class="btn btn-sm btn-ghost" onClick={onClose} disabled={busy}>
                Cancel
              </button>
              <button type="button" class="btn btn-sm btn-error" onClick={confirm} disabled={busy}>
                {busy ? "Deleting…" : "Delete"}
              </button>
            </div>
          </div>
        </div>
      </div>
    </>
  );
}
