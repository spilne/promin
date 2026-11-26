// ---------------------------------------------------------------------------
// McpEndpointsPage — the MCP endpoints this server publishes. Lists each
// endpoint's active version (tools, tenants, file badge, URL path), creates
// and edits them in McpEndpointDrawer, and deletes them.
// ---------------------------------------------------------------------------

import { useMemo, useState } from "preact/hooks";
import { api } from "../../api/client.ts";
import type { RegisteredMcpEndpoint } from "../../../server/routes/mcp-endpoints.ts";
import { useFetch } from "../../hooks/use-fetch.ts";
import { formatRelative } from "../../lib/format.ts";
import { Page, PageHeader } from "../ui/page.tsx";
import { SkeletonRows } from "../ui/skeleton.tsx";
import { McpEndpointDrawer } from "./mcp-endpoint-drawer.tsx";

function blankEndpoint(): RegisteredMcpEndpoint {
  return {
    id: "",
    version: "v1",
    title: null,
    instructions: null,
    tools: [],
    capabilities: [],
    namespaces: null,
    trustClientApproval: false,
    metadata: { description: null, tags: [] },
    createdAt: 0,
    active: true,
    enabled: true,
    trust: "trusted",
  };
}

export function McpEndpointsPage() {
  const { data, loading, error, refresh } = useFetch(() => api.listMcpEndpoints(), [], 30_000);
  const { data: sources, refresh: refreshSources } = useFetch(
    () => api.listMcpEndpointSources(),
    [],
    30_000,
  );
  const fileManaged = useMemo(() => new Set(sources?.fileManaged ?? []), [sources]);
  const [query, setQuery] = useState("");
  const [editing, setEditing] = useState<{
    endpoint: RegisteredMcpEndpoint;
    mode: "create" | "edit";
  } | null>(null);
  const [deleting, setDeleting] = useState<RegisteredMcpEndpoint | null>(null);

  const endpoints = useMemo(() => {
    const list = data?.endpoints ?? [];
    const q = query.trim().toLowerCase();
    const filtered = !q
      ? list
      : list.filter((e) =>
          [e.id, e.title ?? "", e.metadata.description ?? "", ...e.metadata.tags].some((f) =>
            f.toLowerCase().includes(q),
          ),
        );
    return [...filtered].sort((a, b) => a.id.localeCompare(b.id));
  }, [data, query]);

  const reload = () => {
    refresh();
    refreshSources();
  };

  return (
    <Page>
      <PageHeader
        title="MCP endpoints"
        eyebrow="Agent runtime"
        description={
          <>
            Bundles of tools this server publishes over MCP, so Claude Code, Cursor or any MCP
            client can call them. Each tenant reaches an endpoint at{" "}
            <span class="font-mono">/ns/&lt;namespace&gt;/mcp/&lt;id&gt;</span>. Define them here or
            as files in the <span class="font-mono">mcp/</span> folder.
          </>
        }
        actions={
          <button
            type="button"
            class="btn btn-sm btn-primary whitespace-nowrap"
            onClick={() => setEditing({ endpoint: blankEndpoint(), mode: "create" })}
          >
            + New endpoint
          </button>
        }
      />

      {error && <div class="alert alert-error text-xs">{error.message}</div>}

      <input
        class="input input-bordered input-sm font-mono max-w-md"
        placeholder="Search endpoints…"
        value={query}
        onInput={(e) => setQuery((e.target as HTMLInputElement).value)}
      />

      <div class="card bg-base-100 shadow">
        <div class="card-body p-0">
          <table class="table table-sm">
            <thead>
              <tr>
                <th>Endpoint</th>
                <th>Title</th>
                <th class="text-right">Tools</th>
                <th>Namespaces</th>
                <th>Status</th>
                <th>Active version</th>
                <th class="text-right">Actions</th>
              </tr>
            </thead>
            <tbody>
              {loading && !data ? (
                <SkeletonRows rows={3} cols={7} />
              ) : endpoints.length === 0 ? (
                <tr>
                  <td colSpan={7} class="text-center text-base-content/50 py-8">
                    {data && data.endpoints.length === 0
                      ? "No MCP endpoints yet. Create one with “+ New endpoint”."
                      : `No endpoints match "${query}".`}
                  </td>
                </tr>
              ) : (
                endpoints.map((e) => (
                  <tr
                    class="hover cursor-pointer"
                    onClick={() => setEditing({ endpoint: e, mode: "edit" })}
                  >
                    <td class="font-mono text-xs font-semibold">{e.id}</td>
                    <td class="text-xs max-w-xs truncate">
                      {e.title ?? e.metadata.description ?? (
                        <span class="text-base-content/30">—</span>
                      )}
                    </td>
                    <td class="text-right text-xs">{e.tools.length}</td>
                    <td class="text-xs font-mono">
                      {e.namespaces === null ? (
                        <span class="text-base-content/50 font-sans">all</span>
                      ) : (
                        e.namespaces.join(", ")
                      )}
                    </td>
                    <td>
                      <div class="flex flex-wrap gap-1">
                        {e.enabled ? (
                          <span class="badge badge-xs badge-success">enabled</span>
                        ) : (
                          <span class="badge badge-xs badge-error">disabled</span>
                        )}
                        {fileManaged.has(e.id) && (
                          <span class="badge badge-xs badge-ghost">file</span>
                        )}
                      </div>
                    </td>
                    <td class="text-xs text-base-content/60 whitespace-nowrap">
                      <span class="font-mono">{e.version}</span>
                      {e.createdAt > 0 && (
                        <> · {formatRelative(new Date(e.createdAt).toISOString())}</>
                      )}
                    </td>
                    <td class="text-right">
                      <button
                        type="button"
                        class="btn btn-xs btn-ghost text-error"
                        onClick={(ev) => {
                          ev.stopPropagation();
                          setDeleting(e);
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
        <McpEndpointDrawer
          endpoint={editing.endpoint}
          mode={editing.mode}
          fileManaged={fileManaged.has(editing.endpoint.id)}
          onClose={() => setEditing(null)}
          onSaved={() => {
            setEditing(null);
            reload();
          }}
        />
      )}

      {deleting && (
        <DeleteDialog
          endpoint={deleting}
          onClose={() => setDeleting(null)}
          onDeleted={() => {
            setDeleting(null);
            reload();
          }}
        />
      )}
    </Page>
  );
}

function DeleteDialog(props: {
  endpoint: RegisteredMcpEndpoint;
  onClose: () => void;
  onDeleted: () => void;
}) {
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const confirm = async () => {
    setBusy(true);
    setError(null);
    try {
      await api.deleteMcpEndpoint(props.endpoint.id);
      props.onDeleted();
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
      setBusy(false);
    }
  };
  return (
    <>
      <div
        class="fixed inset-0 bg-black/40 z-30 anim-backdrop-in"
        onClick={props.onClose}
        aria-hidden
      />
      <div class="fixed inset-0 z-40 flex items-center justify-center p-4" role="dialog">
        <div class="card bg-base-100 shadow-2xl max-w-md w-full anim-drawer-in">
          <div class="card-body p-4 space-y-3">
            <h3 class="font-semibold">Delete MCP endpoint?</h3>
            <p class="text-sm text-base-content/70">
              Delete <span class="font-mono">{props.endpoint.id}</span> (all versions). Connected
              clients start getting 404. A file-defined endpoint comes back on the next scan unless
              its file is removed.
            </p>
            {error && <div class="alert alert-error text-xs">{error}</div>}
            <div class="flex justify-end gap-2">
              <button
                type="button"
                class="btn btn-sm btn-ghost"
                onClick={props.onClose}
                disabled={busy}
              >
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
