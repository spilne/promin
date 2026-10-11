// ---------------------------------------------------------------------------
// ToolsPage — read-only catalog of every tool the host has wired in.
// Shows tool name, description, source (in-process / file / MCP), trust
// (approval, idempotency, untrusted MCP output), an expandable JSON Schema
// view for parameters, and the MCP servers the catalog could not list.
//
// Drives nothing today — this is the operator-facing 'what tools does
// this deployment have?' surface. The Designer's tool multi-select can
// reuse the same /api/agents/_catalog/tools endpoint with a different UI
// shape.
// ---------------------------------------------------------------------------

import { useMemo, useState } from "preact/hooks";
import { useFetch } from "../../hooks/use-fetch.ts";
import {
  api,
  type ToolCatalogEntryDto,
  type ToolCatalogErrorDto,
  type ToolCatalogHealthDto,
  type ToolCatalogSourceDto,
} from "../../api/client.ts";
import { MetaDivider, Page, PageHeader } from "../ui/page.tsx";
import { SkeletonRows } from "../ui/skeleton.tsx";

export function ToolsPage() {
  const { data, loading, error, refresh } = useFetch(() => api.listCatalogTools(), [], 0);
  const { data: health, refresh: refreshHealth } = useFetch(
    () => api.getToolCatalogHealth(),
    [],
    0,
  );
  const [query, setQuery] = useState("");
  const [sourceFilter, setSourceFilter] = useState<"all" | "in-process" | "file" | "mcp">("all");

  const tools = data?.tools ?? [];
  const catalogErrors = data?.errors ?? [];
  const orphans = health?.orphans ?? [];

  const filtered = useMemo(() => {
    const q = query.trim().toLowerCase();
    return tools.filter((t) => {
      if (sourceFilter !== "all" && t.source.kind !== sourceFilter) return false;
      if (!q) return true;
      return t.name.toLowerCase().includes(q) || t.description.toLowerCase().includes(q);
    });
  }, [tools, query, sourceFilter]);

  const counts = useMemo(() => {
    const c = { "in-process": 0, file: 0, mcp: 0 };
    for (const t of tools) c[t.source.kind] += 1;
    return c;
  }, [tools]);

  if (error) {
    return (
      <Page space="none">
        <div class="alert alert-error">{error.message}</div>
      </Page>
    );
  }

  return (
    <Page>
      <PageHeader
        title="Tools"
        eyebrow="Agent runtime"
        description={
          loading && !data
            ? "Loading tool catalog…"
            : tools.length === 0
              ? "Tool catalog is not configured on this server."
              : "Tools agents can call: in-process functions, file-defined tools, and MCP servers."
        }
        meta={
          tools.length > 0 ? (
            <>
              <span class="font-mono">{tools.length}</span>
              <span>available</span>
              <MetaDivider />
              <span>{counts["in-process"]} in-process</span>
              <MetaDivider />
              <span>{counts.file} file</span>
              <MetaDivider />
              <span>{counts.mcp} MCP</span>
            </>
          ) : undefined
        }
        actions={
          <button
            class="btn btn-sm btn-ghost gap-1"
            onClick={() => {
              refresh();
              refreshHealth();
            }}
          >
            <span aria-hidden="true">↻</span>
            Refresh
          </button>
        }
      />

      {catalogErrors.length > 0 && <CatalogErrorsPanel errors={catalogErrors} />}
      {orphans.length > 0 && <OrphansPanel orphans={orphans} />}

      {tools.length > 0 && (
        <div class="flex items-center gap-2">
          <input
            class="input input-bordered input-sm flex-1 max-w-md font-mono"
            placeholder="Search by name or description…"
            value={query}
            onInput={(e) => setQuery((e.target as HTMLInputElement).value)}
          />
          <select
            class="select select-bordered select-sm"
            value={sourceFilter}
            onChange={(e) =>
              setSourceFilter((e.target as HTMLSelectElement).value as typeof sourceFilter)
            }
          >
            <option value="all">All sources</option>
            <option value="in-process">In-process</option>
            <option value="file">File</option>
            <option value="mcp">MCP</option>
          </select>
        </div>
      )}

      <div class="card bg-base-100 shadow overflow-hidden">
        <div class="overflow-x-auto">
          <table class="table">
            <thead>
              <tr class="bg-base-200 text-xs uppercase tracking-wider text-base-content/50">
                <th class="w-1/4">Name</th>
                <th>Description</th>
                <th class="w-32">Source</th>
                <th class="w-40">Trust</th>
                <th class="w-24 text-right">Params</th>
              </tr>
            </thead>
            <tbody>
              {loading && !data ? (
                <SkeletonRows rows={4} cols={5} />
              ) : filtered.length === 0 ? (
                <tr>
                  <td colSpan={5} class="text-center text-base-content/50 py-8">
                    {tools.length === 0
                      ? "No tools registered. Wire AgentToolCatalog into ZoryaAgents config."
                      : `No tools match "${query}"${sourceFilter !== "all" ? ` in ${sourceFilter}` : ""}.`}
                  </td>
                </tr>
              ) : (
                filtered.map((t) => <ToolRow tool={t} />)
              )}
            </tbody>
          </table>
        </div>
      </div>
    </Page>
  );
}

function ToolRow({ tool }: { tool: ToolCatalogEntryDto }) {
  const [expanded, setExpanded] = useState(false);
  return (
    <>
      <tr class={`hover:bg-base-200 ${tool.enabled ? "" : "opacity-60"}`}>
        <td class="font-mono text-sm">
          {tool.name}
          {!tool.enabled && (
            <span class="badge badge-xs badge-error ml-2" title="Tool is disabled (enabled: false)">
              disabled
            </span>
          )}
        </td>
        <td class="text-xs text-base-content/70">{tool.description}</td>
        <td>
          <SourceBadge source={tool.source} />
        </td>
        <td>
          <TrustBadges tool={tool} />
        </td>
        <td class="text-right">
          <button class="btn btn-xs btn-ghost" onClick={() => setExpanded((v) => !v)}>
            {expanded ? "Hide" : "Show"}
          </button>
        </td>
      </tr>
      {expanded && (
        <tr>
          <td colSpan={5} class="bg-base-200">
            <pre class="text-[11px] font-mono p-2 whitespace-pre-wrap break-words">
              {JSON.stringify(tool.parameters, null, 2)}
            </pre>
          </td>
        </tr>
      )}
    </>
  );
}

function TrustBadges({ tool }: { tool: ToolCatalogEntryDto }) {
  return (
    <div class="flex gap-1 flex-wrap">
      {tool.requireApproval && (
        <span
          class="badge badge-xs badge-warning badge-outline"
          title="Each call waits for approval"
        >
          approval
        </span>
      )}
      {tool.idempotent && (
        <span class="badge badge-xs badge-outline" title="Re-run after a crash">
          idempotent
        </span>
      )}
      {tool.source.kind === "mcp" && (
        <span
          class="badge badge-xs badge-error badge-outline"
          title="Description and output come from the MCP server and are treated as untrusted data"
        >
          untrusted output
        </span>
      )}
    </div>
  );
}

function CatalogErrorsPanel({ errors }: { errors: ToolCatalogErrorDto[] }) {
  return (
    <div class="card bg-error/10 border border-error/30 shadow-sm">
      <div class="card-body p-4 space-y-2">
        <div class="flex items-center gap-2">
          <span class="text-error">⚠</span>
          <h3 class="text-sm font-semibold">MCP servers or tools that could not be listed</h3>
          <span class="badge badge-sm badge-error badge-outline">{errors.length}</span>
        </div>
        <ul class="text-xs space-y-1">
          {errors.map((e) => (
            <li class="font-mono">
              <span class="font-semibold">{e.server}</span>
              {e.tool !== undefined && <span class="opacity-70">/{e.tool}</span>}:{" "}
              <span class="text-base-content/70">{e.message}</span>
            </li>
          ))}
        </ul>
      </div>
    </div>
  );
}

function OrphansPanel({ orphans }: { orphans: ToolCatalogHealthDto["orphans"] }) {
  return (
    <div class="card bg-warning/10 border border-warning/30 shadow-sm">
      <div class="card-body p-4 space-y-2">
        <div class="flex items-center gap-2">
          <span class="text-warning">⚠</span>
          <h3 class="text-sm font-semibold">Recipes reference tools that aren't wired</h3>
          <span class="badge badge-sm badge-warning badge-outline">{orphans.length}</span>
        </div>
        <p class="text-xs text-base-content/60">
          These tool names appear in registered recipes but don't resolve to any in-process / file /
          MCP source. Either re-wire the tool or update the recipe to drop the reference.
        </p>
        <table class="table table-xs">
          <thead>
            <tr class="text-xs uppercase tracking-wider text-base-content/50">
              <th>Tool name</th>
              <th>Referenced by</th>
            </tr>
          </thead>
          <tbody>
            {orphans.map((o) => (
              <tr>
                <td class="font-mono text-sm">{o.toolName}</td>
                <td class="text-xs">
                  <div class="flex gap-1 flex-wrap">
                    {o.recipes.map((r) => (
                      <span class="badge badge-xs badge-outline font-mono">
                        {r.id}
                        <span class="opacity-60">@{r.version}</span>
                      </span>
                    ))}
                  </div>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function SourceBadge({ source }: { source: ToolCatalogSourceDto }) {
  if (source.kind === "in-process") {
    return <span class="badge badge-sm badge-outline font-mono text-xs">in-process</span>;
  }
  if (source.kind === "file") {
    return (
      <span
        class="badge badge-sm badge-outline badge-info font-mono text-xs"
        title={source.path ?? "file"}
      >
        file
      </span>
    );
  }
  return (
    <span
      class="badge badge-sm badge-outline badge-secondary font-mono text-xs"
      title={`MCP server: ${source.server} (${source.transport}), tool '${source.tool}'`}
    >
      mcp:{source.server}
    </span>
  );
}
