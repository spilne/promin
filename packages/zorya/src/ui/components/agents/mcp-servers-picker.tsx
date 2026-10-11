// ---------------------------------------------------------------------------
// McpServersPicker — the agent editor's "MCP servers" section. A row either
// attaches a server from the operator's allowlist (GET
// /api/agents/_catalog/mcp-servers) or, when the server allows it, names a
// Streamable HTTP endpoint the outbound policy vets. A recipe never carries
// a command, env or credential: allowlisted servers (the only ones that get
// credentials) are defined by the operator.
//
// Every tool of an attached server is exposed to the agent as
// `<server>__<tool>` (narrowed by the row's tool list). MCP tools ask for
// approval unless the operator turned it off for that server, and their
// output reaches the model as untrusted reference data.
// ---------------------------------------------------------------------------

import { useMemo } from "preact/hooks";
import type { McpServersCatalogResponse, ToolCatalogEntryDto } from "../../api/client.ts";
import type { McpServerForm } from "../../lib/recipe-mcp-form.ts";

interface McpServersPickerProps {
  catalog: McpServersCatalogResponse | undefined;
  /** Catalog tools, used to show what each allowlisted server offers. */
  tools: ReadonlyArray<ToolCatalogEntryDto>;
  rows: ReadonlyArray<McpServerForm>;
  onChange: (next: ReadonlyArray<McpServerForm>) => void;
}

export function McpServersPicker({ catalog, tools, rows, onChange }: McpServersPickerProps) {
  const servers = catalog?.servers ?? [];
  const attached = useMemo(
    () => new Set(rows.flatMap((r) => (r.kind === "server" ? [r.server] : []))),
    [rows],
  );
  const available = servers.filter((s) => !attached.has(s.name));
  const toolsByServer = useMemo(() => {
    const out = new Map<string, string[]>();
    for (const t of tools) {
      if (t.source.kind !== "mcp") continue;
      out.set(t.source.server, [...(out.get(t.source.server) ?? []), t.source.tool]);
    }
    return out;
  }, [tools]);

  const update = (index: number, next: McpServerForm) =>
    onChange(rows.map((r, i) => (i === index ? next : r)));
  const remove = (index: number) => onChange(rows.filter((_, i) => i !== index));

  return (
    <div class="form-control">
      <div class="flex items-center justify-between mb-1 gap-2">
        <span class="text-xs text-base-content/60 uppercase tracking-wider">
          MCP servers
          <span class="ml-2 text-[10px] text-base-content/40 normal-case tracking-normal">
            {rows.length} attached · {servers.length} allowlisted
          </span>
        </span>
        <div class="flex items-center gap-1">
          {available.length > 0 && (
            <select
              class="select select-bordered select-xs max-w-48 font-mono"
              value=""
              title="Attach an allowlisted MCP server"
              onChange={(e) => {
                const select = e.target as HTMLSelectElement;
                if (select.value) {
                  onChange([...rows, { kind: "server", server: select.value, tools: "" }]);
                }
                select.value = "";
              }}
            >
              <option value="">Attach allowlisted</option>
              {available.map((s) => (
                <option value={s.name}>{s.name}</option>
              ))}
            </select>
          )}
          {catalog?.recipeHttpServers && (
            <button
              type="button"
              class="btn btn-xs btn-ghost"
              onClick={() => onChange([...rows, { kind: "http", name: "", url: "", tools: "" }])}
            >
              HTTP endpoint
            </button>
          )}
        </div>
      </div>

      {rows.length === 0 ? (
        <div class="text-xs text-base-content/40 border border-dashed border-base-300 rounded p-3">
          {catalog?.configured
            ? "No MCP servers attached. Attach one from the operator's allowlist to expose its tools."
            : "MCP is not configured on this server (ZoryaServerConfig.mcp)."}
        </div>
      ) : (
        <div class="space-y-2">
          {rows.map((row, i) =>
            row.kind === "server" ? (
              <AllowlistedRow
                row={row}
                summary={servers.find((s) => s.name === row.server)}
                offered={toolsByServer.get(row.server) ?? []}
                onChange={(next) => update(i, next)}
                onRemove={() => remove(i)}
              />
            ) : (
              <HttpRow row={row} onChange={(next) => update(i, next)} onRemove={() => remove(i)} />
            ),
          )}
        </div>
      )}
    </div>
  );
}

function AllowlistedRow(props: {
  row: Extract<McpServerForm, { kind: "server" }>;
  summary: McpServersCatalogResponse["servers"][number] | undefined;
  offered: ReadonlyArray<string>;
  onChange: (next: McpServerForm) => void;
  onRemove: () => void;
}) {
  const { row, summary } = props;
  return (
    <div class="border border-base-300 rounded p-2 space-y-1">
      <div class="flex items-center gap-2">
        <span class="font-mono text-sm font-semibold">{row.server}</span>
        {summary ? (
          <>
            <span class="badge badge-xs badge-outline font-mono">{summary.transport}</span>
            {summary.requireApproval && (
              <span class="badge badge-xs badge-warning badge-outline">approval</span>
            )}
            {summary.idempotent && <span class="badge badge-xs badge-outline">idempotent</span>}
          </>
        ) : (
          <span class="badge badge-xs badge-error" title="Not in the operator's allowlist">
            not allowlisted
          </span>
        )}
        <button type="button" class="btn btn-xs btn-ghost ml-auto" onClick={props.onRemove}>
          Remove
        </button>
      </div>
      {summary?.description && (
        <div class="text-xs text-base-content/60">{summary.description}</div>
      )}
      <input
        class="input input-bordered input-xs w-full font-mono"
        placeholder={
          props.offered.length > 0
            ? `All tools (${props.offered.join(", ")})`
            : "All tools (comma-separated to narrow)"
        }
        value={row.tools}
        onInput={(e) => props.onChange({ ...row, tools: (e.target as HTMLInputElement).value })}
      />
    </div>
  );
}

function HttpRow(props: {
  row: Extract<McpServerForm, { kind: "http" }>;
  onChange: (next: McpServerForm) => void;
  onRemove: () => void;
}) {
  const { row } = props;
  const set = (patch: Partial<typeof row>) => props.onChange({ ...row, ...patch });
  return (
    <div class="border border-base-300 rounded p-2 space-y-1">
      <div class="flex items-center gap-2">
        <input
          class="input input-bordered input-xs w-40 font-mono"
          placeholder="name"
          value={row.name}
          onInput={(e) => set({ name: (e.target as HTMLInputElement).value })}
        />
        <span class="badge badge-xs badge-outline font-mono">http</span>
        <span
          class="badge badge-xs badge-warning badge-outline"
          title="Recipe-supplied endpoints always require approval"
        >
          approval
        </span>
        <button type="button" class="btn btn-xs btn-ghost ml-auto" onClick={props.onRemove}>
          Remove
        </button>
      </div>
      <input
        class="input input-bordered input-xs w-full font-mono"
        placeholder="https://mcp.example.com/mcp"
        value={row.url}
        onInput={(e) => set({ url: (e.target as HTMLInputElement).value })}
      />
      <input
        class="input input-bordered input-xs w-full font-mono"
        placeholder="tools (blank = all)"
        value={row.tools}
        onInput={(e) => set({ tools: (e.target as HTMLInputElement).value })}
      />
      <div class="text-[10px] text-base-content/50">
        Recipe endpoints get no credentials; an MCP server that needs one must be added to the
        operator's allowlist.
      </div>
    </div>
  );
}
