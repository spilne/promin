// ---------------------------------------------------------------------------
// McpEndpointDrawer — create or edit an MCP endpoint: its id, title,
// instructions, the tools it exposes (shared tools, plus the endpoint's own
// when it is file-defined) with optional per-tool name / description
// overrides, the tenants that may use it, and a Connect panel with the URL
// and ready-to-paste client config. Edit mode also shows what the saved
// version actually exposes and why any binding was left out.
// ---------------------------------------------------------------------------

import type { ComponentChildren } from "preact";
import { useEffect, useMemo, useState } from "preact/hooks";
import { api } from "../../api/client.ts";
import type {
  McpEndpointCatalogTool,
  McpEndpointToolBinding,
  RegisteredMcpEndpoint,
} from "../../../server/routes/mcp-endpoints.ts";
import { useFetch } from "../../hooks/use-fetch.ts";
import { useNamespace } from "../../hooks/use-namespace.ts";
import { VersionsPanel } from "../ui/versions-panel.tsx";

interface Props {
  endpoint: RegisteredMcpEndpoint;
  mode: "create" | "edit";
  /** Who publishes the active version: the `mcp/` folder, host code, or nobody (operator). */
  managedBy: "file" | "code" | null;
  onClose: () => void;
  onSaved: () => void;
}

const ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$/;
const TOOL_NAME_PATTERN = /^[A-Za-z0-9_.-]{1,128}$/;

const SKIP_REASONS: Record<string, string> = {
  not_found: "no such tool",
  disabled: "tool is disabled",
  requires_approval: "needs approval (enable “trust client approval”)",
  delegated: "sub-agent tools can't run over MCP",
  capability_missing: "elevated tool; add its capability",
  invalid_name: "not a valid MCP tool name",
  name_taken: "another tool already uses this name",
};

function bindingKey(b: { tool: string; private?: boolean }): string {
  return `${b.private === true ? "private" : "shared"}:${b.tool}`;
}

export function McpEndpointDrawer({ endpoint, mode, managedBy, onClose, onSaved }: Props) {
  const isCreate = mode === "create";
  const [endpointId, setEndpointId] = useState(endpoint.id);
  const id = isCreate ? endpointId.trim() : endpoint.id;
  const idValid = ID_PATTERN.test(id);

  const [title, setTitle] = useState(endpoint.title ?? "");
  const [description, setDescription] = useState(endpoint.metadata.description ?? "");
  const [instructions, setInstructions] = useState(endpoint.instructions ?? "");
  const [tags, setTags] = useState(endpoint.metadata.tags.join(", "));
  const [namespaces, setNamespaces] = useState((endpoint.namespaces ?? []).join(", "));
  const [capabilities, setCapabilities] = useState(endpoint.capabilities.join(", "));
  const [trustClientApproval, setTrustClientApproval] = useState(endpoint.trustClientApproval);
  const [enabled, setEnabled] = useState(endpoint.enabled);
  const [bindings, setBindings] = useState<McpEndpointToolBinding[]>([...endpoint.tools]);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [reloadKey, setReloadKey] = useState(0);

  const { data: catalog } = useFetch(() => api.mcpEndpointCatalog(), [], 0);
  const { data: exposed } = useFetch(
    () => (isCreate ? Promise.resolve(null) : api.listMcpEndpointTools(endpoint.id)),
    [reloadKey],
    0,
  );

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => e.key === "Escape" && onClose();
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onClose]);

  const ownTools = catalog?.private[id] ?? [];
  const selected = useMemo(() => new Set(bindings.map(bindingKey)), [bindings]);

  const toggle = (tool: string, isPrivate: boolean) => {
    const key = bindingKey({ tool, private: isPrivate });
    setBindings((prev) =>
      selected.has(key)
        ? prev.filter((b) => bindingKey(b) !== key)
        : [...prev, { tool, ...(isPrivate && { private: true }) }],
    );
  };
  const patchBinding = (key: string, patch: { name?: string; description?: string }) => {
    setBindings((prev) =>
      prev.map((b) => {
        if (bindingKey(b) !== key) return b;
        const next: McpEndpointToolBinding = { tool: b.tool, ...(b.private && { private: true }) };
        const name = patch.name !== undefined ? patch.name : b.name;
        const desc = patch.description !== undefined ? patch.description : b.description;
        return {
          ...next,
          ...(name && { name }),
          ...(desc && { description: desc }),
        };
      }),
    );
  };

  const badNames = bindings.filter((b) => b.name !== undefined && !TOOL_NAME_PATTERN.test(b.name));
  const canSave = (!isCreate || idValid) && badNames.length === 0 && !saving;

  const onSubmit = async (e: Event) => {
    e.preventDefault();
    if (!canSave) return;
    setSaving(true);
    setError(null);
    const nsList = parseList(namespaces);
    const content = {
      title: title.trim() || null,
      instructions: instructions.trim() || null,
      tools: bindings,
      capabilities: parseList(capabilities),
      namespaces: nsList.length > 0 ? nsList : null,
      trustClientApproval,
      metadata: { description: description.trim() || null, tags: parseList(tags) },
    };
    try {
      if (isCreate) {
        await api.createMcpEndpoint({ id, ...content });
      } else {
        await api.updateMcpEndpoint(endpoint.id, {
          ...content,
          expectedVersion: endpoint.version,
          ...(enabled !== endpoint.enabled && { enabled }),
        });
      }
      onSaved();
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
      setSaving(false);
    }
  };

  return (
    <>
      <div class="fixed inset-0 bg-black/40 z-30 anim-backdrop-in" onClick={onClose} aria-hidden />
      <aside
        class="fixed top-0 right-0 h-screen w-full max-w-2xl bg-base-100 shadow-2xl z-40 flex flex-col anim-drawer-in"
        role="dialog"
        aria-label={isCreate ? "New MCP endpoint" : "Edit MCP endpoint"}
      >
        <header class="flex items-start justify-between gap-2 p-4 border-b border-base-300">
          <div class="min-w-0">
            <div class="text-xs text-base-content/50 uppercase tracking-wider">
              {isCreate ? "New MCP endpoint" : "Edit MCP endpoint"}
            </div>
            <div class="font-mono text-sm truncate">{id || "—"}</div>
            {managedBy !== null && (
              <div class="text-[10px] text-warning mt-0.5">
                Defined {managedBy === "file" ? "in a file" : "in host code"}. Saving here publishes
                a new version; later {managedBy === "file" ? "file" : "code"} changes wait as drafts
                until promoted.
              </div>
            )}
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
          {isCreate && (
            <Field label="Endpoint id">
              <input
                class="input input-bordered input-sm font-mono"
                placeholder="e.g. billing (letters, digits, - and _)"
                value={endpointId}
                onInput={(e) => setEndpointId((e.target as HTMLInputElement).value)}
                autoFocus
              />
              {id.length > 0 && !idValid && (
                <span class="text-[10px] text-error mt-1">
                  1–64 letters, digits, - or _, starting with a letter or digit.
                </span>
              )}
            </Field>
          )}

          <div class="grid grid-cols-2 gap-3">
            <Field label="Title">
              <input
                class="input input-bordered input-sm"
                placeholder="Shown to MCP clients"
                value={title}
                onInput={(e) => setTitle((e.target as HTMLInputElement).value)}
              />
            </Field>
            <Field label="Description">
              <input
                class="input input-bordered input-sm"
                placeholder="Shown in this dashboard"
                value={description}
                onInput={(e) => setDescription((e.target as HTMLInputElement).value)}
              />
            </Field>
          </div>

          <Field label="Instructions" hint="Sent to the client's model when it connects.">
            <textarea
              class="textarea textarea-bordered text-xs leading-relaxed"
              rows={4}
              placeholder="How and when to use these tools"
              value={instructions}
              onInput={(e) => setInstructions((e.target as HTMLTextAreaElement).value)}
            />
          </Field>

          <section class="space-y-2">
            <div class="text-xs text-base-content/60 uppercase tracking-wider">
              Tools ({bindings.length})
            </div>
            {ownTools.length > 0 && (
              <ToolList
                heading="This endpoint's own tools"
                tools={ownTools}
                isPrivate
                selected={selected}
                onToggle={toggle}
              />
            )}
            <ToolList
              heading="Shared tools"
              tools={catalog?.shared ?? []}
              isPrivate={false}
              selected={selected}
              onToggle={toggle}
            />
            {bindings.length > 0 && (
              <div class="space-y-1">
                <div class="text-[10px] text-base-content/50">
                  Exposed as (leave blank to keep the tool's own name and description)
                </div>
                {bindings.map((b) => {
                  const key = bindingKey(b);
                  const nameBad = b.name !== undefined && !TOOL_NAME_PATTERN.test(b.name);
                  return (
                    <div class="grid grid-cols-[10rem_10rem_1fr] gap-2 items-center" key={key}>
                      <span class="font-mono text-xs truncate" title={key}>
                        {b.tool}
                        {b.private && <span class="badge badge-xs badge-ghost ml-1">own</span>}
                      </span>
                      <input
                        class={`input input-bordered input-xs font-mono ${nameBad ? "input-error" : ""}`}
                        placeholder={b.tool}
                        value={b.name ?? ""}
                        onInput={(e) =>
                          patchBinding(key, { name: (e.target as HTMLInputElement).value })
                        }
                      />
                      <input
                        class="input input-bordered input-xs"
                        placeholder="description override"
                        value={b.description ?? ""}
                        onInput={(e) =>
                          patchBinding(key, { description: (e.target as HTMLInputElement).value })
                        }
                      />
                    </div>
                  );
                })}
              </div>
            )}
          </section>

          <div class="grid grid-cols-2 gap-3">
            <Field label="Namespaces" hint="Tenants that may use it. Empty: every namespace.">
              <input
                class="input input-bordered input-sm font-mono"
                placeholder="all namespaces"
                value={namespaces}
                onInput={(e) => setNamespaces((e.target as HTMLInputElement).value)}
              />
            </Field>
            <Field label="Capabilities" hint="Unlock elevated tools, as on a role.">
              <input
                class="input input-bordered input-sm font-mono"
                placeholder="none"
                value={capabilities}
                onInput={(e) => setCapabilities((e.target as HTMLInputElement).value)}
              />
            </Field>
          </div>

          <Field label="Tags">
            <input
              class="input input-bordered input-sm font-mono"
              placeholder="none"
              value={tags}
              onInput={(e) => setTags((e.target as HTMLInputElement).value)}
            />
          </Field>

          <label class="flex items-start gap-2 cursor-pointer">
            <input
              type="checkbox"
              class="checkbox checkbox-sm mt-0.5"
              checked={trustClientApproval}
              onChange={(e) => setTrustClientApproval((e.target as HTMLInputElement).checked)}
            />
            <span class="text-xs">
              Trust the client's approval
              <span class="block text-[10px] text-base-content/50">
                Expose tools that need approval and rely on the MCP client's own confirmation
                prompt. Off: those tools are left out.
              </span>
            </span>
          </label>

          {!isCreate && (
            <label class="flex items-center gap-2 cursor-pointer">
              <input
                type="checkbox"
                class="toggle toggle-sm"
                checked={enabled}
                onChange={(e) => setEnabled((e.target as HTMLInputElement).checked)}
              />
              <span class="text-xs">Enabled {enabled ? "" : "(clients get 410 Gone)"}</span>
            </label>
          )}

          {exposed && (
            <section class="space-y-1">
              <div class="text-xs text-base-content/60 uppercase tracking-wider">
                Saved version exposes
              </div>
              {exposed.tools.length === 0 && (
                <div class="text-xs text-base-content/50">No tools.</div>
              )}
              <div class="flex flex-wrap gap-1">
                {exposed.tools.map((t) => (
                  <span class="badge badge-sm badge-outline font-mono" title={t.description}>
                    {t.name}
                  </span>
                ))}
              </div>
              {exposed.skipped.map((s) => (
                <div class="text-[11px] text-warning">
                  <span class="font-mono">{s.tool}</span> left out: {SKIP_REASONS[s.code] ?? s.code}
                </div>
              ))}
            </section>
          )}

          {!isCreate && <ConnectPanel id={endpoint.id} />}

          {!isCreate && (
            <VersionsPanel
              reloadKey={`${endpoint.id}:${reloadKey}`}
              load={() => api.listMcpEndpointVersions(endpoint.id)}
              promote={(body) => api.promoteMcpEndpoint(endpoint.id, body)}
              onPromoted={() => {
                setReloadKey((k) => k + 1);
                onSaved();
              }}
              describe={(v) => `${v.tools.length} tools`}
            />
          )}

          {error && <div class="alert alert-error text-xs">{error}</div>}
        </form>

        <footer class="flex justify-end gap-2 p-4 border-t border-base-300">
          <button type="button" class="btn btn-sm btn-ghost" onClick={onClose} disabled={saving}>
            Cancel
          </button>
          <button
            type="button"
            class="btn btn-sm btn-primary"
            disabled={!canSave}
            onClick={(e) => void onSubmit(e)}
          >
            {saving ? "Saving…" : isCreate ? "Create" : "Publish"}
          </button>
        </footer>
      </aside>
    </>
  );
}

function ToolList(props: {
  heading: string;
  tools: ReadonlyArray<McpEndpointCatalogTool>;
  isPrivate: boolean;
  selected: ReadonlySet<string>;
  onToggle: (tool: string, isPrivate: boolean) => void;
}) {
  const [query, setQuery] = useState("");
  const q = query.trim().toLowerCase();
  const tools = q
    ? props.tools.filter((t) => `${t.name} ${t.description}`.toLowerCase().includes(q))
    : props.tools;
  return (
    <div class="border border-base-300 rounded">
      <div class="flex items-center justify-between gap-2 px-2 py-1 border-b border-base-300">
        <span class="text-[11px] text-base-content/60">{props.heading}</span>
        {props.tools.length > 8 && (
          <input
            class="input input-ghost input-xs max-w-40"
            placeholder="filter…"
            value={query}
            onInput={(e) => setQuery((e.target as HTMLInputElement).value)}
          />
        )}
      </div>
      <div class="max-h-48 overflow-y-auto divide-y divide-base-200">
        {tools.length === 0 && <div class="text-xs text-base-content/40 px-2 py-2">No tools.</div>}
        {tools.map((t) => (
          <label class="flex items-start gap-2 px-2 py-1 cursor-pointer hover:bg-base-200">
            <input
              type="checkbox"
              class="checkbox checkbox-xs mt-0.5"
              checked={props.selected.has(bindingKey({ tool: t.name, private: props.isPrivate }))}
              onChange={() => props.onToggle(t.name, props.isPrivate)}
            />
            <span class="min-w-0">
              <span class="font-mono text-xs">{t.name}</span>
              {t.requireApproval && <span class="badge badge-xs badge-warning ml-1">approval</span>}
              {t.kind !== "plain" && <span class="badge badge-xs badge-ghost ml-1">{t.kind}</span>}
              {!t.enabled && <span class="badge badge-xs badge-error ml-1">disabled</span>}
              <span class="block text-[10px] text-base-content/50 truncate">{t.description}</span>
            </span>
          </label>
        ))}
      </div>
    </div>
  );
}

/** URL and client config for the selected namespace (or a placeholder). */
function ConnectPanel({ id }: { id: string }) {
  const [namespace] = useNamespace();
  const ns = namespace || "<namespace>";
  const url = `${window.location.origin}/ns/${ns}/mcp/${id}`;
  const claude =
    `claude mcp add --transport http ${id} ${url} \\\n` +
    `  --header "Authorization: Bearer <zorya-key>" \\\n` +
    `  --header "X-Zorya-Resource: <resource>"`;
  const cursor = JSON.stringify(
    {
      mcpServers: {
        [id]: {
          url,
          headers: { Authorization: "Bearer <zorya-key>", "X-Zorya-Resource": "<resource>" },
        },
      },
    },
    null,
    2,
  );
  return (
    <section class="space-y-2">
      <div class="text-xs text-base-content/60 uppercase tracking-wider">Connect</div>
      <Snippet label="URL" text={url} />
      <Snippet label="Claude Code" text={claude} />
      <Snippet label="Cursor (mcp.json)" text={cursor} />
      <div class="text-[10px] text-base-content/50">
        {namespace
          ? `Namespace "${namespace}" from the namespace switcher.`
          : "Pick a namespace in the switcher to fill it in."}{" "}
        Operators can also use <span class="font-mono">/mcp/{id}</span> (global scope).
        X-Zorya-Resource is needed only by tools that act for a resource.
      </div>
    </section>
  );
}

function Snippet({ label, text }: { label: string; text: string }) {
  const [copied, setCopied] = useState(false);
  return (
    <div>
      <div class="flex items-center justify-between">
        <span class="text-[10px] text-base-content/50">{label}</span>
        <button
          type="button"
          class="btn btn-ghost btn-xs"
          onClick={() => {
            void navigator.clipboard.writeText(text).then(() => {
              setCopied(true);
              setTimeout(() => setCopied(false), 1_500);
            });
          }}
        >
          {copied ? "Copied" : "Copy"}
        </button>
      </div>
      <pre class="bg-base-200 rounded p-2 text-[11px] font-mono whitespace-pre-wrap break-all">
        {text}
      </pre>
    </div>
  );
}

function Field(props: { label: string; hint?: string; children: ComponentChildren }) {
  return (
    <label class="form-control">
      <span class="text-xs text-base-content/60 mb-1 uppercase tracking-wider">{props.label}</span>
      {props.children}
      {props.hint && <span class="text-[10px] text-base-content/40 mt-1">{props.hint}</span>}
    </label>
  );
}

function parseList(raw: string): string[] {
  return raw
    .split(",")
    .map((s) => s.trim())
    .filter((s) => s.length > 0);
}
