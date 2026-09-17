// ---------------------------------------------------------------------------
// RoleEditDrawer — operator-facing edit form for a role: the behavioral
// bundle an agent binds (persona prompt + tools + skills + capabilities).
//
// Mirrors the field sections of AgentEditDrawer (layered system prompt,
// tool / skill multi-select, comma-list inputs) but edits a role registry
// row directly — no model, credentials, or memory config (those live on the
// agent binding). Create mode lets the operator type the role id; edit mode
// pins it and PATCHes in place.
// ---------------------------------------------------------------------------

import { useEffect, useMemo, useState } from "preact/hooks";
import { api, type ToolCatalogEntryDto } from "../../api/client.ts";
import type { RegisteredRole, RoleDefinition, RoleMetadata } from "../../../server/routes/roles.ts";
import type { SkillCatalogEntry } from "../../../server/routes/agent-catalog.ts";
import { useFetch } from "../../hooks/use-fetch.ts";

interface Props {
  role: RegisteredRole;
  /**
   * "edit" (default) PATCHes in place against `role.id`. "create" POSTs a new
   * role keyed on the typed id. The caller passes a blank `role` template to
   * seed the form defaults (empty prompt / tools).
   */
  mode?: "create" | "edit";
  onClose: () => void;
  onSaved: (saved: RegisteredRole) => void;
}

export function RoleEditDrawer({ role, mode = "edit", onClose, onSaved }: Props) {
  const isCreate = mode === "create";
  // Create mode lets the operator type the role id; edit mode pins it.
  const [roleId, setRoleId] = useState(role.id);
  const trimmedId = roleId.trim();
  // Mirror the gateway's id rule (rejects empty / `_`-prefixed) so the
  // operator sees the problem before the round-trip.
  const idValid = trimmedId.length > 0 && !trimmedId.startsWith("_");

  const [description, setDescription] = useState(role.metadata.description ?? "");
  const [tags, setTags] = useState(role.metadata.tags.join(", "));
  const [suggestedSecrets, setSuggestedSecrets] = useState(
    (role.metadata.suggestedSecrets ?? []).join(", "),
  );

  // `systemPrompt` can be a plain string OR the layered `{ base, layers }`
  // form. The textarea drives `base`, the LayersSection drives `layers`.
  // Save emits the layered form when layers are non-empty, plain string
  // otherwise (or null when both are empty).
  const initialSystemPrompt = (() => {
    const sp = role.definition.systemPrompt ?? null;
    if (sp === null) return "";
    return typeof sp === "string" ? sp : sp.base;
  })();
  const initialLayers = (() => {
    const sp = role.definition.systemPrompt ?? null;
    if (sp === null || typeof sp === "string") return [] as string[];
    return [...(sp.layers ?? [])];
  })();
  const [systemPrompt, setSystemPrompt] = useState(initialSystemPrompt);
  const [selectedLayers, setSelectedLayers] = useState<ReadonlyArray<string>>(initialLayers);

  const [selectedTools, setSelectedTools] = useState<ReadonlySet<string>>(
    new Set(role.definition.tools ?? []),
  );
  // Skill catalog selection. Held as a set of skill ids; the role stores
  // `{ id }[]` (versions pinned at resolve time).
  const [selectedSkills, setSelectedSkills] = useState<ReadonlySet<string>>(
    new Set((role.definition.skills ?? []).map((s) => s.id)),
  );
  const [capabilities, setCapabilities] = useState((role.definition.capabilities ?? []).join(", "));

  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const { data: toolsData } = useFetch(() => api.listCatalogTools(), [], 0);
  const tools = useMemo(() => toolsData?.tools ?? [], [toolsData]);
  const { data: skillsData } = useFetch(() => api.listCatalogSkills(), [], 0);
  const catalogSkills = useMemo(() => skillsData?.skills ?? [], [skillsData]);
  const { data: fragmentsData } = useFetch(() => api.listCatalogFragments(), [], 0);
  const catalogFragments = useMemo(() => fragmentsData?.fragments ?? [], [fragmentsData]);

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => e.key === "Escape" && onClose();
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onClose]);

  const buildPayload = (): { definition: RoleDefinition; metadata: RoleMetadata } => {
    // Emit the layered shape when the operator picked layers; fall back to
    // the plain-string form (or null) otherwise.
    const trimmedBase = systemPrompt.trim();
    const promptValue =
      selectedLayers.length > 0
        ? { base: trimmedBase, layers: [...selectedLayers] }
        : trimmedBase || null;
    // Empty selection → omit `skills` entirely so the role stays clean.
    const skillRefs = Array.from(selectedSkills)
      .sort()
      .map((id) => ({ id }));
    const caps = parseList(capabilities);
    const definition: RoleDefinition = {
      systemPrompt: promptValue,
      tools: Array.from(selectedTools).sort(),
      ...(skillRefs.length > 0 ? { skills: skillRefs } : {}),
      ...(caps.length > 0 ? { capabilities: caps } : {}),
    };
    const secrets = parseList(suggestedSecrets);
    const metadata: RoleMetadata = {
      description: description.trim() || null,
      tags: parseList(tags),
      ...(secrets.length > 0 ? { suggestedSecrets: secrets } : {}),
    };
    return { definition, metadata };
  };

  const save = async (): Promise<RegisteredRole | null> => {
    setError(null);
    const { definition, metadata } = buildPayload();
    try {
      if (isCreate) {
        return await api.createRole({ id: trimmedId, definition, metadata });
      }
      return await api.updateRole(role.id, { definition, metadata });
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
      return null;
    }
  };

  const onSubmit = async (e: Event) => {
    e.preventDefault();
    setSaving(true);
    const saved = await save();
    setSaving(false);
    if (!saved) return;
    onSaved(saved);
    onClose();
  };

  return (
    <>
      <div class="fixed inset-0 bg-black/40 z-30 anim-backdrop-in" onClick={onClose} aria-hidden />
      <aside
        class="fixed top-0 right-0 h-screen w-full max-w-2xl bg-base-100 shadow-2xl z-40 flex flex-col anim-drawer-in"
        role="dialog"
        aria-label="Edit role"
      >
        <header class="flex items-start justify-between gap-2 p-4 border-b border-base-300">
          <div class="min-w-0">
            <div class="text-xs text-base-content/50 uppercase tracking-wider">
              {isCreate ? "New role" : "Edit role"}
            </div>
            <div class="font-mono text-sm truncate">{isCreate ? trimmedId || "—" : role.id}</div>
            <div class="text-[10px] text-base-content/40 mt-0.5">
              {isCreate
                ? "Creates a new role in the registry. Fill in an id, prompt + tools, and save."
                : "Updates the role in place. Agents binding it by ref pick up the change live."}
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
          {isCreate && (
            <label class="form-control">
              <span class="text-xs text-base-content/60 mb-1 uppercase tracking-wider">
                Role id
              </span>
              <input
                class="input input-bordered input-sm font-mono"
                placeholder="e.g. support-persona (letters, digits, - and _; can't start with _)"
                value={roleId}
                onInput={(e) => setRoleId((e.target as HTMLInputElement).value)}
                autoFocus
              />
              {trimmedId.length > 0 && !idValid && (
                <span class="text-[10px] text-error mt-1">Id can't start with an underscore.</span>
              )}
            </label>
          )}

          <label class="form-control">
            <span class="text-xs text-base-content/60 mb-1 uppercase tracking-wider">
              Description
            </span>
            <input
              class="input input-bordered input-sm"
              placeholder="Short description shown in role listings"
              value={description}
              onInput={(e) => setDescription((e.target as HTMLInputElement).value)}
            />
          </label>

          <label class="form-control">
            <span class="text-xs text-base-content/60 mb-1 uppercase tracking-wider">
              System prompt
            </span>
            <textarea
              class="textarea textarea-bordered font-mono text-xs leading-relaxed"
              rows={12}
              placeholder="(empty — role carries no static system prompt)"
              value={systemPrompt}
              onInput={(e) => setSystemPrompt((e.target as HTMLTextAreaElement).value)}
            />
            <span class="text-[10px] text-base-content/40 mt-1">
              Drives the prompt `base`. Add fragment layers below to compose a layered prompt.
            </span>
          </label>

          <LayersSection
            fragments={catalogFragments}
            selected={selectedLayers}
            onChange={setSelectedLayers}
          />

          <ToolPicker tools={tools} selected={selectedTools} onChange={setSelectedTools} />

          <SkillPicker
            skills={catalogSkills}
            selected={selectedSkills}
            onChange={setSelectedSkills}
          />

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
            <span class="text-xs text-base-content/60 mb-1 uppercase tracking-wider">
              Suggested secrets (comma-separated, optional)
            </span>
            <input
              class="input input-bordered input-sm font-mono"
              placeholder="ANTHROPIC_API_KEY, SLACK_TOKEN"
              value={suggestedSecrets}
              onInput={(e) => setSuggestedSecrets((e.target as HTMLInputElement).value)}
            />
            <span class="text-[10px] text-base-content/40 mt-1">
              Hint to operators about which secrets an agent binding this role likely needs.
            </span>
          </label>

          {error && <div class="alert alert-error text-xs">{error}</div>}

          <div class="flex justify-end gap-2 pt-2">
            <button type="button" class="btn btn-sm btn-ghost" onClick={onClose}>
              Cancel
            </button>
            <button
              type="submit"
              class="btn btn-sm btn-primary"
              disabled={saving || (isCreate && !idValid)}
              title={isCreate && !idValid ? "Enter a valid role id to create." : undefined}
            >
              {saving
                ? isCreate
                  ? "Creating…"
                  : "Saving…"
                : isCreate
                  ? "Create role"
                  : "Save changes"}
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

interface LayersSectionProps {
  fragments: ReadonlyArray<{ key: string; content: string }>;
  selected: ReadonlyArray<string>;
  onChange: (next: ReadonlyArray<string>) => void;
}

// Layered-prompt editor. The system-prompt textarea drives `base`; this
// section drives `layers[]`. Order matters (concatenation order at resolve
// time), so each chip has ↑ / ↓ controls. ✕ removes. An "Add layer"
// dropdown lists catalog fragments not yet selected.
//
// Hidden when no fragment catalog is wired AND no layers are already pinned.
function LayersSection({ fragments, selected, onChange }: LayersSectionProps) {
  const fragmentByKey = useMemo(() => {
    const m = new Map<string, string>();
    for (const f of fragments) m.set(f.key, f.content);
    return m;
  }, [fragments]);

  const availableToAdd = useMemo(
    () => fragments.filter((f) => !selected.includes(f.key)).map((f) => f.key),
    [fragments, selected],
  );

  if (fragments.length === 0 && selected.length === 0) return null;

  const move = (index: number, delta: -1 | 1) => {
    const next = [...selected];
    const target = index + delta;
    if (target < 0 || target >= next.length) return;
    [next[index], next[target]] = [next[target]!, next[index]!];
    onChange(next);
  };
  const remove = (index: number) => {
    const next = [...selected];
    next.splice(index, 1);
    onChange(next);
  };
  const add = (key: string) => {
    if (!key || selected.includes(key)) return;
    onChange([...selected, key]);
  };

  return (
    <div class="form-control">
      <div class="flex items-center justify-between mb-1">
        <span class="text-xs text-base-content/60 uppercase tracking-wider">
          Prompt layers
          <span class="ml-2 text-[10px] text-base-content/40 normal-case tracking-normal">
            appended after the base in this order
          </span>
        </span>
      </div>

      {selected.length === 0 ? (
        <div class="text-[11px] text-base-content/40 border border-dashed border-base-300 rounded p-2">
          No layers — the system prompt is just the base above.
        </div>
      ) : (
        <ul class="space-y-1">
          {selected.map((key, i) => {
            const broken = !fragmentByKey.has(key);
            return (
              <li class="flex items-center gap-2 bg-base-200 rounded px-2 py-1">
                <span class="text-[10px] text-base-content/50 font-mono w-4">{i + 1}.</span>
                <span
                  class={`font-mono text-xs flex-1 truncate ${broken ? "text-error" : ""}`}
                  title={broken ? "Fragment not in catalog — broken ref" : fragmentByKey.get(key)}
                >
                  {key}
                  {broken && <span class="ml-1">⚠</span>}
                </span>
                <button
                  type="button"
                  class="btn btn-xs btn-ghost"
                  disabled={i === 0}
                  title="Move up"
                  onClick={() => move(i, -1)}
                >
                  ↑
                </button>
                <button
                  type="button"
                  class="btn btn-xs btn-ghost"
                  disabled={i === selected.length - 1}
                  title="Move down"
                  onClick={() => move(i, 1)}
                >
                  ↓
                </button>
                <button
                  type="button"
                  class="btn btn-xs btn-ghost text-error"
                  title="Remove layer"
                  onClick={() => remove(i)}
                >
                  ✕
                </button>
              </li>
            );
          })}
        </ul>
      )}

      {availableToAdd.length > 0 && (
        <div class="flex items-center gap-2 mt-2">
          <span class="text-[10px] text-base-content/50">Add layer:</span>
          <select
            class="select select-bordered select-xs flex-1 font-mono"
            value=""
            onChange={(e) => {
              const v = (e.target as HTMLSelectElement).value;
              if (v) {
                add(v);
                (e.target as HTMLSelectElement).value = "";
              }
            }}
          >
            <option value="">— pick a fragment —</option>
            {availableToAdd.map((k) => (
              <option value={k}>{k}</option>
            ))}
          </select>
        </div>
      )}
    </div>
  );
}

interface ToolPickerProps {
  tools: ReadonlyArray<ToolCatalogEntryDto>;
  selected: ReadonlySet<string>;
  onChange: (next: ReadonlySet<string>) => void;
}

function ToolPicker({ tools, selected, onChange }: ToolPickerProps) {
  const [query, setQuery] = useState("");
  const [sourceFilter, setSourceFilter] = useState<"all" | "in-process" | "file" | "mcp">("all");
  const [showOnlySelected, setShowOnlySelected] = useState(false);

  const catalogByName = useMemo(() => {
    const m = new Map<string, ToolCatalogEntryDto>();
    for (const t of tools) m.set(t.name, t);
    return m;
  }, [tools]);

  // Tools selected on the role but not in the live catalog — surfaced so
  // operators can clear broken refs.
  const brokenRefs = useMemo(
    () =>
      Array.from(selected)
        .filter((name) => !catalogByName.has(name))
        .sort(),
    [selected, catalogByName],
  );

  const filtered = useMemo(() => {
    const q = query.trim().toLowerCase();
    return tools
      .filter((t) => {
        if (showOnlySelected && !selected.has(t.name)) return false;
        if (sourceFilter !== "all" && t.source.kind !== sourceFilter) return false;
        if (!q) return true;
        return t.name.toLowerCase().includes(q) || t.description.toLowerCase().includes(q);
      })
      .sort((a, b) => {
        const aSel = selected.has(a.name) ? 0 : 1;
        const bSel = selected.has(b.name) ? 0 : 1;
        if (aSel !== bSel) return aSel - bSel;
        if (a.enabled !== b.enabled) return a.enabled ? -1 : 1;
        return a.name.localeCompare(b.name);
      });
  }, [tools, query, sourceFilter, showOnlySelected, selected]);

  const toggle = (name: string) => {
    const next = new Set(selected);
    if (next.has(name)) next.delete(name);
    else next.add(name);
    onChange(next);
  };

  return (
    <div class="form-control">
      <div class="flex items-center justify-between mb-1">
        <span class="text-xs text-base-content/60 uppercase tracking-wider">
          Tools
          <span class="ml-2 text-[10px] text-base-content/40 normal-case tracking-normal">
            {selected.size} selected · {tools.length} available
          </span>
        </span>
        {selected.size > 0 && (
          <button
            type="button"
            class="text-[10px] text-base-content/50 hover:text-base-content underline"
            onClick={() => onChange(new Set())}
          >
            Clear all
          </button>
        )}
      </div>

      {brokenRefs.length > 0 && (
        <div class="alert alert-warning py-2 mb-2 text-xs">
          <span class="font-semibold">
            ⚠ {brokenRefs.length} tool{brokenRefs.length === 1 ? "" : "s"} not in catalog:
          </span>
          <div class="flex flex-wrap gap-1 mt-1">
            {brokenRefs.map((name) => (
              <button
                type="button"
                class="badge badge-sm badge-warning gap-1 cursor-pointer"
                title="Click to remove from role"
                onClick={() => toggle(name)}
              >
                {name} <span>✕</span>
              </button>
            ))}
          </div>
        </div>
      )}

      <div class="flex items-center gap-1 mb-2">
        <input
          class="input input-bordered input-xs flex-1 font-mono"
          placeholder="Search…"
          value={query}
          onInput={(e) => setQuery((e.target as HTMLInputElement).value)}
        />
        <select
          class="select select-bordered select-xs"
          value={sourceFilter}
          onChange={(e) =>
            setSourceFilter((e.target as HTMLSelectElement).value as typeof sourceFilter)
          }
        >
          <option value="all">All</option>
          <option value="in-process">In-proc</option>
          <option value="file">File</option>
          <option value="mcp">MCP</option>
        </select>
        <button
          type="button"
          class={`btn btn-xs ${showOnlySelected ? "btn-primary" : "btn-ghost"}`}
          title="Show only currently-selected tools"
          onClick={() => setShowOnlySelected((v) => !v)}
        >
          {showOnlySelected ? "✓ selected" : "selected"}
        </button>
      </div>

      <div class="border border-base-300 rounded max-h-64 overflow-y-auto">
        {tools.length === 0 ? (
          <div class="text-xs text-base-content/40 p-3 text-center">
            No tool catalog wired on this server. Tool names round-trip as free-form strings.
          </div>
        ) : filtered.length === 0 ? (
          <div class="text-xs text-base-content/40 p-3 text-center">No tools match.</div>
        ) : (
          <ul class="divide-y divide-base-300">
            {filtered.map((t) => (
              <li class={`p-2 hover:bg-base-200 ${t.enabled ? "" : "opacity-60"}`}>
                <label class="flex items-start gap-2 cursor-pointer">
                  <input
                    type="checkbox"
                    class="checkbox checkbox-xs mt-0.5"
                    checked={selected.has(t.name)}
                    onChange={() => toggle(t.name)}
                  />
                  <div class="flex-1 min-w-0">
                    <div class="flex items-center gap-2 flex-wrap">
                      <span class="font-mono text-xs">{t.name}</span>
                      <ToolSourceBadge source={t.source} />
                      {!t.enabled && (
                        <span
                          class="badge badge-xs badge-error"
                          title="Tool is disabled (enabled: false)"
                        >
                          disabled
                        </span>
                      )}
                      {t.requiredSecrets.length > 0 && (
                        <span
                          class="badge badge-xs badge-ghost"
                          title={`Requires secret${
                            t.requiredSecrets.length === 1 ? "" : "s"
                          }: ${t.requiredSecrets.join(", ")}`}
                        >
                          🔑{" "}
                          {t.requiredSecrets.length === 1
                            ? t.requiredSecrets[0]
                            : `${t.requiredSecrets.length} secrets`}
                        </span>
                      )}
                      {t.usesMemory && (
                        <span
                          class="badge badge-xs badge-ghost"
                          title="Reads / writes scoped memory"
                        >
                          🧠 memory
                        </span>
                      )}
                    </div>
                    {t.description && (
                      <div class="text-[10px] text-base-content/50 truncate mt-0.5">
                        {t.description}
                      </div>
                    )}
                  </div>
                </label>
              </li>
            ))}
          </ul>
        )}
      </div>
    </div>
  );
}

interface SkillPickerProps {
  skills: ReadonlyArray<SkillCatalogEntry>;
  selected: ReadonlySet<string>;
  onChange: (next: ReadonlySet<string>) => void;
}

// Mirrors ToolPicker for the skill catalog. Checking one adds its id to the
// role's `skills`.
function SkillPicker({ skills, selected, onChange }: SkillPickerProps) {
  const [query, setQuery] = useState("");
  const [showOnlySelected, setShowOnlySelected] = useState(false);

  const catalogById = useMemo(() => {
    const m = new Map<string, SkillCatalogEntry>();
    for (const s of skills) m.set(s.id, s);
    return m;
  }, [skills]);

  const brokenRefs = useMemo(
    () =>
      Array.from(selected)
        .filter((id) => !catalogById.has(id))
        .sort(),
    [selected, catalogById],
  );

  const filtered = useMemo(() => {
    const q = query.trim().toLowerCase();
    return skills
      .filter((s) => {
        if (showOnlySelected && !selected.has(s.id)) return false;
        if (!q) return true;
        return (
          s.id.toLowerCase().includes(q) ||
          s.description.toLowerCase().includes(q) ||
          s.whenToUse.toLowerCase().includes(q)
        );
      })
      .sort((a, b) => {
        const aSel = selected.has(a.id) ? 0 : 1;
        const bSel = selected.has(b.id) ? 0 : 1;
        if (aSel !== bSel) return aSel - bSel;
        if (a.enabled !== b.enabled) return a.enabled ? -1 : 1;
        return a.id.localeCompare(b.id);
      });
  }, [skills, query, showOnlySelected, selected]);

  const toggle = (id: string) => {
    const next = new Set(selected);
    if (next.has(id)) next.delete(id);
    else next.add(id);
    onChange(next);
  };

  return (
    <div class="form-control">
      <div class="flex items-center justify-between mb-1">
        <span class="text-xs text-base-content/60 uppercase tracking-wider">
          Skills
          <span class="ml-2 text-[10px] text-base-content/40 normal-case tracking-normal">
            {selected.size} selected · {skills.length} available
          </span>
        </span>
        {selected.size > 0 && (
          <button
            type="button"
            class="text-[10px] text-base-content/50 hover:text-base-content underline"
            onClick={() => onChange(new Set())}
          >
            Clear all
          </button>
        )}
      </div>

      {brokenRefs.length > 0 && (
        <div class="alert alert-warning py-2 mb-2 text-xs">
          <span class="font-semibold">
            ⚠ {brokenRefs.length} skill{brokenRefs.length === 1 ? "" : "s"} not in catalog:
          </span>
          <div class="flex flex-wrap gap-1 mt-1">
            {brokenRefs.map((id) => (
              <button
                type="button"
                class="badge badge-sm badge-warning gap-1 cursor-pointer"
                title="Click to remove from role"
                onClick={() => toggle(id)}
              >
                {id} <span>✕</span>
              </button>
            ))}
          </div>
        </div>
      )}

      <div class="flex items-center gap-1 mb-2">
        <input
          class="input input-bordered input-xs flex-1 font-mono"
          placeholder="Search skills…"
          value={query}
          onInput={(e) => setQuery((e.target as HTMLInputElement).value)}
        />
        <button
          type="button"
          class={`btn btn-xs ${showOnlySelected ? "btn-primary" : "btn-ghost"}`}
          title="Show only currently-selected skills"
          onClick={() => setShowOnlySelected((v) => !v)}
        >
          {showOnlySelected ? "✓ selected" : "selected"}
        </button>
      </div>

      <div class="border border-base-300 rounded max-h-64 overflow-y-auto">
        {skills.length === 0 ? (
          <div class="text-xs text-base-content/40 p-3 text-center">
            No skills registered. Create one in the Skills page, then attach it here.
          </div>
        ) : filtered.length === 0 ? (
          <div class="text-xs text-base-content/40 p-3 text-center">No skills match.</div>
        ) : (
          <ul class="divide-y divide-base-300">
            {filtered.map((s) => (
              <li class={`p-2 hover:bg-base-200 ${s.enabled ? "" : "opacity-60"}`}>
                <label class="flex items-start gap-2 cursor-pointer">
                  <input
                    type="checkbox"
                    class="checkbox checkbox-xs mt-0.5"
                    checked={selected.has(s.id)}
                    onChange={() => toggle(s.id)}
                  />
                  <div class="flex-1 min-w-0">
                    <div class="flex items-center gap-2 flex-wrap">
                      <span class="font-mono text-xs">{s.id}</span>
                      <span class="text-[10px] text-base-content/40">{s.version}</span>
                      {!s.enabled && (
                        <span class="badge badge-xs badge-error" title="Skill is disabled">
                          disabled
                        </span>
                      )}
                      {s.tags.map((t) => (
                        <span class="badge badge-xs badge-ghost">{t}</span>
                      ))}
                    </div>
                    <div class="text-[10px] text-base-content/50 truncate mt-0.5">
                      {s.description}
                    </div>
                    {s.whenToUse && s.whenToUse !== s.description && (
                      <div class="text-[10px] text-base-content/40 truncate">
                        Use when: {s.whenToUse}
                      </div>
                    )}
                  </div>
                </label>
              </li>
            ))}
          </ul>
        )}
      </div>
    </div>
  );
}

function ToolSourceBadge({ source }: { source: ToolCatalogEntryDto["source"] }) {
  if (source.kind === "in-process") {
    return <span class="badge badge-xs badge-outline font-mono text-[9px]">in-proc</span>;
  }
  if (source.kind === "file") {
    return <span class="badge badge-xs badge-outline badge-info font-mono text-[9px]">file</span>;
  }
  return (
    <span
      class="badge badge-xs badge-outline badge-secondary font-mono text-[9px]"
      title={`MCP server: ${source.server}`}
    >
      mcp:{source.server}
    </span>
  );
}
