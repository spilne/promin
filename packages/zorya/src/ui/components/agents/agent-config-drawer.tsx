// ---------------------------------------------------------------------------
// Agent ⚙ Manage drawer — tabbed surface for everything you do to a recipe.
//
// Tabs (icon · label):
//   ◎ View      — read-only recipe overview (identity, model, tools, ...)
//   ✎ Edit      — opens the dedicated AgentEditDrawer (Phase 2: embed)
//   ⧉ Clone     — opens the dedicated AgentCloneDialog (Phase 2: embed)
//   ✚ Secrets   — embedded AgentSecretsPanel (no flicker)
//   ≡ Versions  — opens the dedicated AgentVersionsModal (Phase 2: embed)
//   ↓ Export TS — embedded snippet view + copy (no flicker)
//
// Phase 1 ships View / Secrets / Export embedded inline; Edit / Clone /
// Versions still launch their standalone dedicated surfaces from the tab
// (the embed-mode prop pattern shipped on AgentSecretsPanel is the
// template for embedding those in a follow-up).
// ---------------------------------------------------------------------------

import type * as preact from "preact";
import { useEffect, useMemo, useState } from "preact/hooks";
import type { RegisteredAgent } from "../../../server/routes/agents.ts";
import { inlineRoleDefinition } from "../../lib/role.ts";
import { exportRecipeAsTs } from "../../lib/export-recipe-ts.ts";
import { prompt, toast } from "../../lib/dialogs.ts";
import { api } from "../../api/client.ts";
import { formatRelative } from "../../lib/format.ts";
import { JsonBlock } from "../ui/json-block.tsx";
import { Skeleton } from "../ui/skeleton.tsx";
import { AgentCloneDialog } from "./agent-clone-dialog.tsx";
import { AgentEditDrawer } from "./agent-edit-drawer.tsx";
import { AgentSecretsPanel } from "./agent-secrets-panel.tsx";
import { AgentVersionsModal } from "./agent-versions-modal.tsx";
import type { Tenant } from "./agent-detail.tsx";

export type ManageTab = "view" | "edit" | "clone" | "secrets" | "versions" | "export";

interface TabSpec {
  readonly key: ManageTab;
  readonly icon: string;
  readonly label: string;
  readonly title: string;
}

const TABS: ReadonlyArray<TabSpec> = [
  { key: "view", icon: "◎", label: "View", title: "Read-only recipe overview" },
  {
    key: "edit",
    icon: "✎",
    label: "Edit",
    title: "Edit description, system prompt, model, tools, capabilities, tags",
  },
  { key: "clone", icon: "⧉", label: "Clone", title: "Fork this recipe into a new agent" },
  {
    key: "secrets",
    icon: "✚",
    label: "Secrets",
    title: "Manage the BYOK key for this recipe's model.credentialRef",
  },
  {
    key: "versions",
    icon: "≡",
    label: "Versions",
    title: "Browse versions, side-by-side diff",
  },
  { key: "export", icon: "↓", label: "Export TS", title: "Export this recipe as a TS snippet" },
];

interface Props {
  agent: RegisteredAgent | undefined;
  onClose: () => void;
  /** Tenant for the embedded Edit + Secrets tabs. */
  tenant?: Tenant;
  /** Tenant namespace for the embedded Secrets / Clone tabs. */
  namespaceId?: string;
  initialTab?: ManageTab;
  /** Embedded Edit tab — called after a successful save. */
  onSaved?: (updated: RegisteredAgent) => void;
  /** Embedded Edit tab — Save & Test variant. */
  onSavedAndTest?: (updated: RegisteredAgent) => void;
  /** Embedded Clone tab — called with the new agent's id once cloned. */
  onCloned?: (newId: string) => void;
}

export function AgentConfigDrawer({
  agent,
  onClose,
  tenant,
  namespaceId,
  initialTab,
  onSaved,
  onSavedAndTest,
  onCloned,
}: Props) {
  const [tab, setTab] = useState<ManageTab>(initialTab ?? "view");

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => e.key === "Escape" && onClose();
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onClose]);

  return (
    <>
      <div class="fixed inset-0 bg-black/40 z-30 anim-backdrop-in" onClick={onClose} aria-hidden />
      <aside
        class="fixed top-0 right-0 h-screen w-full max-w-2xl bg-base-100 shadow-2xl
               z-40 flex flex-col anim-drawer-in"
        role="dialog"
        aria-label="Manage agent"
      >
        <header class="flex items-start justify-between gap-2 p-4 border-b border-base-300">
          <div class="min-w-0">
            <div class="text-xs text-base-content/50 uppercase tracking-wider">Manage agent</div>
            <div class="font-mono text-sm truncate">{agent?.id ?? "…"}</div>
            <div class="text-[10px] text-base-content/40 mt-0.5">
              Recipe view + actions. Recipe values override host defaults; unset fields inherit from
              the host's resolver.
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

        <div
          class="flex flex-wrap gap-0.5 px-2 py-2 border-b border-base-300"
          role="tablist"
          aria-label="Manage agent tabs"
        >
          {TABS.map((t) => (
            <button
              key={t.key}
              type="button"
              role="tab"
              aria-selected={tab === t.key}
              class={`btn btn-sm gap-1 ${
                tab === t.key ? "btn-primary btn-outline" : "btn-ghost text-base-content/70"
              }`}
              onClick={() => setTab(t.key)}
              title={t.title}
            >
              <span aria-hidden>{t.icon}</span> {t.label}
            </button>
          ))}
        </div>

        <div class="flex-1 overflow-y-auto p-4 space-y-4">
          {!agent ? (
            <div class="space-y-2">
              <Skeleton w="w-full" h="h-4" />
              <Skeleton w="w-3/4" h="h-4" />
              <Skeleton w="w-1/2" h="h-4" />
            </div>
          ) : tab === "view" ? (
            <RecipeBody agent={agent} onChanged={onSaved} />
          ) : tab === "secrets" ? (
            <AgentSecretsPanel source={agent} namespaceId={namespaceId} onClose={onClose} embed />
          ) : tab === "export" ? (
            <ExportTsBody agent={agent} />
          ) : tab === "edit" ? (
            <AgentEditDrawer
              agent={agent}
              tenant={tenant}
              onClose={onClose}
              onSaved={(updated) => onSaved?.(updated)}
              {...(onSavedAndTest && {
                onSavedAndTest: (updated: RegisteredAgent) => onSavedAndTest(updated),
              })}
              embed
            />
          ) : tab === "clone" ? (
            <AgentCloneDialog
              source={agent}
              onClose={onClose}
              onCloned={(newId) => onCloned?.(newId)}
              embed
            />
          ) : (
            <AgentVersionsModal
              agentId={agent.id}
              initialVersion={agent.version}
              onClose={onClose}
              embed
            />
          )}
        </div>
      </aside>
    </>
  );
}

function ExportTsBody({ agent }: { agent: RegisteredAgent }) {
  const snippet = useMemo(() => exportRecipeAsTs(agent), [agent]);
  const [copied, setCopied] = useState(false);

  const copy = async (): Promise<void> => {
    try {
      await navigator.clipboard.writeText(snippet);
      setCopied(true);
      setTimeout(() => setCopied(false), 1500);
    } catch {
      toast("Clipboard write failed", { variant: "error" });
    }
  };

  return (
    <div class="space-y-3">
      <div class="flex items-center justify-between gap-2">
        <div class="text-xs text-base-content/60">
          Paste into your registry-bootstrap module to round-trip this recipe to VCS.
        </div>
        <button
          type="button"
          class={`btn btn-xs ${copied ? "btn-success" : "btn-primary"}`}
          onClick={() => void copy()}
        >
          {copied ? "✓ Copied" : "Copy"}
        </button>
      </div>
      <pre class="text-xs font-mono p-3 overflow-auto max-h-[60vh] bg-base-200 rounded whitespace-pre">
        {snippet}
      </pre>
    </div>
  );
}

function RecipeBody({
  agent,
  onChanged,
}: {
  agent: RegisteredAgent;
  onChanged?: (updated: RegisteredAgent) => void;
}) {
  const isLocal = agent.backend.type === "local";
  const local = isLocal ? agent.backend : null;
  // Behavioral fields live on the role binding now. `ref` bindings resolve
  // to undefined here (no I/O); current recipes are all inline.
  const roleDef = local ? inlineRoleDefinition(local.role) : undefined;
  const sp = roleDef?.systemPrompt ?? null;
  const systemPromptText = sp === null ? null : typeof sp === "string" ? sp : sp.base;
  const tools = roleDef?.tools ?? [];
  const knowledge = local?.knowledge ?? [];
  // Binding shape — `ref` is a shared live link; `inline` is one-off and can
  // be lifted into the registry via "Save as role" (extract-role).
  const binding = local?.role;
  const roleRef = binding && "ref" in binding ? binding.ref : null;

  const onSaveAsRole = async () => {
    const roleId = await prompt({
      title: "Save as role",
      label: "New role id",
      placeholder: "e.g. support-persona",
      confirmLabel: "Save as role",
    });
    if (!roleId) return;
    try {
      const { agent: updated } = await api.extractRole(agent.id, { roleId: roleId.trim() });
      toast(`Saved role "${roleId.trim()}" — this agent now references it.`, {
        variant: "success",
      });
      onChanged?.(updated);
    } catch (err) {
      toast(err instanceof Error ? err.message : String(err), { variant: "error" });
    }
  };

  return (
    <>
      <Section label="Identity">
        <KV k="id" v={<span class="font-mono">{agent.id}</span>} />
        <KV k="version" v={<span class="font-mono">{agent.version}</span>} />
        <KV k="backend" v={<span class="font-mono">{agent.backend.type}</span>} />
        {agent.metadata.description && <KV k="description" v={agent.metadata.description} />}
        <KV
          k="created"
          v={
            <span class="text-base-content/70">
              {new Date(agent.createdAt).toISOString()}{" "}
              <span class="text-base-content/40">
                ({formatRelative(new Date(agent.createdAt).toISOString())})
              </span>
            </span>
          }
        />
        <KV
          k="updated"
          v={
            <span class="text-base-content/70">
              {new Date(agent.updatedAt).toISOString()}{" "}
              <span class="text-base-content/40">
                ({formatRelative(new Date(agent.updatedAt).toISOString())})
              </span>
            </span>
          }
        />
      </Section>

      {local && (
        <Section label="Model">
          <KV k="provider" v={<span class="font-mono">{local.model.provider}</span>} />
          <KV k="id" v={<span class="font-mono">{local.model.id}</span>} />
          {local.maxStepsPerTurn !== undefined && (
            <KV k="maxStepsPerTurn" v={<span class="font-mono">{local.maxStepsPerTurn}</span>} />
          )}
          {local.maxTurns !== undefined && (
            <KV k="maxTurns" v={<span class="font-mono">{local.maxTurns}</span>} />
          )}
        </Section>
      )}

      {local && (
        <Section label="Role">
          {roleRef ? (
            <KV
              k="binding"
              v={
                <span class="text-base-content/70">
                  ref →{" "}
                  <span class="font-mono">
                    {roleRef.id}
                    {roleRef.version ? `@${roleRef.version}` : ""}
                  </span>{" "}
                  <span class="text-base-content/40">(shared — edit it on the Roles page)</span>
                </span>
              }
            />
          ) : (
            <div class="flex items-center justify-between gap-2">
              <span class="text-xs text-base-content/60">
                Inline role — this agent owns its behavior directly.
              </span>
              <button class="btn btn-xs btn-ghost" onClick={onSaveAsRole}>
                Save as role…
              </button>
            </div>
          )}
        </Section>
      )}

      {local && (
        <Section label="System prompt">
          {systemPromptText ? (
            <pre class="bg-base-200 p-3 rounded text-xs whitespace-pre-wrap break-words font-mono leading-relaxed max-h-[40vh] overflow-y-auto">
              {systemPromptText}
            </pre>
          ) : (
            <div class="text-xs text-base-content/40 italic">
              (none — agent runs without a static system prompt; the cascade-resolved one from
              MemoryStore.resolveContext is the only system content)
            </div>
          )}
        </Section>
      )}

      {local && (
        <Section label={`Tools (${tools.length})`}>
          {tools.length === 0 ? (
            <div class="text-xs text-base-content/40 italic">(none)</div>
          ) : (
            <div class="flex gap-1 flex-wrap">
              {tools.map((t) => (
                <span class="badge badge-outline font-mono text-xs">{t}</span>
              ))}
            </div>
          )}
          <div class="text-[10px] text-base-content/40 mt-2">
            Tool implementations are wired by the host (resolver), not stored on the recipe. Names
            listed here must match a key in the host's tool map.
          </div>
        </Section>
      )}

      {local && (
        <Section label={`Knowledge / RAG (${knowledge.length})`}>
          {knowledge.length === 0 ? (
            <div class="text-xs text-base-content/40 italic">(none)</div>
          ) : (
            <div class="space-y-2">
              {knowledge.map((k) => (
                <div class="rounded border border-base-300 bg-base-200/60 p-2 text-xs">
                  <div class="flex items-center gap-2 flex-wrap">
                    <span class="font-mono">{k.id}</span>
                    <span class="badge badge-xs badge-outline">{k.mode ?? "tool"}</span>
                    {k.name && <span class="badge badge-xs badge-ghost font-mono">{k.name}</span>}
                    {k.topK !== undefined && (
                      <span class="badge badge-xs badge-ghost font-mono">topK {k.topK}</span>
                    )}
                    {k.includeSources === false && (
                      <span class="badge badge-xs badge-warning badge-outline">no sources</span>
                    )}
                    {k.includeScores && <span class="badge badge-xs badge-ghost">scores</span>}
                    {k.maxChunkCharacters !== undefined && (
                      <span class="badge badge-xs badge-ghost font-mono">
                        max {k.maxChunkCharacters} chars
                      </span>
                    )}
                  </div>
                  {k.description && (
                    <div class="text-[10px] text-base-content/50 mt-1">{k.description}</div>
                  )}
                </div>
              ))}
            </div>
          )}
          <div class="text-[10px] text-base-content/40 mt-2">
            The recipe stores retriever ids and per-binding options. Concrete retrievers are wired
            by the host resolver.
          </div>
        </Section>
      )}

      <Section label="Metadata">
        {agent.metadata.capabilities.length > 0 && (
          <KV
            k="capabilities"
            v={
              <div class="flex gap-1 flex-wrap">
                {agent.metadata.capabilities.map((c) => (
                  <span class="badge badge-sm badge-info badge-outline">{c}</span>
                ))}
              </div>
            }
          />
        )}
        {agent.metadata.tags.length > 0 && (
          <KV
            k="tags"
            v={
              <div class="flex gap-1 flex-wrap">
                {agent.metadata.tags.map((t) => (
                  <span class="badge badge-sm badge-ghost">{t}</span>
                ))}
              </div>
            }
          />
        )}
        {agent.metadata.capabilities.length === 0 && agent.metadata.tags.length === 0 && (
          <div class="text-xs text-base-content/40 italic">(none)</div>
        )}
      </Section>

      {local &&
        (local.autoCompact !== undefined ||
          local.autoDistill !== undefined ||
          local.contextBudget !== undefined) && (
          <Section label="Runtime knobs (recipe overrides)">
            {local.autoCompact !== undefined && (
              <KV
                k="autoCompact"
                v={
                  local.autoCompact === false ? (
                    <span class="text-base-content/50 italic">false (explicitly off)</span>
                  ) : (
                    <JsonBlock value={local.autoCompact} maxH="max-h-32" />
                  )
                }
              />
            )}
            {local.autoDistill !== undefined && (
              <KV
                k="autoDistill"
                v={
                  local.autoDistill === false ? (
                    <span class="text-base-content/50 italic">false (explicitly off)</span>
                  ) : (
                    <JsonBlock value={local.autoDistill} maxH="max-h-32" />
                  )
                }
              />
            )}
            {local.contextBudget !== undefined && (
              <KV k="contextBudget" v={<JsonBlock value={local.contextBudget} maxH="max-h-32" />} />
            )}
            <div class="text-[10px] text-base-content/40 mt-2">
              Recipe values override the host's defaults. Fields the recipe doesn't set inherit from
              the host's resolver. The <code class="bg-base-300 px-1 rounded">when</code> predicate
              (closure-based) is host-only and not editable from a recipe.
            </div>
          </Section>
        )}

      {local && local.extra && Object.keys(local.extra).length > 0 && (
        <Section label="Extra (backend.extra)">
          <JsonBlock value={local.extra} maxH="max-h-60" />
        </Section>
      )}
    </>
  );
}

function Section({ label, children }: { label: string; children: preact.ComponentChildren }) {
  return (
    <section>
      <div class="text-[10px] uppercase tracking-wider text-base-content/50 mb-2">{label}</div>
      <div class="space-y-1.5">{children}</div>
    </section>
  );
}

function KV({ k, v }: { k: string; v: preact.ComponentChildren }) {
  return (
    <div class="grid grid-cols-[120px_1fr] gap-3 text-xs items-baseline">
      <span class="text-base-content/50 font-mono">{k}</span>
      <span class="break-words">{v}</span>
    </div>
  );
}
