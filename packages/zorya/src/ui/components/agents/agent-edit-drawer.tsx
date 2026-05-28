// ---------------------------------------------------------------------------
// AgentEditDrawer — operator-facing edit form for recipe fields:
// description, system prompt, model, credential ref, capabilities,
// tags, advanced limits (maxStepsPerTurn / maxTurns).
//
// Model dropdown is sourced from GET /api/agents/_catalog/models when
// the host wires a `ModelCatalog`; falls back to a free-form
// `provider::id` input when no catalog is available.
//
// Tool list, side-by-side version diff, and "test in chat" draft
// preview are separate components that compose into this drawer.
// ---------------------------------------------------------------------------

import { useEffect, useMemo, useState } from "preact/hooks";
import { api, type ModelCatalogEntryDto, type ToolCatalogEntryDto } from "../../api/client.ts";
import type { RegisteredAgent } from "../../../server/routes/agents.ts";
import type { SkillCatalogEntry } from "../../../server/routes/agent-catalog.ts";
import { systemToolsFor, SystemToolsSection } from "./system-tools.tsx";
import { useFetch } from "../../hooks/use-fetch.ts";
import { DraftTestModal } from "./agent-draft-test-modal.tsx";
import type { Tenant } from "./agent-detail.tsx";
import {
  buildAutoCompact,
  buildAutoDistill,
  buildContextBudget,
  initAutoCompact,
  initAutoDistill,
  initContextBudget,
  type AutoCompactForm,
  type AutoDistillForm,
  type CompactionMode,
  type ContextBudgetForm,
  type RunMode,
} from "../../lib/recipe-memory-form.ts";

interface Props {
  agent: RegisteredAgent;
  /**
   * Current tenant scope. When set, a "Test draft" button appears —
   * it chats an uncommitted draft of the current edits in this scope.
   */
  tenant?: Tenant;
  onClose: () => void;
  onSaved: (updated: RegisteredAgent) => void;
  /**
   * Optional: when set, "Save & Test" button appears next to "Save changes".
   * Save commits as usual; on success the drawer closes and the parent gets
   * the saved recipe so it can open a fresh thread for iteration.
   */
  onSavedAndTest?: (updated: RegisteredAgent) => void;
  /**
   * When true, render only the editor form (no backdrop, no positioning,
   * no own header). The host owns the surrounding chrome — e.g. a tab
   * inside the agent's ⚙ Manage drawer.
   */
  embed?: boolean;
}

export function AgentEditDrawer({ agent, tenant, onClose, onSaved, onSavedAndTest, embed }: Props) {
  const isLocal = agent.backend.type === "local";
  const [description, setDescription] = useState(agent.metadata.description ?? "");
  // `systemPrompt` on the recipe can be either a plain string OR the layered
  // `{ base, layers }` form (promin-kx26). The drawer's input is plain-string
  // only; surface the `base` for editing. Saving writes a plain string back,
  // which loses layers — but file-managed role recipes (the only ones using
  // the layered form today) have Save disabled, so the loss can't happen
  // through this path. Layered-prompt-aware UI is future work.
  const initialSystemPrompt = (() => {
    if (!isLocal) return "";
    const sp = agent.backend.systemPrompt;
    if (sp === null) return "";
    return typeof sp === "string" ? sp : sp.base;
  })();
  const [systemPrompt, setSystemPrompt] = useState(initialSystemPrompt);
  const [capabilities, setCapabilities] = useState(agent.metadata.capabilities.join(", "));
  const [tags, setTags] = useState(agent.metadata.tags.join(", "));
  const [enabled, setEnabled] = useState(agent.metadata.enabled !== false);
  // Model selection (local backends only).
  const initialProvider = isLocal ? agent.backend.model.provider : "";
  const initialModelId = isLocal ? agent.backend.model.id : "";
  const [modelKey, setModelKey] = useState(`${initialProvider}::${initialModelId}`);
  const [credentialRef, setCredentialRef] = useState(
    isLocal ? (agent.backend.model.credentialRef ?? "") : "",
  );
  // Advanced limits.
  const [maxStepsPerTurn, setMaxStepsPerTurn] = useState<string>(
    isLocal && agent.backend.maxStepsPerTurn !== undefined
      ? String(agent.backend.maxStepsPerTurn)
      : "",
  );
  const [maxTurns, setMaxTurns] = useState<string>(
    isLocal && agent.backend.maxTurns !== undefined ? String(agent.backend.maxTurns) : "",
  );

  // Memory & compaction (local backends only).
  const [autoCompact, setAutoCompact] = useState<AutoCompactForm>(() =>
    initAutoCompact(isLocal ? agent.backend.autoCompact : undefined),
  );
  const [autoDistill, setAutoDistill] = useState<AutoDistillForm>(() =>
    initAutoDistill(isLocal ? agent.backend.autoDistill : undefined),
  );
  const [contextBudget, setContextBudget] = useState<ContextBudgetForm>(() =>
    initContextBudget(isLocal ? agent.backend.contextBudget : undefined),
  );
  const [showMemory, setShowMemory] = useState(false);

  // Tool selection (local backends only).
  const [selectedTools, setSelectedTools] = useState<ReadonlySet<string>>(
    new Set(isLocal ? agent.backend.tools : []),
  );
  // Skill catalog selection (local backends only). Held as a set of skill
  // ids; versions are pinned at resolve time, so the recipe stores `{id}`.
  const [selectedSkills, setSelectedSkills] = useState<ReadonlySet<string>>(
    new Set(isLocal ? (agent.backend.skills ?? []).map((s) => s.id) : []),
  );

  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [showAdvanced, setShowAdvanced] = useState(false);
  const [publishOpen, setPublishOpen] = useState(false);
  const [publishVersion, setPublishVersion] = useState(() => bumpVersion(agent.version));
  // Id of the draft recipe currently being chat-tested, if any.
  const [draftTestId, setDraftTestId] = useState<string | null>(null);

  const { data: modelsData } = useFetch(() => api.listCatalogModels(), [], 0);
  const models = useMemo(() => modelsData?.models ?? [], [modelsData]);
  const { data: toolsData } = useFetch(() => api.listCatalogTools(), [], 0);
  const tools = useMemo(() => toolsData?.tools ?? [], [toolsData]);
  const { data: skillsData } = useFetch(() => api.listCatalogSkills(), [], 0);
  const catalogSkills = useMemo(() => skillsData?.skills ?? [], [skillsData]);
  // Self-fetch which agent recipes are file-managed. When THIS one is, Save
  // would be silently overwritten by the next scan tick — show a warning +
  // disable Save (Publish stays enabled; a new version is operator-managed
  // and doesn't conflict with the file's version). Mirrors the skills page.
  const { data: agentSourcesData } = useFetch(() => api.listAgentSources(), [], 0);
  const fileManaged = useMemo(
    () => (agentSourcesData?.fileManaged ?? []).includes(agent.id),
    [agentSourcesData, agent.id],
  );

  useEffect(() => {
    if (embed) return; // host drawer owns close
    const onKey = (e: KeyboardEvent) => e.key === "Escape" && onClose();
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onClose, embed]);

  const parseLimit = (raw: string): number | undefined => {
    const trimmed = raw.trim();
    if (!trimmed) return undefined;
    const n = Number.parseInt(trimmed, 10);
    return Number.isFinite(n) && n > 0 ? n : undefined;
  };

  // Build the (backend, metadata) shape from current form state — used
  // by both Save (PATCH in place) and Publish (POST a new version).
  const buildPayload = (): {
    backend: RegisteredAgent["backend"];
    metadata: Partial<RegisteredAgent["metadata"]>;
  } => {
    const metadata: Partial<RegisteredAgent["metadata"]> = {
      description: description.trim() || null,
      capabilities: parseList(capabilities),
      tags: parseList(tags),
      enabled,
    };
    if (!isLocal) {
      return { backend: agent.backend, metadata };
    }
    const [provider, modelId] = modelKey.split("::");
    const trimmedCred = credentialRef.trim();
    const steps = parseLimit(maxStepsPerTurn);
    const turns = parseLimit(maxTurns);
    // `undefined` → omit the field (inherit host default); `false` →
    // explicitly disabled; object → recipe-level config.
    const autoCompactValue = buildAutoCompact(autoCompact);
    const autoDistillValue = buildAutoDistill(autoDistill);
    const contextBudgetValue = buildContextBudget(contextBudget);
    const {
      maxStepsPerTurn: _stripSteps,
      maxTurns: _stripTurns,
      autoCompact: _stripAutoCompact,
      autoDistill: _stripAutoDistill,
      contextBudget: _stripContextBudget,
      skills: _stripSkills,
      ...backendBase
    } = agent.backend;
    // Empty selection → omit `skills` entirely so the recipe stays clean.
    const skillRefs = Array.from(selectedSkills)
      .sort()
      .map((id) => ({ id }));
    const backend = {
      ...backendBase,
      systemPrompt: systemPrompt.trim() || null,
      model: {
        provider: provider || agent.backend.model.provider,
        id: modelId || agent.backend.model.id,
        ...(trimmedCred && { credentialRef: trimmedCred }),
      },
      tools: Array.from(selectedTools).sort(),
      ...(skillRefs.length > 0 ? { skills: skillRefs } : {}),
      ...(steps !== undefined ? { maxStepsPerTurn: steps } : {}),
      ...(turns !== undefined ? { maxTurns: turns } : {}),
      ...(autoCompactValue !== undefined ? { autoCompact: autoCompactValue } : {}),
      ...(autoDistillValue !== undefined ? { autoDistill: autoDistillValue } : {}),
      ...(contextBudgetValue !== undefined ? { contextBudget: contextBudgetValue } : {}),
    } as typeof agent.backend;
    return { backend, metadata };
  };

  const save = async (): Promise<RegisteredAgent | null> => {
    setError(null);
    const { backend, metadata } = buildPayload();
    const updates: Parameters<typeof api.updateAgent>[1] = {
      metadata,
      ...(isLocal && { backend }),
    };
    try {
      return await api.updateAgent(agent.id, updates);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
      return null;
    }
  };

  const onSubmit = async (e: Event) => {
    e.preventDefault();
    setSaving(true);
    const updated = await save();
    setSaving(false);
    if (!updated) return;
    onSaved(updated);
    onClose();
  };

  const onSaveAndTestClick = async () => {
    setSaving(true);
    const updated = await save();
    setSaving(false);
    if (!updated || !onSavedAndTest) return;
    onSavedAndTest(updated);
    onClose();
  };

  // Register an uncommitted draft of the current edits and open the
  // test-chat modal against it. Nothing touches the real recipe.
  const onTestDraftClick = async () => {
    setSaving(true);
    setError(null);
    try {
      const { backend, metadata } = buildPayload();
      const { recipe } = await api.createDraft({ backend, metadata, sourceId: agent.id });
      setDraftTestId(recipe.id);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setSaving(false);
    }
  };

  // Closing the test modal discards the draft. A browser closed with the
  // modal still open is caught by the server's TTL sweep.
  const closeDraftTest = () => {
    if (draftTestId) void api.deleteDraft(draftTestId);
    setDraftTestId(null);
  };

  const onPublish = async (newVersion: string) => {
    setSaving(true);
    setError(null);
    try {
      const { backend, metadata } = buildPayload();
      const created = await api.createAgent({
        id: agent.id,
        version: newVersion,
        backend,
        metadata,
      });
      onSaved(created);
      onClose();
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setSaving(false);
    }
  };

  const groupedModels = useMemo(() => groupModelsByProvider(models), [models]);

  return (
    <>
      {!embed && (
        <div
          class="fixed inset-0 bg-black/40 z-30 anim-backdrop-in"
          onClick={onClose}
          aria-hidden
        />
      )}
      <aside
        class={
          embed
            ? "flex flex-col"
            : "fixed top-0 right-0 h-screen w-full max-w-2xl bg-base-100 shadow-2xl z-40 flex flex-col anim-drawer-in"
        }
        {...(embed ? {} : { role: "dialog", "aria-label": "Edit agent" })}
      >
        {!embed && (
          <header class="flex items-start justify-between gap-2 p-4 border-b border-base-300">
            <div class="min-w-0">
              <div class="text-xs text-base-content/50 uppercase tracking-wider">Edit agent</div>
              <div class="font-mono text-sm truncate">{agent.id}</div>
              <div class="text-[10px] text-base-content/40 mt-0.5">
                Updates the latest version in place. Changing model or tools needs a programmatic
                edit until the full Designer ships.
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
        )}

        <form
          class={embed ? "space-y-4" : "flex-1 overflow-y-auto p-4 space-y-4"}
          onSubmit={onSubmit}
        >
          {fileManaged && (
            <div class="alert alert-warning text-xs">
              📄 This agent recipe is defined by a file on disk. Saving in place is disabled — edit
              the source file, or use <span class="font-mono">Publish new version</span> to create
              an operator-managed copy. The next scan would overwrite any in-place change.
            </div>
          )}
          <label class="form-control">
            <span class="text-xs text-base-content/60 mb-1 uppercase tracking-wider">
              Description
            </span>
            <input
              class="input input-bordered input-sm"
              placeholder="Short description shown in agent listings"
              value={description}
              onInput={(e) => setDescription((e.target as HTMLInputElement).value)}
            />
          </label>

          {isLocal && (
            <label class="form-control">
              <span class="text-xs text-base-content/60 mb-1 uppercase tracking-wider">Model</span>
              {models.length > 0 ? (
                <select
                  class="select select-bordered select-sm font-mono"
                  value={modelKey}
                  onChange={(e) => setModelKey((e.target as HTMLSelectElement).value)}
                >
                  {!groupedModels.some((g) =>
                    g.models.some((m) => `${m.provider}::${m.id}` === modelKey),
                  ) && <option value={modelKey}>{modelKey} (current — not in catalog)</option>}
                  {groupedModels.map((group) => (
                    <optgroup label={group.provider}>
                      {group.models.map((m) => (
                        <option value={`${m.provider}::${m.id}`}>
                          {m.displayName ?? m.id}
                          {m.contextLimit ? ` · ${formatContext(m.contextLimit)}` : ""}
                          {m.costTier ? ` · ${m.costTier}` : ""}
                        </option>
                      ))}
                    </optgroup>
                  ))}
                </select>
              ) : (
                <input
                  class="input input-bordered input-sm font-mono"
                  placeholder="provider::id (e.g. anthropic::claude-sonnet-4-6)"
                  value={modelKey}
                  onInput={(e) => setModelKey((e.target as HTMLInputElement).value)}
                />
              )}
              <span class="text-[10px] text-base-content/40 mt-1">
                Catalog-backed dropdown. Falls back to free-form when host hasn't wired a
                ModelCatalog.
              </span>
            </label>
          )}

          {isLocal && (
            <label class="form-control">
              <span class="text-xs text-base-content/60 mb-1 uppercase tracking-wider">
                Credential reference (optional)
              </span>
              <input
                class="input input-bordered input-sm font-mono"
                placeholder="e.g. ANTHROPIC_API_KEY (BYOK — leave empty to use host default)"
                value={credentialRef}
                onInput={(e) => setCredentialRef((e.target as HTMLInputElement).value)}
              />
              <span class="text-[10px] text-base-content/40 mt-1">
                When set, resolver fetches this secret from SecretsStorage at request time (cascade:
                resource → namespace → global).
              </span>
            </label>
          )}

          {isLocal && (
            <label class="form-control">
              <span class="text-xs text-base-content/60 mb-1 uppercase tracking-wider">
                System prompt
              </span>
              <textarea
                class="textarea textarea-bordered font-mono text-xs leading-relaxed"
                rows={12}
                placeholder="(empty — agent runs without a static system prompt)"
                value={systemPrompt}
                onInput={(e) => setSystemPrompt((e.target as HTMLTextAreaElement).value)}
              />
              <span class="text-[10px] text-base-content/40 mt-1">
                Memory cascade rules (CLAUDE.md / namespace.staticRules) are layered on top by
                MemoryStore.resolveContext at turn time.
              </span>
            </label>
          )}

          {isLocal && (
            <ToolPicker tools={tools} selected={selectedTools} onChange={setSelectedTools} />
          )}

          {isLocal && (
            <SkillPicker
              skills={catalogSkills}
              selected={selectedSkills}
              onChange={setSelectedSkills}
            />
          )}

          {isLocal && (
            <SystemToolsSection
              tools={systemToolsFor({
                skillCount: selectedSkills.size,
                hasNetwork: agent.backend.type === "local" && agent.backend.network !== undefined,
              })}
            />
          )}

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
            <span class="text-xs text-base-content/60 mb-1 uppercase tracking-wider">Status</span>
            <label class="cursor-pointer label justify-start gap-2 px-0 py-1">
              <input
                type="checkbox"
                class="toggle toggle-sm toggle-success"
                checked={enabled}
                onChange={(e) => setEnabled((e.target as HTMLInputElement).checked)}
              />
              <span class="text-sm">
                {enabled ? "Enabled" : "Disabled"}
                <span class="text-[10px] text-base-content/50 ml-1">
                  {enabled
                    ? "— recipe accepts invocations"
                    : "— invocations rejected with 410, threads + history still browsable"}
                </span>
              </span>
            </label>
          </label>

          {isLocal && (
            <div class="form-control">
              <button
                type="button"
                class="text-xs text-base-content/60 uppercase tracking-wider flex items-center gap-1 hover:text-base-content"
                onClick={() => setShowAdvanced((v) => !v)}
              >
                <span>{showAdvanced ? "▼" : "▶"}</span>
                Advanced limits
              </button>
              {showAdvanced && (
                <div class="grid grid-cols-2 gap-3 mt-2">
                  <label class="form-control">
                    <span class="text-[10px] text-base-content/50 mb-1">Max steps per turn</span>
                    <input
                      class="input input-bordered input-sm font-mono"
                      type="number"
                      min="1"
                      placeholder="(host default)"
                      value={maxStepsPerTurn}
                      onInput={(e) => setMaxStepsPerTurn((e.target as HTMLInputElement).value)}
                    />
                  </label>
                  <label class="form-control">
                    <span class="text-[10px] text-base-content/50 mb-1">Max turns</span>
                    <input
                      class="input input-bordered input-sm font-mono"
                      type="number"
                      min="1"
                      placeholder="(unbounded)"
                      value={maxTurns}
                      onInput={(e) => setMaxTurns((e.target as HTMLInputElement).value)}
                    />
                  </label>
                </div>
              )}
            </div>
          )}

          {isLocal && (
            <div class="form-control">
              <button
                type="button"
                class="text-xs text-base-content/60 uppercase tracking-wider flex items-center gap-1 hover:text-base-content"
                onClick={() => setShowMemory((v) => !v)}
              >
                <span>{showMemory ? "▼" : "▶"}</span>
                Memory &amp; compaction
              </button>
              {showMemory && (
                <div class="mt-2 space-y-4">
                  {/* autoCompact — in-thread roll-up */}
                  <div class="space-y-2">
                    <div class="flex items-center justify-between gap-2">
                      <span class="text-[10px] uppercase tracking-wider text-base-content/50">
                        In-thread compaction
                      </span>
                      <select
                        class="select select-bordered select-xs"
                        value={autoCompact.mode}
                        onChange={(e) =>
                          setAutoCompact((f) => ({
                            ...f,
                            mode: (e.target as HTMLSelectElement).value as CompactionMode,
                          }))
                        }
                      >
                        <option value="inherit">Inherit host default</option>
                        <option value="off">Off</option>
                        <option value="on">Custom</option>
                      </select>
                    </div>
                    {autoCompact.mode === "on" && (
                      <div class="grid grid-cols-2 gap-2 pl-1">
                        <NumField
                          label="Message threshold"
                          value={autoCompact.messageThreshold}
                          onChange={(v) => setAutoCompact((f) => ({ ...f, messageThreshold: v }))}
                        />
                        <NumField
                          label="Token threshold"
                          value={autoCompact.tokenThreshold}
                          onChange={(v) => setAutoCompact((f) => ({ ...f, tokenThreshold: v }))}
                        />
                        <NumField
                          label="Context limit"
                          value={autoCompact.contextLimit}
                          onChange={(v) => setAutoCompact((f) => ({ ...f, contextLimit: v }))}
                        />
                        <NumField
                          label="Compress at"
                          value={autoCompact.compressAt}
                          onChange={(v) => setAutoCompact((f) => ({ ...f, compressAt: v }))}
                        />
                        <NumField
                          label="Keep recent"
                          value={autoCompact.keepRecent}
                          onChange={(v) => setAutoCompact((f) => ({ ...f, keepRecent: v }))}
                        />
                        <RunModeField
                          value={autoCompact.runMode}
                          onChange={(v) => setAutoCompact((f) => ({ ...f, runMode: v }))}
                        />
                      </div>
                    )}
                  </div>

                  {/* autoDistill — cross-thread roll-up */}
                  <div class="space-y-2">
                    <div class="flex items-center justify-between gap-2">
                      <span class="text-[10px] uppercase tracking-wider text-base-content/50">
                        Cross-thread distillation
                      </span>
                      <select
                        class="select select-bordered select-xs"
                        value={autoDistill.mode}
                        onChange={(e) =>
                          setAutoDistill((f) => ({
                            ...f,
                            mode: (e.target as HTMLSelectElement).value as CompactionMode,
                          }))
                        }
                      >
                        <option value="inherit">Inherit host default</option>
                        <option value="off">Off</option>
                        <option value="on">Custom</option>
                      </select>
                    </div>
                    {autoDistill.mode === "on" && (
                      <div class="space-y-2 pl-1">
                        <div class="grid grid-cols-2 gap-2">
                          <NumField
                            label="Message threshold"
                            value={autoDistill.messageThreshold}
                            onChange={(v) => setAutoDistill((f) => ({ ...f, messageThreshold: v }))}
                          />
                          <NumField
                            label="Token threshold"
                            value={autoDistill.tokenThreshold}
                            onChange={(v) => setAutoDistill((f) => ({ ...f, tokenThreshold: v }))}
                          />
                          <NumField
                            label="Interval (ms)"
                            value={autoDistill.intervalMs}
                            onChange={(v) => setAutoDistill((f) => ({ ...f, intervalMs: v }))}
                          />
                          <RunModeField
                            value={autoDistill.runMode}
                            onChange={(v) => setAutoDistill((f) => ({ ...f, runMode: v }))}
                          />
                        </div>
                        <label class="cursor-pointer label justify-start gap-2 px-0 py-0">
                          <input
                            type="checkbox"
                            class="checkbox checkbox-xs"
                            checked={autoDistill.force}
                            onChange={(e) =>
                              setAutoDistill((f) => ({
                                ...f,
                                force: (e.target as HTMLInputElement).checked,
                              }))
                            }
                          />
                          <span class="text-[10px] text-base-content/60">
                            Force — re-distill even when a prior summary exists
                          </span>
                        </label>
                      </div>
                    )}
                  </div>

                  {/* contextBudget — per-turn prompt assembly */}
                  <div class="space-y-2">
                    <span class="text-[10px] uppercase tracking-wider text-base-content/50">
                      Context budget
                    </span>
                    <div class="grid grid-cols-2 gap-2 pl-1">
                      <NumField
                        label="Max message tokens"
                        value={contextBudget.maxMessageTokens}
                        onChange={(v) => setContextBudget((f) => ({ ...f, maxMessageTokens: v }))}
                      />
                      <NumField
                        label="Max episode tokens"
                        value={contextBudget.maxEpisodeTokens}
                        onChange={(v) => setContextBudget((f) => ({ ...f, maxEpisodeTokens: v }))}
                      />
                    </div>
                    <span class="text-[10px] text-base-content/40">
                      Leave Max message tokens blank to inherit the host default.
                    </span>
                  </div>
                </div>
              )}
            </div>
          )}

          {error && <div class="alert alert-error text-xs">{error}</div>}

          {publishOpen && (
            <div class="card bg-base-200 border border-base-300 p-3 space-y-2">
              <div class="text-xs uppercase tracking-wider text-base-content/60">
                Publish new version
              </div>
              <div class="text-[10px] text-base-content/50">
                Creates a new recipe row keyed on (id, version). Previous version stays intact —
                roll back via the Versions panel.
              </div>
              <label class="form-control">
                <span class="text-[10px] text-base-content/50 mb-1">
                  Version string (current: <code>{agent.version}</code>)
                </span>
                <input
                  class="input input-bordered input-xs font-mono"
                  value={publishVersion}
                  onInput={(e) => setPublishVersion((e.target as HTMLInputElement).value)}
                />
              </label>
              <div class="flex justify-end gap-2">
                <button
                  type="button"
                  class="btn btn-xs btn-ghost"
                  onClick={() => setPublishOpen(false)}
                >
                  Cancel
                </button>
                <button
                  type="button"
                  class="btn btn-xs btn-primary"
                  disabled={
                    saving || !publishVersion.trim() || publishVersion.trim() === agent.version
                  }
                  onClick={() => onPublish(publishVersion.trim())}
                >
                  {saving ? "Publishing…" : `Publish ${publishVersion.trim() || "?"}`}
                </button>
              </div>
            </div>
          )}

          <div class="flex justify-end gap-2 pt-2">
            <button type="button" class="btn btn-sm btn-ghost" onClick={onClose}>
              Cancel
            </button>
            {!publishOpen && (
              <button
                type="button"
                class="btn btn-sm btn-ghost"
                onClick={() => setPublishOpen(true)}
                title="Create a new version row instead of overwriting in place"
              >
                Publish new version…
              </button>
            )}
            {isLocal && tenant && !publishOpen && (
              <button
                type="button"
                class="btn btn-sm btn-ghost"
                disabled={saving}
                onClick={onTestDraftClick}
                title="Chat these edits in a throwaway draft — nothing is committed"
              >
                {saving ? "…" : "Test draft"}
              </button>
            )}
            {onSavedAndTest && !publishOpen && (
              <button
                type="button"
                class="btn btn-sm btn-secondary"
                disabled={saving || fileManaged}
                onClick={onSaveAndTestClick}
                title={
                  fileManaged
                    ? "Disabled — this recipe is defined by a file on disk."
                    : "Save edits and open a fresh chat thread to test"
                }
              >
                {saving ? "Saving…" : "Save & test"}
              </button>
            )}
            <button
              type="submit"
              class="btn btn-sm btn-primary"
              disabled={saving || fileManaged}
              title={
                fileManaged
                  ? "Disabled — this recipe is defined by a file on disk. Edit the source file or use Publish new version."
                  : undefined
              }
            >
              {saving ? "Saving…" : "Save changes"}
            </button>
          </div>
        </form>
      </aside>

      {draftTestId && tenant && (
        <DraftTestModal
          draftId={draftTestId}
          sourceId={agent.id}
          tenant={tenant}
          onClose={closeDraftTest}
        />
      )}
    </>
  );
}

// "v1" → "v2"; "1.2.0" → "1.2.1"; falls back to "<v>+1" when no
// trailing integer can be found.
function bumpVersion(current: string): string {
  const m = current.match(/^(.*?)(\d+)([^\d]*)$/);
  if (!m) return `${current}-next`;
  const [, prefix, num, suffix] = m;
  return `${prefix}${Number.parseInt(num!, 10) + 1}${suffix ?? ""}`;
}

function parseList(raw: string): string[] {
  return raw
    .split(",")
    .map((s) => s.trim())
    .filter((s) => s.length > 0);
}

/** Compact labelled number input — blank means "unset / inherit". */
function NumField({
  label,
  value,
  onChange,
}: {
  label: string;
  value: string;
  onChange: (v: string) => void;
}) {
  return (
    <label class="form-control">
      <span class="text-[10px] text-base-content/50 mb-1">{label}</span>
      <input
        class="input input-bordered input-xs font-mono"
        type="number"
        min="1"
        placeholder="(unset)"
        value={value}
        onInput={(e) => onChange((e.target as HTMLInputElement).value)}
      />
    </label>
  );
}

/** background / blocking selector for autoCompact / autoDistill. */
function RunModeField({ value, onChange }: { value: RunMode; onChange: (v: RunMode) => void }) {
  return (
    <label class="form-control">
      <span class="text-[10px] text-base-content/50 mb-1">Run mode</span>
      <select
        class="select select-bordered select-xs"
        value={value}
        onChange={(e) => onChange((e.target as HTMLSelectElement).value as RunMode)}
      >
        <option value="background">background</option>
        <option value="blocking">blocking</option>
      </select>
    </label>
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

  // Tools selected on the recipe but not present in the live catalog —
  // surface separately so operators can see/clear broken refs.
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
        // Selected → top, then enabled, then alpha.
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
                title="Click to remove from recipe"
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

// Mirrors ToolPicker for the skill catalog. Skills are instruction blocks
// the agent can load on demand (loadSkill); checking one adds its id to the
// recipe's `backend.skills`. No source/secrets/memory axes — skills are
// plain instructions — so the row shows description + "use when" + tags.
function SkillPicker({ skills, selected, onChange }: SkillPickerProps) {
  const [query, setQuery] = useState("");
  const [showOnlySelected, setShowOnlySelected] = useState(false);

  const catalogById = useMemo(() => {
    const m = new Map<string, SkillCatalogEntry>();
    for (const s of skills) m.set(s.id, s);
    return m;
  }, [skills]);

  // Skills pinned on the recipe but not in the live catalog (deleted, or a
  // not-yet-registered ref) — surfaced so operators can clear them.
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
                title="Click to remove from recipe"
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

interface ModelGroup {
  provider: string;
  models: ModelCatalogEntryDto[];
}

function groupModelsByProvider(models: ReadonlyArray<ModelCatalogEntryDto>): ModelGroup[] {
  const byProvider = new Map<string, ModelCatalogEntryDto[]>();
  for (const m of models) {
    const list = byProvider.get(m.provider);
    if (list) list.push(m);
    else byProvider.set(m.provider, [m]);
  }
  return Array.from(byProvider.entries()).map(([provider, models]) => ({ provider, models }));
}

function formatContext(tokens: number): string {
  if (tokens >= 1_000_000) return `${(tokens / 1_000_000).toFixed(1)}M ctx`;
  if (tokens >= 1_000) return `${Math.round(tokens / 1_000)}k ctx`;
  return `${tokens} ctx`;
}
