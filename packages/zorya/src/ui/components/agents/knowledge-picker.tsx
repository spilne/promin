// ---------------------------------------------------------------------------
// KnowledgePicker — the agent editor's "Knowledge / RAG" section. Each row
// binds a host-wired retriever (by id) and says how the model reaches it:
// as a search tool, as context injected before every turn, or both.
//
// Ids come from GET /api/agents/_catalog/retrievers when the host wires a
// RetrieverRegistry; "Manual id" still lets an operator bind an id the
// catalog doesn't list (resolve then fails fast until the host wires it).
// The recipe stores ids and per-binding options only — never the retriever.
// ---------------------------------------------------------------------------

import { useMemo } from "preact/hooks";
import { retrieverToolName, type AgentKnowledgeMode } from "@promin/agent/browser";
import type { RetrieverCatalogEntryDto } from "../../api/client.ts";
import { emptyKnowledgeForm, type KnowledgeForm } from "../../lib/recipe-knowledge-form.ts";

interface KnowledgePickerProps {
  retrievers: ReadonlyArray<RetrieverCatalogEntryDto>;
  bindings: ReadonlyArray<KnowledgeForm>;
  onChange: (next: ReadonlyArray<KnowledgeForm>) => void;
}

export function KnowledgePicker({ retrievers, bindings, onChange }: KnowledgePickerProps) {
  const catalogById = useMemo(
    () => new Map(retrievers.map((r) => [r.id, r] as const)),
    [retrievers],
  );
  const boundIds = useMemo(
    () => new Set(bindings.map((k) => k.id.trim()).filter(Boolean)),
    [bindings],
  );
  const availableToAdd = useMemo(
    () => retrievers.filter((r) => !boundIds.has(r.id)),
    [retrievers, boundIds],
  );
  // Only meaningful when a catalog exists — without one every id is "unknown".
  const unresolved = useMemo(
    () => (retrievers.length === 0 ? [] : [...boundIds].filter((id) => !catalogById.has(id))),
    [boundIds, catalogById, retrievers.length],
  );

  const update = (index: number, patch: Partial<KnowledgeForm>) =>
    onChange(bindings.map((k, i) => (i === index ? { ...k, ...patch } : k)));
  const remove = (index: number) => onChange(bindings.filter((_, i) => i !== index));
  const addFromCatalog = (id: string) => {
    if (!id || boundIds.has(id)) return;
    const description = catalogById.get(id)?.description;
    onChange([
      ...bindings,
      emptyKnowledgeForm({ id, ...(description !== undefined && { description }) }),
    ]);
  };

  return (
    <div class="form-control">
      <div class="flex items-center justify-between mb-1 gap-2">
        <span class="text-xs text-base-content/60 uppercase tracking-wider">
          Knowledge / RAG
          <span class="ml-2 text-[10px] text-base-content/40 normal-case tracking-normal">
            {boundIds.size} bound · {retrievers.length} available
          </span>
        </span>
        <div class="flex items-center gap-1">
          {availableToAdd.length > 0 && (
            <select
              class="select select-bordered select-xs max-w-48 font-mono"
              value=""
              title="Add a host-wired retriever"
              onChange={(e) => {
                const select = e.target as HTMLSelectElement;
                addFromCatalog(select.value);
                select.value = "";
              }}
            >
              <option value="">Add from catalog</option>
              {availableToAdd.map((r) => (
                <option value={r.id}>{r.id}</option>
              ))}
            </select>
          )}
          <button
            type="button"
            class="btn btn-xs btn-ghost"
            onClick={() => onChange([...bindings, emptyKnowledgeForm()])}
          >
            Manual id
          </button>
        </div>
      </div>

      {unresolved.length > 0 && (
        <div class="alert alert-warning py-2 mb-2 text-xs flex-col items-start">
          <span class="font-semibold">
            {unresolved.length} retriever{unresolved.length === 1 ? "" : "s"} not in catalog:
          </span>
          <div class="flex flex-wrap gap-1">
            {unresolved.map((id) => (
              <span class="badge badge-sm badge-warning font-mono">{id}</span>
            ))}
          </div>
        </div>
      )}

      {bindings.length === 0 ? (
        <div class="text-xs text-base-content/40 border border-dashed border-base-300 rounded p-3">
          {retrievers.length > 0
            ? "No retrievers attached. Add one from the catalog to expose it as a search tool, automatic context, or both."
            : "No retriever catalog is wired on this server. Use Manual id to bind a retriever the host resolver provides."}
        </div>
      ) : (
        <div class="space-y-2">
          {bindings.map((k, i) => (
            <KnowledgeRow
              binding={k}
              catalogEntry={catalogById.get(k.id.trim())}
              hasCatalog={retrievers.length > 0}
              onChange={(patch) => update(i, patch)}
              onRemove={() => remove(i)}
            />
          ))}
        </div>
      )}

      <span class="text-[10px] text-base-content/40 mt-1">
        The recipe stores retriever ids only; the host resolver supplies the retrievers. An id the
        host doesn't provide fails when the agent is resolved.
      </span>
    </div>
  );
}

function KnowledgeRow({
  binding: k,
  catalogEntry,
  hasCatalog,
  onChange,
  onRemove,
}: {
  binding: KnowledgeForm;
  catalogEntry: RetrieverCatalogEntryDto | undefined;
  hasCatalog: boolean;
  onChange: (patch: Partial<KnowledgeForm>) => void;
  onRemove: () => void;
}) {
  const id = k.id.trim();
  const idOk = id.length > 0 && (!hasCatalog || catalogEntry !== undefined);
  const value = (e: Event) => (e.target as HTMLInputElement).value;
  return (
    <div class="rounded border border-base-300 bg-base-100 p-3 space-y-2">
      <div class="grid grid-cols-1 sm:grid-cols-[1fr_150px_auto] gap-2 items-end">
        <label class="form-control">
          <span class="text-[10px] text-base-content/50 mb-1">Retriever id</span>
          <input
            class={`input input-bordered input-xs font-mono ${idOk ? "" : "input-warning"}`}
            placeholder="docs"
            value={k.id}
            onInput={(e) => onChange({ id: value(e) })}
          />
        </label>
        <label class="form-control">
          <span class="text-[10px] text-base-content/50 mb-1">Mode</span>
          <select
            class="select select-bordered select-xs"
            value={k.mode}
            onChange={(e) =>
              onChange({ mode: (e.target as HTMLSelectElement).value as AgentKnowledgeMode })
            }
          >
            <option value="tool">tool</option>
            <option value="context">context</option>
            <option value="tool-and-context">tool + context</option>
          </select>
        </label>
        <button
          type="button"
          class="btn btn-xs btn-ghost text-error"
          title="Remove retriever binding"
          onClick={onRemove}
        >
          Remove
        </button>
      </div>

      {catalogEntry && (catalogEntry.description || catalogEntry.tags.length > 0) && (
        <div class="flex items-center gap-1 flex-wrap">
          {catalogEntry.description && (
            <span class="text-[10px] text-base-content/50">{catalogEntry.description}</span>
          )}
          {catalogEntry.tags.map((tag) => (
            <span class="badge badge-xs badge-ghost">{tag}</span>
          ))}
        </div>
      )}

      <div class="grid grid-cols-1 sm:grid-cols-2 gap-2">
        <label class="form-control">
          <span class="text-[10px] text-base-content/50 mb-1">Tool name override</span>
          <input
            class="input input-bordered input-xs font-mono"
            placeholder={retrieverToolName(id || "default")}
            value={k.name}
            disabled={k.mode === "context"}
            onInput={(e) => onChange({ name: value(e) })}
          />
        </label>
        <label class="form-control">
          <span class="text-[10px] text-base-content/50 mb-1">Top K</span>
          <input
            class="input input-bordered input-xs font-mono"
            type="number"
            min="1"
            placeholder="retriever default"
            value={k.topK}
            onInput={(e) => onChange({ topK: value(e) })}
          />
        </label>
        <label class="form-control sm:col-span-2">
          <span class="text-[10px] text-base-content/50 mb-1">Description override</span>
          <input
            class="input input-bordered input-xs"
            placeholder="Shown to the model when this retriever is exposed as a tool"
            value={k.description}
            disabled={k.mode === "context"}
            onInput={(e) => onChange({ description: value(e) })}
          />
        </label>
        <label class="form-control">
          <span class="text-[10px] text-base-content/50 mb-1">Max chunk characters</span>
          <input
            class="input input-bordered input-xs font-mono"
            type="number"
            min="1"
            placeholder="unlimited"
            value={k.maxChunkCharacters}
            onInput={(e) => onChange({ maxChunkCharacters: value(e) })}
          />
        </label>
        <div class="flex items-end gap-3 pb-1">
          <label class="cursor-pointer label justify-start gap-2 px-0 py-0">
            <input
              type="checkbox"
              class="checkbox checkbox-xs"
              checked={k.includeSources}
              onChange={(e) => onChange({ includeSources: (e.target as HTMLInputElement).checked })}
            />
            <span class="text-[10px] text-base-content/60">sources</span>
          </label>
          <label class="cursor-pointer label justify-start gap-2 px-0 py-0">
            <input
              type="checkbox"
              class="checkbox checkbox-xs"
              checked={k.includeScores}
              onChange={(e) => onChange({ includeScores: (e.target as HTMLInputElement).checked })}
            />
            <span class="text-[10px] text-base-content/60">scores</span>
          </label>
        </div>
      </div>
    </div>
  );
}
