import { useEffect, useMemo, useState } from "preact/hooks";
import { useFetch } from "../../hooks/use-fetch.ts";
import {
  api,
  type KnowledgeBaseChunkDto,
  type KnowledgeBaseSearchResultDto,
  type KnowledgeBaseSourceDto,
} from "../../api/client.ts";
import { Page, PageHeader } from "../ui/page.tsx";
import { SkeletonRows } from "../ui/skeleton.tsx";
import { useNamespace } from "../../hooks/use-namespace.ts";
import { confirm } from "../../lib/dialogs.ts";

export function KnowledgeBasesPage() {
  const [namespace] = useNamespace();
  const { data, loading, error, refresh } = useFetch(
    () => api.listKnowledgeBases({ namespace: namespace || undefined }),
    [namespace],
    0,
  );
  const bases = data?.knowledgeBases ?? [];
  const [selectedId, setSelectedId] = useState("");
  const [query, setQuery] = useState("");
  const [submittedQuery, setSubmittedQuery] = useState("");
  const [results, setResults] = useState<KnowledgeBaseSearchResultDto[]>([]);
  const [searching, setSearching] = useState(false);
  const [searchError, setSearchError] = useState<string | null>(null);
  const [sources, setSources] = useState<KnowledgeBaseSourceDto[]>([]);
  const [chunks, setChunks] = useState<KnowledgeBaseChunkDto[]>([]);
  const [sourcesLoading, setSourcesLoading] = useState(false);
  const [sourceError, setSourceError] = useState<string | null>(null);
  const [showCreate, setShowCreate] = useState(false);
  const [newId, setNewId] = useState("");
  const [newDescription, setNewDescription] = useState("");
  const [sourceId, setSourceId] = useState("");
  const [sourceTitle, setSourceTitle] = useState("");
  const [sourceText, setSourceText] = useState("");
  const [saving, setSaving] = useState(false);

  useEffect(() => {
    if (!selectedId && bases[0]) setSelectedId(bases[0].id);
    if (selectedId && !bases.some((base) => base.id === selectedId))
      setSelectedId(bases[0]?.id ?? "");
  }, [bases, selectedId]);

  const selected = useMemo(() => bases.find((base) => base.id === selectedId), [bases, selectedId]);

  useEffect(() => {
    if (!selectedId) {
      setSources([]);
      return;
    }
    let active = true;
    setSourcesLoading(true);
    setSourceError(null);
    void api
      .listKnowledgeBaseSources(selectedId, namespace || undefined)
      .then((response) => active && setSources(response.sources))
      .catch((err) => active && setSourceError(err instanceof Error ? err.message : String(err)))
      .finally(() => active && setSourcesLoading(false));
    return () => {
      active = false;
    };
  }, [selectedId, namespace]);

  const search = async (event: Event) => {
    event.preventDefault();
    if (!selectedId || !query.trim()) return;
    setSearching(true);
    setSearchError(null);
    setSubmittedQuery(query.trim());
    try {
      const response = await api.searchKnowledgeBase(selectedId, {
        query: query.trim(),
        topK: 8,
        ...(namespace ? { namespace } : {}),
      });
      setResults(response.results);
    } catch (err) {
      setResults([]);
      setSearchError(err instanceof Error ? err.message : String(err));
    } finally {
      setSearching(false);
    }
  };

  const createKnowledgeBase = async (event: Event) => {
    event.preventDefault();
    if (!newId.trim()) return;
    setSaving(true);
    setSourceError(null);
    try {
      const response = await api.createKnowledgeBase({
        id: newId.trim(),
        ...(namespace ? { namespace } : {}),
        ...(newDescription.trim() ? { description: newDescription.trim() } : {}),
      });
      setShowCreate(false);
      setNewId("");
      setNewDescription("");
      await refresh();
      setSelectedId(response.knowledgeBase.id);
    } catch (err) {
      setSourceError(err instanceof Error ? err.message : String(err));
    } finally {
      setSaving(false);
    }
  };

  const ingestSource = async (event: Event) => {
    event.preventDefault();
    if (!selectedId || !sourceId.trim() || !sourceText.trim()) return;
    setSaving(true);
    setSourceError(null);
    try {
      await api.ingestKnowledgeBaseSource(selectedId, {
        id: sourceId.trim(),
        text: sourceText,
        ...(namespace ? { namespace } : {}),
        ...(sourceTitle.trim() ? { title: sourceTitle.trim() } : {}),
      });
      setSourceId("");
      setSourceTitle("");
      setSourceText("");
      const response = await api.listKnowledgeBaseSources(selectedId, namespace || undefined);
      setSources(response.sources);
      await refresh();
    } catch (err) {
      setSourceError(err instanceof Error ? err.message : String(err));
    } finally {
      setSaving(false);
    }
  };

  const removeSource = async (id: string) => {
    setSaving(true);
    setSourceError(null);
    try {
      await api.deleteKnowledgeBaseSource(selectedId, id, namespace || undefined);
      setSources((current) => current.filter((source) => source.id !== id));
      setChunks([]);
      await refresh();
    } catch (err) {
      setSourceError(err instanceof Error ? err.message : String(err));
    } finally {
      setSaving(false);
    }
  };

  const inspectChunks = async (source: KnowledgeBaseSourceDto) => {
    if (!selectedId) return;
    setSourceError(null);
    try {
      const response = await api.listKnowledgeBaseChunks(
        selectedId,
        namespace || undefined,
        source.id,
      );
      setChunks(response.chunks);
    } catch (err) {
      setSourceError(err instanceof Error ? err.message : String(err));
    }
  };

  const removeKnowledgeBase = async () => {
    if (!selectedId) return;
    const ok = await confirm({
      title: "Delete knowledge base?",
      message: `This removes ${selectedId} and all indexed sources.`,
      confirmLabel: "Delete",
      variant: "danger",
    });
    if (!ok) return;
    setSaving(true);
    try {
      await api.deleteKnowledgeBase(selectedId, namespace || undefined);
      setResults([]);
      setSources([]);
      setChunks([]);
      await refresh();
    } catch (err) {
      setSourceError(err instanceof Error ? err.message : String(err));
    } finally {
      setSaving(false);
    }
  };

  return (
    <Page>
      <PageHeader
        title="Knowledge"
        eyebrow="Retrieval and RAG"
        description={
          loading && !data
            ? "Loading knowledge-base registry..."
            : bases.length === 0
              ? "No knowledge bases are wired to this server."
              : `${bases.length} runtime knowledge base${bases.length === 1 ? "" : "s"} available to agents.`
        }
        actions={
          <>
            <button
              type="button"
              class="btn btn-sm btn-primary gap-1"
              onClick={() => setShowCreate((open) => !open)}
            >
              <span aria-hidden="true">+</span> New knowledge base
            </button>
            <button
              type="button"
              class="btn btn-sm btn-ghost gap-1"
              onClick={refresh}
              title="Refresh"
            >
              <span aria-hidden="true">↻</span> Refresh
            </button>
          </>
        }
      />

      {error && <div class="alert alert-error text-sm">{error.message}</div>}

      {showCreate && (
        <form
          class="card bg-base-100 shadow-sm border border-primary/30"
          onSubmit={createKnowledgeBase}
        >
          <div class="card-body p-4 gap-3">
            <div>
              <h2 class="font-semibold">Create knowledge base</h2>
              <p class="text-xs text-base-content/55 mt-1">
                This creates a runtime-managed text index in the current namespace.
              </p>
            </div>
            <div class="grid grid-cols-1 md:grid-cols-2 gap-2">
              <label class="form-control">
                <span class="label-text text-xs">Stable id</span>
                <input
                  class="input input-bordered input-sm"
                  placeholder="product-docs"
                  value={newId}
                  onInput={(event) => setNewId((event.target as HTMLInputElement).value)}
                />
              </label>
              <label class="form-control">
                <span class="label-text text-xs">Description</span>
                <input
                  class="input input-bordered input-sm"
                  placeholder="Support and product reference"
                  value={newDescription}
                  onInput={(event) => setNewDescription((event.target as HTMLInputElement).value)}
                />
              </label>
            </div>
            <div class="flex justify-end gap-2">
              <button
                type="button"
                class="btn btn-sm btn-ghost"
                onClick={() => setShowCreate(false)}
              >
                Cancel
              </button>
              <button
                type="submit"
                class="btn btn-sm btn-primary"
                disabled={!newId.trim() || saving}
              >
                {saving ? "Creating..." : "Create"}
              </button>
            </div>
          </div>
        </form>
      )}

      <div class="grid grid-cols-1 xl:grid-cols-[18rem_minmax(0,1fr)] gap-4 items-start">
        <section class="card bg-base-100 shadow-sm border border-base-300">
          <div class="card-body p-4">
            <div class="flex items-center justify-between gap-2 mb-2">
              <h2 class="font-semibold">Knowledge bases</h2>
              <span class="badge badge-sm badge-ghost">{bases.length}</span>
            </div>
            {loading && !data ? (
              <SkeletonRows rows={4} cols={1} />
            ) : bases.length === 0 ? (
              <div class="text-sm text-base-content/55 leading-relaxed">
                Create a managed knowledge base here, or register a runtime retriever from the
                server host for read-only access.
              </div>
            ) : (
              <div class="space-y-1">
                {bases.map((base) => (
                  <button
                    key={base.id}
                    type="button"
                    aria-pressed={base.id === selectedId}
                    class={`w-full text-left rounded border px-3 py-2 transition-colors ${
                      base.id === selectedId
                        ? "border-primary/40 bg-primary/10"
                        : "border-transparent hover:border-base-300 hover:bg-base-200"
                    }`}
                    onClick={() => {
                      setSelectedId(base.id);
                      setResults([]);
                      setSubmittedQuery("");
                      setSearchError(null);
                    }}
                  >
                    <div class="font-mono text-sm truncate">{base.id}</div>
                    {base.description && (
                      <div class="text-xs text-base-content/55 mt-1">{base.description}</div>
                    )}
                    {base.tags.length > 0 && (
                      <div class="flex flex-wrap gap-1 mt-2">
                        {base.tags.map((tag) => (
                          <span key={tag} class="badge badge-xs badge-outline">
                            {tag}
                          </span>
                        ))}
                      </div>
                    )}
                  </button>
                ))}
              </div>
            )}
          </div>
        </section>

        <section class="space-y-4">
          <div class="card bg-base-100 shadow-sm border border-base-300">
            <div class="card-body p-4 space-y-3">
              <div>
                <h2 class="font-semibold">Search playground</h2>
                <p class="text-xs text-base-content/55 mt-1">
                  Inspect the chunks an agent would receive before attaching this knowledge base.
                </p>
              </div>
              <form class="flex flex-col sm:flex-row gap-2" onSubmit={search}>
                <label class="sr-only" for="knowledge-search">
                  Search knowledge base
                </label>
                <input
                  id="knowledge-search"
                  class="input input-bordered input-sm flex-1"
                  placeholder={selected ? `Search ${selected.id}...` : "Select a knowledge base..."}
                  value={query}
                  disabled={!selectedId || searching}
                  onInput={(event) => setQuery((event.target as HTMLInputElement).value)}
                />
                <button
                  class="btn btn-sm btn-primary"
                  type="submit"
                  disabled={!selectedId || !query.trim() || searching}
                >
                  {searching ? "Searching..." : "Search"}
                </button>
              </form>
              {searchError && <div class="alert alert-error text-xs">{searchError}</div>}
            </div>
          </div>

          {selected && (
            <div class="card bg-base-100 shadow-sm border border-base-300">
              <div class="card-body p-4 gap-3">
                <div class="flex items-start justify-between gap-3">
                  <div>
                    <h2 class="font-semibold">Sources</h2>
                    <p class="text-xs text-base-content/55 mt-1">
                      {selected.sourceCount ?? sources.length} source
                      {(selected.sourceCount ?? sources.length) === 1 ? "" : "s"} ·{" "}
                      {selected.chunkCount ?? "-"} chunks
                    </p>
                  </div>
                  <div class="flex items-center gap-2">
                    {selected.status && (
                      <span
                        class={`badge badge-sm ${selected.status === "ready" ? "badge-success" : "badge-warning"}`}
                      >
                        {selected.status}
                      </span>
                    )}
                    <button
                      type="button"
                      class="btn btn-xs btn-ghost text-error"
                      onClick={() => void removeKnowledgeBase()}
                      disabled={saving}
                    >
                      Delete
                    </button>
                  </div>
                </div>
                <form
                  class="grid grid-cols-1 md:grid-cols-[10rem_minmax(0,12rem)_minmax(0,1fr)_auto] gap-2 items-end"
                  onSubmit={ingestSource}
                >
                  <label class="form-control">
                    <span class="label-text text-xs">Source id</span>
                    <input
                      class="input input-bordered input-sm"
                      placeholder="runbook"
                      value={sourceId}
                      onInput={(event) => setSourceId((event.target as HTMLInputElement).value)}
                    />
                  </label>
                  <label class="form-control">
                    <span class="label-text text-xs">Title</span>
                    <input
                      class="input input-bordered input-sm"
                      placeholder="On-call guide"
                      value={sourceTitle}
                      onInput={(event) => setSourceTitle((event.target as HTMLInputElement).value)}
                    />
                  </label>
                  <label class="form-control">
                    <span class="label-text text-xs">Text</span>
                    <textarea
                      class="textarea textarea-bordered textarea-sm min-h-10"
                      placeholder="Paste source text..."
                      value={sourceText}
                      onInput={(event) =>
                        setSourceText((event.target as HTMLTextAreaElement).value)
                      }
                    />
                  </label>
                  <button
                    type="submit"
                    class="btn btn-sm btn-primary"
                    disabled={!sourceId.trim() || !sourceText.trim() || saving}
                  >
                    {saving ? "Saving..." : "Ingest"}
                  </button>
                </form>
                {sourceError && <div class="alert alert-error text-xs">{sourceError}</div>}
                {sourcesLoading ? (
                  <SkeletonRows rows={2} cols={1} />
                ) : sources.length === 0 ? (
                  <div class="text-sm text-base-content/50 border border-dashed border-base-300 rounded p-4">
                    No sources yet. Ingest a document to make this knowledge base useful.
                  </div>
                ) : (
                  <div class="divide-y divide-base-300 border border-base-300 rounded">
                    {sources.map((source) => (
                      <div
                        key={source.id}
                        class="flex flex-col sm:flex-row sm:items-center justify-between gap-2 px-3 py-2"
                      >
                        <div class="min-w-0">
                          <div class="font-mono text-sm truncate">{source.id}</div>
                          <div class="text-xs text-base-content/55 truncate">
                            {source.title ?? source.uri ?? "Untitled source"} · {source.chunkCount}{" "}
                            chunks
                          </div>
                          {source.status === "failed" && (
                            <div class="text-xs text-error mt-1">{source.error}</div>
                          )}
                        </div>
                        <div class="flex shrink-0 gap-1">
                          <button
                            type="button"
                            class="btn btn-xs btn-ghost"
                            onClick={() => void inspectChunks(source)}
                          >
                            Inspect chunks
                          </button>
                          <button
                            type="button"
                            class="btn btn-xs btn-ghost text-error"
                            disabled={saving}
                            onClick={() => void removeSource(source.id)}
                          >
                            Remove
                          </button>
                        </div>
                      </div>
                    ))}
                  </div>
                )}
                {chunks.length > 0 && (
                  <div class="space-y-2">
                    <div class="text-xs font-medium text-base-content/60">Inspected chunks</div>
                    {chunks.map((chunk) => (
                      <article key={chunk.id} class="border border-base-300 rounded p-3">
                        <div class="flex justify-between gap-2 text-[11px] text-base-content/45 font-mono">
                          <span>{chunk.id}</span>
                          <span>chunk {chunk.index}</span>
                        </div>
                        <p class="text-sm leading-relaxed whitespace-pre-wrap mt-2">{chunk.text}</p>
                      </article>
                    ))}
                  </div>
                )}
              </div>
            </div>
          )}

          {submittedQuery && !searching && !searchError && (
            <div class="text-xs text-base-content/55">
              {results.length} result{results.length === 1 ? "" : "s"} for{" "}
              <span class="font-mono">{submittedQuery}</span>
            </div>
          )}
          {searching ? (
            <SkeletonRows rows={3} cols={1} />
          ) : results.length > 0 ? (
            <div class="space-y-3">
              {results.map((result) => (
                <ResultCard key={`${result.id}-${result.index}`} result={result} />
              ))}
            </div>
          ) : (
            <div class="border border-dashed border-base-300 rounded p-8 text-center text-sm text-base-content/45">
              {submittedQuery
                ? `No matching chunks for ${submittedQuery}.`
                : selected
                  ? "Run a search to inspect retrieved chunks and source metadata."
                  : "Select a knowledge base to begin."}
            </div>
          )}
        </section>
      </div>
    </Page>
  );
}

function ResultCard({ result }: { result: KnowledgeBaseSearchResultDto }) {
  return (
    <article class="card bg-base-100 shadow-sm border border-base-300">
      <div class="card-body p-4 gap-2">
        <div class="flex items-start justify-between gap-3">
          <div class="min-w-0">
            <div class="font-semibold text-sm truncate">
              {result.source.title ?? result.source.id}
            </div>
            <div class="font-mono text-[11px] text-base-content/45 truncate">
              {result.source.uri ?? result.source.id}
            </div>
          </div>
          <span class="badge badge-sm badge-info shrink-0">score {result.score.toFixed(3)}</span>
        </div>
        <p class="text-sm leading-relaxed whitespace-pre-wrap">{result.text}</p>
        <div class="flex flex-wrap gap-1">
          {result.source.tags.map((tag) => (
            <span key={tag} class="badge badge-xs badge-outline">
              {tag}
            </span>
          ))}
          <span class="badge badge-xs badge-ghost">chunk {result.index}</span>
        </div>
      </div>
    </article>
  );
}
