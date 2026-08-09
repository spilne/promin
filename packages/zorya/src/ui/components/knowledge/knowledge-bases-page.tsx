import { useEffect, useMemo, useState } from "preact/hooks";
import { useFetch } from "../../hooks/use-fetch.ts";
import { api, type KnowledgeBaseSearchResultDto } from "../../api/client.ts";
import { Page, PageHeader } from "../ui/page.tsx";
import { SkeletonRows } from "../ui/skeleton.tsx";

export function KnowledgeBasesPage() {
  const { data, loading, error, refresh } = useFetch(() => api.listKnowledgeBases(), [], 0);
  const bases = data?.knowledgeBases ?? [];
  const [selectedId, setSelectedId] = useState("");
  const [query, setQuery] = useState("");
  const [submittedQuery, setSubmittedQuery] = useState("");
  const [results, setResults] = useState<KnowledgeBaseSearchResultDto[]>([]);
  const [searching, setSearching] = useState(false);
  const [searchError, setSearchError] = useState<string | null>(null);

  useEffect(() => {
    if (!selectedId && bases[0]) setSelectedId(bases[0].id);
    if (selectedId && !bases.some((base) => base.id === selectedId))
      setSelectedId(bases[0]?.id ?? "");
  }, [bases, selectedId]);

  const selected = useMemo(() => bases.find((base) => base.id === selectedId), [bases, selectedId]);

  const search = async (event: Event) => {
    event.preventDefault();
    if (!selectedId || !query.trim()) return;
    setSearching(true);
    setSearchError(null);
    setSubmittedQuery(query.trim());
    try {
      const response = await api.searchKnowledgeBase(selectedId, { query: query.trim(), topK: 8 });
      setResults(response.results);
    } catch (err) {
      setResults([]);
      setSearchError(err instanceof Error ? err.message : String(err));
    } finally {
      setSearching(false);
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
          <button
            type="button"
            class="btn btn-sm btn-ghost gap-1"
            onClick={refresh}
            title="Refresh"
          >
            <span aria-hidden="true">↻</span> Refresh
          </button>
        }
      />

      {error && <div class="alert alert-error text-sm">{error.message}</div>}

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
                Runtime retrievers are registered by the server host. Managed ingestion and vector
                configuration will be added to the knowledge-base management surface.
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
