import { useEffect, useMemo, useState } from "preact/hooks";
import {
  api,
  type KnowledgeBaseChunkDto,
  type KnowledgeBaseEntryDto,
  type KnowledgeBaseSearchResultDto,
  type KnowledgeBaseSourceDto,
} from "../../api/client.ts";
import { useNamespace } from "../../hooks/use-namespace.ts";
import { useFetch } from "../../hooks/use-fetch.ts";
import { confirm } from "../../lib/dialogs.ts";
import { Page, PageHeader } from "../ui/page.tsx";
import { SkeletonRows } from "../ui/skeleton.tsx";

type WorkspaceTab = "search" | "add" | "sources";
type AddMode = "paste" | "file" | "url";

export function KnowledgeBasesPage() {
  const [namespace] = useNamespace();
  const scope = namespace || undefined;
  const { data, loading, error, refresh } = useFetch(
    () => api.listKnowledgeBases({ namespace: scope }),
    [scope],
    0,
  );
  const bases = data?.knowledgeBases ?? [];
  const [selectedId, setSelectedId] = useState("");
  const [tab, setTab] = useState<WorkspaceTab>("search");
  const [showCreate, setShowCreate] = useState(false);
  const [newId, setNewId] = useState("");
  const [newDescription, setNewDescription] = useState("");
  const [busy, setBusy] = useState(false);
  const [notice, setNotice] = useState<string | null>(null);

  useEffect(() => {
    if (!selectedId && bases[0]) setSelectedId(bases[0].id);
    if (selectedId && !bases.some((base) => base.id === selectedId)) {
      setSelectedId(bases[0]?.id ?? "");
    }
  }, [bases, selectedId]);

  const selected = useMemo(() => bases.find((base) => base.id === selectedId), [bases, selectedId]);

  const createKnowledgeBase = async (event: Event) => {
    event.preventDefault();
    if (!newId.trim()) return;
    setBusy(true);
    setNotice(null);
    try {
      const response = await api.createKnowledgeBase({
        id: newId.trim(),
        ...(scope ? { namespace: scope } : {}),
        ...(newDescription.trim() ? { description: newDescription.trim() } : {}),
      });
      setNewId("");
      setNewDescription("");
      setShowCreate(false);
      await refresh();
      setSelectedId(response.knowledgeBase.id);
      setTab("add");
    } catch (err) {
      setNotice(errorMessage(err));
    } finally {
      setBusy(false);
    }
  };

  const deleteKnowledgeBase = async () => {
    if (!selectedId) return;
    const confirmed = await confirm({
      title: "Delete knowledge base?",
      message: `This removes ${selectedId} and all indexed sources.`,
      confirmLabel: "Delete",
      variant: "danger",
    });
    if (!confirmed) return;
    setBusy(true);
    try {
      await api.deleteKnowledgeBase(selectedId, scope);
      setSelectedId("");
      setTab("search");
      await refresh();
    } catch (err) {
      setNotice(errorMessage(err));
    } finally {
      setBusy(false);
    }
  };

  return (
    <Page className="max-w-[1520px]">
      <PageHeader
        title="Knowledge bases"
        eyebrow="Retrieval and RAG"
        description={
          loading && !data
            ? "Loading the knowledge registry..."
            : `${bases.length} knowledge base${bases.length === 1 ? "" : "s"} in the current scope.`
        }
        actions={
          <>
            <button
              class="btn btn-sm btn-primary"
              type="button"
              onClick={() => setShowCreate((open) => !open)}
            >
              <span aria-hidden="true">+</span> New knowledge base
            </button>
            <button
              class="btn btn-sm btn-ghost"
              type="button"
              onClick={refresh}
              title="Refresh knowledge bases"
            >
              <span aria-hidden="true">↻</span>
            </button>
          </>
        }
      />

      {error && <div class="alert alert-error text-sm">{error.message}</div>}
      {notice && <div class="alert alert-error text-sm">{notice}</div>}

      {showCreate && (
        <form class="border border-primary/30 bg-base-100 p-4" onSubmit={createKnowledgeBase}>
          <div class="mb-3">
            <h2 class="font-semibold">Start a knowledge base</h2>
            <p class="mt-1 text-xs text-base-content/55">
              Define the container first. The next screen lets you paste text or upload a file as
              its first source.
            </p>
          </div>
          <div class="flex flex-col gap-3 md:flex-row md:items-end">
            <label class="form-control flex-1">
              <span class="label-text text-xs">Knowledge base id</span>
              <input
                class="input input-bordered input-sm"
                placeholder="product-docs"
                value={newId}
                onInput={(event) => setNewId((event.target as HTMLInputElement).value)}
              />
            </label>
            <label class="form-control flex-[2]">
              <span class="label-text text-xs">
                Description <span class="text-base-content/40">optional</span>
              </span>
              <input
                class="input input-bordered input-sm"
                placeholder="Support and product reference"
                value={newDescription}
                onInput={(event) => setNewDescription((event.target as HTMLInputElement).value)}
              />
            </label>
            <div class="flex gap-2">
              <button
                class="btn btn-sm btn-ghost"
                type="button"
                onClick={() => setShowCreate(false)}
              >
                Cancel
              </button>
              <button class="btn btn-sm btn-primary" type="submit" disabled={!newId.trim() || busy}>
                {busy ? "Creating..." : "Create and add sources"}
              </button>
            </div>
          </div>
        </form>
      )}

      <div class="grid grid-cols-1 items-start gap-6 xl:grid-cols-[18rem_minmax(0,1fr)]">
        <aside class="border border-base-300 bg-base-100">
          <div class="flex items-center justify-between border-b border-base-300 px-4 py-3">
            <h2 class="text-sm font-semibold">Knowledge bases</h2>
            <span class="badge badge-sm badge-ghost">{bases.length}</span>
          </div>
          {loading && !data ? (
            <div class="p-4">
              <SkeletonRows rows={4} cols={1} />
            </div>
          ) : bases.length === 0 ? (
            <div class="p-4 text-sm leading-relaxed text-base-content/55">
              Create a knowledge base to start adding sources.
            </div>
          ) : (
            <div class="p-2">
              {bases.map((base) => (
                <button
                  key={base.id}
                  type="button"
                  aria-pressed={base.id === selectedId}
                  class={`mb-1 w-full rounded border px-3 py-3 text-left transition-colors ${base.id === selectedId ? "border-primary/40 bg-primary/10" : "border-transparent hover:border-base-300 hover:bg-base-200"}`}
                  onClick={() => {
                    setSelectedId(base.id);
                    setTab("search");
                    setNotice(null);
                  }}
                >
                  <div class="truncate font-mono text-sm">{base.id}</div>
                  <div class="mt-1 truncate text-xs text-base-content/55">
                    {base.description ?? "No description"}
                  </div>
                  <div class="mt-2 flex gap-2 text-[11px] text-base-content/45">
                    <span>{base.sourceCount ?? 0} sources</span>
                    <span>{base.chunkCount ?? 0} chunks</span>
                  </div>
                </button>
              ))}
            </div>
          )}
        </aside>

        {selected ? (
          <KnowledgeWorkspace
            base={selected}
            namespace={scope}
            tab={tab}
            setTab={setTab}
            busy={busy}
            setBusy={setBusy}
            setNotice={setNotice}
            onDelete={deleteKnowledgeBase}
            onRefresh={refresh}
          />
        ) : (
          <div class="border border-dashed border-base-300 p-12 text-center">
            <div class="text-lg font-semibold">No knowledge base selected</div>
            <p class="mx-auto mt-2 max-w-md text-sm text-base-content/55">
              Create a knowledge base, then add sources through paste or file upload. The search
              playground will show what agents can retrieve.
            </p>
            <button
              class="btn btn-sm btn-primary mt-5"
              type="button"
              onClick={() => setShowCreate(true)}
            >
              Create knowledge base
            </button>
          </div>
        )}
      </div>
    </Page>
  );
}

function KnowledgeWorkspace(props: {
  base: KnowledgeBaseEntryDto;
  namespace?: string;
  tab: WorkspaceTab;
  setTab: (tab: WorkspaceTab) => void;
  busy: boolean;
  setBusy: (busy: boolean) => void;
  setNotice: (notice: string | null) => void;
  onDelete: () => Promise<void>;
  onRefresh: () => Promise<unknown> | void;
}) {
  const { base, namespace, tab, setTab, busy, setBusy, setNotice, onDelete, onRefresh } = props;
  const [sources, setSources] = useState<KnowledgeBaseSourceDto[]>([]);
  const [loading, setLoading] = useState(true);
  const [query, setQuery] = useState("");
  const [submittedQuery, setSubmittedQuery] = useState("");
  const [results, setResults] = useState<KnowledgeBaseSearchResultDto[]>([]);
  const [searching, setSearching] = useState(false);
  const [chunks, setChunks] = useState<KnowledgeBaseChunkDto[]>([]);

  const reloadSources = async () => {
    setLoading(true);
    try {
      setSources((await api.listKnowledgeBaseSources(base.id, namespace)).sources);
    } catch (err) {
      setNotice(errorMessage(err));
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    void reloadSources();
  }, [base.id, namespace]);

  const search = async (event: Event) => {
    event.preventDefault();
    if (!query.trim()) return;
    setSearching(true);
    setNotice(null);
    setSubmittedQuery(query.trim());
    try {
      const response = await api.searchKnowledgeBase(base.id, {
        query: query.trim(),
        topK: 8,
        ...(namespace ? { namespace } : {}),
      });
      setResults(response.results);
    } catch (err) {
      setResults([]);
      setNotice(errorMessage(err));
    } finally {
      setSearching(false);
    }
  };

  return (
    <section class="min-w-0">
      <div class="flex flex-col gap-3 border-b border-base-300 pb-4 sm:flex-row sm:items-start sm:justify-between">
        <div class="min-w-0">
          <div class="flex flex-wrap items-center gap-2">
            <h2 class="truncate font-mono text-xl font-semibold">{base.id}</h2>
            <span
              class={`badge badge-sm ${base.status === "degraded" ? "badge-warning" : "badge-success"}`}
            >
              {base.status ?? "ready"}
            </span>
          </div>
          <p class="mt-1 text-sm text-base-content/55">{base.description ?? "No description"}</p>
        </div>
        <button
          class="btn btn-sm btn-ghost text-error"
          type="button"
          onClick={() => void onDelete()}
          disabled={busy}
        >
          Delete
        </button>
      </div>

      <nav class="mt-4 flex gap-1 border-b border-base-300" aria-label="Knowledge base views">
        <WorkspaceTabButton active={tab === "search"} onClick={() => setTab("search")}>
          Search
        </WorkspaceTabButton>
        <WorkspaceTabButton active={tab === "add"} onClick={() => setTab("add")}>
          Add sources
        </WorkspaceTabButton>
        <WorkspaceTabButton active={tab === "sources"} onClick={() => setTab("sources")}>
          Sources <span class="badge badge-xs badge-ghost ml-1">{sources.length}</span>
        </WorkspaceTabButton>
      </nav>

      {tab === "search" && (
        <SearchPanel
          query={query}
          setQuery={setQuery}
          submittedQuery={submittedQuery}
          results={results}
          searching={searching}
          onSearch={search}
        />
      )}
      {tab === "add" && (
        <AddSourcePanel
          baseId={base.id}
          namespace={namespace}
          busy={busy}
          setBusy={setBusy}
          setNotice={setNotice}
          onComplete={async () => {
            await reloadSources();
            await onRefresh();
            setTab("sources");
          }}
        />
      )}
      {tab === "sources" && (
        <SourcesPanel
          baseId={base.id}
          namespace={namespace}
          sources={sources}
          chunks={chunks}
          loading={loading}
          onInspect={async (sourceId) => {
            try {
              setChunks((await api.listKnowledgeBaseChunks(base.id, namespace, sourceId)).chunks);
            } catch (err) {
              setNotice(errorMessage(err));
            }
          }}
          onRemove={async (sourceId) => {
            const ok = await confirm({
              title: "Remove source?",
              message: `Remove ${sourceId} and its indexed chunks?`,
              confirmLabel: "Remove",
              variant: "danger",
            });
            if (!ok) return;
            setBusy(true);
            try {
              await api.deleteKnowledgeBaseSource(base.id, sourceId, namespace);
              setChunks([]);
              await reloadSources();
              await onRefresh();
            } catch (err) {
              setNotice(errorMessage(err));
            } finally {
              setBusy(false);
            }
          }}
        />
      )}
    </section>
  );
}

function WorkspaceTabButton(props: {
  active: boolean;
  onClick: () => void;
  children: preact.ComponentChildren;
}) {
  return (
    <button
      type="button"
      class={`border-b-2 px-3 py-2 text-sm ${props.active ? "border-primary text-primary" : "border-transparent text-base-content/55 hover:text-base-content"}`}
      onClick={props.onClick}
    >
      {props.children}
    </button>
  );
}

function SearchPanel(props: {
  query: string;
  setQuery: (value: string) => void;
  submittedQuery: string;
  results: KnowledgeBaseSearchResultDto[];
  searching: boolean;
  onSearch: (event: Event) => Promise<void>;
}) {
  const { query, setQuery, submittedQuery, results, searching, onSearch } = props;
  return (
    <div class="pt-5">
      <div class="border border-base-300 bg-base-100 p-4">
        <h3 class="font-semibold">Test retrieval</h3>
        <p class="mt-1 text-xs text-base-content/55">
          Run the same style of search an agent will use and inspect source-backed results.
        </p>
        <form class="mt-4 flex flex-col gap-2 sm:flex-row" onSubmit={onSearch}>
          <label class="sr-only" for="knowledge-search">
            Search knowledge base
          </label>
          <input
            id="knowledge-search"
            class="input input-bordered flex-1"
            placeholder="Ask about your sources..."
            value={query}
            disabled={searching}
            onInput={(event) => setQuery((event.target as HTMLInputElement).value)}
          />
          <button class="btn btn-primary" type="submit" disabled={!query.trim() || searching}>
            {searching ? "Searching..." : "Search"}
          </button>
        </form>
      </div>
      <div class="mt-4">
        {searching ? (
          <SkeletonRows rows={3} cols={1} />
        ) : results.length > 0 ? (
          <div class="space-y-3">
            {results.map((result) => (
              <ResultCard key={`${result.id}-${result.index}`} result={result} />
            ))}
          </div>
        ) : (
          <div class="border border-dashed border-base-300 p-10 text-center text-sm text-base-content/50">
            {submittedQuery
              ? `No matching chunks for ${submittedQuery}.`
              : "Search to inspect retrieved chunks and citations."}
          </div>
        )}
      </div>
    </div>
  );
}

function AddSourcePanel(props: {
  baseId: string;
  namespace?: string;
  busy: boolean;
  setBusy: (busy: boolean) => void;
  setNotice: (notice: string | null) => void;
  onComplete: () => Promise<void>;
}) {
  const { baseId, namespace, busy, setBusy, setNotice, onComplete } = props;
  const [mode, setMode] = useState<AddMode>("paste");
  const [sourceId, setSourceId] = useState("");
  const [title, setTitle] = useState("");
  const [text, setText] = useState("");
  const [fileName, setFileName] = useState("");
  const [url, setUrl] = useState("");

  const ingest = async (event: Event) => {
    event.preventDefault();
    if (!sourceId.trim() || (mode === "url" ? !url.trim() : !text.trim())) return;
    setBusy(true);
    setNotice(null);
    try {
      if (mode === "url") {
        await api.importKnowledgeBaseSource(baseId, {
          kind: "url",
          config: {
            url: url.trim(),
            id: sourceId.trim(),
            ...(title.trim() ? { title: title.trim() } : {}),
          },
          ...(namespace ? { namespace } : {}),
        });
      } else {
        await api.ingestKnowledgeBaseSource(baseId, {
          id: sourceId.trim(),
          text,
          ...(namespace ? { namespace } : {}),
          ...(title.trim() ? { title: title.trim() } : {}),
        });
      }
      setSourceId("");
      setTitle("");
      setText("");
      setFileName("");
      setUrl("");
      await onComplete();
    } catch (err) {
      setNotice(errorMessage(err));
    } finally {
      setBusy(false);
    }
  };

  const readFile = (event: Event) => {
    const file = (event.target as HTMLInputElement).files?.[0];
    if (!file) return;
    setFileName(file.name);
    if (!sourceId) setSourceId(file.name.replace(/\.[^.]+$/, "").replace(/[^A-Za-z0-9._-]+/g, "-"));
    if (!title) setTitle(file.name);
    const reader = new FileReader();
    reader.onload = () => setText(typeof reader.result === "string" ? reader.result : "");
    reader.onerror = () => setNotice("Could not read this file.");
    reader.readAsText(file);
  };

  return (
    <div class="pt-5">
      <div class="border border-base-300 bg-base-100 p-4">
        <div class="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
          <div>
            <h3 class="font-semibold">Add a source</h3>
            <p class="mt-1 text-xs text-base-content/55">
              Choose how to bring content into this knowledge base.
            </p>
          </div>
          <div class="join" role="tablist" aria-label="Source type">
            <button
              type="button"
              role="tab"
              aria-selected={mode === "paste"}
              class={`join-item btn btn-sm ${mode === "paste" ? "btn-primary" : "btn-ghost"}`}
              onClick={() => setMode("paste")}
            >
              Paste text
            </button>
            <button
              type="button"
              role="tab"
              aria-selected={mode === "file"}
              class={`join-item btn btn-sm ${mode === "file" ? "btn-primary" : "btn-ghost"}`}
              onClick={() => setMode("file")}
            >
              Upload file
            </button>
            <button
              type="button"
              role="tab"
              aria-selected={mode === "url"}
              class={`join-item btn btn-sm ${mode === "url" ? "btn-primary" : "btn-ghost"}`}
              onClick={() => setMode("url")}
            >
              Import URL
            </button>
          </div>
        </div>
        <form class="mt-5 space-y-3" onSubmit={ingest}>
          {mode === "file" && (
            <label class="flex cursor-pointer items-center justify-between rounded border border-dashed border-base-300 px-3 py-4 hover:border-primary/50">
              <span class="text-sm">{fileName || "Choose a text or markdown file"}</span>
              <span class="btn btn-sm btn-ghost">Browse</span>
              <input
                class="hidden"
                type="file"
                accept=".txt,.md,.markdown,.csv,text/plain,text/markdown,text/csv"
                onChange={readFile}
              />
            </label>
          )}
          {mode === "url" && (
            <label class="form-control">
              <span class="label-text text-xs">Web page URL</span>
              <input
                class="input input-bordered"
                type="url"
                placeholder="https://docs.example.com/guide"
                value={url}
                onInput={(event) => setUrl((event.target as HTMLInputElement).value)}
              />
            </label>
          )}
          <div class="grid grid-cols-1 gap-3 sm:grid-cols-2">
            <label class="form-control">
              <span class="label-text text-xs">Source id</span>
              <input
                class="input input-bordered input-sm"
                placeholder="on-call-guide"
                value={sourceId}
                onInput={(event) => setSourceId((event.target as HTMLInputElement).value)}
              />
            </label>
            <label class="form-control">
              <span class="label-text text-xs">
                Title <span class="text-base-content/40">optional</span>
              </span>
              <input
                class="input input-bordered input-sm"
                placeholder="On-call guide"
                value={title}
                onInput={(event) => setTitle((event.target as HTMLInputElement).value)}
              />
            </label>
          </div>
          {mode !== "url" && (
            <label class="form-control">
              <span class="label-text text-xs">Content</span>
              <textarea
                class="textarea textarea-bordered min-h-48"
                placeholder={
                  mode === "file"
                    ? "File content will appear here..."
                    : "Paste documentation, policies, or runbooks here..."
                }
                value={text}
                onInput={(event) => setText((event.target as HTMLTextAreaElement).value)}
              />
            </label>
          )}
          <div class="flex justify-end">
            <button
              class="btn btn-primary"
              type="submit"
              disabled={!sourceId.trim() || (mode === "url" ? !url.trim() : !text.trim()) || busy}
            >
              {busy ? "Indexing..." : mode === "url" ? "Import and index" : "Add source"}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}

function SourcesPanel(props: {
  baseId: string;
  namespace?: string;
  sources: KnowledgeBaseSourceDto[];
  chunks: KnowledgeBaseChunkDto[];
  loading: boolean;
  onInspect: (sourceId: string) => Promise<void>;
  onRemove: (sourceId: string) => Promise<void>;
}) {
  const { sources, chunks, loading, onInspect, onRemove } = props;
  return (
    <div class="pt-5">
      <div class="border border-base-300 bg-base-100">
        <div class="border-b border-base-300 px-4 py-3">
          <h3 class="font-semibold">Indexed sources</h3>
          <p class="mt-1 text-xs text-base-content/55">
            Manage documents and inspect the chunks generated for retrieval.
          </p>
        </div>
        {loading ? (
          <div class="p-4">
            <SkeletonRows rows={3} cols={1} />
          </div>
        ) : sources.length === 0 ? (
          <div class="p-10 text-center text-sm text-base-content/50">
            No sources yet. Use Add sources to index your first document.
          </div>
        ) : (
          <div class="divide-y divide-base-300">
            {sources.map((source) => (
              <div
                key={source.id}
                class="flex flex-col gap-3 px-4 py-3 sm:flex-row sm:items-center sm:justify-between"
              >
                <div class="min-w-0">
                  <div class="truncate font-mono text-sm">{source.id}</div>
                  <div class="mt-1 text-xs text-base-content/55">
                    {source.title ?? source.uri ?? "Untitled source"} · {source.chunkCount} chunks
                  </div>
                  {source.status === "failed" && (
                    <div class="mt-1 text-xs text-error">{source.error}</div>
                  )}
                </div>
                <div class="flex shrink-0 gap-2">
                  <button
                    class="btn btn-xs btn-ghost"
                    type="button"
                    onClick={() => void onInspect(source.id)}
                  >
                    Inspect chunks
                  </button>
                  <button
                    class="btn btn-xs btn-ghost text-error"
                    type="button"
                    onClick={() => void onRemove(source.id)}
                  >
                    Remove
                  </button>
                </div>
              </div>
            ))}
          </div>
        )}
      </div>
      {chunks.length > 0 && (
        <div class="mt-4 space-y-2">
          <div class="text-xs font-medium uppercase tracking-[0.12em] text-base-content/45">
            Chunk inspection
          </div>
          {chunks.map((chunk) => (
            <article key={chunk.id} class="border border-base-300 bg-base-100 p-3">
              <div class="flex justify-between gap-2 font-mono text-[11px] text-base-content/45">
                <span>{chunk.id}</span>
                <span>chunk {chunk.index}</span>
              </div>
              <p class="mt-2 whitespace-pre-wrap text-sm leading-relaxed">{chunk.text}</p>
            </article>
          ))}
        </div>
      )}
    </div>
  );
}

function ResultCard({ result }: { result: KnowledgeBaseSearchResultDto }) {
  return (
    <article class="border border-base-300 bg-base-100 p-4">
      <div class="flex items-start justify-between gap-3">
        <div class="min-w-0">
          <div class="truncate text-sm font-semibold">
            {result.source.title ?? result.source.id}
          </div>
          <div class="truncate font-mono text-[11px] text-base-content/45">
            {result.source.uri ?? result.source.id}
          </div>
        </div>
        <span class="badge badge-sm badge-info shrink-0">{result.score.toFixed(3)}</span>
      </div>
      <p class="mt-3 whitespace-pre-wrap text-sm leading-relaxed">{result.text}</p>
      <div class="mt-3 flex flex-wrap gap-1">
        {result.source.tags.map((tag) => (
          <span key={tag} class="badge badge-xs badge-outline">
            {tag}
          </span>
        ))}
        <span class="badge badge-xs badge-ghost">chunk {result.index}</span>
      </div>
    </article>
  );
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
