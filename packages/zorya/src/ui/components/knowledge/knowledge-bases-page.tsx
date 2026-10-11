// ---------------------------------------------------------------------------
// KnowledgeBasesPage — operator UI for managed knowledge bases. Pick a base
// on the left; on the right, test retrieval the way an agent's search tool
// runs it, add sources (paste text, upload a text file, or import a URL when
// the server registers the `url` adapter), and inspect each source's chunks.
// A source whose indexing failed is listed with the reason and can be
// re-indexed from its stored text. Agents bind a base by id in the agent
// editor's Knowledge / RAG section.
// ---------------------------------------------------------------------------

import { useEffect, useMemo, useState } from "preact/hooks";
import type { ComponentChildren } from "preact";
import { api, ApiError } from "../../api/client.ts";
import type {
  KnowledgeBase,
  KnowledgeChunksResponse,
  KnowledgeSearchResultDto,
  KnowledgeSourceSummary,
} from "../../../server/routes/knowledge-bases.ts";
import { useFetch } from "../../hooks/use-fetch.ts";
import { confirm, toast } from "../../lib/dialogs.ts";
import { Page } from "../ui/page.tsx";
import { SkeletonRows } from "../ui/skeleton.tsx";

type WorkspaceTab = "search" | "add" | "sources";
type AddMode = "paste" | "file" | "url";

/** Uploads go through the JSON API, whose body cap defaults to 1 MiB. */
const MAX_UPLOAD_BYTES = 900_000;

export function KnowledgeBasesPage() {
  const { data, loading, error, refresh } = useFetch(() => api.listKnowledgeBases(), [], 0);
  const { data: adapters } = useFetch(() => api.listKnowledgeSourceAdapters(), [], 0);
  const bases = useMemo(() => data?.knowledgeBases ?? [], [data]);
  const [selectedId, setSelectedId] = useState("");
  const [tab, setTab] = useState<WorkspaceTab>("search");
  const [showCreate, setShowCreate] = useState(false);

  useEffect(() => {
    if (bases.length === 0) return;
    if (!bases.some((base) => base.id === selectedId)) setSelectedId(bases[0]!.id);
  }, [bases, selectedId]);

  const selected = bases.find((base) => base.id === selectedId);

  const onDelete = async (id: string) => {
    if (
      !(await confirm({
        title: `Delete knowledge base "${id}"`,
        message: "This removes the base, every source and every indexed chunk.",
        confirmLabel: "Delete",
        variant: "danger",
      }))
    ) {
      return;
    }
    try {
      await api.deleteKnowledgeBase(id);
    } catch (err) {
      if (!(err instanceof ApiError && err.code === "knowledge_base_in_use")) {
        toast(errorMessage(err), { variant: "error" });
        return;
      }
      const force = await confirm({
        title: "Agents still use this knowledge base",
        message: `${err.detail ?? ""} Agents that bind it fail to resolve until the binding is removed. Delete anyway?`,
        confirmLabel: "Delete anyway",
        variant: "danger",
      });
      if (!force) return;
      try {
        await api.deleteKnowledgeBase(id, { force: true });
      } catch (forced) {
        toast(errorMessage(forced), { variant: "error" });
        return;
      }
    }
    toast(`Deleted knowledge base "${id}"`, { variant: "success" });
    setSelectedId("");
    refresh();
  };

  return (
    <Page>
      <div class="flex items-start justify-between gap-4">
        <div>
          <h2 class="text-xl font-semibold">Knowledge bases</h2>
          <p class="text-xs text-base-content/50 max-w-2xl">
            Named document collections agents search. Add sources here, test retrieval, then bind a
            base to an agent in its editor's Knowledge / RAG section — the agent gets a search tool
            or injected context backed by it.
          </p>
        </div>
        <div class="flex gap-2">
          <button type="button" class="btn btn-sm btn-ghost" onClick={refresh} title="Refresh">
            ↻
          </button>
          <button type="button" class="btn btn-sm btn-primary" onClick={() => setShowCreate(true)}>
            New knowledge base
          </button>
        </div>
      </div>

      {error && <div class="alert alert-error text-xs">{error.message}</div>}

      {showCreate && (
        <CreateKnowledgeBaseForm
          existingIds={bases.map((b) => b.id)}
          onCancel={() => setShowCreate(false)}
          onCreated={(base) => {
            setShowCreate(false);
            setSelectedId(base.id);
            setTab("add");
            refresh();
          }}
        />
      )}

      <div class="grid grid-cols-1 items-start gap-4 lg:grid-cols-[16rem_minmax(0,1fr)]">
        <aside class="card bg-base-100 shadow overflow-hidden">
          <div class="flex items-center justify-between border-b border-base-300 px-4 py-2">
            <span class="text-xs uppercase tracking-wider text-base-content/50">Bases</span>
            <span class="badge badge-sm badge-ghost">{bases.length}</span>
          </div>
          {loading && !data ? (
            <table class="table table-sm">
              <tbody>
                <SkeletonRows rows={3} cols={1} />
              </tbody>
            </table>
          ) : bases.length === 0 ? (
            <div class="p-4 text-xs text-base-content/50">
              No knowledge bases yet. Create one, then add its first source.
            </div>
          ) : (
            <ul class="menu menu-sm p-2">
              {bases.map((base) => (
                <li key={base.id}>
                  <button
                    type="button"
                    class={base.id === selectedId ? "active" : ""}
                    onClick={() => {
                      setSelectedId(base.id);
                      setTab("search");
                    }}
                  >
                    <div class="min-w-0 text-left">
                      <div class="truncate font-mono text-xs">{base.id}</div>
                      <div class="text-[11px] opacity-60">
                        {base.sourceCount} sources · {base.chunkCount} chunks
                        {base.status === "degraded" && (
                          <span class="text-warning"> · degraded</span>
                        )}
                      </div>
                    </div>
                  </button>
                </li>
              ))}
            </ul>
          )}
        </aside>

        {selected ? (
          <KnowledgeWorkspace
            key={selected.id}
            base={selected}
            tab={tab}
            setTab={setTab}
            canImportUrl={adapters?.kinds.includes("url") ?? false}
            onChanged={refresh}
            onDelete={() => onDelete(selected.id)}
          />
        ) : (
          <div class="card bg-base-100 shadow p-10 text-center">
            <div class="font-semibold">No knowledge base selected</div>
            <p class="mx-auto mt-2 max-w-md text-xs text-base-content/50">
              Create a knowledge base, add sources by pasting text, uploading a file or importing a
              URL, and search it here to see what agents will retrieve.
            </p>
            <div>
              <button
                type="button"
                class="btn btn-sm btn-primary mt-4"
                onClick={() => setShowCreate(true)}
              >
                Create knowledge base
              </button>
            </div>
          </div>
        )}
      </div>
    </Page>
  );
}

function CreateKnowledgeBaseForm(props: {
  existingIds: ReadonlyArray<string>;
  onCancel: () => void;
  onCreated: (base: KnowledgeBase) => void;
}) {
  const [id, setId] = useState("");
  const [description, setDescription] = useState("");
  const [tags, setTags] = useState("");
  const [saving, setSaving] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const clash = props.existingIds.includes(id.trim());

  const submit = async (event: Event) => {
    event.preventDefault();
    setErr(null);
    setSaving(true);
    try {
      const base = await api.createKnowledgeBase({
        id: id.trim(),
        ...(description.trim() && { description: description.trim() }),
        ...(splitTags(tags).length > 0 && { tags: splitTags(tags) }),
      });
      toast(`Created knowledge base "${base.id}"`, { variant: "success" });
      props.onCreated(base);
    } catch (e) {
      setErr(errorMessage(e));
    } finally {
      setSaving(false);
    }
  };

  return (
    <form class="card bg-base-100 shadow p-4 space-y-3" onSubmit={submit}>
      <div>
        <h3 class="font-semibold">New knowledge base</h3>
        <p class="text-xs text-base-content/50">
          The id is what agents bind in backend.knowledge. Add sources on the next screen.
        </p>
      </div>
      <div class="grid grid-cols-1 gap-3 md:grid-cols-[1fr_2fr_1fr]">
        <label class="form-control">
          <span class="text-xs text-base-content/60 mb-1 uppercase tracking-wider">Id</span>
          <input
            class="input input-bordered input-sm font-mono"
            placeholder="product-docs"
            value={id}
            onInput={(e) => setId((e.target as HTMLInputElement).value)}
          />
          {clash && <span class="text-xs text-error mt-1">This id is taken.</span>}
        </label>
        <label class="form-control">
          <span class="text-xs text-base-content/60 mb-1 uppercase tracking-wider">
            Description
          </span>
          <input
            class="input input-bordered input-sm"
            placeholder="Support and product reference"
            value={description}
            onInput={(e) => setDescription((e.target as HTMLInputElement).value)}
          />
        </label>
        <label class="form-control">
          <span class="text-xs text-base-content/60 mb-1 uppercase tracking-wider">Tags</span>
          <input
            class="input input-bordered input-sm"
            placeholder="public, product"
            value={tags}
            onInput={(e) => setTags((e.target as HTMLInputElement).value)}
          />
        </label>
      </div>
      {err && <div class="alert alert-error text-xs">{err}</div>}
      <div class="flex justify-end gap-2">
        <button type="button" class="btn btn-sm btn-ghost" onClick={props.onCancel}>
          Cancel
        </button>
        <button
          type="submit"
          class="btn btn-sm btn-primary"
          disabled={!id.trim() || clash || saving}
        >
          {saving ? "Creating…" : "Create and add sources"}
        </button>
      </div>
    </form>
  );
}

function KnowledgeWorkspace(props: {
  base: KnowledgeBase;
  tab: WorkspaceTab;
  setTab: (tab: WorkspaceTab) => void;
  canImportUrl: boolean;
  onChanged: () => void;
  onDelete: () => void;
}) {
  const { base, tab, setTab } = props;
  const sources = useFetch(() => api.listKnowledgeSources(base.id), [base.id, base.updatedAt], 0);
  const sourceList = sources.data?.sources ?? [];
  const failed = sourceList.filter((s) => s.status === "failed").length;

  const changed = () => {
    sources.refresh();
    props.onChanged();
  };

  return (
    <section class="min-w-0 space-y-4">
      <div class="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
        <div class="min-w-0">
          <div class="flex flex-wrap items-center gap-2">
            <h3 class="truncate font-mono text-lg font-semibold">{base.id}</h3>
            <span
              class={`badge badge-sm ${base.status === "degraded" ? "badge-warning" : "badge-success"}`}
            >
              {base.status}
            </span>
            {base.tags.map((tag) => (
              <span key={tag} class="badge badge-sm badge-outline">
                {tag}
              </span>
            ))}
          </div>
          <p class="mt-1 text-xs text-base-content/50">{base.description ?? "No description"}</p>
          <p class="mt-1 font-mono text-[11px] text-base-content/40">
            backend.knowledge: [{"{"} id: "{base.id}" {"}"}]
          </p>
        </div>
        <button type="button" class="btn btn-sm btn-ghost text-error" onClick={props.onDelete}>
          Delete
        </button>
      </div>

      {failed > 0 && (
        <div class="alert alert-warning text-xs">
          {failed} source{failed === 1 ? "" : "s"} failed to index and {failed === 1 ? "is" : "are"}{" "}
          not searchable. See Sources for the reason; re-index once the cause is fixed.
        </div>
      )}

      <div role="tablist" class="tabs tabs-bordered">
        <TabButton active={tab === "search"} onClick={() => setTab("search")}>
          Search
        </TabButton>
        <TabButton active={tab === "add"} onClick={() => setTab("add")}>
          Add sources
        </TabButton>
        <TabButton active={tab === "sources"} onClick={() => setTab("sources")}>
          Sources <span class="badge badge-xs badge-ghost ml-1">{sourceList.length}</span>
        </TabButton>
      </div>

      {tab === "search" && <SearchPanel baseId={base.id} />}
      {tab === "add" && (
        <AddSourcePanel
          baseId={base.id}
          canImportUrl={props.canImportUrl}
          onAdded={() => {
            changed();
            setTab("sources");
          }}
          onFailed={changed}
        />
      )}
      {tab === "sources" && (
        <SourcesPanel
          baseId={base.id}
          sources={sourceList}
          loading={sources.loading && !sources.data}
          error={sources.error?.message}
          onChanged={changed}
        />
      )}
    </section>
  );
}

function TabButton(props: { active: boolean; onClick: () => void; children: ComponentChildren }) {
  return (
    <button
      type="button"
      role="tab"
      aria-selected={props.active}
      class={`tab ${props.active ? "tab-active" : ""}`}
      onClick={props.onClick}
    >
      {props.children}
    </button>
  );
}

function SearchPanel({ baseId }: { baseId: string }) {
  const [query, setQuery] = useState("");
  const [topK, setTopK] = useState(8);
  const [submitted, setSubmitted] = useState("");
  const [results, setResults] = useState<KnowledgeSearchResultDto[]>([]);
  const [searching, setSearching] = useState(false);
  const [err, setErr] = useState<string | null>(null);

  const search = async (event: Event) => {
    event.preventDefault();
    const q = query.trim();
    if (!q) return;
    setSearching(true);
    setErr(null);
    setSubmitted(q);
    try {
      setResults((await api.searchKnowledgeBase(baseId, { query: q, topK })).results);
    } catch (e) {
      setResults([]);
      setErr(errorMessage(e));
    } finally {
      setSearching(false);
    }
  };

  return (
    <div class="space-y-4">
      <form class="card bg-base-100 shadow p-4 space-y-2" onSubmit={search}>
        <p class="text-xs text-base-content/50">
          Runs the query an agent's search tool would and shows the chunks it would get back.
        </p>
        <div class="flex flex-col gap-2 sm:flex-row">
          <label class="sr-only" for="knowledge-search">
            Search the knowledge base
          </label>
          <input
            id="knowledge-search"
            class="input input-bordered input-sm flex-1"
            placeholder="Ask about your sources…"
            value={query}
            onInput={(e) => setQuery((e.target as HTMLInputElement).value)}
          />
          <label class="flex items-center gap-1 text-xs text-base-content/60">
            top
            <input
              type="number"
              min={1}
              max={50}
              class="input input-bordered input-sm w-16"
              value={topK}
              onInput={(e) =>
                setTopK(
                  Math.min(
                    50,
                    Math.max(1, Math.round(Number((e.target as HTMLInputElement).value) || 8)),
                  ),
                )
              }
            />
          </label>
          <button
            type="submit"
            class="btn btn-sm btn-primary"
            disabled={!query.trim() || searching}
          >
            {searching ? "Searching…" : "Search"}
          </button>
        </div>
      </form>
      {err && <div class="alert alert-error text-xs">{err}</div>}
      {searching ? (
        <table class="table table-sm">
          <tbody>
            <SkeletonRows rows={3} cols={1} />
          </tbody>
        </table>
      ) : results.length > 0 ? (
        <div class="space-y-3">
          {results.map((result) => (
            <ResultCard key={result.id} result={result} />
          ))}
        </div>
      ) : (
        <div class="text-center text-xs text-base-content/40 py-6">
          {submitted ? `No chunks match "${submitted}".` : "Search to see retrieved chunks."}
        </div>
      )}
    </div>
  );
}

function ResultCard({ result }: { result: KnowledgeSearchResultDto }) {
  return (
    <article class="card bg-base-100 shadow p-4">
      <div class="flex items-start justify-between gap-3">
        <div class="min-w-0">
          <div class="truncate text-sm font-semibold">
            {result.source.title ?? result.source.id}
          </div>
          <div class="truncate font-mono text-[11px] text-base-content/40">
            {result.source.uri ?? result.source.id} · chunk {result.index}
          </div>
        </div>
        <span class="badge badge-sm badge-info shrink-0" title="Relevance score">
          {result.score.toFixed(3)}
        </span>
      </div>
      <p class="mt-2 whitespace-pre-wrap text-xs leading-relaxed">{result.text}</p>
      {result.source.tags.length > 0 && (
        <div class="mt-2 flex flex-wrap gap-1">
          {result.source.tags.map((tag) => (
            <span key={tag} class="badge badge-xs badge-outline">
              {tag}
            </span>
          ))}
        </div>
      )}
    </article>
  );
}

function AddSourcePanel(props: {
  baseId: string;
  canImportUrl: boolean;
  onAdded: () => void;
  onFailed: () => void;
}) {
  const [mode, setMode] = useState<AddMode>("paste");
  const [sourceId, setSourceId] = useState("");
  const [title, setTitle] = useState("");
  const [tags, setTags] = useState("");
  const [text, setText] = useState("");
  const [fileName, setFileName] = useState("");
  const [url, setUrl] = useState("");
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);

  const modes: Array<{ id: AddMode; label: string }> = [
    { id: "paste", label: "Paste text" },
    { id: "file", label: "Upload file" },
    ...(props.canImportUrl ? [{ id: "url" as const, label: "Import URL" }] : []),
  ];
  const ready = mode === "url" ? url.trim().length > 0 : sourceId.trim() && text.trim();

  const submit = async (event: Event) => {
    event.preventDefault();
    setErr(null);
    setBusy(true);
    const tagList = splitTags(tags);
    try {
      if (mode === "url") {
        const { sources } = await api.importKnowledgeSources(props.baseId, {
          kind: "url",
          config: {
            url: url.trim(),
            ...(sourceId.trim() && { id: sourceId.trim() }),
            ...(title.trim() && { title: title.trim() }),
            ...(tagList.length > 0 && { tags: tagList }),
          },
        });
        toast(`Imported ${sources.map((s) => s.id).join(", ")}`, { variant: "success" });
      } else {
        const source = await api.ingestKnowledgeSource(props.baseId, {
          id: sourceId.trim(),
          text,
          ...(title.trim() && { title: title.trim() }),
          ...(fileName && mode === "file" && { uri: fileName }),
          ...(tagList.length > 0 && { tags: tagList }),
        });
        toast(`Indexed "${source.id}" into ${source.chunkCount} chunks`, { variant: "success" });
      }
      setSourceId("");
      setTitle("");
      setTags("");
      setText("");
      setFileName("");
      setUrl("");
      props.onAdded();
    } catch (e) {
      setErr(errorMessage(e));
      // A failed ingestion is still recorded as a failed source.
      if (e instanceof ApiError && e.code === "ingest_failed") props.onFailed();
    } finally {
      setBusy(false);
    }
  };

  const readFile = (event: Event) => {
    const file = (event.target as HTMLInputElement).files?.[0];
    if (!file) return;
    setErr(null);
    if (file.size > MAX_UPLOAD_BYTES) {
      setErr(
        `${file.name} is ${Math.round(file.size / 1024)} KB; uploads are limited to about ${Math.round(MAX_UPLOAD_BYTES / 1024)} KB.`,
      );
      return;
    }
    setFileName(file.name);
    if (!sourceId) setSourceId(file.name.replace(/\.[^.]+$/, "").replace(/[^A-Za-z0-9._-]+/g, "-"));
    if (!title) setTitle(file.name);
    const reader = new FileReader();
    reader.onload = () => setText(typeof reader.result === "string" ? reader.result : "");
    reader.onerror = () => setErr("Could not read this file.");
    reader.readAsText(file);
  };

  return (
    <form class="card bg-base-100 shadow p-4 space-y-3" onSubmit={submit}>
      <div class="flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
        <p class="text-xs text-base-content/50">
          Sources are chunked and indexed on submit. Re-using a source id replaces that source.
        </p>
        <div class="join" role="tablist" aria-label="Source type">
          {modes.map((m) => (
            <button
              key={m.id}
              type="button"
              role="tab"
              aria-selected={mode === m.id}
              class={`join-item btn btn-xs ${mode === m.id ? "btn-primary" : "btn-ghost"}`}
              onClick={() => setMode(m.id)}
            >
              {m.label}
            </button>
          ))}
        </div>
      </div>

      {mode === "file" && (
        <label class="flex cursor-pointer items-center justify-between rounded border border-dashed border-base-300 px-3 py-3 hover:border-primary/50">
          <span class="text-xs">{fileName || "Choose a text, markdown or CSV file"}</span>
          <span class="btn btn-xs btn-ghost">Browse</span>
          <input
            class="hidden"
            type="file"
            accept=".txt,.md,.markdown,.csv,.json,text/plain,text/markdown,text/csv,application/json"
            onChange={readFile}
          />
        </label>
      )}
      {mode === "url" && (
        <label class="form-control">
          <span class="text-xs text-base-content/60 mb-1 uppercase tracking-wider">URL</span>
          <input
            class="input input-bordered input-sm"
            type="url"
            placeholder="https://docs.example.com/guide"
            value={url}
            onInput={(e) => setUrl((e.target as HTMLInputElement).value)}
          />
        </label>
      )}

      <div class="grid grid-cols-1 gap-3 sm:grid-cols-3">
        <Field label={mode === "url" ? "Source id (default: the URL)" : "Source id"}>
          <input
            class="input input-bordered input-sm font-mono"
            placeholder="on-call-guide"
            value={sourceId}
            onInput={(e) => setSourceId((e.target as HTMLInputElement).value)}
          />
        </Field>
        <Field label="Title">
          <input
            class="input input-bordered input-sm"
            placeholder="On-call guide"
            value={title}
            onInput={(e) => setTitle((e.target as HTMLInputElement).value)}
          />
        </Field>
        <Field label="Tags">
          <input
            class="input input-bordered input-sm"
            placeholder="ops, oncall"
            value={tags}
            onInput={(e) => setTags((e.target as HTMLInputElement).value)}
          />
        </Field>
      </div>

      {mode !== "url" && (
        <Field label="Content">
          <textarea
            class="textarea textarea-bordered font-mono text-xs min-h-48"
            placeholder={
              mode === "file"
                ? "The file's content appears here…"
                : "Paste documentation, policies or runbooks…"
            }
            value={text}
            onInput={(e) => setText((e.target as HTMLTextAreaElement).value)}
          />
        </Field>
      )}

      {err && <div class="alert alert-error text-xs">{err}</div>}

      <div class="flex justify-end">
        <button type="submit" class="btn btn-sm btn-primary" disabled={!ready || busy}>
          {busy ? "Indexing…" : mode === "url" ? "Import and index" : "Add source"}
        </button>
      </div>
    </form>
  );
}

function Field(props: { label: string; children: ComponentChildren }) {
  return (
    <label class="form-control">
      <span class="text-xs text-base-content/60 mb-1 uppercase tracking-wider">{props.label}</span>
      {props.children}
    </label>
  );
}

const CHUNK_PAGE = 50;

function SourcesPanel(props: {
  baseId: string;
  sources: KnowledgeSourceSummary[];
  loading: boolean;
  error?: string;
  onChanged: () => void;
}) {
  const { baseId, sources } = props;
  const [inspected, setInspected] = useState<{ sourceId: string; page: KnowledgeChunksResponse }>();
  const [busy, setBusy] = useState<string | null>(null);

  const inspect = async (params: { sourceId: string; offset?: number }) => {
    const { sourceId, offset = 0 } = params;
    try {
      const page = await api.listKnowledgeChunks(baseId, { sourceId, limit: CHUNK_PAGE, offset });
      setInspected((prev) =>
        prev && prev.sourceId === sourceId && offset > 0
          ? { sourceId, page: { ...page, chunks: [...prev.page.chunks, ...page.chunks] } }
          : { sourceId, page },
      );
    } catch (e) {
      toast(errorMessage(e), { variant: "error" });
    }
  };

  const reindex = async (sourceId: string) => {
    setBusy(sourceId);
    try {
      const source = await api.reindexKnowledgeSource(baseId, sourceId);
      toast(`Re-indexed "${source.id}" into ${source.chunkCount} chunks`, { variant: "success" });
    } catch (e) {
      toast(errorMessage(e), { variant: "error" });
    } finally {
      setBusy(null);
      props.onChanged();
    }
  };

  const remove = async (sourceId: string) => {
    if (
      !(await confirm({
        title: `Remove source "${sourceId}"`,
        message: "Its chunks are dropped from the index.",
        confirmLabel: "Remove",
        variant: "danger",
      }))
    ) {
      return;
    }
    setBusy(sourceId);
    try {
      await api.deleteKnowledgeSource(baseId, sourceId);
      if (inspected?.sourceId === sourceId) setInspected(undefined);
      toast(`Removed "${sourceId}"`, { variant: "success" });
    } catch (e) {
      toast(errorMessage(e), { variant: "error" });
    } finally {
      setBusy(null);
      props.onChanged();
    }
  };

  return (
    <div class="space-y-4">
      {props.error && <div class="alert alert-error text-xs">{props.error}</div>}
      <div class="card bg-base-100 shadow overflow-hidden">
        <div class="overflow-x-auto">
          <table class="table table-sm">
            <thead>
              <tr class="bg-base-200 text-xs uppercase tracking-wider text-base-content/50">
                <th>Source</th>
                <th>Status</th>
                <th class="text-right">Chunks</th>
                <th class="text-right">Actions</th>
              </tr>
            </thead>
            <tbody>
              {props.loading ? (
                <SkeletonRows rows={3} cols={4} />
              ) : sources.length === 0 ? (
                <tr>
                  <td colSpan={4} class="text-center text-base-content/40 py-6 text-sm">
                    No sources yet. Use Add sources to index the first document.
                  </td>
                </tr>
              ) : (
                sources.map((source) => (
                  <tr key={source.id} class="hover align-top">
                    <td class="max-w-md">
                      <div class="truncate font-mono text-xs" title={source.id}>
                        {source.id}
                      </div>
                      <div class="truncate text-[11px] text-base-content/50">
                        {source.title ?? source.uri ?? "Untitled"}
                      </div>
                    </td>
                    <td>
                      <span
                        class={`badge badge-xs ${source.status === "failed" ? "badge-error" : "badge-success"}`}
                      >
                        {source.status}
                      </span>
                      {source.error && (
                        <div class="mt-1 max-w-xs text-[11px] text-error break-words">
                          {source.error}
                        </div>
                      )}
                    </td>
                    <td class="text-right text-xs">{source.chunkCount}</td>
                    <td class="text-right whitespace-nowrap">
                      {source.status === "ready" ? (
                        <button
                          type="button"
                          class="btn btn-xs btn-ghost"
                          onClick={() => void inspect({ sourceId: source.id })}
                        >
                          Chunks
                        </button>
                      ) : (
                        <button
                          type="button"
                          class="btn btn-xs btn-ghost"
                          disabled={busy === source.id}
                          onClick={() => void reindex(source.id)}
                        >
                          {busy === source.id ? "Indexing…" : "Re-index"}
                        </button>
                      )}
                      <button
                        type="button"
                        class="btn btn-xs btn-ghost text-error"
                        disabled={busy === source.id}
                        onClick={() => void remove(source.id)}
                      >
                        Remove
                      </button>
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </div>

      {inspected && (
        <div class="space-y-2">
          <div class="flex items-center justify-between">
            <span class="text-xs uppercase tracking-wider text-base-content/50">
              Chunks of {inspected.sourceId} ({inspected.page.chunks.length} of{" "}
              {inspected.page.total})
            </span>
            <button
              type="button"
              class="btn btn-xs btn-ghost"
              onClick={() => setInspected(undefined)}
            >
              Close
            </button>
          </div>
          {inspected.page.chunks.map((chunk) => (
            <article key={chunk.id} class="card bg-base-100 shadow p-3">
              <div class="flex justify-between gap-2 font-mono text-[11px] text-base-content/40">
                <span>{chunk.id}</span>
                <span>
                  chunk {chunk.index} · {chunk.text.length} chars
                </span>
              </div>
              <p class="mt-2 whitespace-pre-wrap text-xs leading-relaxed">{chunk.text}</p>
            </article>
          ))}
          {inspected.page.chunks.length < inspected.page.total && (
            <button
              type="button"
              class="btn btn-xs btn-ghost"
              onClick={() =>
                void inspect({
                  sourceId: inspected.sourceId,
                  offset: inspected.page.chunks.length,
                })
              }
            >
              Load more
            </button>
          )}
        </div>
      )}
    </div>
  );
}

function splitTags(raw: string): string[] {
  return [
    ...new Set(
      raw
        .split(",")
        .map((t) => t.trim())
        .filter(Boolean),
    ),
  ];
}

function errorMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}
