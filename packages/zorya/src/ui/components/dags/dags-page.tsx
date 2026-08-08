// ---------------------------------------------------------------------------
// DagsPage — registry browser + run trigger for AgenticDagRecipe.
//
// Lists registered DAGs (latest-per-id). Click → detail panel showing
// the graph structure (indented adjacency view), version history, and
// a Run button that POSTs to /api/dags/:id/run.
//
// Live graph canvas with running-node highlight is a future slice; v0
// gives the operator the trigger surface + post-run result drill-down.
// ---------------------------------------------------------------------------

import { useEffect, useMemo, useState } from "preact/hooks";
import { useFetch } from "../../hooks/use-fetch.ts";
import { api, type DagDto, type DagRunResultDto } from "../../api/client.ts";
import { Page, PageHeader } from "../ui/page.tsx";

export function DagsPage() {
  const { data, loading, error, refresh } = useFetch(() => api.listDags(), [], 0);
  const dags = data?.dags ?? [];
  const [selectedId, setSelectedId] = useState<string | null>(null);

  // Auto-select first DAG once data loads (mirrors the runs page pattern).
  useEffect(() => {
    if (selectedId === null && dags.length > 0) setSelectedId(dags[0]!.id);
  }, [dags, selectedId]);

  if (error) {
    return (
      <Page space="none">
        <div class="alert alert-error">{error.message}</div>
      </Page>
    );
  }

  return (
    <Page>
      <PageHeader
        title="DAGs"
        eyebrow="Agent runtime"
        description={
          loading && !data
            ? "Loading…"
            : dags.length === 0
              ? "No DAGs registered. Wire ZoryaDags into your server config + register graphs via POST /api/dags."
              : `${dags.length} registered`
        }
        actions={
          <button class="btn btn-sm btn-ghost gap-1" onClick={() => refresh()}>
            <span>↻</span> Refresh
          </button>
        }
      />

      <div class="grid grid-cols-1 md:grid-cols-[280px_1fr] gap-4 min-h-[60vh]">
        <aside class="card bg-base-100 shadow overflow-hidden">
          {loading && !data ? (
            <div class="p-4 text-xs text-base-content/40 text-center">Loading…</div>
          ) : dags.length === 0 ? (
            <div class="p-4 text-xs text-base-content/40 text-center">No DAGs yet.</div>
          ) : (
            <ul class="divide-y divide-base-300">
              {dags.map((d) => (
                <li>
                  <button
                    class={`w-full text-left p-3 hover:bg-base-200 ${
                      selectedId === d.id ? "bg-base-200" : ""
                    }`}
                    onClick={() => setSelectedId(d.id)}
                  >
                    <div class="font-mono text-sm">{d.id}</div>
                    <div class="text-[10px] text-base-content/50 mt-0.5">
                      {d.version} · {d.nodes.length} nodes · {d.edges.length} edges
                    </div>
                    {d.metadata?.description && (
                      <div class="text-[11px] text-base-content/60 mt-1 line-clamp-2">
                        {d.metadata.description}
                      </div>
                    )}
                  </button>
                </li>
              ))}
            </ul>
          )}
        </aside>

        {selectedId ? (
          <DagDetail dag={dags.find((d) => d.id === selectedId) ?? null} />
        ) : (
          <div class="card bg-base-100 shadow flex items-center justify-center text-sm text-base-content/40 p-8">
            Select a DAG from the left.
          </div>
        )}
      </div>
    </Page>
  );
}

function DagDetail({ dag }: { dag: DagDto | null }) {
  const [running, setRunning] = useState(false);
  const [topic, setTopic] = useState("what makes a good engineering team");
  const [extraJson, setExtraJson] = useState("");
  const [result, setResult] = useState<DagRunResultDto | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [showAdvanced, setShowAdvanced] = useState(false);

  if (!dag) {
    return <div class="p-8 text-center text-base-content/40">Loading…</div>;
  }

  const trigger = async () => {
    setRunning(true);
    setError(null);
    setResult(null);
    try {
      let initialInput: Record<string, unknown> = { topic };
      if (showAdvanced && extraJson.trim()) {
        const parsed = JSON.parse(extraJson) as Record<string, unknown>;
        initialInput = { ...initialInput, ...parsed };
      }
      const res = await api.runDag(dag.id, { initialInput });
      setResult(res);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setRunning(false);
    }
  };

  return (
    <div class="space-y-4">
      <div class="card bg-base-100 shadow p-4 space-y-2">
        <div class="flex items-start justify-between gap-2">
          <div class="min-w-0">
            <div class="text-xs text-base-content/50 uppercase tracking-wider">DAG</div>
            <div class="font-mono text-sm">
              {dag.id}@{dag.version}
            </div>
            {dag.metadata?.description && (
              <div class="text-xs text-base-content/60 mt-1">{dag.metadata.description}</div>
            )}
            {dag.metadata?.tags && dag.metadata.tags.length > 0 && (
              <div class="flex gap-1 mt-2">
                {dag.metadata.tags.map((t) => (
                  <span class="badge badge-xs badge-outline">{t}</span>
                ))}
              </div>
            )}
          </div>
          <div class="text-[10px] text-base-content/40 font-mono text-right">
            entry: {dag.entry.join(",")}
            <br />
            terminals: {dag.terminals.join(",")}
          </div>
        </div>
      </div>

      <div class="card bg-base-100 shadow p-4 space-y-3">
        <div class="text-xs uppercase tracking-wider text-base-content/60">Run</div>
        <label class="form-control">
          <span class="text-[10px] text-base-content/50 mb-1">Topic (initial input)</span>
          <input
            class="input input-bordered input-sm"
            value={topic}
            onInput={(e) => setTopic((e.target as HTMLInputElement).value)}
          />
        </label>
        <button
          type="button"
          class="text-[10px] text-base-content/50 uppercase tracking-wider hover:text-base-content"
          onClick={() => setShowAdvanced((v) => !v)}
        >
          {showAdvanced ? "▼" : "▶"} Advanced
        </button>
        {showAdvanced && (
          <label class="form-control">
            <span class="text-[10px] text-base-content/50 mb-1">
              Extra initialInput (JSON, merged with topic)
            </span>
            <textarea
              class="textarea textarea-bordered font-mono text-xs"
              rows={4}
              placeholder='{ "depth": "deep" }'
              value={extraJson}
              onInput={(e) => setExtraJson((e.target as HTMLTextAreaElement).value)}
            />
          </label>
        )}
        <button class="btn btn-sm btn-primary self-start" disabled={running} onClick={trigger}>
          {running ? "Running…" : "▶ Run"}
        </button>
      </div>

      {error && <div class="alert alert-error text-xs">{error}</div>}

      {result && <RunResult result={result} dag={dag} />}

      <DagStructure dag={dag} />
    </div>
  );
}

function RunResult({ result, dag }: { result: DagRunResultDto; dag: DagDto }) {
  const r = result.result;
  return (
    <div class="card bg-base-100 shadow p-4 space-y-2">
      <div class="flex items-center justify-between">
        <div class="text-xs uppercase tracking-wider text-base-content/60">Result</div>
        <div class="flex items-center gap-2">
          {r.ok ? (
            <span class="badge badge-sm badge-success">ok</span>
          ) : (
            <span class="badge badge-sm badge-error">failed</span>
          )}
          <span class="badge badge-sm badge-outline font-mono text-[10px]">
            {result.workflowId}
          </span>
        </div>
      </div>
      <div class="text-[10px] text-base-content/50 font-mono">
        completed: {Object.keys(r.nodeOutputs).length} / {dag.nodes.length}
        {r.skipped.length > 0 && ` · skipped: ${r.skipped.join(", ")}`}
        {Object.keys(r.errors).length > 0 && ` · errors: ${Object.keys(r.errors).length}`}
      </div>
      {dag.nodes.map((n) => {
        const out = r.nodeOutputs[n.id];
        const err = r.errors[n.id];
        const skipped = r.skipped.includes(n.id);
        return (
          <details class="border border-base-300 rounded">
            <summary class="cursor-pointer px-2 py-1 text-xs hover:bg-base-200 flex items-center gap-2">
              {err && <span class="badge badge-xs badge-error">failed</span>}
              {skipped && <span class="badge badge-xs badge-warning">skipped</span>}
              {!err && !skipped && out !== undefined && (
                <span class="badge badge-xs badge-success">ok</span>
              )}
              <span class="font-mono">{n.id}</span>
              <span class="text-[10px] text-base-content/40">→ {n.agentId}</span>
            </summary>
            <pre class="bg-base-200 p-2 text-[11px] font-mono whitespace-pre-wrap break-words">
              {err
                ? `Error: ${err}`
                : skipped
                  ? "(skipped — upstream dependency unsatisfied)"
                  : typeof out === "string"
                    ? out
                    : JSON.stringify(out, null, 2)}
            </pre>
          </details>
        );
      })}
    </div>
  );
}

function DagStructure({ dag }: { dag: DagDto }) {
  const adjacency = useMemo(() => {
    const m = new Map<string, string[]>();
    for (const n of dag.nodes) m.set(n.id, []);
    for (const e of dag.edges) m.get(e.from)?.push(e.to);
    return m;
  }, [dag]);

  return (
    <div class="card bg-base-100 shadow p-4 space-y-2">
      <div class="text-xs uppercase tracking-wider text-base-content/60">Structure</div>
      <ul class="space-y-1 text-xs font-mono">
        {dag.nodes.map((n) => (
          <li>
            <span class={dag.entry.includes(n.id) ? "text-info" : ""}>
              {dag.entry.includes(n.id) ? "▶" : "·"}
            </span>{" "}
            <span class="font-semibold">{n.id}</span>
            <span class="text-base-content/40"> ({n.agentId})</span>
            {dag.terminals.includes(n.id) && (
              <span class="ml-2 badge badge-xs badge-outline">terminal</span>
            )}
            {(adjacency.get(n.id) ?? []).length > 0 && (
              <span class="text-base-content/50">
                {" → "}
                {(adjacency.get(n.id) ?? []).join(", ")}
              </span>
            )}
          </li>
        ))}
      </ul>
    </div>
  );
}
