// ---------------------------------------------------------------------------
// EvalsPage — eval suites, runs against agent recipe versions, run
// comparison and promotion gates.
//
// Left: the registered suites. Right, for the selected suite: its versions
// (make one active), a Run form (agent + any of its versions, drafts
// included), the suite's runs (newest first; a running one is polled for
// progress), the selected run's per-case results and scores, and a
// side-by-side comparison of two runs (e.g. the active version vs a
// draft). Below: the promotion gates — "a version of agent X becomes
// active only once this suite scored at least T on it".
// ---------------------------------------------------------------------------

import { useEffect, useMemo, useState } from "preact/hooks";
import {
  api,
  type EvalRunRecord,
  type RegisteredEvalSuite,
  type RunDiff,
  type StoredEvalRun,
} from "../../api/client.ts";
import { useFetch } from "../../hooks/use-fetch.ts";
import { useNamespace } from "../../hooks/use-namespace.ts";
import { formatRelative } from "../../lib/format.ts";
import { Page, PageHeader } from "../ui/page.tsx";
import { VersionsPanel } from "../ui/versions-panel.tsx";

/** How often a running run is re-read. */
const PROGRESS_POLL_MS = 1_000;

export function EvalsPage() {
  const { data, loading, error, refresh } = useFetch(() => api.listEvalSuites(), [], 0);
  const suites = data?.suites ?? [];
  const [selectedId, setSelectedId] = useState<string | null>(null);

  useEffect(() => {
    if (selectedId === null && suites.length > 0) setSelectedId(suites[0]!.id);
  }, [suites, selectedId]);

  if (error) {
    return (
      <Page space="none">
        <div class="alert alert-error">{error.message}</div>
      </Page>
    );
  }

  const selected = suites.find((s) => s.id === selectedId) ?? null;
  return (
    <Page>
      <PageHeader
        title="Evals"
        eyebrow="Agent runtime"
        description={
          loading && !data
            ? "Loading…"
            : suites.length === 0
              ? "No eval suites. Register one with POST /api/evals/suites."
              : "Run a suite against any agent version — drafts included — compare runs, and gate promotion on the results."
        }
        meta={
          suites.length > 0 ? (
            <>
              <span class="font-mono">{suites.length}</span>
              <span>suites</span>
            </>
          ) : undefined
        }
        actions={
          <button class="btn btn-sm btn-ghost gap-1" onClick={() => refresh()}>
            <span aria-hidden="true">↻</span>
            Refresh
          </button>
        }
      />

      <div class="grid grid-cols-1 md:grid-cols-[280px_1fr] gap-4 min-h-[50vh]">
        <aside class="card bg-base-100 shadow overflow-hidden self-start">
          {suites.length === 0 ? (
            <div class="p-4 text-xs text-base-content/40 text-center">No suites yet.</div>
          ) : (
            <ul class="divide-y divide-base-300">
              {suites.map((s) => (
                <li>
                  <button
                    class={`w-full text-left p-3 hover:bg-base-200 ${selectedId === s.id ? "bg-base-200" : ""}`}
                    onClick={() => setSelectedId(s.id)}
                  >
                    <div class="font-mono text-sm">{s.id}</div>
                    <div class="text-[10px] text-base-content/50 mt-0.5">
                      {s.version} · {datasetSize(s)} cases · {s.scorers.length} scorers
                    </div>
                    {s.description && (
                      <div class="text-[11px] text-base-content/60 mt-1 line-clamp-2">
                        {s.description}
                      </div>
                    )}
                  </button>
                </li>
              ))}
            </ul>
          )}
        </aside>

        {selected ? (
          <SuiteDetail key={selected.id} suite={selected} onChanged={refresh} />
        ) : (
          <div class="card bg-base-100 shadow flex items-center justify-center text-sm text-base-content/40 p-8">
            Select a suite from the left.
          </div>
        )}
      </div>

      <GatesPanel suites={suites} />
    </Page>
  );
}

function datasetSize(suite: RegisteredEvalSuite): string {
  return suite.dataset.kind === "inline" ? String(suite.dataset.cases.length) : "stored";
}

function SuiteDetail({ suite, onChanged }: { suite: RegisteredEvalSuite; onChanged: () => void }) {
  const [selectedRun, setSelectedRun] = useState<string | null>(null);
  const [compare, setCompare] = useState<{ baseline: string; candidate: string } | null>(null);
  const [globalNamespace] = useNamespace();
  const namespaceId = globalNamespace || "default";
  const runs = useFetch(
    () => api.listEvalRuns({ suiteId: suite.id, namespaceId, limit: 50 }),
    [suite.id, namespaceId],
    0,
  );
  const anyRunning = (runs.data?.runs ?? []).some((r) => r.status === "running");
  useEffect(() => {
    if (!anyRunning) return;
    const handle = setInterval(() => runs.refresh(), PROGRESS_POLL_MS);
    return () => clearInterval(handle);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [anyRunning]);

  return (
    <div class="space-y-4">
      <div class="card bg-base-100 shadow p-4 space-y-2">
        <div class="text-xs text-base-content/50 uppercase tracking-wider">Suite</div>
        <div class="font-mono text-sm">
          {suite.id}@{suite.version}
        </div>
        {suite.description && <div class="text-xs text-base-content/60">{suite.description}</div>}
        <div class="flex flex-wrap gap-1">
          {suite.scorers.map((s) => (
            <span class="badge badge-sm badge-outline font-mono">
              {s.id ?? s.kind}
              {s.threshold !== undefined ? ` ≥ ${s.threshold}` : ""}
            </span>
          ))}
          {suite.metadata.agentIds.map((a) => (
            <span class="badge badge-sm badge-ghost">for {a}</span>
          ))}
        </div>
      </div>

      <div class="card bg-base-100 shadow p-4">
        <VersionsPanel
          reloadKey={`${suite.id}@${suite.version}`}
          load={() => api.listEvalSuiteVersions({ id: suite.id })}
          promote={(p) => api.promoteEvalSuite({ id: suite.id, ...p })}
          onPromoted={() => onChanged()}
          describe={(v) => `${datasetSize(v)} cases · ${v.scorers.length} scorers`}
        />
      </div>

      <RunForm
        suite={suite}
        onStarted={(runId) => {
          setSelectedRun(runId);
          runs.refresh();
        }}
      />

      <RunsTable
        runs={runs.data?.runs ?? []}
        selected={selectedRun}
        onSelect={setSelectedRun}
        onCompare={setCompare}
      />

      {compare && <CompareView compare={compare} onClose={() => setCompare(null)} />}
      {selectedRun && <RunDetail runId={selectedRun} onChanged={() => runs.refresh()} />}
    </div>
  );
}

function RunForm({
  suite,
  onStarted,
}: {
  suite: RegisteredEvalSuite;
  onStarted: (runId: string) => void;
}) {
  const [globalNamespace] = useNamespace();
  const namespaceId = globalNamespace || "default";
  const agents = useFetch(() => api.listAgents({ limit: 500 }), [], 0);
  const [agentId, setAgentId] = useState<string>(suite.metadata.agentIds[0] ?? "");
  const versions = useFetch(
    () => (agentId ? api.listAgentVersions(agentId) : Promise.resolve({ versions: [] })),
    [agentId],
    0,
  );
  const [agentVersion, setAgentVersion] = useState<string>("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!agentId && agents.data?.agents[0]) setAgentId(agents.data.agents[0].id);
  }, [agents.data, agentId]);
  useEffect(() => setAgentVersion(""), [agentId]);

  const start = async () => {
    setBusy(true);
    setError(null);
    try {
      const started = await api.startEvalRun({
        suiteId: suite.id,
        suiteVersion: suite.version,
        agentId,
        ...(agentVersion && { agentVersion }),
        namespaceId,
      });
      onStarted(started.runId);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  return (
    <div class="card bg-base-100 shadow p-4 space-y-3">
      <div class="text-xs uppercase tracking-wider text-base-content/60">Run</div>
      <div class="grid grid-cols-1 sm:grid-cols-2 gap-2">
        <label class="form-control">
          <span class="text-[10px] text-base-content/50 mb-1">Agent</span>
          <select
            class="select select-bordered select-sm font-mono"
            value={agentId}
            onChange={(e) => setAgentId((e.target as HTMLSelectElement).value)}
          >
            {(agents.data?.agents ?? []).map((a) => (
              <option value={a.id}>{a.id}</option>
            ))}
          </select>
        </label>
        <label class="form-control">
          <span class="text-[10px] text-base-content/50 mb-1">Version (drafts included)</span>
          <select
            class="select select-bordered select-sm font-mono"
            value={agentVersion}
            onChange={(e) => setAgentVersion((e.target as HTMLSelectElement).value)}
          >
            <option value="">active</option>
            {[...(versions.data?.versions ?? [])].reverse().map((v) => (
              <option value={v.version}>
                {v.version}
                {v.active ? " (active)" : " (draft)"}
              </option>
            ))}
          </select>
        </label>
      </div>
      <div class="text-[10px] text-base-content/50">
        Runs in namespace <span class="font-mono">{namespaceId}</span> (its facts, knowledge bases,
        credentials and budget) under a resource of its own; approval-gated tools are refused.
        Durable: a restart resumes it, recording a case it cut short as interrupted.
      </div>
      <button
        class="btn btn-sm btn-primary self-start"
        disabled={busy || !agentId}
        onClick={() => void start()}
      >
        {busy ? "Starting…" : "▶ Run suite"}
      </button>
      {error && <div class="alert alert-error text-xs">{error}</div>}
    </div>
  );
}

function statusBadge(status: EvalRunRecord["status"]): string {
  return status === "completed"
    ? "badge-success"
    : status === "running"
      ? "badge-info"
      : status === "cancelled"
        ? "badge-warning"
        : "badge-error";
}

function RunsTable({
  runs,
  selected,
  onSelect,
  onCompare,
}: {
  runs: EvalRunRecord[];
  selected: string | null;
  onSelect: (runId: string) => void;
  onCompare: (pair: { baseline: string; candidate: string }) => void;
}) {
  const [picked, setPicked] = useState<string[]>([]);
  const toggle = (runId: string) =>
    setPicked((prev) =>
      prev.includes(runId) ? prev.filter((id) => id !== runId) : [...prev, runId].slice(-2),
    );
  return (
    <div class="card bg-base-100 shadow p-4 space-y-2">
      <div class="flex items-center justify-between">
        <div class="text-xs uppercase tracking-wider text-base-content/60">Runs</div>
        <button
          class="btn btn-xs btn-outline"
          disabled={picked.length !== 2}
          title="Pick two runs (older first is the baseline)"
          onClick={() => onCompare({ baseline: picked[0]!, candidate: picked[1]! })}
        >
          Compare {picked.length}/2
        </button>
      </div>
      {runs.length === 0 ? (
        <div class="text-xs text-base-content/40">No runs of this suite yet.</div>
      ) : (
        <table class="table table-xs">
          <thead>
            <tr class="text-[10px] uppercase tracking-wider text-base-content/50">
              <th />
              <th>Agent</th>
              <th>Suite</th>
              <th>Status</th>
              <th>Progress</th>
              <th>Pass rate</th>
              <th>Started</th>
            </tr>
          </thead>
          <tbody>
            {runs.map((r) => (
              <tr
                class={`cursor-pointer hover:bg-base-200 ${selected === r.runId ? "bg-base-200" : ""}`}
                onClick={() => onSelect(r.runId)}
              >
                <td onClick={(e) => e.stopPropagation()}>
                  <input
                    type="checkbox"
                    class="checkbox checkbox-xs"
                    checked={picked.includes(r.runId)}
                    onChange={() => toggle(r.runId)}
                  />
                </td>
                <td class="font-mono">
                  {r.targetId}@{r.targetVersion}
                </td>
                <td class="font-mono">{r.suiteVersion}</td>
                <td>
                  <span class={`badge badge-xs ${statusBadge(r.status)}`}>{r.status}</span>
                </td>
                <td>
                  <progress
                    class="progress progress-primary w-20"
                    value={r.completedCases}
                    max={Math.max(1, r.totalCases)}
                  />
                  <span class="ml-1 font-mono text-[10px]">
                    {r.completedCases}/{r.totalCases}
                  </span>
                </td>
                <td class="font-mono">{r.totals ? pct(r.totals.passRate) : "—"}</td>
                <td class="text-[10px]">{formatRelative(new Date(r.startedAt).toISOString())}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}

function pct(value: number): string {
  return `${Math.round(value * 1000) / 10}%`;
}

function RunDetail({ runId, onChanged }: { runId: string; onChanged: () => void }) {
  const [run, setRun] = useState<StoredEvalRun | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let stopped = false;
    let handle: ReturnType<typeof setTimeout> | undefined;
    const read = async () => {
      try {
        const next = await api.getEvalRun({ runId });
        if (stopped) return;
        setRun(next);
        if (next.status === "running") handle = setTimeout(() => void read(), PROGRESS_POLL_MS);
        else onChanged();
      } catch (e) {
        if (!stopped) setError(e instanceof Error ? e.message : String(e));
      }
    };
    void read();
    return () => {
      stopped = true;
      if (handle !== undefined) clearTimeout(handle);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [runId]);

  if (error) return <div class="alert alert-error text-xs">{error}</div>;
  if (!run) return <div class="text-xs text-base-content/40">Loading run…</div>;
  return (
    <div class="card bg-base-100 shadow p-4 space-y-3">
      <div class="flex items-center justify-between gap-2">
        <div class="min-w-0">
          <div class="text-xs uppercase tracking-wider text-base-content/60">Run</div>
          <a class="link font-mono text-xs" href={`#/runs/${encodeURIComponent(run.runId)}`}>
            {run.runId}
          </a>
        </div>
        <div class="flex items-center gap-2">
          <span class={`badge badge-sm ${statusBadge(run.status)}`}>{run.status}</span>
          {run.status === "running" && (
            <button
              class="btn btn-xs btn-outline btn-error"
              onClick={() => void api.cancelEvalRun({ runId: run.runId }).then(setRun)}
            >
              Cancel
            </button>
          )}
        </div>
      </div>
      <progress
        class="progress progress-primary w-full"
        value={run.completedCases}
        max={Math.max(1, run.totalCases)}
      />
      {run.error && <div class="alert alert-error text-xs">{run.error}</div>}
      {run.totals && (
        <div class="flex flex-wrap gap-2 text-xs">
          <span class="badge badge-outline">pass rate {pct(run.totals.passRate)}</span>
          {Object.entries(run.totals.perScorer).map(([id, s]) => (
            <span class="badge badge-ghost font-mono">
              {id}: mean {s.meanValue.toFixed(2)} · pass {pct(s.passRate)}
            </span>
          ))}
        </div>
      )}
      <table class="table table-xs">
        <thead>
          <tr class="text-[10px] uppercase tracking-wider text-base-content/50">
            <th>Case</th>
            <th>Verdict</th>
            <th>Scores</th>
            <th>Output</th>
          </tr>
        </thead>
        <tbody>
          {run.caseResults.map((c) => {
            const sample = c.samples[0];
            return (
              <tr>
                <td class="font-mono">{c.caseId}</td>
                <td>
                  <span class={`badge badge-xs ${c.passed ? "badge-success" : "badge-error"}`}>
                    {c.passed ? "pass" : "fail"}
                  </span>
                  {c.samples.length > 1 && <span class="ml-1 text-[10px]">{pct(c.passRate)}</span>}
                </td>
                <td class="space-y-0.5">
                  {(sample?.scores ?? []).map((s) => (
                    <div class="text-[10px] font-mono" title={s.reason ?? ""}>
                      <span class={s.passed ? "text-success" : "text-error"}>
                        {s.scorerId} {s.value.toFixed(2)}
                      </span>
                      {s.reason && <span class="text-base-content/50"> — {s.reason}</span>}
                    </div>
                  ))}
                </td>
                <td class="max-w-md truncate text-[11px]" title={outputText(sample?.output)}>
                  {outputText(sample?.output)}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

function outputText(output: { output: unknown; error?: string } | undefined): string {
  if (!output) return "";
  if (output.error !== undefined) return `error: ${output.error}`;
  return typeof output.output === "string" ? output.output : JSON.stringify(output.output);
}

function CompareView({
  compare,
  onClose,
}: {
  compare: { baseline: string; candidate: string };
  onClose: () => void;
}) {
  const { data, error } = useFetch<RunDiff>(
    () => api.compareEvalRuns(compare),
    [compare.baseline, compare.candidate],
    0,
  );
  const tag = (s: RunDiff["baseline"]) => `${s.targetId}@${s.targetVersion ?? "?"}`;
  const candidateById = useMemo(
    () => new Map((data?.candidate.caseResults ?? []).map((c) => [c.caseId, c])),
    [data],
  );
  return (
    <div class="card bg-base-100 shadow p-4 space-y-3">
      <div class="flex items-center justify-between">
        <div class="text-xs uppercase tracking-wider text-base-content/60">Compare</div>
        <button class="btn btn-xs btn-ghost" onClick={onClose}>
          ✕
        </button>
      </div>
      {error && <div class="alert alert-error text-xs">{error.message}</div>}
      {data && (
        <>
          <div class="grid grid-cols-2 gap-2 text-xs">
            <div class="p-2 rounded bg-base-200">
              <div class="font-mono">{tag(data.baseline)}</div>
              <div>pass rate {pct(data.baseline.passRate)}</div>
            </div>
            <div class="p-2 rounded bg-base-200">
              <div class="font-mono">{tag(data.candidate)}</div>
              <div>
                pass rate {pct(data.candidate.passRate)}{" "}
                <span class={data.passRateDelta < 0 ? "text-error" : "text-success"}>
                  ({data.passRateDelta >= 0 ? "+" : ""}
                  {pct(data.passRateDelta)})
                </span>
              </div>
            </div>
          </div>
          <table class="table table-xs">
            <thead>
              <tr class="text-[10px] uppercase tracking-wider text-base-content/50">
                <th>Case</th>
                <th>{tag(data.baseline)}</th>
                <th>{tag(data.candidate)}</th>
                <th>Change</th>
              </tr>
            </thead>
            <tbody>
              {data.baseline.caseResults.map((b) => {
                const c = candidateById.get(b.caseId);
                const diff = data.perCase.find((p) => p.caseId === b.caseId);
                return (
                  <tr>
                    <td class="font-mono">{b.caseId}</td>
                    <td class="text-[11px] max-w-xs truncate">
                      {b.passed ? "✓ " : "✗ "}
                      {outputText(b.samples[0]?.output)}
                    </td>
                    <td class="text-[11px] max-w-xs truncate">
                      {c ? `${c.passed ? "✓ " : "✗ "}${outputText(c.samples[0]?.output)}` : "—"}
                    </td>
                    <td>
                      {diff && diff.status !== "unchanged" && (
                        <span
                          class={`badge badge-xs ${diff.status === "regressed" ? "badge-error" : "badge-success"}`}
                        >
                          {diff.status}
                        </span>
                      )}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </>
      )}
    </div>
  );
}

function GatesPanel({ suites }: { suites: RegisteredEvalSuite[] }) {
  const gates = useFetch(() => api.listEvalGates(), [], 0);
  const [agentId, setAgentId] = useState("");
  const [suiteId, setSuiteId] = useState("");
  const [minScore, setMinScore] = useState("0.9");
  const [error, setError] = useState<string | null>(null);

  const save = async () => {
    setError(null);
    try {
      await api.putEvalGate({
        agentId,
        suiteId: suiteId || (suites[0]?.id ?? ""),
        minScore: Number(minScore),
      });
      gates.refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  };

  return (
    <div class="card bg-base-100 shadow p-4 space-y-3">
      <div class="text-xs uppercase tracking-wider text-base-content/60">Promotion gates</div>
      <div class="text-[11px] text-base-content/50">
        A gated agent's version becomes active (promote, publishing edit) only once the suite scored
        at least the minimum on it. Operators can override when promoting.
      </div>
      {(gates.data?.gates ?? []).length > 0 && (
        <table class="table table-xs">
          <thead>
            <tr class="text-[10px] uppercase tracking-wider text-base-content/50">
              <th>Agent</th>
              <th>Suite</th>
              <th>Min score</th>
              <th />
            </tr>
          </thead>
          <tbody>
            {(gates.data?.gates ?? []).map((g) => (
              <tr>
                <td class="font-mono">{g.id}</td>
                <td class="font-mono">
                  {g.suiteId}
                  {g.suiteVersion
                    ? `@${g.suiteVersion}`
                    : ` (active version${g.suiteVersionAtSet ? `; ${g.suiteVersionAtSet} when set` : ""})`}
                </td>
                <td class="font-mono">
                  {g.minScore}
                  {g.scorerId ? ` (${g.scorerId})` : " (pass rate)"}
                </td>
                <td>
                  <button
                    class="btn btn-xs btn-ghost"
                    onClick={() =>
                      void api.deleteEvalGate({ agentId: g.id }).then(() => gates.refresh())
                    }
                  >
                    Remove
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
      <div class="flex flex-wrap items-end gap-2">
        <label class="form-control">
          <span class="text-[10px] text-base-content/50 mb-1">Agent id</span>
          <input
            class="input input-bordered input-sm font-mono"
            value={agentId}
            onInput={(e) => setAgentId((e.target as HTMLInputElement).value)}
          />
        </label>
        <label class="form-control">
          <span class="text-[10px] text-base-content/50 mb-1">Suite</span>
          <select
            class="select select-bordered select-sm font-mono"
            value={suiteId}
            onChange={(e) => setSuiteId((e.target as HTMLSelectElement).value)}
          >
            {suites.map((s) => (
              <option value={s.id}>{s.id}</option>
            ))}
          </select>
        </label>
        <label class="form-control">
          <span class="text-[10px] text-base-content/50 mb-1">Min score (0..1)</span>
          <input
            class="input input-bordered input-sm w-24 font-mono"
            value={minScore}
            onInput={(e) => setMinScore((e.target as HTMLInputElement).value)}
          />
        </label>
        <button
          class="btn btn-sm btn-outline"
          disabled={!agentId || suites.length === 0}
          onClick={() => void save()}
        >
          Set gate
        </button>
      </div>
      {error && <div class="alert alert-error text-xs">{error}</div>}
    </div>
  );
}
