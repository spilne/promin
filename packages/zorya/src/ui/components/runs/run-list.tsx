import { useEffect, useRef, useState } from "preact/hooks";
import { useFetch } from "../../hooks/use-fetch.ts";
import { api } from "../../api/client.ts";
import type { WorkflowStatus } from "@promin/workflow";
import type { RunListQuery } from "../../../server/api-types.ts";
import { StatsBar } from "./stats-bar.tsx";
import { StatusBadge } from "../ui/status-badge.tsx";
import { SkeletonRows } from "../ui/skeleton.tsx";
import { Sparkline } from "../ui/sparkline.tsx";
import { Combobox } from "../ui/combobox.tsx";
import type { SparklinesResponse } from "../../../server/routes/grid.ts";
import { formatDuration, formatRelative, WORKFLOW_STATUS_VISUAL } from "../../lib/format.ts";

interface NamesAndTypes {
  names: string[];
  types: string[];
  namespaces: string[];
}

interface RunListProps {
  onOpen: (id: string) => void;
  /** Filter state from the URL. Source of truth — changes here re-render. */
  queryParams?: URLSearchParams;
  /** Emit filter changes back to the URL hash. */
  onQueryChange?: (params: URLSearchParams) => void;
}

const STATUS_FILTERS: Array<WorkflowStatus | "all"> = [
  "all",
  "running",
  "suspended",
  "pending",
  "completed",
  "failed",
];

function isWorkflowStatus(s: string): s is WorkflowStatus {
  return STATUS_FILTERS.includes(s as WorkflowStatus | "all") && s !== "all";
}

export function RunList({ onOpen, queryParams, onQueryChange }: RunListProps) {
  const initialName = queryParams?.get("name") ?? "";
  const initialType = queryParams?.get("type") ?? "";
  const initialNamespace = queryParams?.get("namespace") ?? "";
  const initialStatusRaw = queryParams?.get("status") ?? "all";
  const initialStatus: WorkflowStatus | "all" = isWorkflowStatus(initialStatusRaw)
    ? initialStatusRaw
    : "all";
  const initialPage = Math.max(1, Number.parseInt(queryParams?.get("page") ?? "1", 10) || 1);

  const initialVersion = queryParams?.get("version") ?? "";

  const [name, setName] = useState(initialName);
  const [type, setType] = useState(initialType);
  const [namespace, setNamespace] = useState(initialNamespace);
  const [status, setStatus] = useState<WorkflowStatus | "all">(initialStatus);
  const [version, setVersion] = useState(initialVersion);
  const [page, setPage] = useState(initialPage);
  const [meta, setMeta] = useState<NamesAndTypes>({ names: [], types: [], namespaces: [] });
  const [sparklines, setSparklines] = useState<SparklinesResponse>({});

  useEffect(() => {
    api
      .listWorkflowNames()
      .then((r) =>
        setMeta({ names: r.names, types: r.types ?? [], namespaces: r.namespaces ?? [] }),
      )
      .catch(() => {});
  }, []);

  // Refresh sparklines alongside the main fetch so rows and sparkbars stay
  // in step as new runs arrive.
  useEffect(() => {
    let cancelled = false;
    const load = () =>
      api
        .getSparklines(14)
        .then((r) => !cancelled && setSparklines(r))
        .catch(() => {});
    load();
    const h = setInterval(load, 5000);
    return () => {
      cancelled = true;
      clearInterval(h);
    };
  }, []);

  const PAGE_SIZE = 25;

  // Changing filters resets to page 1 — but NOT on the initial mount, or
  // else the `?page=N` in the URL would be clobbered on refresh.
  const firstFilterChange = useRef(true);
  useEffect(() => {
    if (firstFilterChange.current) {
      firstFilterChange.current = false;
      return;
    }
    setPage(1);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [name, type, namespace, status, version]);

  // Sync local state back to URL so filters survive refresh / share links.
  useEffect(() => {
    if (!onQueryChange) return;
    const qp = new URLSearchParams();
    if (name) qp.set("name", name);
    if (type) qp.set("type", type);
    if (namespace) qp.set("namespace", namespace);
    if (status !== "all") qp.set("status", status);
    if (version) qp.set("version", version);
    if (page > 1) qp.set("page", String(page));
    onQueryChange(qp);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [name, type, namespace, status, version, page]);

  const query: RunListQuery = {
    name: name || undefined,
    type: type || undefined,
    namespace: namespace || undefined,
    status: status === "all" ? undefined : status,
    version: version || undefined,
    limit: PAGE_SIZE,
    offset: (page - 1) * PAGE_SIZE,
  };
  const { data, loading, error, refresh } = useFetch(
    () => api.listRuns(query),
    [name, type, namespace, status, version, page],
    5000,
  );

  return (
    <div class="anim-page p-4 max-w-[1400px] mx-auto space-y-4">
      <div class="flex items-end justify-between">
        <div>
          <h2 class="text-xl font-semibold">Runs</h2>
          <p class="text-sm text-base-content/50">Live · auto-refreshes every 5s</p>
        </div>
        <button class="btn btn-sm btn-ghost gap-1" onClick={() => refresh()}>
          <span>↻</span>
          Refresh
        </button>
      </div>

      <StatsBar
        onPickStatus={(s) => {
          setStatus(s ?? "all");
        }}
      />

      <SearchBar onOpen={onOpen} onSetName={(v) => setName(v)} />

      {/* Filter bar (chip-style) */}
      <div class="flex items-center gap-2 flex-wrap justify-end">
        <div class="join">
          {STATUS_FILTERS.map((s) => {
            const active = status === s;
            const label = s === "all" ? "All" : WORKFLOW_STATUS_VISUAL[s as WorkflowStatus].label;
            return (
              <button
                class={`btn btn-sm join-item ${active ? "btn-primary" : "btn-ghost"}`}
                onClick={() => setStatus(s)}
              >
                {label}
              </button>
            );
          })}
        </div>
        <div class="h-5 w-px bg-base-content/20 mx-1" />
        <Combobox
          class="w-40"
          value={name}
          onChange={setName}
          placeholder="All names"
          options={[{ value: "", label: "All names" }, ...meta.names.map((n) => ({ value: n }))]}
        />
        {meta.types.length > 0 && (
          <Combobox
            class="w-40"
            value={type}
            onChange={setType}
            placeholder="All types"
            options={[{ value: "", label: "All types" }, ...meta.types.map((t) => ({ value: t }))]}
          />
        )}
        {meta.namespaces.length > 0 && (
          <Combobox
            class="w-40"
            value={namespace}
            onChange={setNamespace}
            placeholder="All namespaces"
            options={[
              { value: "", label: "All namespaces" },
              ...meta.namespaces.map((n) => ({ value: n })),
            ]}
          />
        )}
        <input
          class="input input-bordered input-sm w-28 font-mono"
          placeholder="version"
          value={version}
          onInput={(e) => setVersion((e.target as HTMLInputElement).value)}
        />
        {(name || type || namespace || status !== "all" || version) && (
          <button
            class="btn btn-sm btn-ghost"
            onClick={() => {
              setName("");
              setType("");
              setNamespace("");
              setStatus("all");
              setVersion("");
            }}
          >
            Clear
          </button>
        )}
      </div>

      {error && (
        <div class="alert alert-error text-sm">
          <span>Failed to load: {error.message}</span>
        </div>
      )}

      <div class="card bg-base-100 shadow overflow-hidden">
        <div class="overflow-x-auto">
          <table class="table">
            <thead>
              <tr class="bg-base-200 text-xs uppercase tracking-wider text-base-content/50">
                <th>ID</th>
                <th>Name</th>
                <th>Recent</th>
                <th>Type</th>
                <th>Namespace</th>
                <th>Status</th>
                <th>Started</th>
                <th>Duration</th>
              </tr>
            </thead>
            <tbody>
              {loading && !data && <SkeletonRows rows={10} cols={8} />}
              {data && data.runs.length === 0 && !loading && (
                <tr>
                  <td colSpan={8} class="text-center py-12">
                    <div class="text-base-content/50">No runs match the current filters</div>
                  </td>
                </tr>
              )}
              {data?.runs.map((r) => (
                <tr class="hover:bg-base-200 cursor-pointer" onClick={() => onOpen(r.workflowId)}>
                  <td class="font-mono text-sm">{r.workflowId}</td>
                  <td>{r.workflowName}</td>
                  <td>
                    <Sparkline runs={sparklines[r.workflowName] ?? []} />
                  </td>
                  <td class="text-base-content/60">{r.workflowType ?? "—"}</td>
                  <td>
                    {r.namespace ? (
                      <span class="badge badge-sm badge-ghost font-mono">{r.namespace}</span>
                    ) : (
                      <span class="text-base-content/40">—</span>
                    )}
                  </td>
                  <td>
                    <StatusBadge status={r.status} />
                  </td>
                  <td class="text-base-content/60">{formatRelative(r.createdAt)}</td>
                  <td class="font-mono text-sm">{formatDuration(r.totalMs)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        {/* Pagination */}
        <div class="flex items-center justify-between px-4 py-2 border-t border-base-content/10 text-sm">
          <div class="text-base-content/60">
            {data && data.runs.length > 0 ? (
              <>
                Page {page} ·{" "}
                <span class="font-mono">
                  {(page - 1) * PAGE_SIZE + 1}–{(page - 1) * PAGE_SIZE + data.runs.length}
                </span>
              </>
            ) : (
              "—"
            )}
          </div>
          <div class="flex gap-1">
            <button
              class="btn btn-sm btn-ghost"
              disabled={page <= 1}
              onClick={() => setPage((p) => Math.max(1, p - 1))}
            >
              ← Prev
            </button>
            <button
              class="btn btn-sm btn-ghost"
              disabled={!data || data.runs.length < PAGE_SIZE}
              onClick={() => setPage((p) => p + 1)}
            >
              Next →
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}

/**
 * Search input that resolves an exact workflow id first, then falls back to
 * filtering the list by workflow name. Submit on Enter or via the button.
 */
function SearchBar({
  onOpen,
  onSetName,
}: {
  onOpen: (id: string) => void;
  onSetName: (name: string) => void;
}) {
  const [query, setQuery] = useState("");
  const [busy, setBusy] = useState(false);
  const [hint, setHint] = useState<string | undefined>(undefined);

  const submit = async () => {
    const q = query.trim();
    if (!q) return;
    setBusy(true);
    setHint(undefined);
    try {
      const run = await api.getRun(q).catch(() => undefined);
      if (run) {
        onOpen(q);
        setQuery("");
        return;
      }
      onSetName(q);
      setHint(`No run with id "${q}" — filtered by name instead.`);
    } finally {
      setBusy(false);
    }
  };

  return (
    <div class="flex items-center gap-2 justify-end">
      {hint && <span class="text-xs text-base-content/60">{hint}</span>}
      <input
        class="input input-bordered input-sm w-full max-w-md font-mono"
        placeholder="Search by workflow id, then name…"
        value={query}
        disabled={busy}
        onInput={(e) => setQuery((e.target as HTMLInputElement).value)}
        onKeyDown={(e) => {
          if (e.key === "Enter") void submit();
        }}
      />
      <button class="btn btn-sm btn-primary" onClick={submit} disabled={busy || !query.trim()}>
        {busy ? "…" : "Search"}
      </button>
    </div>
  );
}
