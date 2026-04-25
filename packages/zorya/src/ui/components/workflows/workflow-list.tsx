import { useEffect, useMemo, useState } from "preact/hooks";
import { useFetch } from "../../hooks/use-fetch.ts";
import { useNamespace } from "../../hooks/use-namespace.ts";
import { api } from "../../api/client.ts";
import type { WorkflowDefDto } from "../../../server/routes/workflow-defs.ts";
import type { SparklinesResponse } from "../../../server/routes/grid.ts";
import { Sparkline } from "../ui/sparkline.tsx";
import { SkeletonRows } from "../ui/skeleton.tsx";
import { EmptyState } from "../ui/empty-state.tsx";
import { Pagination } from "../ui/pagination.tsx";
import { TriggerModal } from "./trigger-modal.tsx";

const PAGE_SIZE = 20;

type WorkflowSortCol = "name" | "type" | "version" | "steps";
type SortState = { col: WorkflowSortCol; dir: "asc" | "desc" } | null;

interface WorkflowListProps {
  onOpenRun: (id: string) => void;
  /** Navigate to the workflow-detail page (DAG, sample input, recent runs). */
  onOpenWorkflow: (name: string) => void;
  /** Navigate to the runs list pre-filtered by this workflow name. */
  onOpenWorkflowRuns?: (name: string) => void;
}

/**
 * Directory of every registered workflow definition. Shows type, step
 * count, recent activity sparkline, and a [Trigger] action per row.
 */
export function WorkflowList({ onOpenRun, onOpenWorkflow, onOpenWorkflowRuns }: WorkflowListProps) {
  const [namespace] = useNamespace();
  const { data, loading, error, refresh } = useFetch(() => api.listWorkflowDefs(), [], 30_000);
  const { data: sparklines } = useFetch<SparklinesResponse>(() => api.getSparklines(14), [], 5000);
  // Names of workflows that have run in the currently-selected namespace.
  // Used to filter the registry view so users see only the workflows
  // relevant to their tenant. When namespace is unset the filter is
  // skipped — the registry is the union of all definitions.
  const { data: runNamesInNs } = useFetch(
    () => api.listWorkflowNames({ namespace: namespace || undefined }),
    [namespace],
    30_000,
  );
  const [triggering, setTriggering] = useState<WorkflowDefDto | undefined>(undefined);
  const [query, setQuery] = useState("");
  const [page, setPage] = useState(1);
  const [sort, setSort] = useState<SortState>(null);
  // Opt out of the namespace filter — workflow defs are not themselves
  // namespaced, so this lets the user see "everything that could be
  // triggered into this tenant" rather than just "everything that has
  // already run here".
  const [showAll, setShowAll] = useState(false);

  const filtered = useMemo(() => {
    const all = data?.workflows ?? [];
    const q = query.trim().toLowerCase();
    const namesInNs = namespace && !showAll ? new Set(runNamesInNs?.names ?? []) : null;
    return all.filter((w) => {
      if (namesInNs && !namesInNs.has(w.name)) return false;
      if (!q) return true;
      const fields = [w.name, w.type ?? "", w.version ?? "", ...(w.versions ?? [])];
      return fields.some((f) => f.toLowerCase().includes(q));
    });
  }, [data, query, namespace, runNamesInNs, showAll]);

  const totalDefs = data?.workflows.length ?? 0;
  const hiddenByNamespace = namespace && !showAll ? totalDefs - filtered.length : 0;

  const sorted = useMemo(() => {
    if (!sort) return filtered;
    const sign = sort.dir === "asc" ? 1 : -1;
    const copy = [...filtered];
    copy.sort((a, b) => {
      const av = workflowSortKey(a, sort.col);
      const bv = workflowSortKey(b, sort.col);
      if (av === undefined && bv === undefined) return 0;
      if (av === undefined) return 1;
      if (bv === undefined) return -1;
      if (av < bv) return -1 * sign;
      if (av > bv) return 1 * sign;
      return 0;
    });
    return copy;
  }, [filtered, sort]);

  const paged = useMemo(
    () => sorted.slice((page - 1) * PAGE_SIZE, page * PAGE_SIZE),
    [sorted, page],
  );

  function cycleSort(col: WorkflowSortCol) {
    setSort((cur) => {
      if (!cur || cur.col !== col) return { col, dir: "asc" };
      if (cur.dir === "asc") return { col, dir: "desc" };
      return null;
    });
  }

  // Reset to page 1 whenever the search narrows the list.
  useEffect(() => {
    setPage(1);
  }, [query]);

  return (
    <div class="anim-page p-4 max-w-[1400px] mx-auto space-y-4">
      <div class="flex items-end justify-between">
        <div>
          <h2 class="text-xl font-semibold">Workflows</h2>
          <p class="text-xs text-base-content/50">
            {data ? (
              <>
                {totalDefs} registered
                {namespace && !showAll && hiddenByNamespace > 0 ? (
                  <>
                    {" · "}
                    <span class="text-warning/80">
                      filtered to <span class="font-mono">{namespace}</span> ({hiddenByNamespace}{" "}
                      hidden)
                    </span>
                  </>
                ) : null}
              </>
            ) : (
              "Loading…"
            )}
          </p>
        </div>
        <button class="btn btn-sm btn-ghost gap-1" onClick={() => refresh()}>
          <span>↻</span>
          Refresh
        </button>
      </div>

      <div class="flex items-center justify-end gap-2">
        {namespace && (
          <label class="text-xs text-base-content/60 flex items-center gap-1 cursor-pointer">
            <input
              type="checkbox"
              class="checkbox checkbox-xs"
              checked={showAll}
              onChange={(e) => setShowAll((e.target as HTMLInputElement).checked)}
            />
            Show all (ignore namespace filter)
          </label>
        )}
        <input
          class="input input-bordered input-sm w-full max-w-md font-mono"
          placeholder="Search by name, type, version…"
          value={query}
          onInput={(e) => setQuery((e.target as HTMLInputElement).value)}
        />
      </div>

      {error && <div class="alert alert-error text-sm">{error.message}</div>}

      <div class="card bg-base-100 shadow overflow-hidden">
        <div class="overflow-x-auto">
          <table class="table">
            <thead>
              <tr class="bg-base-200 text-xs uppercase tracking-wider text-base-content/50">
                <SortableTh col="name" label="Name" sort={sort} onClick={cycleSort} />
                <SortableTh col="type" label="Type" sort={sort} onClick={cycleSort} />
                <SortableTh col="version" label="Version" sort={sort} onClick={cycleSort} />
                <SortableTh col="steps" label="Steps" sort={sort} onClick={cycleSort} />
                <th>Recent</th>
                <th />
              </tr>
            </thead>
            <tbody>
              {loading && !data && <SkeletonRows rows={6} cols={6} />}
              {data && data.workflows.length === 0 && !loading && (
                <tr>
                  <td colSpan={6}>
                    <EmptyState
                      message="No workflows registered."
                      hint="Pass a `workflows` map to ZoryaServer, or use scanWorkflowsFolder()."
                    />
                  </td>
                </tr>
              )}
              {data && filtered.length === 0 && data.workflows.length > 0 && (
                <tr>
                  <td colSpan={6}>
                    <EmptyState
                      message="No workflows match the search."
                      hint="Clear the search to see them all."
                    />
                  </td>
                </tr>
              )}
              {paged.map((w) => (
                <tr class="hover:bg-base-200">
                  <td>
                    <button
                      class="btn btn-sm btn-ghost font-mono normal-case"
                      onClick={() => onOpenWorkflow(w.name)}
                    >
                      {w.name}
                    </button>
                  </td>
                  <td class="text-base-content/60">{w.type ?? "—"}</td>
                  <td class="font-mono text-sm text-base-content/60">{w.version ?? "—"}</td>
                  <td class="font-mono text-sm">{w.steps.length}</td>
                  <td>
                    <Sparkline runs={sparklines?.[w.name] ?? []} />
                  </td>
                  <td class="text-right">
                    <div class="flex gap-1 justify-end">
                      {onOpenWorkflowRuns && (
                        <button
                          class="btn btn-sm btn-ghost"
                          onClick={() => onOpenWorkflowRuns(w.name)}
                          title="Jump to the runs list filtered by this workflow"
                        >
                          Runs
                        </button>
                      )}
                      <button
                        class="btn btn-sm btn-ghost"
                        onClick={() => onOpenWorkflow(w.name)}
                        title="Inspect DAG and recent runs"
                      >
                        View
                      </button>
                      <button class="btn btn-sm btn-primary" onClick={() => setTriggering(w)}>
                        Trigger
                      </button>
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {filtered.length > 0 && (
        <Pagination
          page={page}
          pageSize={PAGE_SIZE}
          total={filtered.length}
          onChange={setPage}
          itemsLabel="workflows"
        />
      )}

      {triggering && (
        <TriggerModal
          def={triggering}
          onClose={() => setTriggering(undefined)}
          onTriggered={(id) => {
            setTriggering(undefined);
            onOpenRun(id);
          }}
        />
      )}
    </div>
  );
}

function workflowSortKey(w: WorkflowDefDto, col: WorkflowSortCol): number | string | undefined {
  switch (col) {
    case "name":
      return w.name;
    case "type":
      return w.type ?? undefined;
    case "version":
      return w.version ?? undefined;
    case "steps":
      return w.steps.length;
  }
}

function SortableTh({
  col,
  label,
  sort,
  onClick,
}: {
  col: WorkflowSortCol;
  label: string;
  sort: SortState;
  onClick: (col: WorkflowSortCol) => void;
}) {
  const active = sort?.col === col;
  const indicator = active ? (sort!.dir === "asc" ? "▲" : "▼") : "↕";
  return (
    <th class="cursor-pointer select-none hover:text-base-content" onClick={() => onClick(col)}>
      <span class="inline-flex items-center gap-1">
        {label}
        <span class={`text-[0.6rem] ${active ? "opacity-100" : "opacity-20"}`}>{indicator}</span>
      </span>
    </th>
  );
}
