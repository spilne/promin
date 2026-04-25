import { useEffect, useMemo, useRef, useState } from "preact/hooks";
import { useFetch } from "../../hooks/use-fetch.ts";
import { useNamespace } from "../../hooks/use-namespace.ts";
import { api } from "../../api/client.ts";
import type { WorkflowOrderBy, WorkflowStatus } from "@promin/workflow";
import type { RunListQuery } from "../../../server/api-types.ts";
import { StatsBar } from "./stats-bar.tsx";
import { StatusBadge } from "../ui/status-badge.tsx";
import { SkeletonRows } from "../ui/skeleton.tsx";
import { Sparkline } from "../ui/sparkline.tsx";
import type { SparklinesResponse } from "../../../server/routes/grid.ts";
import { formatDuration, formatRelative, WORKFLOW_STATUS_VISUAL } from "../../lib/format.ts";
import {
  parseSearchQuery,
  serializeQuery,
  hasAnyFilter,
  type ParsedSearchQuery,
} from "../../lib/smart-search.ts";

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
  const initialStatusRaw = queryParams?.get("status") ?? "all";
  const initialStatus: WorkflowStatus | "all" = isWorkflowStatus(initialStatusRaw)
    ? initialStatusRaw
    : "all";
  const initialPage = Math.max(1, Number.parseInt(queryParams?.get("page") ?? "1", 10) || 1);
  const initialSort = parseSortParam(queryParams?.get("sort"));
  // The smart-search input is the single source of truth for name / type /
  // version / namespace / metadata / id / freeText filters. We accept either
  // the unified `?q=` form or the legacy per-field params (`?name=`, etc.)
  // so old bookmarks keep working.
  const initialQueryText = queryParams?.get("q") ?? buildQueryFromLegacyParams(queryParams);

  const [searchInput, setSearchInput] = useState(initialQueryText);
  const [appliedSearch, setAppliedSearch] = useState(initialQueryText);
  const parsedSearch = useMemo<ParsedSearchQuery>(
    () => parseSearchQuery(appliedSearch),
    [appliedSearch],
  );

  const [status, setStatus] = useState<WorkflowStatus | "all">(initialStatus);
  const [page, setPage] = useState(initialPage);
  const [sort, setSort] = useState<{ orderBy: WorkflowOrderBy; orderDir: "asc" | "desc" } | null>(
    initialSort,
  );
  // Namespace is a global scope set via the sidebar switcher. Pages observe
  // it and include in their API fetches.
  const [namespace] = useNamespace();
  const [sparklines, setSparklines] = useState<SparklinesResponse>({});

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
  }, [appliedSearch, status, namespace]);

  // Sync local state back to URL so filters survive refresh / share links.
  // Namespace is NOT on the URL — it lives in the global sidebar switcher
  // (localStorage-backed) so it persists across pages and refreshes.
  useEffect(() => {
    if (!onQueryChange) return;
    const qp = new URLSearchParams();
    if (appliedSearch) qp.set("q", appliedSearch);
    if (status !== "all") qp.set("status", status);
    if (page > 1) qp.set("page", String(page));
    if (sort) qp.set("sort", `${sort.orderBy}:${sort.orderDir}`);
    onQueryChange(qp);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [appliedSearch, status, page, sort]);

  // freeText (bare tokens with no `field:` or `key=` prefix) is treated as a
  // name shortcut — `name:` always wins when both are present, so power users
  // can type explicit field syntax without losing the bare-text affordance.
  const effectiveName = parsedSearch.name ?? parsedSearch.freeText;

  const query: RunListQuery = {
    name: effectiveName || undefined,
    type: parsedSearch.type || undefined,
    namespace: parsedSearch.namespace || namespace || undefined,
    status: status === "all" ? undefined : status,
    version: parsedSearch.version || undefined,
    metadata: parsedSearch.metadata,
    limit: PAGE_SIZE,
    offset: (page - 1) * PAGE_SIZE,
    orderBy: sort?.orderBy,
    orderDir: sort?.orderDir,
  };
  const { data, loading, error, refresh } = useFetch(
    () => api.listRuns(query),
    [appliedSearch, namespace, status, page, sort?.orderBy, sort?.orderDir],
    5000,
  );

  /**
   * Click cycle for a header: unsorted → desc → asc → unsorted (back to
   * server default = createdAt desc). Per-column so clicking a different
   * column starts at desc immediately.
   */
  function cycleSort(col: WorkflowOrderBy) {
    setSort((cur) => {
      if (!cur || cur.orderBy !== col) return { orderBy: col, orderDir: "desc" };
      if (cur.orderDir === "desc") return { orderBy: col, orderDir: "asc" };
      return null;
    });
  }

  /**
   * Commit the current input as the active filter set. When the input is
   * pure free text (no `field:` / `key=` clauses), try to resolve it as a
   * workflow id first — that preserves the original SearchBar's "type an
   * id, jump straight to the run" affordance now that the dedicated bar is
   * gone.
   */
  async function submitSearch() {
    const text = searchInput.trim();
    if (!text) {
      setAppliedSearch("");
      return;
    }
    const parsed = parseSearchQuery(text);
    const onlyFreeText =
      !!parsed.freeText &&
      !parsed.name &&
      !parsed.type &&
      !parsed.version &&
      !parsed.namespace &&
      !parsed.id &&
      !parsed.metadata;
    if (onlyFreeText) {
      const run = await api.getRun(parsed.freeText!).catch(() => undefined);
      if (run) {
        onOpen(parsed.freeText!);
        return;
      }
    }
    setAppliedSearch(text);
  }

  function clearAllFilters() {
    setSearchInput("");
    setAppliedSearch("");
    setStatus("all");
  }

  /**
   * Drop a single parsed clause and re-commit. Editing the chip preview
   * should feel as immediate as clicking the row's "X" — no Enter required.
   */
  function removeClause(field: keyof ParsedSearchQuery, metadataKey?: string) {
    const next: ParsedSearchQuery = { ...parsedSearch };
    if (field === "metadata" && metadataKey && next.metadata) {
      const { [metadataKey]: _drop, ...rest } = next.metadata;
      next.metadata = Object.keys(rest).length > 0 ? rest : undefined;
    } else {
      delete next[field];
    }
    const text = serializeQuery(next);
    setSearchInput(text);
    setAppliedSearch(text);
  }

  const hasFilters = hasAnyFilter(parsedSearch) || status !== "all";

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

      {/* Status quick-pick chips. Independent toggle row above the smart
          search so the most-common filter stays a single click away. */}
      <div class="flex items-center gap-2 justify-end">
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
      </div>

      {/* Smart search row: one input handles id, name, type, version,
          namespace, and metadata via `field:value` / `key=value` syntax;
          bare text falls back to id-or-name lookup on submit. The Clear
          button sits to the right of the (fixed-width) input so it can
          fade in/out without shifting the input's position. */}
      <div class="flex items-start gap-2">
        <div class="flex-1 min-w-0">
          <div class="relative">
            <input
              class="input input-bordered input-sm w-full font-mono pr-10"
              placeholder='Search id, or "name:foo type:bar version:v2 userId=u_42"'
              value={searchInput}
              title={
                "One field replaces id/name/type/version/namespace/metadata.\n" +
                "Examples:\n" +
                "  wf-abc123                        — find run by id (jumps if exact)\n" +
                "  onboarding                       — name shortcut\n" +
                "  name:onboarding type:webhook     — structured field filter\n" +
                'name:"my workflow" version:v2     — quote values with spaces\n' +
                "  userId=u_42 retries=3 dryRun=true — metadata (JSON values parse)\n" +
                "Press Enter to apply."
              }
              onInput={(e) => setSearchInput((e.target as HTMLInputElement).value)}
              onKeyDown={(e) => {
                if (e.key === "Enter") void submitSearch();
              }}
              onBlur={() => {
                if (searchInput !== appliedSearch) void submitSearch();
              }}
            />
            {searchInput && (
              <button
                class="btn btn-xs btn-ghost btn-circle absolute right-1 top-1/2 -translate-y-1/2"
                aria-label="Clear search input"
                onClick={() => {
                  setSearchInput("");
                  setAppliedSearch("");
                }}
              >
                ×
              </button>
            )}
          </div>
          {/* Parsed-clause chips — click the × to drop a single filter. */}
          {hasAnyFilter(parsedSearch) && (
            <div class="flex items-center gap-1 flex-wrap mt-2">
              {(["name", "type", "version", "namespace", "id"] as const).map((f) =>
                parsedSearch[f] ? (
                  <FilterChip label={`${f}: ${parsedSearch[f]}`} onRemove={() => removeClause(f)} />
                ) : null,
              )}
              {parsedSearch.metadata &&
                Object.entries(parsedSearch.metadata).map(([k, v]) => (
                  <FilterChip
                    label={`${k}=${typeof v === "string" ? v : JSON.stringify(v)}`}
                    onRemove={() => removeClause("metadata", k)}
                  />
                ))}
              {parsedSearch.freeText && !parsedSearch.name && (
                <FilterChip
                  label={`name: ${parsedSearch.freeText}`}
                  onRemove={() => removeClause("freeText")}
                />
              )}
            </div>
          )}
        </div>
        {hasFilters && (
          <button class="btn btn-sm btn-ghost anim-fade-in" onClick={clearAllFilters}>
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
                <SortableTh col="name" label="Name" sort={sort} onClick={cycleSort} />
                <th>Recent</th>
                <th>Type</th>
                <th>Namespace</th>
                <SortableTh col="status" label="Status" sort={sort} onClick={cycleSort} />
                <SortableTh col="createdAt" label="Started" sort={sort} onClick={cycleSort} />
                <SortableTh col="duration" label="Duration" sort={sort} onClick={cycleSort} />
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

function SortableTh({
  col,
  label,
  sort,
  onClick,
}: {
  col: WorkflowOrderBy;
  label: string;
  sort: { orderBy: WorkflowOrderBy; orderDir: "asc" | "desc" } | null;
  onClick: (col: WorkflowOrderBy) => void;
}) {
  const active = sort?.orderBy === col;
  const indicator = active ? (sort!.orderDir === "asc" ? "▲" : "▼") : "";
  return (
    <th
      class="cursor-pointer select-none hover:text-base-content"
      onClick={() => onClick(col)}
      title={
        active
          ? sort!.orderDir === "desc"
            ? `Sorted ${label} descending. Click to reverse.`
            : `Sorted ${label} ascending. Click to clear.`
          : `Click to sort by ${label}.`
      }
    >
      <span class="inline-flex items-center gap-1">
        {label}
        <span class={`text-[0.6rem] ${active ? "opacity-100" : "opacity-20"}`}>
          {indicator || "↕"}
        </span>
      </span>
    </th>
  );
}

const VALID_ORDER_BY: ReadonlyArray<WorkflowOrderBy> = [
  "createdAt",
  "startedAt",
  "completedAt",
  "duration",
  "status",
  "name",
];

function parseSortParam(
  raw: string | null | undefined,
): { orderBy: WorkflowOrderBy; orderDir: "asc" | "desc" } | null {
  if (!raw) return null;
  const [col, dir] = raw.split(":");
  const orderBy = VALID_ORDER_BY.find((c) => c === col);
  if (!orderBy) return null;
  const orderDir: "asc" | "desc" = dir === "asc" ? "asc" : "desc";
  return { orderBy, orderDir };
}

/**
 * Backwards-compat: rebuild the smart-search text from the legacy
 * per-field URL params (`?name=`, `?type=`, `?version=`, `?metadata=`) so
 * old bookmarks land on the right filters even though the UI no longer
 * writes them. Returns "" when no legacy params are present.
 */
function buildQueryFromLegacyParams(qp: URLSearchParams | undefined): string {
  if (!qp) return "";
  const parts: string[] = [];
  for (const f of ["name", "type", "version", "namespace"] as const) {
    const v = qp.get(f);
    if (v) parts.push(quoteIfNeeded(`${f}:${v}`, v));
  }
  const metadataRaw = qp.get("metadata");
  if (metadataRaw) {
    try {
      const obj = JSON.parse(metadataRaw);
      if (obj && typeof obj === "object" && !Array.isArray(obj)) {
        for (const [k, v] of Object.entries(obj as Record<string, unknown>)) {
          const s = typeof v === "string" ? v : JSON.stringify(v);
          parts.push(quoteIfNeeded(`${k}=${s}`, s));
        }
      }
    } catch {
      // Drop malformed metadata silently — same forgiveness as the route.
    }
  }
  return parts.join(" ");
}

function quoteIfNeeded(token: string, value: string): string {
  if (!/[\s"]/.test(value)) return token;
  // Replace the unquoted value with a quoted version, preserving the
  // `field:` or `key=` prefix.
  const sep =
    token.indexOf(":") >= 0 && (token.indexOf("=") < 0 || token.indexOf(":") < token.indexOf("="))
      ? ":"
      : "=";
  const sepIdx = token.indexOf(sep);
  const prefix = token.slice(0, sepIdx + 1);
  return `${prefix}"${value.replace(/"/g, '\\"')}"`;
}

function FilterChip({ label, onRemove }: { label: string; onRemove: () => void }) {
  return (
    <span class="badge badge-sm badge-outline gap-1 font-mono anim-fade-in">
      {label}
      <button
        class="text-base-content/60 hover:text-error leading-none"
        aria-label={`Remove ${label}`}
        onClick={onRemove}
      >
        ×
      </button>
    </span>
  );
}
