// ---------------------------------------------------------------------------
// WorkflowVersionsPanel — version timeline + promote / rollback for one
// workflow. Renders inline on the workflow-detail page so the lifecycle
// lives next to the workflow it belongs to (instead of a separate page).
//
// Hidden when no versions are registered — most workflows are
// unversioned and we don't want a "no registered versions" panel
// cluttering every detail page.
// ---------------------------------------------------------------------------

import { useEffect, useMemo, useState } from "preact/hooks";
import { useFetch } from "../../hooks/use-fetch.ts";
import { api, ApiError } from "../../api/client.ts";
import type { DeploymentDto } from "../../../server/routes/deployments.ts";
import { confirm, toast } from "../../lib/dialogs.ts";

interface Props {
  /** Workflow name. */
  name: string;
  /** Drill into the runs list filtered by `(name, version)`. */
  onOpenRuns: (version: string) => void;
}

export function WorkflowVersionsPanel({ name, onOpenRuns }: Props) {
  const { data, loading, error, refresh } = useFetch(
    () => api.listDeployments(name),
    [name],
    10_000,
  );
  const [actingVersion, setActingVersion] = useState<string | null>(null);

  const sorted = useMemo(() => {
    const versions = data?.deployments ?? [];
    return [...versions].sort(
      (a, b) => new Date(a.registeredAt).getTime() - new Date(b.registeredAt).getTime(),
    );
  }, [data]);
  const active = sorted.find((d) => d.status === "active");

  // Hide the panel for workflows without registered versions — keeps the
  // detail page clean for unversioned workflows.
  if (!loading && sorted.length === 0 && !error) return null;

  const promote = async (version: string): Promise<void> => {
    setActingVersion(version);
    try {
      await api.promoteDeployment(name, version);
      toast(`Promoted ${name}@${version}`, { variant: "success" });
      refresh();
    } catch (err) {
      const msg = err instanceof ApiError ? err.message : String(err);
      toast(`Promote failed: ${msg}`, { variant: "error" });
    } finally {
      setActingVersion(null);
    }
  };

  const rollback = async (toVersion: string): Promise<void> => {
    if (!active) return;
    const ok = await confirm({
      title: `Roll back ${name}?`,
      message:
        `This will archive ${name}@${active.version} (current active) and ` +
        `promote ${name}@${toVersion}. In-flight runs continue under the ` +
        `version they started on.`,
      confirmLabel: "Roll back",
    });
    if (!ok) return;
    setActingVersion(toVersion);
    try {
      await api.rollbackDeployment(name, toVersion);
      toast(`Rolled back ${name} → ${toVersion}`, { variant: "success" });
      refresh();
    } catch (err) {
      const msg = err instanceof ApiError ? err.message : String(err);
      toast(`Rollback failed: ${msg}`, { variant: "error" });
    } finally {
      setActingVersion(null);
    }
  };

  return (
    <div class="card bg-base-100 shadow">
      <div class="card-body p-4 space-y-3">
        <div class="flex items-center justify-between">
          <h3 class="text-xs font-semibold text-base-content/50 uppercase tracking-wide">
            Versions
          </h3>
          <div class="text-xs text-base-content/60">
            {active ? (
              <>
                active: <span class="font-mono text-base-content/90">{active.version}</span>
              </>
            ) : (
              <span class="italic">no active version — runs use latest-registered</span>
            )}
          </div>
        </div>

        {error && <div class="text-error text-sm">Failed to load: {error.message}</div>}

        {sorted.length > 0 && (
          <ul class="divide-y divide-base-content/5">
            {sorted.map((d) => (
              <VersionRow
                key={d.version}
                deployment={d}
                isActive={active?.version === d.version}
                hasActive={!!active}
                acting={actingVersion === d.version}
                onPromote={() => promote(d.version)}
                onRollback={() => rollback(d.version)}
                onOpenRuns={() => onOpenRuns(d.version)}
              />
            ))}
          </ul>
        )}
      </div>
    </div>
  );
}

interface VersionRowProps {
  deployment: DeploymentDto;
  isActive: boolean;
  hasActive: boolean;
  acting: boolean;
  onPromote: () => void;
  onRollback: () => void;
  onOpenRuns: () => void;
}

function VersionRow({
  deployment: d,
  isActive,
  hasActive,
  acting,
  onPromote,
  onRollback,
  onOpenRuns,
}: VersionRowProps) {
  return (
    <li class="py-2 flex items-center gap-3 text-sm flex-wrap">
      <StatusBadge status={d.status} />
      <span class="font-mono text-base-content/90">v{d.version}</span>
      <span class="text-xs text-base-content/50">
        registered <RelativeTime iso={d.registeredAt} />
        {d.activeAt && (
          <>
            {" · "}active <RelativeTime iso={d.activeAt} />
          </>
        )}
        {d.archivedAt && (
          <>
            {" · "}archived <RelativeTime iso={d.archivedAt} />
          </>
        )}
        {d.contentHash && (
          <>
            {" · "}#<span class="font-mono">{d.contentHash.slice(0, 8)}</span>
          </>
        )}
      </span>
      <div class="ml-auto flex items-center gap-2">
        <button class="btn btn-ghost btn-xs" onClick={onOpenRuns}>
          runs
        </button>
        {!isActive && d.status !== "archived" && (
          <button
            class="btn btn-primary btn-xs"
            disabled={acting}
            onClick={hasActive ? onRollback : onPromote}
          >
            {acting ? "…" : hasActive ? "rollback to" : "promote"}
          </button>
        )}
        {isActive && <span class="text-xs text-base-content/40">(active)</span>}
      </div>
    </li>
  );
}

function StatusBadge({ status }: { status: DeploymentDto["status"] }) {
  const cls =
    status === "active" ? "badge-success" : status === "archived" ? "badge-warning" : "badge-ghost";
  return <span class={`badge badge-sm ${cls}`}>{status}</span>;
}

function RelativeTime({ iso }: { iso: string }) {
  const [, force] = useState(0);
  useEffect(() => {
    const t = setInterval(() => force((n) => n + 1), 30_000);
    return () => clearInterval(t);
  }, []);
  const t = new Date(iso).getTime();
  const now = Date.now();
  const diffMs = now - t;
  const sec = Math.round(diffMs / 1000);
  if (sec < 60) return <>just now</>;
  const min = Math.round(sec / 60);
  if (min < 60) return <>{min}m ago</>;
  const hr = Math.round(min / 60);
  if (hr < 24) return <>{hr}h ago</>;
  const day = Math.round(hr / 24);
  return <>{day}d ago</>;
}
