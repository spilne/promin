// ---------------------------------------------------------------------------
// DeploymentList — top-level deployments view. One stacked timeline row per
// registered workflow, showing all version records with status badges +
// promote/rollback affordances.
//
// Backed by the lifecycle methods on WorkflowVersionRegistry (added in the
// registry-lifecycle commit). Coordinator routing on `findActive` is
// deferred — the UI surfaces the model so operators can promote/rollback;
// runs continue to dispatch via the existing `latest` rule until Phase B.
// ---------------------------------------------------------------------------

import { useEffect, useMemo, useState } from "preact/hooks";
import { useFetch } from "../../hooks/use-fetch.ts";
import { api, ApiError } from "../../api/client.ts";
import type { DeploymentDto } from "../../../server/routes/deployments.ts";
import { Page } from "../ui/page.tsx";
import { SkeletonRows } from "../ui/skeleton.tsx";
import { EmptyState } from "../ui/empty-state.tsx";
import { confirm, toast } from "../../lib/dialogs.ts";

interface Props {
  /** Drill into the runs list filtered by (workflowName, version). */
  onOpenRuns: (name: string, version: string) => void;
}

export function DeploymentList({ onOpenRuns }: Props) {
  const { data, loading, error, refresh } = useFetch(() => api.listWorkflowDefs(), [], 30_000);

  const workflowNames = useMemo(() => (data?.workflows ?? []).map((w) => w.name).sort(), [data]);

  return (
    <Page>
      <div>
        <h1 class="text-2xl font-semibold">Deployments</h1>
        <p class="text-sm text-base-content/60 mt-1">
          Promote or roll back which version a workflow's new runs target.
        </p>
      </div>
      {error && (
        <div class="alert alert-error mb-4">
          Failed to load workflows: {(error as Error).message}
        </div>
      )}
      {loading && !data && <SkeletonRows rows={4} />}
      {!loading && workflowNames.length === 0 && (
        <EmptyState
          message="No registered workflows"
          hint="Register a workflow definition to see its version timeline here."
        />
      )}
      <div class="space-y-4">
        {workflowNames.map((name) => (
          <DeploymentTimelineRow
            key={name}
            name={name}
            onOpenRuns={onOpenRuns}
            onChange={refresh}
          />
        ))}
      </div>
    </Page>
  );
}

interface RowProps {
  name: string;
  onOpenRuns: (name: string, version: string) => void;
  onChange: () => void;
}

function DeploymentTimelineRow({ name, onOpenRuns, onChange }: RowProps) {
  const { data, loading, error, refresh } = useFetch(
    () => api.listDeployments(name),
    [name],
    10_000,
  );
  const [actingVersion, setActingVersion] = useState<string | null>(null);

  const deployments = data?.deployments ?? [];
  const sorted = useMemo(() => {
    // registeredAt-desc from server; we want newest at the right edge of
    // the timeline (timeline reads left = oldest, right = newest).
    return [...deployments].sort(
      (a, b) => new Date(a.registeredAt).getTime() - new Date(b.registeredAt).getTime(),
    );
  }, [deployments]);

  const active = sorted.find((d) => d.status === "active");

  const promote = async (version: string): Promise<void> => {
    setActingVersion(version);
    try {
      await api.promoteDeployment(name, version);
      toast(`Promoted ${name}@${version}`, { variant: "success" });
      refresh();
      onChange();
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
      onChange();
    } catch (err) {
      const msg = err instanceof ApiError ? err.message : String(err);
      toast(`Rollback failed: ${msg}`, { variant: "error" });
    } finally {
      setActingVersion(null);
    }
  };

  return (
    <div class="card bg-base-100 border border-base-content/10">
      <div class="card-body p-4">
        <div class="flex items-center justify-between mb-3">
          <h3 class="font-semibold">{name}</h3>
          <div class="text-xs text-base-content/60">
            {active ? (
              <>
                active: <span class="font-mono text-base-content/90">{active.version}</span>
              </>
            ) : (
              <span class="italic">no active version</span>
            )}
          </div>
        </div>

        {error && <div class="text-error text-sm">Failed to load deployments: {error.message}</div>}
        {loading && !data && <SkeletonRows rows={1} />}
        {!loading && sorted.length === 0 && (
          <div class="text-sm text-base-content/50 italic">No registered versions.</div>
        )}

        {sorted.length > 0 && (
          <ul class="divide-y divide-base-content/5">
            {sorted.map((d) => (
              <DeploymentRow
                key={d.version}
                deployment={d}
                isActive={active?.version === d.version}
                hasActive={!!active}
                acting={actingVersion === d.version}
                onPromote={() => promote(d.version)}
                onRollback={() => rollback(d.version)}
                onOpenRuns={() => onOpenRuns(name, d.version)}
              />
            ))}
          </ul>
        )}
      </div>
    </div>
  );
}

interface DeploymentRowProps {
  deployment: DeploymentDto;
  isActive: boolean;
  hasActive: boolean;
  acting: boolean;
  onPromote: () => void;
  onRollback: () => void;
  onOpenRuns: () => void;
}

function DeploymentRow({
  deployment: d,
  isActive,
  hasActive,
  acting,
  onPromote,
  onRollback,
  onOpenRuns,
}: DeploymentRowProps) {
  return (
    <li class="py-2 flex items-center gap-3 text-sm">
      <StatusBadge status={d.status} />
      <span class="font-mono text-base-content/90">{d.version}</span>
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
