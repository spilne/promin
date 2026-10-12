// ---------------------------------------------------------------------------
// VersionsPanel — the version history of one registry entry (role, skill,
// fragment, DAG): newest first, the active one marked, any other one made
// active with one click (publish a draft, or roll back). The promote is
// conditional on the active version the panel showed, so a concurrent
// change answers 412 instead of being overwritten.
// ---------------------------------------------------------------------------

import { useMemo, useState } from "preact/hooks";
import { useFetch } from "../../hooks/use-fetch.ts";
import { formatRelative } from "../../lib/format.ts";

export interface VersionRow {
  readonly version: string;
  readonly active: boolean;
  readonly createdAt: number;
  readonly trust?: "trusted" | "needs-review";
}

interface Props<T extends VersionRow, R> {
  /** Changes whenever the panel should re-read (e.g. the entry id). */
  readonly reloadKey: string;
  readonly load: () => Promise<{ versions: T[] }>;
  readonly promote: (params: { version: string; expectedVersion?: string }) => Promise<R>;
  /** Called with the promote's answer (the new active version). */
  readonly onPromoted?: (promoted: R) => void;
  /** One short line about a version (e.g. a content preview). */
  readonly describe?: (row: T) => string;
}

export function VersionsPanel<T extends VersionRow, R>({
  reloadKey,
  load,
  promote,
  onPromoted,
  describe,
}: Props<T, R>) {
  const { data, loading, error, refresh } = useFetch(load, [reloadKey], 0);
  const versions = useMemo(() => [...(data?.versions ?? [])].reverse(), [data]);
  const active = versions.find((v) => v.active)?.version;
  const [busy, setBusy] = useState<string | null>(null);
  const [promoteError, setPromoteError] = useState<string | null>(null);

  const makeActive = async (version: string) => {
    setBusy(version);
    setPromoteError(null);
    try {
      const promoted = await promote({
        version,
        ...(active !== undefined && { expectedVersion: active }),
      });
      refresh();
      if (onPromoted) onPromoted(promoted);
    } catch (err) {
      setPromoteError(err instanceof Error ? err.message : String(err));
    } finally {
      setBusy(null);
    }
  };

  return (
    <div class="form-control">
      <span class="text-xs text-base-content/60 mb-1 uppercase tracking-wider">
        Versions
        <span class="ml-2 text-[10px] text-base-content/40 normal-case tracking-normal">
          saving publishes a new version; earlier ones stay here
        </span>
      </span>
      {error && <div class="alert alert-error text-xs">{error.message}</div>}
      {promoteError && <div class="alert alert-error text-xs">{promoteError}</div>}
      {loading && !data ? (
        <div class="text-[11px] text-base-content/40">Loading…</div>
      ) : (
        <ul class="space-y-1">
          {versions.map((v) => (
            <li class="flex items-center gap-2 bg-base-200 rounded px-2 py-1 text-xs">
              <span class="font-mono">{v.version}</span>
              <span class="text-[10px] text-base-content/40">
                {formatRelative(new Date(v.createdAt).toISOString())}
              </span>
              {v.trust === "needs-review" && (
                <span class="badge badge-xs badge-warning">needs review</span>
              )}
              <span class="flex-1 truncate text-[10px] text-base-content/50">
                {describe ? describe(v) : ""}
              </span>
              {v.active ? (
                <span class="badge badge-xs badge-success">active</span>
              ) : (
                <button
                  type="button"
                  class="btn btn-xs btn-outline"
                  disabled={busy !== null}
                  title="Make this the active version"
                  onClick={() => void makeActive(v.version)}
                >
                  {busy === v.version ? "…" : "Make active"}
                </button>
              )}
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}
