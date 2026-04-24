import { useEffect, useState } from "preact/hooks";
import { api } from "../../../api/client.ts";
import type { SignalDto } from "../../../../server/routes/run-extras.ts";
import { EmptyState } from "../../ui/empty-state.tsx";
import { JsonBlock } from "../../ui/json-block.tsx";
import { formatRelative } from "../../../lib/format.ts";

export function SignalsTab({ runId }: { runId: string }) {
  const [data, setData] = useState<SignalDto[] | undefined>(undefined);
  const [error, setError] = useState<string | undefined>(undefined);

  useEffect(() => {
    let cancelled = false;
    api
      .getRunSignals(runId)
      .then((r) => !cancelled && setData(r.signals))
      .catch((e) => !cancelled && setError(String(e)));
    return () => {
      cancelled = true;
    };
  }, [runId]);

  if (error) return <div class="alert alert-error text-sm">{error}</div>;
  if (!data) return <div class="text-sm text-base-content/50">Loading…</div>;
  if (data.length === 0) {
    return (
      <EmptyState
        message="No signals delivered to this run."
        hint="Signals are async inputs — click 'Send signal' above to deliver one."
      />
    );
  }

  return (
    <div class="space-y-2">
      {data.map((s) => (
        <div class="card bg-base-200">
          <div class="card-body p-3 space-y-2">
            <div class="flex items-center gap-2">
              <span class="badge badge-sm badge-info">📨</span>
              <span class="font-mono text-sm">{s.signalName}</span>
              <div class="flex-1" />
              <span class="text-xs text-base-content/60">{formatRelative(s.deliveredAt)}</span>
            </div>
            {s.payload !== null && s.payload !== undefined && (
              <JsonBlock value={s.payload} maxH="max-h-40" />
            )}
          </div>
        </div>
      ))}
    </div>
  );
}
