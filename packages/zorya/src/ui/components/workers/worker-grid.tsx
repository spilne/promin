import { useFetch } from "../../hooks/use-fetch.ts";
import { api } from "../../api/client.ts";
import type { WorkerDto } from "../../../server/api-types.ts";
import { formatRelative } from "../../lib/format.ts";

export function WorkerGrid() {
  const { data, loading, error } = useFetch(() => api.listWorkers(), [], 5000);

  if (loading && !data) {
    return <div class="p-4 max-w-7xl mx-auto text-base-content/60">Loading workers…</div>;
  }
  if (error) {
    return (
      <div class="p-4 max-w-7xl mx-auto">
        <div class="alert alert-error">{error.message}</div>
      </div>
    );
  }
  const workers = data?.workers ?? [];
  const online = workers.filter((w) => w.status === "online").length;
  const offline = workers.length - online;

  return (
    <div class="p-4 max-w-7xl mx-auto space-y-4">
      <div class="flex items-center gap-2">
        <h2 class="text-xl font-semibold">Workers</h2>
        <span class="text-base-content/60">
          · {online} online
          {offline > 0 && `, ${offline} offline`}
        </span>
      </div>

      {workers.length === 0 && (
        <div class="card bg-base-100 shadow">
          <div class="card-body py-8 text-center text-base-content/50">No workers registered</div>
        </div>
      )}

      <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-3">
        {workers.map((w) => (
          <WorkerCard worker={w} />
        ))}
      </div>
    </div>
  );
}

function WorkerCard({ worker }: { worker: WorkerDto }) {
  const online = worker.status === "online";
  return (
    <div class="card bg-base-100 shadow">
      <div class="card-body p-4">
        <div class="flex items-center gap-2">
          <span class={`w-2.5 h-2.5 rounded-full ${online ? "bg-success" : "bg-error"}`} />
          <span class="font-mono text-sm">{worker.workerId}</span>
          <div class="flex-1" />
          {worker.queue && <span class="badge badge-ghost badge-sm">{worker.queue}</span>}
        </div>
        <dl class="text-xs text-base-content/70 mt-2 grid grid-cols-2 gap-y-1">
          <dt>Active</dt>
          <dd class="font-mono text-right">{worker.activeTasks}</dd>
          <dt>Done today</dt>
          <dd class="font-mono text-right">{worker.completedToday}</dd>
          {worker.lastHeartbeatAt && (
            <>
              <dt>Last seen</dt>
              <dd class="text-right">{formatRelative(worker.lastHeartbeatAt)}</dd>
            </>
          )}
        </dl>
      </div>
    </div>
  );
}
