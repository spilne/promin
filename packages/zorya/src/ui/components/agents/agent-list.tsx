import { useMemo, useState } from "preact/hooks";
import { useFetch } from "../../hooks/use-fetch.ts";
import { api } from "../../api/client.ts";
import type { RegisteredAgent } from "../../../server/routes/agents.ts";
import { Page } from "../ui/page.tsx";
import { SkeletonRows } from "../ui/skeleton.tsx";
import { formatRelative } from "../../lib/format.ts";

interface AgentListProps {
  onOpen: (id: string) => void;
}

export function AgentList({ onOpen }: AgentListProps) {
  const { data, loading, error, refresh } = useFetch(() => api.listAgents(), [], 30_000);
  const { data: sourcesData } = useFetch(() => api.listAgentSources(), [], 30_000);
  const fileManaged = useMemo(() => new Set(sourcesData?.fileManaged ?? []), [sourcesData]);
  const [query, setQuery] = useState("");

  const agents = data?.agents ?? [];
  const filtered = useMemo(() => {
    const q = query.trim().toLowerCase();
    if (!q) return agents;
    return agents.filter((a) => {
      const fields = [
        a.id,
        a.metadata.description ?? "",
        ...a.metadata.capabilities,
        ...a.metadata.tags,
        a.backend.type === "local" ? `${a.backend.model.provider}/${a.backend.model.id}` : "",
      ];
      return fields.some((f) => f.toLowerCase().includes(q));
    });
  }, [agents, query]);

  if (loading && !data) {
    return (
      <Page>
        <div>
          <h2 class="text-xl font-semibold">Agents</h2>
          <p class="text-xs text-base-content/50">Loading…</p>
        </div>
        <div class="card bg-base-100 shadow overflow-hidden">
          <table class="table">
            <thead>
              <tr class="bg-base-200 text-xs uppercase tracking-wider text-base-content/50">
                <th>Agent</th>
                <th>Model</th>
                <th>Tools</th>
                <th>Capabilities</th>
                <th>Tags</th>
                <th>Updated</th>
              </tr>
            </thead>
            <tbody>
              <SkeletonRows rows={4} cols={6} />
            </tbody>
          </table>
        </div>
      </Page>
    );
  }
  if (error) {
    return (
      <Page space="none">
        <div class="alert alert-error">{error.message}</div>
      </Page>
    );
  }

  const configured = data !== undefined;

  return (
    <Page>
      <div class="flex items-end justify-between">
        <div>
          <h2 class="text-xl font-semibold">Agents</h2>
          <p class="text-xs text-base-content/50">
            {configured
              ? `${agents.length} registered · auto-refreshes every 30s`
              : "Agent gateway not configured on this server"}
          </p>
        </div>
        <button class="btn btn-sm btn-ghost gap-1" onClick={() => refresh()}>
          <span>↻</span>
          Refresh
        </button>
      </div>

      {agents.length === 0 && (
        <div class="card bg-base-100 shadow">
          <div class="card-body py-8 text-center text-base-content/50">
            No agents registered. Pass <code class="bg-base-300 px-1 rounded">agents</code> to{" "}
            <code class="bg-base-300 px-1 rounded">ZoryaServer</code> with a populated registry.
          </div>
        </div>
      )}

      {agents.length > 0 && (
        <div class="flex items-center justify-end">
          <input
            class="input input-bordered input-sm w-full max-w-md font-mono"
            placeholder="Search by id, capability, tag, model…"
            value={query}
            onInput={(e) => setQuery((e.target as HTMLInputElement).value)}
          />
        </div>
      )}

      {agents.length > 0 && filtered.length === 0 && (
        <div class="card bg-base-100 shadow">
          <div class="card-body py-8 text-center text-base-content/50">
            No agents match "{query}".
          </div>
        </div>
      )}

      {filtered.length > 0 && (
        <div class="card bg-base-100 shadow overflow-hidden">
          <div class="overflow-x-auto">
            <table class="table">
              <thead>
                <tr class="bg-base-200 text-xs uppercase tracking-wider text-base-content/50">
                  <th>Agent</th>
                  <th>Model</th>
                  <th>Tools</th>
                  <th>Capabilities</th>
                  <th>Tags</th>
                  <th>Updated</th>
                </tr>
              </thead>
              <tbody>
                {filtered.map((a) => (
                  <AgentRow agent={a} onOpen={onOpen} fileManaged={fileManaged.has(a.id)} />
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </Page>
  );
}

function AgentRow({
  agent,
  onOpen,
  fileManaged,
}: {
  agent: RegisteredAgent;
  onOpen: (id: string) => void;
  fileManaged: boolean;
}) {
  const model =
    agent.backend.type === "local"
      ? `${agent.backend.model.provider}/${agent.backend.model.id}`
      : "—";
  const tools = agent.backend.type === "local" ? agent.backend.tools : [];
  return (
    <tr class="hover:bg-base-200 cursor-pointer" onClick={() => onOpen(agent.id)}>
      <td>
        <div class="flex flex-col">
          <button
            class="link link-hover font-mono text-left"
            onClick={(e) => {
              e.stopPropagation();
              onOpen(agent.id);
            }}
          >
            {agent.id}
          </button>
          {agent.metadata.description && (
            <span class="text-xs text-base-content/60">{agent.metadata.description}</span>
          )}
          <span class="text-[10px] text-base-content/40 font-mono">v{agent.version}</span>
        </div>
      </td>
      <td class="font-mono text-xs">{model}</td>
      <td class="text-sm">
        {tools.length > 0 ? (
          <span class="font-mono text-xs">{tools.join(", ")}</span>
        ) : (
          <span class="text-base-content/40">—</span>
        )}
      </td>
      <td>
        {agent.metadata.capabilities.length === 0 ? (
          <span class="text-base-content/40">—</span>
        ) : (
          <div class="flex gap-1 flex-wrap">
            {agent.metadata.capabilities.map((c) => (
              <span class="badge badge-sm badge-info badge-outline">{c}</span>
            ))}
          </div>
        )}
      </td>
      <td>
        {!fileManaged && agent.metadata.tags.length === 0 ? (
          <span class="text-base-content/40">—</span>
        ) : (
          <div class="flex gap-1 flex-wrap">
            {fileManaged && (
              <span
                class="badge badge-sm badge-info gap-1"
                title="Defined by a file on disk — edit the source file, not in the UI"
              >
                📄 file
              </span>
            )}
            {agent.metadata.tags.map((t) => (
              <span class="badge badge-sm badge-ghost">{t}</span>
            ))}
          </div>
        )}
      </td>
      <td class="text-sm text-base-content/60">
        {formatRelative(new Date(agent.updatedAt).toISOString())}
      </td>
    </tr>
  );
}
