// ---------------------------------------------------------------------------
// "Your agents" — flat list of AgentInstance rows for the active tenant.
// Each row is one (registeredAgentId, namespaceId, ownerId) tuple with its
// own working memory + facts + threads in the cascade. The page lets the
// operator filter, rename, and wipe.
//
// Filter UX
// ---------
// Namespace comes from the global sidebar switcher (`useNamespace`) so the
// whole dashboard scopes consistently. Only Owner stays on this page, since
// it's an instance-level dimension the global selector doesn't cover.
// ---------------------------------------------------------------------------

import { useEffect, useMemo, useState } from "preact/hooks";
import { useNamespace } from "../../hooks/use-namespace.ts";
import { instancesApi, type AgentInstanceDto } from "../../api/client.ts";
import { Page, PageHeader } from "../ui/page.tsx";
import { SkeletonRows } from "../ui/skeleton.tsx";
import { formatRelative } from "../../lib/format.ts";

interface Props {
  onOpenAgent: (id: string) => void;
}

const DEFAULT_OWNER = "alice";

export function InstanceList({ onOpenAgent }: Props) {
  const [globalNamespace] = useNamespace();
  const namespaceId = globalNamespace || "default";
  const [ownerId, setOwnerId] = useState(DEFAULT_OWNER);
  const [data, setData] = useState<AgentInstanceDto[] | undefined>(undefined);
  const [error, setError] = useState<string | null>(null);
  const [refreshTick, setRefreshTick] = useState(0);

  // Refetch on filter / refresh changes. The 30s auto-refresh is a sensible
  // default for a list that doesn't change often — operators can hit Refresh
  // for an immediate update.
  useEffect(() => {
    let cancelled = false;
    setError(null);
    instancesApi
      .list({
        ...(namespaceId.trim() && { namespaceId: namespaceId.trim() }),
        ...(ownerId.trim() && { ownerId: ownerId.trim() }),
        limit: 200,
      })
      .then((r) => !cancelled && setData(r.instances))
      .catch((e) => !cancelled && setError(e instanceof Error ? e.message : String(e)));
    const interval = setInterval(() => setRefreshTick((t) => t + 1), 30_000);
    return () => {
      cancelled = true;
      clearInterval(interval);
    };
  }, [namespaceId, ownerId, refreshTick]);

  const groupedByAgent = useMemo(() => {
    if (!data) return new Map<string, AgentInstanceDto[]>();
    const out = new Map<string, AgentInstanceDto[]>();
    for (const inst of data) {
      const list = out.get(inst.registeredAgentId) ?? [];
      list.push(inst);
      out.set(inst.registeredAgentId, list);
    }
    return out;
  }, [data]);

  return (
    <Page>
      <PageHeader
        title="Instances"
        eyebrow="Agent runtime"
        description={<>Long-lived per-owner agent instances across all agents in this namespace.</>}
        actions={
          <button class="btn btn-sm btn-ghost gap-1" onClick={() => setRefreshTick((t) => t + 1)}>
            <span>↻</span>
            Refresh
          </button>
        }
      />

      <div class="card bg-base-100 shadow">
        <div class="card-body gap-3">
          <p class="text-sm">
            An <strong>agent instance</strong> is a long-lived relationship between one agent and
            one owner (a user, team, or any entity you choose). Each instance gets its own isolated
            working memory, facts, and conversation threads — separate from every other (agent,
            owner) pair.
          </p>
          <div class="grid grid-cols-1 sm:grid-cols-2 gap-3 text-xs">
            <div class="rounded-lg border border-base-300 px-3 py-2 space-y-1">
              <div class="font-semibold text-base-content/70">
                Without instances — shared memory
              </div>
              <div class="text-base-content/60">
                All agents share a single memory row for the owner. If{" "}
                <code class="bg-base-200 px-1 rounded">writer</code> learns "Alice prefers terse
                replies", <code class="bg-base-200 px-1 rounded">reviewer</code> sees it too. Good
                when you want a unified view of the owner across agents.
              </div>
            </div>
            <div class="rounded-lg border border-primary/30 bg-primary/5 px-3 py-2 space-y-1">
              <div class="font-semibold text-base-content/70">With instances — isolated memory</div>
              <div class="text-base-content/60">
                Each (agent, owner) pair has its own memory row. Alice's{" "}
                <code class="bg-base-200 px-1 rounded">writer</code> scratchpad is invisible to her{" "}
                <code class="bg-base-200 px-1 rounded">reviewer</code>. Good when agents have
                distinct jobs and shouldn't bleed context into each other.
              </div>
            </div>
          </div>
          <p class="text-xs text-base-content/50">
            Create instances from an agent's detail page — open any agent, click{" "}
            <strong>instances ↗</strong> in the identity bar, then <strong>+ New instance…</strong>
          </p>
        </div>
      </div>

      <div class="card bg-base-100 shadow">
        <div class="card-body py-3 flex-row gap-3 items-end flex-wrap">
          <label class="form-control flex-1 min-w-[180px]">
            <span class="label-text text-xs">Owner</span>
            <input
              class="input input-bordered input-sm font-mono"
              value={ownerId}
              onInput={(e) => setOwnerId((e.target as HTMLInputElement).value)}
              placeholder="(any)"
            />
          </label>
        </div>
      </div>

      {error && <div class="alert alert-error text-xs">{error}</div>}

      {!data && !error && (
        <div class="card bg-base-100 shadow overflow-hidden">
          <table class="table">
            <thead>
              <tr class="bg-base-200 text-xs uppercase tracking-wider text-base-content/50">
                <th>Agent</th>
                <th>Owner</th>
                <th>Display name</th>
                <th>Created</th>
                <th class="text-right">Actions</th>
              </tr>
            </thead>
            <tbody>
              <SkeletonRows rows={3} cols={5} />
            </tbody>
          </table>
        </div>
      )}

      {data && data.length === 0 && (
        <div class="card bg-base-100 shadow">
          <div class="card-body py-8 text-center text-base-content/50 text-sm">
            No instances for{" "}
            <code class="bg-base-300 px-1 rounded font-mono">
              {namespaceId.trim() || "(any namespace)"} · {ownerId.trim() || "(any owner)"}
            </code>
            . Instances are created when you invoke an agent with{" "}
            <code class="bg-base-300 px-1 rounded">ownerId</code> in the body, or via{" "}
            <code class="bg-base-300 px-1 rounded">POST /api/agents/:id/instances</code>.
          </div>
        </div>
      )}

      {data && data.length > 0 && (
        <div class="space-y-4">
          {Array.from(groupedByAgent.entries()).map(([agentId, instances]) => (
            <AgentGroup
              agentId={agentId}
              instances={instances}
              onOpenAgent={onOpenAgent}
              onChange={() => setRefreshTick((t) => t + 1)}
            />
          ))}
        </div>
      )}
    </Page>
  );
}

function AgentGroup({
  agentId,
  instances,
  onOpenAgent,
  onChange,
}: {
  agentId: string;
  instances: AgentInstanceDto[];
  onOpenAgent: (id: string) => void;
  onChange: () => void;
}) {
  return (
    <div class="card bg-base-100 shadow overflow-hidden">
      <div class="card-body py-3 px-4 border-b border-base-300 flex-row items-center gap-2">
        <button
          class="link link-hover font-mono text-sm"
          onClick={() => onOpenAgent(agentId)}
          title="Open agent recipe"
        >
          {agentId}
        </button>
        <span class="text-xs text-base-content/50">
          {instances.length} instance{instances.length === 1 ? "" : "s"}
        </span>
      </div>
      <div class="overflow-x-auto">
        <table class="table">
          <thead>
            <tr class="bg-base-200 text-xs uppercase tracking-wider text-base-content/50">
              <th>Owner</th>
              <th>Display name</th>
              <th>Created</th>
              <th class="text-right">Actions</th>
            </tr>
          </thead>
          <tbody>
            {instances.map((inst) => (
              <InstanceRow instance={inst} onChange={onChange} />
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function InstanceRow({ instance, onChange }: { instance: AgentInstanceDto; onChange: () => void }) {
  const [editing, setEditing] = useState(false);
  const [draft, setDraft] = useState(instance.displayName ?? "");
  const [saving, setSaving] = useState(false);
  const [pendingWipe, setPendingWipe] = useState(false);
  const [wipeMsg, setWipeMsg] = useState<string | null>(null);

  async function rename() {
    setSaving(true);
    try {
      const trimmed = draft.trim();
      await instancesApi.rename(
        instance.registeredAgentId,
        instance.id,
        trimmed.length === 0 ? null : trimmed,
      );
      setEditing(false);
      onChange();
    } catch (e) {
      setWipeMsg(e instanceof Error ? e.message : String(e));
    } finally {
      setSaving(false);
    }
  }

  async function wipe() {
    setPendingWipe(false);
    try {
      const result = await instancesApi.wipe(instance.registeredAgentId, instance.id);
      setWipeMsg(
        `Wiped — ${result.threadsDeleted} thread${result.threadsDeleted === 1 ? "" : "s"}, ${result.factsDeleted} fact${result.factsDeleted === 1 ? "" : "s"}, ${result.episodesDeleted} episode${result.episodesDeleted === 1 ? "" : "s"}.`,
      );
      // Brief delay so the operator sees the toast before the row disappears.
      setTimeout(onChange, 500);
    } catch (e) {
      setWipeMsg(e instanceof Error ? e.message : String(e));
    }
  }

  return (
    <tr class="hover:bg-base-200">
      <td class="font-mono text-xs">{instance.ownerId}</td>
      <td>
        {editing ? (
          <div class="flex gap-1">
            <input
              class="input input-bordered input-xs flex-1"
              value={draft}
              onInput={(e) => setDraft((e.target as HTMLInputElement).value)}
              placeholder="(no name)"
              disabled={saving}
              onKeyDown={(e) => {
                if (e.key === "Enter") void rename();
                if (e.key === "Escape") setEditing(false);
              }}
            />
            <button class="btn btn-xs btn-primary" disabled={saving} onClick={rename}>
              {saving ? "…" : "Save"}
            </button>
            <button
              class="btn btn-xs btn-ghost"
              disabled={saving}
              onClick={() => {
                setDraft(instance.displayName ?? "");
                setEditing(false);
              }}
            >
              ✕
            </button>
          </div>
        ) : (
          <span class={instance.displayName ? "" : "text-base-content/40 italic"}>
            {instance.displayName ?? "(unnamed)"}
          </span>
        )}
        {wipeMsg && <div class="text-[10px] text-base-content/60 mt-1 font-mono">{wipeMsg}</div>}
      </td>
      <td class="text-xs text-base-content/60">
        {formatRelative(new Date(instance.createdAt).toISOString())}
      </td>
      <td class="text-right">
        <div class="flex gap-1 justify-end items-center">
          {!editing && (
            <button
              class="btn btn-xs btn-ghost"
              title="Rename"
              onClick={() => {
                setDraft(instance.displayName ?? "");
                setEditing(true);
                setWipeMsg(null);
              }}
            >
              ✎
            </button>
          )}
          {pendingWipe ? (
            <>
              <button class="btn btn-xs btn-error" onClick={wipe} title="Confirm wipe">
                Wipe
              </button>
              <button
                class="btn btn-xs btn-ghost"
                onClick={() => setPendingWipe(false)}
                title="Cancel"
              >
                ✕
              </button>
            </>
          ) : (
            <button
              class="btn btn-xs btn-ghost text-error/80 hover:text-error"
              title="Wipe (deletes working memory, facts, episodes, threads)"
              onClick={() => {
                setPendingWipe(true);
                setWipeMsg(null);
              }}
            >
              ⌫
            </button>
          )}
        </div>
      </td>
    </tr>
  );
}
