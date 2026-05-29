// ---------------------------------------------------------------------------
// RolesPage — gallery of cloneable role templates (recipes with
// `metadata.template: true`). The closest equivalent to oh-my-pi's "pick a
// role and start from a tuned baseline" UX. The user picks a role, clones
// it via the existing clone dialog, and lands on the new agent's detail.
// ---------------------------------------------------------------------------

import { useMemo, useState } from "preact/hooks";
import { useFetch } from "../../hooks/use-fetch.ts";
import { api } from "../../api/client.ts";
import type { RegisteredAgent } from "../../../server/routes/agents.ts";
import { Page } from "../ui/page.tsx";
import { SkeletonRows } from "../ui/skeleton.tsx";
import { AgentCloneDialog } from "../agents/agent-clone-dialog.tsx";

interface RolesPageProps {
  /** Opens a particular agent detail (used after cloning). */
  onOpenAgent: (id: string) => void;
}

export function RolesPage({ onOpenAgent }: RolesPageProps) {
  const { data, loading, error } = useFetch(() => api.listAgents(), [], 30_000);
  const [query, setQuery] = useState("");
  const [cloneSource, setCloneSource] = useState<RegisteredAgent | null>(null);

  const roles = useMemo(() => {
    const list = (data?.agents ?? []).filter((a) => a.metadata.template === true);
    const q = query.trim().toLowerCase();
    if (!q) return list;
    return list.filter((a) => {
      const fields = [
        a.id,
        a.metadata.description ?? "",
        ...a.metadata.capabilities,
        ...a.metadata.tags,
      ];
      return fields.some((f) => f.toLowerCase().includes(q));
    });
  }, [data, query]);

  return (
    <Page>
      <div>
        <h2 class="text-xl font-semibold">Roles</h2>
        <p class="text-xs text-base-content/50 max-w-2xl">
          Cloneable role templates — pick one, fork it with your own id and credentials, and start
          from a tuned baseline (system prompt + fragment layers + relevant skills already wired).
        </p>
      </div>

      {error && <div class="alert alert-error text-xs">{error.message}</div>}

      <input
        class="input input-bordered input-sm font-mono max-w-md"
        placeholder="Search roles…"
        value={query}
        onInput={(e) => setQuery((e.target as HTMLInputElement).value)}
      />

      {loading && !data ? (
        <div class="card bg-base-100 shadow">
          <div class="card-body">
            <SkeletonRows rows={3} cols={1} />
          </div>
        </div>
      ) : roles.length === 0 ? (
        <div class="card bg-base-100 shadow">
          <div class="card-body py-8 text-center text-base-content/50">
            {data?.agents.length === 0
              ? "No agents registered yet."
              : query
                ? `No roles match "${query}".`
                : "No role templates registered. Drop a recipe with metadata.template: true into the scan folder."}
          </div>
        </div>
      ) : (
        <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-3">
          {roles.map((a) => (
            <RoleCard role={a} onClone={() => setCloneSource(a)} />
          ))}
        </div>
      )}

      {cloneSource && (
        <AgentCloneDialog
          source={cloneSource}
          onClose={() => setCloneSource(null)}
          onCloned={(newId) => {
            setCloneSource(null);
            onOpenAgent(newId);
          }}
        />
      )}
    </Page>
  );
}

function RoleCard({ role, onClone }: { role: RegisteredAgent; onClone: () => void }) {
  const model =
    role.backend.type === "local"
      ? `${role.backend.model.provider}/${role.backend.model.id}`
      : role.backend.type;
  const isLayered =
    role.backend.type === "local" &&
    typeof role.backend.systemPrompt === "object" &&
    role.backend.systemPrompt !== null;
  const layers =
    isLayered && role.backend.type === "local"
      ? ((role.backend.systemPrompt as { layers?: ReadonlyArray<string> }).layers ?? [])
      : [];
  const skills = role.backend.type === "local" ? (role.backend.skills ?? []) : [];

  return (
    <div class="card bg-base-100 shadow hover:shadow-md transition-shadow">
      <div class="card-body p-4 space-y-2">
        <div class="flex items-start justify-between gap-2">
          <h3 class="font-mono text-sm font-semibold">{role.id}</h3>
          <span class="badge badge-xs badge-outline font-mono">{model}</span>
        </div>
        {role.metadata.description && (
          <p class="text-xs text-base-content/70">{role.metadata.description}</p>
        )}
        <div class="flex flex-wrap gap-1">
          {role.metadata.capabilities.map((c) => (
            <span class="badge badge-xs badge-info badge-outline">{c}</span>
          ))}
          {role.metadata.tags.map((t) => (
            <span class="badge badge-xs badge-ghost">{t}</span>
          ))}
        </div>
        {(layers.length > 0 || skills.length > 0) && (
          <div class="text-[10px] text-base-content/50 space-y-0.5 pt-1 border-t border-base-300">
            {layers.length > 0 && (
              <div>
                <span class="opacity-70">layers: </span>
                <span class="font-mono">{layers.join(", ")}</span>
              </div>
            )}
            {skills.length > 0 && (
              <div>
                <span class="opacity-70">skills: </span>
                <span class="font-mono">{skills.map((s) => s.id).join(", ")}</span>
              </div>
            )}
          </div>
        )}
        <div class="flex justify-end pt-1">
          <button type="button" class="btn btn-xs btn-primary" onClick={onClone}>
            🍴 Clone &amp; customize
          </button>
        </div>
      </div>
    </div>
  );
}
