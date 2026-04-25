// ---------------------------------------------------------------------------
// Agent config drawer — read-only view of the registered recipe.
//
// Shows what's stored in the AgentRegistry for this agent: model, system
// prompt, tool list, metadata (description / capabilities / tags),
// timestamps, and any extra knobs in `backend.extra`. All fields are
// rendered with an explicit "Read-only" framing — editing lands when the
// Agent Designer (promin-gsze) ships.
//
// Runtime config (autoCompact / autoDistill / contextBudget) is NOT in the
// recipe — it lives in the host's resolver — so this drawer doesn't show
// it. The header notes that distinction so an operator looking for "why
// does this thread auto-compact at message 12?" knows where to look
// (host code, not the recipe).
// ---------------------------------------------------------------------------

import { useEffect } from "preact/hooks";
import type { RegisteredAgent } from "../../../server/routes/agents.ts";
import { Skeleton } from "../ui/skeleton.tsx";
import { JsonBlock } from "../ui/json-block.tsx";
import { formatRelative } from "../../lib/format.ts";

interface Props {
  agent: RegisteredAgent | undefined;
  onClose: () => void;
}

export function AgentConfigDrawer({ agent, onClose }: Props) {
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => e.key === "Escape" && onClose();
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onClose]);

  return (
    <>
      <div class="fixed inset-0 bg-black/40 z-30 anim-backdrop-in" onClick={onClose} aria-hidden />
      <aside
        class="fixed top-0 right-0 h-screen w-full max-w-2xl bg-base-100 shadow-2xl
               z-40 flex flex-col anim-drawer-in"
        role="dialog"
        aria-label="Agent config"
      >
        <header class="flex items-start justify-between gap-2 p-4 border-b border-base-300">
          <div class="min-w-0">
            <div class="text-xs text-base-content/50 uppercase tracking-wider">Agent config</div>
            <div class="font-mono text-sm truncate">{agent?.id ?? "…"}</div>
            <div class="text-[10px] text-base-content/40 mt-0.5">
              Read-only recipe. Runtime knobs (auto-compact, context budget) live in the
              host'&apos;s resolver, not here.
            </div>
          </div>
          <button
            class="btn btn-sm btn-ghost"
            onClick={onClose}
            aria-label="Close"
            title="Close (Esc)"
          >
            ✕
          </button>
        </header>

        <div class="flex-1 overflow-y-auto p-4 space-y-4">
          {!agent ? (
            <div class="space-y-2">
              <Skeleton w="w-full" h="h-4" />
              <Skeleton w="w-3/4" h="h-4" />
              <Skeleton w="w-1/2" h="h-4" />
            </div>
          ) : (
            <RecipeBody agent={agent} />
          )}
        </div>
      </aside>
    </>
  );
}

function RecipeBody({ agent }: { agent: RegisteredAgent }) {
  const isLocal = agent.backend.type === "local";
  const local = isLocal ? agent.backend : null;
  return (
    <>
      <Section label="Identity">
        <KV k="id" v={<span class="font-mono">{agent.id}</span>} />
        <KV k="version" v={<span class="font-mono">{agent.version}</span>} />
        <KV k="backend" v={<span class="font-mono">{agent.backend.type}</span>} />
        {agent.metadata.description && <KV k="description" v={agent.metadata.description} />}
        <KV
          k="created"
          v={
            <span class="text-base-content/70">
              {new Date(agent.createdAt).toISOString()}{" "}
              <span class="text-base-content/40">
                ({formatRelative(new Date(agent.createdAt).toISOString())})
              </span>
            </span>
          }
        />
        <KV
          k="updated"
          v={
            <span class="text-base-content/70">
              {new Date(agent.updatedAt).toISOString()}{" "}
              <span class="text-base-content/40">
                ({formatRelative(new Date(agent.updatedAt).toISOString())})
              </span>
            </span>
          }
        />
      </Section>

      {local && (
        <Section label="Model">
          <KV k="provider" v={<span class="font-mono">{local.model.provider}</span>} />
          <KV k="id" v={<span class="font-mono">{local.model.id}</span>} />
          {local.maxStepsPerTurn !== undefined && (
            <KV k="maxStepsPerTurn" v={<span class="font-mono">{local.maxStepsPerTurn}</span>} />
          )}
          {local.maxTurns !== undefined && (
            <KV k="maxTurns" v={<span class="font-mono">{local.maxTurns}</span>} />
          )}
        </Section>
      )}

      {local && (
        <Section label="System prompt">
          {local.systemPrompt ? (
            <pre class="bg-base-200 p-3 rounded text-xs whitespace-pre-wrap break-words font-mono leading-relaxed max-h-[40vh] overflow-y-auto">
              {local.systemPrompt}
            </pre>
          ) : (
            <div class="text-xs text-base-content/40 italic">
              (none — agent runs without a static system prompt; the cascade-resolved one from
              MemoryStore.resolveContext is the only system content)
            </div>
          )}
        </Section>
      )}

      {local && (
        <Section label={`Tools (${local.tools.length})`}>
          {local.tools.length === 0 ? (
            <div class="text-xs text-base-content/40 italic">(none)</div>
          ) : (
            <div class="flex gap-1 flex-wrap">
              {local.tools.map((t) => (
                <span class="badge badge-outline font-mono text-xs">{t}</span>
              ))}
            </div>
          )}
          <div class="text-[10px] text-base-content/40 mt-2">
            Tool implementations are wired by the host (resolver), not stored on the recipe. Names
            listed here must match a key in the host'&apos;s tool map.
          </div>
        </Section>
      )}

      <Section label="Metadata">
        {agent.metadata.capabilities.length > 0 && (
          <KV
            k="capabilities"
            v={
              <div class="flex gap-1 flex-wrap">
                {agent.metadata.capabilities.map((c) => (
                  <span class="badge badge-sm badge-info badge-outline">{c}</span>
                ))}
              </div>
            }
          />
        )}
        {agent.metadata.tags.length > 0 && (
          <KV
            k="tags"
            v={
              <div class="flex gap-1 flex-wrap">
                {agent.metadata.tags.map((t) => (
                  <span class="badge badge-sm badge-ghost">{t}</span>
                ))}
              </div>
            }
          />
        )}
        {agent.metadata.capabilities.length === 0 && agent.metadata.tags.length === 0 && (
          <div class="text-xs text-base-content/40 italic">(none)</div>
        )}
      </Section>

      {local && local.extra && Object.keys(local.extra).length > 0 && (
        <Section label="Extra (backend.extra)">
          <JsonBlock value={local.extra} maxH="max-h-60" />
        </Section>
      )}

      <div class="alert alert-info text-xs">
        <span>
          Editing lands with the Agent Designer (promin-gsze). For now, change the recipe via{" "}
          <code class="bg-base-300 px-1 rounded">examples/agents/*.ts</code> or{" "}
          <code class="bg-base-300 px-1 rounded">agentRegistry.register(...)</code>.
        </span>
      </div>
    </>
  );
}

function Section({ label, children }: { label: string; children: preact.ComponentChildren }) {
  return (
    <section>
      <div class="text-[10px] uppercase tracking-wider text-base-content/50 mb-2">{label}</div>
      <div class="space-y-1.5">{children}</div>
    </section>
  );
}

function KV({ k, v }: { k: string; v: preact.ComponentChildren }) {
  return (
    <div class="grid grid-cols-[120px_1fr] gap-3 text-xs items-baseline">
      <span class="text-base-content/50 font-mono">{k}</span>
      <span class="break-words">{v}</span>
    </div>
  );
}

import type * as preact from "preact";
