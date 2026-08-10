// ---------------------------------------------------------------------------
// System (auto-injected) tools — the tools the resolver attaches to a live
// agent that DON'T live in `backend.tools`: `loadSkill` (when the agent has
// skills), RAG search tools (when `backend.knowledge` exposes retrievers as
// tools), and `findAgent` / `callAgent` (when `backend.network` is set).
//
// These are per-agent and conditional, so they're a computed view of the
// recipe / form state — not entries in the host-wide tool catalog. Surfaced
// read-only so operators see they're present and why, without being able to
// toggle them (the resolver owns them).
// ---------------------------------------------------------------------------

export interface SystemTool {
  readonly name: string;
  readonly reason: string;
}

export interface KnowledgeSystemToolBinding {
  readonly id: string;
  readonly name?: string;
  readonly mode?: "tool" | "context" | "tool-and-context";
}

export function systemToolsFor(opts: {
  readonly skillCount: number;
  readonly knowledgeToolCount?: number;
  readonly knowledgeTools?: ReadonlyArray<KnowledgeSystemToolBinding>;
  readonly hasNetwork: boolean;
}): SystemTool[] {
  const out: SystemTool[] = [];
  if (opts.skillCount > 0) {
    out.push({
      name: "loadSkill",
      reason: `Auto-injected because this agent has ${opts.skillCount} skill${
        opts.skillCount === 1 ? "" : "s"
      }. The model calls it to load a skill's instructions on demand.`,
    });
  }
  const knowledgeTools: ReadonlyArray<KnowledgeSystemToolBinding> =
    opts.knowledgeTools ??
    Array.from({ length: opts.knowledgeToolCount ?? 0 }, () => ({
      id: "default",
    }));
  for (const binding of knowledgeTools.filter((k) => k.id && k.mode !== "context")) {
    out.push({
      name: binding.name?.trim() || retrieverToolName(binding.id),
      reason: "Auto-injected because this agent exposes this knowledge binding as a tool.",
    });
  }
  if (opts.hasNetwork) {
    out.push({
      name: "findAgent",
      reason: "Auto-injected because this agent opts into the network — discovers peer agents.",
    });
    out.push({
      name: "callAgent",
      reason: "Auto-injected because this agent opts into the network — delegates to peer agents.",
    });
  }
  return out;
}

function retrieverToolName(id: string): string {
  if (id === "default") return "search_knowledge_base";
  return `search_${id.replace(/[^A-Za-z0-9_]+/g, "_").replace(/^_+|_+$/g, "") || "knowledge"}`;
}

/** Read-only chips for the auto-injected system tools. Renders nothing when empty. */
export function SystemToolChips({ tools }: { tools: ReadonlyArray<SystemTool> }) {
  if (tools.length === 0) return null;
  return (
    <div class="flex flex-wrap gap-1">
      {tools.map((t) => (
        <span class="badge badge-sm badge-ghost gap-1 font-mono cursor-help" title={t.reason}>
          <span class="opacity-60">⚙</span>
          {t.name}
        </span>
      ))}
    </div>
  );
}

/** Labeled section used in the agent editor. */
export function SystemToolsSection({ tools }: { tools: ReadonlyArray<SystemTool> }) {
  if (tools.length === 0) return null;
  return (
    <div class="form-control">
      <span class="text-xs text-base-content/60 mb-1 uppercase tracking-wider">
        System tools
        <span class="ml-2 text-[10px] text-base-content/40 normal-case tracking-normal">
          auto-injected at resolve · not editable
        </span>
      </span>
      <SystemToolChips tools={tools} />
    </div>
  );
}
