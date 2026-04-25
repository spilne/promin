// ---------------------------------------------------------------------------
// Memory explorer drawer — read-only browser over the three-scope cascade
// (namespace → resource → thread) for the current tenant.
//
// Launched from an agent's chat header (the `⌬ Memory` button), but the
// drawer is decoupled from the chat: switching the inspected thread here
// doesn't change which thread the chat is composing into. Scope picker
// + thread picker live inside the drawer so an operator can browse
// across the whole (namespace, resource) without leaving.
//
// Tabs:
//   Prompt    — resolveContext() output. What the model actually sees.
//   Namespace — tenant-wide static rules + facts + episodes.
//   Resource  — per-user static rules + facts + episodes.
//   Thread    — per-thread working memory + facts + episodes + messages.
//
// Backed by GET /api/memory/inspect (one round-trip per scope change) and
// GET /api/agents/:id/threads (one round-trip per drawer open + after
// each thread switch).
// ---------------------------------------------------------------------------

import { useEffect, useMemo, useState } from "preact/hooks";
import { api, memoryApi } from "../../api/client.ts";
import { Combobox, type ComboboxOption } from "../ui/combobox.tsx";
import type {
  EpisodicRecord,
  Fact,
  MemoryInspectResponse,
  StoredMessage,
} from "../../../server/routes/memory.ts";
import type { ThreadSummary } from "../../../server/routes/agents.ts";
import { Skeleton } from "../ui/skeleton.tsx";
import { JsonBlock } from "../ui/json-block.tsx";
import { formatRelative } from "../../lib/format.ts";

interface Props {
  agentId: string;
  namespaceId: string;
  resourceId?: string;
  /** Initial thread to inspect. Internal state takes over after first render. */
  initialThreadId?: string;
  onClose: () => void;
}

type Tab = "prompt" | "namespace" | "resource" | "thread";

const NO_THREAD = "__none__";

export function MemoryInspector({
  agentId,
  namespaceId,
  resourceId,
  initialThreadId,
  onClose,
}: Props) {
  const [data, setData] = useState<MemoryInspectResponse | undefined>(undefined);
  const [error, setError] = useState<string | undefined>(undefined);
  const [currentThread, setCurrentThread] = useState<string | undefined>(initialThreadId);
  const [tab, setTab] = useState<Tab>(initialThreadId ? "prompt" : "namespace");
  const [fullscreen, setFullscreen] = useState(false);
  const [threads, setThreads] = useState<ThreadSummary[]>([]);

  // Inspect snapshot — refetched whenever the inspected scope changes.
  useEffect(() => {
    let cancelled = false;
    setData(undefined);
    setError(undefined);
    memoryApi
      .inspect({ namespaceId, resourceId, threadId: currentThread })
      .then((r) => !cancelled && setData(r))
      .catch((e) => !cancelled && setError(e instanceof Error ? e.message : String(e)));
    return () => {
      cancelled = true;
    };
  }, [namespaceId, resourceId, currentThread]);

  // Thread list for the picker — only available when both namespace and
  // resource are set (the gateway route requires both).
  useEffect(() => {
    if (!resourceId) {
      setThreads([]);
      return;
    }
    let cancelled = false;
    api
      .listAgentThreads(agentId, { namespaceId, resourceId, limit: 100 })
      .then((r) => !cancelled && setThreads(r.threads))
      .catch(() => !cancelled && setThreads([]));
    return () => {
      cancelled = true;
    };
  }, [agentId, namespaceId, resourceId]);

  // If the operator clicks into the Thread tab and there's no current
  // thread, hop to a sensible default (the most recent thread).
  useEffect(() => {
    if ((tab === "thread" || tab === "prompt") && !currentThread && threads.length > 0) {
      setCurrentThread(threads[0]!.id);
    }
  }, [tab, currentThread, threads]);

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => e.key === "Escape" && onClose();
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onClose]);

  // Tabs the snapshot has data for. We always show namespace; resource and
  // thread surface only when their scope is in the request.
  const tabs: Array<{ id: Tab; label: string; show: boolean }> = [
    { id: "prompt", label: "Prompt", show: !!currentThread },
    { id: "namespace", label: "Namespace", show: true },
    { id: "resource", label: "Resource", show: !!resourceId },
    { id: "thread", label: "Thread", show: !!currentThread },
  ];
  const visibleTabs = tabs.filter((t) => t.show);

  // Sort threads newest-first for the picker so recent activity is on top.
  const sortedThreads = useMemo(
    () => [...threads].sort((a, b) => b.lastActiveAt - a.lastActiveAt),
    [threads],
  );

  return (
    <>
      <div class="fixed inset-0 bg-black/40 z-30 anim-backdrop-in" onClick={onClose} aria-hidden />
      <aside
        class={`fixed top-0 right-0 h-screen bg-base-100 shadow-2xl z-40 flex flex-col anim-drawer-in transition-[max-width] duration-150 ${
          fullscreen ? "w-screen max-w-none" : "w-full max-w-2xl"
        }`}
        role="dialog"
        aria-label="Memory explorer"
      >
        <header class="flex items-start justify-between gap-2 p-4 border-b border-base-300">
          <div class="min-w-0 flex-1">
            <div class="text-xs text-base-content/50 uppercase tracking-wider">Memory explorer</div>
            <div class="font-mono text-sm truncate">
              {namespaceId}
              {resourceId && <span class="text-base-content/50"> · {resourceId}</span>}
              {currentThread && <span class="text-base-content/50"> · {currentThread}</span>}
            </div>
          </div>
          <div class="flex gap-1 shrink-0">
            <button
              class="btn btn-sm btn-ghost"
              onClick={() => setFullscreen((v) => !v)}
              aria-label={fullscreen ? "Exit fullscreen" : "Fullscreen"}
              title={fullscreen ? "Exit fullscreen" : "Fullscreen"}
            >
              {fullscreen ? "⤡" : "⤢"}
            </button>
            <button
              class="btn btn-sm btn-ghost"
              onClick={onClose}
              aria-label="Close"
              title="Close (Esc)"
            >
              ✕
            </button>
          </div>
        </header>

        {resourceId && (
          <div class="px-4 py-2 border-b border-base-300 flex items-center gap-2 text-xs">
            <span class="text-base-content/60">Thread</span>
            <Combobox
              class="flex-1 font-mono"
              size="xs"
              placeholder="(none — namespace + resource only)"
              value={currentThread ?? NO_THREAD}
              onChange={(v) => setCurrentThread(v === NO_THREAD ? undefined : v)}
              // Initial / fallback option list — keeps the trigger label
              // populated for whatever thread is currently selected even
              // before the async loader has a chance to populate the panel.
              options={comboOptions(currentThread, sortedThreads)}
              // Backend search — fires on open and per keystroke
              // (debounced inside Combobox). The route filters via
              // SQL LIKE on thread_id, so this scales beyond the page-
              // size hint.
              loadOptions={async (q) => {
                const res = await api.listAgentThreads(agentId, {
                  namespaceId,
                  resourceId,
                  q,
                  limit: 100,
                });
                return [
                  { value: NO_THREAD, label: "(none — namespace + resource only)" },
                  ...res.threads.map((t) => threadOption(t)),
                ];
              }}
            />
            {currentThread && (
              <>
                <CompactButton
                  agentId={agentId}
                  threadId={currentThread}
                  namespaceId={namespaceId}
                  resourceId={resourceId}
                  onDone={() => {
                    // Refetch the snapshot so Thread → Episodes shows
                    // the freshly written rollup.
                    memoryApi
                      .inspect({ namespaceId, resourceId, threadId: currentThread })
                      .then(setData)
                      .catch(() => {});
                  }}
                />
                <DistillButton
                  agentId={agentId}
                  threadId={currentThread}
                  namespaceId={namespaceId}
                  resourceId={resourceId}
                  onDone={() => {
                    // Refetch the snapshot so Resource → Facts/Episodes
                    // shows the freshly written rows.
                    memoryApi
                      .inspect({ namespaceId, resourceId, threadId: currentThread })
                      .then(setData)
                      .catch(() => {});
                  }}
                />
              </>
            )}
          </div>
        )}

        <div class="px-4 pt-3 border-b border-base-300">
          <div role="tablist" class="tabs tabs-bordered">
            {visibleTabs.map((t) => (
              <button
                role="tab"
                class={`tab ${tab === t.id ? "tab-active" : ""}`}
                onClick={() => setTab(t.id)}
              >
                {t.label}
              </button>
            ))}
          </div>
        </div>

        <div class="flex-1 overflow-y-auto p-4 space-y-4">
          {!data && !error && (
            <>
              <Skeleton w="w-full" h="h-4" />
              <Skeleton w="w-3/4" h="h-4" />
              <Skeleton w="w-1/2" h="h-4" />
            </>
          )}
          {error && <div class="alert alert-error text-xs">{error}</div>}
          {data && tab === "prompt" && <PromptTab data={data} />}
          {data && tab === "namespace" && <NamespaceTab data={data} />}
          {data && tab === "resource" && <ResourceTab data={data} />}
          {data && tab === "thread" && <ThreadTab data={data} />}
        </div>
      </aside>
    </>
  );
}

function PromptTab({ data }: { data: MemoryInspectResponse }) {
  if (!data.resolved) {
    return (
      <div class="text-sm text-base-content/60">
        No thread selected — open a thread in the chat console first to see the resolved system
        prompt.
      </div>
    );
  }
  return (
    <section class="space-y-3">
      <div>
        <SectionLabel>Resolved system prompt</SectionLabel>
        <p class="text-xs text-base-content/60 mb-2">
          What the LLM sees after the cascade collapses (namespace → resource → thread). Trimmed
          message tail: {data.resolved.messageCount} message
          {data.resolved.messageCount === 1 ? "" : "s"}.
        </p>
        <pre class="bg-base-200 p-3 rounded text-xs whitespace-pre-wrap break-words font-mono leading-relaxed max-h-[60vh] overflow-y-auto">
          {data.resolved.systemPrompt || <em class="opacity-60">(empty)</em>}
        </pre>
      </div>
    </section>
  );
}

function NamespaceTab({ data }: { data: MemoryInspectResponse }) {
  const ns = data.namespace;
  return (
    <section class="space-y-4">
      <ScopeHeader
        title={`namespace: ${data.namespaceId}`}
        row={ns.row}
        emptyHint="Namespace row not yet created — appears after the first agent invocation in this tenant."
      />
      <RulesAndWorking
        rules={ns.row?.staticRules ?? null}
        working={ns.row?.workingMemory ?? null}
      />
      <FactList facts={ns.facts} scope="namespace" />
      <EpisodeList episodes={ns.episodes} scope="namespace" />
    </section>
  );
}

function ResourceTab({ data }: { data: MemoryInspectResponse }) {
  if (!data.resource) return <div class="text-sm text-base-content/60">No resource scope.</div>;
  const r = data.resource;
  return (
    <section class="space-y-4">
      <ScopeHeader
        title={`resource: ${data.resourceId ?? "?"}`}
        row={r.row}
        emptyHint="Resource row not yet created — appears after the first agent invocation for this user."
      />
      <RulesAndWorking rules={r.row?.staticRules ?? null} working={r.row?.workingMemory ?? null} />
      <FactList facts={r.facts} scope="resource" />
      <EpisodeList episodes={r.episodes} scope="resource" />
    </section>
  );
}

function ThreadTab({ data }: { data: MemoryInspectResponse }) {
  if (!data.thread) return <div class="text-sm text-base-content/60">No thread scope.</div>;
  const t = data.thread;
  return (
    <section class="space-y-4">
      <ScopeHeader
        title={`thread: ${data.threadId ?? "?"}`}
        row={t.row}
        emptyHint="Thread row not yet created."
      />
      {/* Threads don't carry static rules — only working memory. */}
      <div>
        <SectionLabel>Working memory</SectionLabel>
        <pre class="bg-base-200 p-2 rounded text-xs whitespace-pre-wrap break-words font-mono leading-relaxed max-h-40 overflow-y-auto">
          {t.row?.workingMemory ?? <em class="opacity-60">(empty)</em>}
        </pre>
      </div>
      <FactList facts={t.facts} scope="thread" />
      <EpisodeList episodes={t.episodes} scope="thread" />
      <MessageList messages={t.messages} />
    </section>
  );
}

function ScopeHeader({
  title,
  row,
  emptyHint,
}: {
  title: string;
  row: { inheritFromParent?: boolean; createdAt?: number; updatedAt?: number } | null;
  emptyHint: string;
}) {
  return (
    <div class="flex items-start justify-between gap-3">
      <div class="font-mono text-sm">{title}</div>
      {row ? (
        <div class="flex flex-col items-end gap-1 text-[10px] text-base-content/60">
          <span class="font-mono">
            inherit: {row.inheritFromParent === false ? "false (cut)" : "true"}
          </span>
          {row.updatedAt !== undefined && (
            <span>updated {formatRelative(new Date(row.updatedAt).toISOString())}</span>
          )}
        </div>
      ) : (
        <span class="text-[10px] text-base-content/50 italic max-w-[60%] text-right">
          {emptyHint}
        </span>
      )}
    </div>
  );
}

function RulesAndWorking({ rules, working }: { rules: string | null; working: string | null }) {
  return (
    <div class="grid grid-cols-1 gap-3">
      <div>
        <SectionLabel>Static rules</SectionLabel>
        <pre class="bg-base-200 p-2 rounded text-xs whitespace-pre-wrap break-words font-mono leading-relaxed max-h-40 overflow-y-auto">
          {rules ?? <em class="opacity-60">(empty)</em>}
        </pre>
      </div>
      <div>
        <SectionLabel>Working memory</SectionLabel>
        <pre class="bg-base-200 p-2 rounded text-xs whitespace-pre-wrap break-words font-mono leading-relaxed max-h-40 overflow-y-auto">
          {working ?? <em class="opacity-60">(empty)</em>}
        </pre>
      </div>
    </div>
  );
}

function FactList({ facts, scope }: { facts: Fact[]; scope: string }) {
  return (
    <div>
      <SectionLabel>
        Facts <span class="text-base-content/40 font-mono">[{scope}]</span>
      </SectionLabel>
      {facts.length === 0 ? (
        <div class="text-xs text-base-content/40 italic">(no facts)</div>
      ) : (
        <ol class="space-y-1 list-decimal list-inside">
          {facts.map((f) => (
            <li class="text-xs leading-relaxed">
              <span class="text-base-content/80">{f.text}</span>
              <span class="text-[10px] text-base-content/40 ml-2 font-mono">
                {formatRelative(new Date(f.createdAt).toISOString())}
              </span>
            </li>
          ))}
        </ol>
      )}
    </div>
  );
}

function EpisodeList({ episodes, scope }: { episodes: EpisodicRecord[]; scope: string }) {
  return (
    <div>
      <SectionLabel>
        Episodes <span class="text-base-content/40 font-mono">[{scope}]</span>
      </SectionLabel>
      {episodes.length === 0 ? (
        <div class="text-xs text-base-content/40 italic">(no episodes)</div>
      ) : (
        <ul class="space-y-2">
          {episodes.map((e) => (
            <li class="bg-base-200 p-2 rounded text-xs">
              <div class="flex items-center justify-between mb-1">
                <span class="font-mono text-[10px] text-base-content/50">
                  salience {e.salience.toFixed(2)} ·{" "}
                  {formatRelative(new Date(e.createdAt).toISOString())}
                </span>
                {e.outcome && (
                  <span class="badge badge-xs badge-ghost truncate max-w-[40%]" title={e.outcome}>
                    {e.outcome}
                  </span>
                )}
              </div>
              <div class="whitespace-pre-wrap leading-relaxed">{e.summary}</div>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}

function MessageList({ messages }: { messages: StoredMessage[] }) {
  return (
    <div>
      <SectionLabel>
        Messages <span class="text-base-content/40 font-mono">[{messages.length}]</span>
      </SectionLabel>
      {messages.length === 0 ? (
        <div class="text-xs text-base-content/40 italic">(empty thread)</div>
      ) : (
        <ul class="space-y-1">
          {messages.map((m) => (
            <li class="text-xs">
              <span class="font-mono text-[10px] text-base-content/50 mr-2">#{m.seq}</span>
              <span class="badge badge-xs badge-ghost mr-2">{m.role}</span>
              <span class="whitespace-pre-wrap break-words">{renderMessageContent(m)}</span>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}

function renderMessageContent(m: StoredMessage): preact.JSX.Element | string {
  if (m.role === "tool") return `tool result for ${m.toolCallId}: ${m.content}`;
  if (m.role === "assistant") {
    const text = m.content ?? "";
    const tools = m.toolCalls ?? [];
    if (tools.length === 0) return text;
    return (
      <span>
        {text}
        {tools.length > 0 && (
          <span class="block mt-1">
            <JsonBlock value={tools} maxH="max-h-32" />
          </span>
        )}
      </span>
    );
  }
  return m.content;
}

function SectionLabel({ children }: { children: preact.ComponentChildren }) {
  return (
    <div class="text-[10px] uppercase tracking-wider text-base-content/50 mb-1">{children}</div>
  );
}

import type * as preact from "preact";
import { toast } from "../../lib/dialogs.ts";

function threadOption(t: ThreadSummary): ComboboxOption {
  return {
    value: t.id,
    label: t.id,
    hint: `${t.messageCount} msg · ${formatRelative(new Date(t.lastActiveAt).toISOString())}`,
  };
}

/**
 * Static fallback list shown by Combobox until the async loader resolves.
 * Includes the "no thread" sentinel + the currently-selected thread (if
 * any) so the trigger label always has something to render.
 */
function comboOptions(
  currentThread: string | undefined,
  threads: ReadonlyArray<ThreadSummary>,
): ComboboxOption[] {
  const out: ComboboxOption[] = [{ value: NO_THREAD, label: "(none — namespace + resource only)" }];
  for (const t of threads) out.push(threadOption(t));
  if (currentThread && !threads.some((t) => t.id === currentThread)) {
    out.push({ value: currentThread, label: currentThread });
  }
  return out;
}

function CompactButton({
  agentId,
  threadId,
  namespaceId,
  resourceId,
  onDone,
}: {
  agentId: string;
  threadId: string;
  namespaceId: string;
  resourceId: string;
  onDone: () => void;
}) {
  const [pending, setPending] = useState(false);
  const compact = async () => {
    if (pending) return;
    setPending(true);
    try {
      const res = await api.compactAgentThread(agentId, threadId, {
        namespaceId,
        resourceId,
        // No keepRecent override — let the host's default kick in.
      });
      const sal = res.episode.salience.toFixed(2);
      toast(`Compacted (salience ${sal})`, { variant: "success" });
      onDone();
    } catch (e) {
      toast(`Compact failed: ${e instanceof Error ? e.message : String(e)}`, { variant: "error" });
    } finally {
      setPending(false);
    }
  };
  return (
    <button
      class="btn btn-xs btn-ghost"
      onClick={compact}
      disabled={pending}
      title="Roll up the oldest portion of this thread into a ThreadEpisode (frees message budget; gist survives)"
    >
      {pending ? "Compacting…" : "↧ Compact"}
    </button>
  );
}

function DistillButton({
  agentId,
  threadId,
  namespaceId,
  resourceId,
  onDone,
}: {
  agentId: string;
  threadId: string;
  namespaceId: string;
  resourceId: string;
  onDone: () => void;
}) {
  const [pending, setPending] = useState(false);
  const distill = async () => {
    if (pending) return;
    setPending(true);
    try {
      const res = await api.distillAgentThread(agentId, threadId, {
        namespaceId,
        resourceId,
        force: true,
      });
      const sal = res.episode.salience.toFixed(2);
      toast(`Distilled (salience ${sal})`, { variant: "success" });
      onDone();
    } catch (e) {
      toast(`Distill failed: ${e instanceof Error ? e.message : String(e)}`, { variant: "error" });
    } finally {
      setPending(false);
    }
  };
  return (
    <button
      class="btn btn-xs btn-primary"
      onClick={distill}
      disabled={pending}
      title="Summarise this thread into a resource-scope episode + facts so future threads pick it up"
    >
      {pending ? "Distilling…" : "↯ Distill"}
    </button>
  );
}
