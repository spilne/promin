import type * as preact from "preact";
import { useEffect, useMemo, useRef, useState } from "preact/hooks";
import { useFetch } from "../../hooks/use-fetch.ts";
import { useNamespace } from "../../hooks/use-namespace.ts";
import { api } from "../../api/client.ts";
import type { Message, RegisteredAgent, ThreadSummary } from "../../../server/routes/agents.ts";
import { Page } from "../ui/page.tsx";
import { Skeleton } from "../ui/skeleton.tsx";
import { formatRelative } from "../../lib/format.ts";
import { toast } from "../../lib/dialogs.ts";
import { Markdown } from "../../lib/markdown.tsx";
import { MemoryInspector } from "./memory-inspector.tsx";
import { AgentConfigDrawer } from "./agent-config-drawer.tsx";

interface AgentDetailProps {
  id: string;
  onBack: () => void;
}

// Local-only persistence: only the resource (owner / user) id lives in
// localStorage. Namespace comes from the global sidebar switcher
// (`useNamespace`) so the entire dashboard scopes consistently and the
// agent page doesn't fight the global selector with its own input.
const RESOURCE_KEY = "zorya_agent_resource";
const ACTIVE_THREAD_KEY = "zorya_agent_active_thread";

interface Tenant {
  namespaceId: string;
  resourceId: string;
}

function loadResourceId(): string {
  return localStorage.getItem(RESOURCE_KEY) ?? "alice";
}

function saveResourceId(id: string) {
  localStorage.setItem(RESOURCE_KEY, id);
}

export function AgentDetail({ id, onBack }: AgentDetailProps) {
  // Namespace comes from the global sidebar switcher; only resourceId is
  // page-local state. Default to "default" when no namespace is selected
  // so the agent API still has something to scope by — same convention
  // the rest of the dashboard uses.
  const [globalNamespace] = useNamespace();
  const [resourceId, setResourceId] = useState<string>(loadResourceId);
  const tenant = useMemo<Tenant>(
    () => ({ namespaceId: globalNamespace || "default", resourceId }),
    [globalNamespace, resourceId],
  );
  const setTenant = (t: Tenant) => setResourceId(t.resourceId);
  const [activeThread, setActiveThread] = useState<string | null>(() =>
    localStorage.getItem(`${ACTIVE_THREAD_KEY}:${id}`),
  );
  const [inspectorOpen, setInspectorOpen] = useState(false);
  const [configOpen, setConfigOpen] = useState(false);

  // Persist resource id + active thread (namespace already persists in
  // the global useNamespace store).
  useEffect(() => saveResourceId(resourceId), [resourceId]);
  useEffect(() => {
    if (activeThread) localStorage.setItem(`${ACTIVE_THREAD_KEY}:${id}`, activeThread);
    else localStorage.removeItem(`${ACTIVE_THREAD_KEY}:${id}`);
  }, [activeThread, id]);

  const { data: agent, error } = useFetch(() => api.getAgent(id), [id]);

  const { data: threadsResp, refresh: refreshThreads } = useFetch(
    () => api.listAgentThreads(id, tenant),
    [id, tenant.namespaceId, tenant.resourceId],
    15_000,
  );

  if (error) {
    return (
      <Page space="none">
        <div class="alert alert-error">{error.message}</div>
      </Page>
    );
  }

  const threads = threadsResp?.threads ?? [];

  return (
    <Page space="none">
      <div class="flex flex-col gap-3">
        <div class="flex items-center justify-between">
          <div class="flex items-center gap-3">
            <button class="btn btn-sm btn-ghost" onClick={onBack}>
              ← Agents
            </button>
            <div>
              <h2 class="text-xl font-semibold font-mono">{id}</h2>
              {agent?.metadata.description && (
                <p class="text-xs text-base-content/60">{agent.metadata.description}</p>
              )}
            </div>
          </div>
          <div class="flex items-center gap-3">
            {agent && <AgentMetaBadges agent={agent} />}
            <button
              class="btn btn-md btn-square btn-ghost text-2xl"
              onClick={() => setConfigOpen(true)}
              aria-label="Agent config"
              title="View this agent's recipe — system prompt, tools, model, metadata"
            >
              ⚙
            </button>
          </div>
        </div>

        <TenantBar tenant={tenant} onChange={setTenant} />

        <div class="grid grid-cols-1 md:grid-cols-[260px_1fr] gap-4 min-h-[60vh]">
          <ThreadSidebar
            agentId={id}
            tenant={tenant}
            threads={threads}
            activeThread={activeThread}
            onSelect={setActiveThread}
            onNew={(threadId) => {
              setActiveThread(threadId);
              refreshThreads();
            }}
            onRenamed={refreshThreads}
          />
          {activeThread ? (
            <ChatPane
              key={`${id}:${activeThread}:${tenant.namespaceId}:${tenant.resourceId}`}
              agentId={id}
              threadId={activeThread}
              tenant={tenant}
              onTurnComplete={refreshThreads}
              onInspect={() => setInspectorOpen(true)}
            />
          ) : (
            <div class="card bg-base-100 shadow flex items-center justify-center text-center text-base-content/50 p-8">
              <div>
                <p class="text-lg mb-2">No thread selected</p>
                <p class="text-sm">
                  Pick a thread from the sidebar or start a new one to begin chatting.
                </p>
                <button class="btn btn-xs btn-ghost mt-3" onClick={() => setInspectorOpen(true)}>
                  Browse tenant memory →
                </button>
              </div>
            </div>
          )}
        </div>
      </div>

      {inspectorOpen && (
        <MemoryInspector
          agentId={id}
          namespaceId={tenant.namespaceId}
          resourceId={tenant.resourceId || undefined}
          initialThreadId={activeThread ?? undefined}
          onClose={() => setInspectorOpen(false)}
        />
      )}

      {configOpen && <AgentConfigDrawer agent={agent} onClose={() => setConfigOpen(false)} />}
    </Page>
  );
}

function AgentMetaBadges({ agent }: { agent: RegisteredAgent }) {
  const model =
    agent.backend.type === "local"
      ? `${agent.backend.model.provider}/${agent.backend.model.id}`
      : agent.backend.type;
  return (
    <div class="flex gap-1 flex-wrap items-center">
      <span class="badge badge-sm badge-outline font-mono">v{agent.version}</span>
      <span class="badge badge-sm badge-outline font-mono">{model}</span>
      {agent.metadata.capabilities.map((c) => (
        <span class="badge badge-sm badge-info badge-outline">{c}</span>
      ))}
      {agent.metadata.tags.map((t) => (
        <span class="badge badge-sm badge-ghost">{t}</span>
      ))}
    </div>
  );
}

function TenantBar({ tenant, onChange }: { tenant: Tenant; onChange: (t: Tenant) => void }) {
  return (
    <div class="card bg-base-200 px-3 py-2 flex flex-row gap-3 items-center text-xs">
      <span class="text-base-content/60">Tenant scope</span>
      <span class="flex items-center gap-1">
        <span class="text-base-content/60">namespaceId</span>
        <span class="badge badge-sm badge-ghost font-mono">{tenant.namespaceId}</span>
        <span class="text-base-content/30 text-[10px]">(sidebar)</span>
      </span>
      <label class="flex items-center gap-1">
        <span class="text-base-content/60">resourceId</span>
        <input
          class="input input-bordered input-xs font-mono w-32"
          value={tenant.resourceId}
          onInput={(e) => onChange({ ...tenant, resourceId: (e.target as HTMLInputElement).value })}
        />
      </label>
      <span class="text-base-content/40 ml-auto">
        Threads + memory cascade are scoped to this pair.
      </span>
    </div>
  );
}

function ThreadSidebar({
  agentId,
  tenant,
  threads,
  activeThread,
  onSelect,
  onNew,
  onRenamed,
}: {
  agentId: string;
  tenant: Tenant;
  threads: ThreadSummary[];
  activeThread: string | null;
  onSelect: (id: string) => void;
  onNew: (id: string) => void;
  onRenamed: () => void;
}) {
  const [newId, setNewId] = useState("");
  const [renamingId, setRenamingId] = useState<string | null>(null);
  const [renameDraft, setRenameDraft] = useState("");

  const startNew = () => {
    const trimmed = newId.trim();
    const id = trimmed || `chat-${new Date().toISOString().slice(0, 19).replace(/[:T]/g, "-")}`;
    setNewId("");
    onNew(id);
  };

  const beginRename = (t: ThreadSummary) => {
    setRenamingId(t.id);
    const current = t.title ?? "";
    setRenameDraft(current);
  };

  const commitRename = async (t: ThreadSummary) => {
    const next = renameDraft.trim();
    setRenamingId(null);
    const current = t.title ?? "";
    if (next === current) return;
    try {
      await api.renameAgentThread(agentId, t.id, {
        namespaceId: tenant.namespaceId,
        resourceId: tenant.resourceId || undefined,
        title: next || null,
      });
      onRenamed();
    } catch (err) {
      toast(`Rename failed: ${err instanceof Error ? err.message : String(err)}`, {
        variant: "error",
      });
    }
  };

  return (
    <aside class="card bg-base-100 shadow overflow-hidden flex flex-col">
      <div class="px-3 py-2 border-b border-base-content/10 flex items-center justify-between">
        <span class="font-semibold text-sm">Threads</span>
        <span class="text-xs text-base-content/50 font-mono">{threads.length}</span>
      </div>
      <div class="px-3 py-2 border-b border-base-content/10 flex gap-1">
        <input
          class="input input-bordered input-xs flex-1 font-mono"
          placeholder="thread id (auto if blank)"
          value={newId}
          onInput={(e) => setNewId((e.target as HTMLInputElement).value)}
          onKeyDown={(e) => {
            if (e.key === "Enter") startNew();
          }}
        />
        <button class="btn btn-xs btn-primary" onClick={startNew}>
          + New
        </button>
      </div>
      <ul class="menu menu-sm flex-1 overflow-y-auto p-1">
        {threads.length === 0 && (
          <li class="px-3 py-4 text-xs text-base-content/40 text-center">
            No threads for {tenant.namespaceId}/{tenant.resourceId}.
          </li>
        )}
        {threads.map((t) => {
          const title = t.title ?? "";
          const isRenaming = renamingId === t.id;
          return (
            <li>
              <div
                class={`flex flex-col items-start gap-0 px-2 py-1 rounded cursor-pointer hover:bg-base-200 ${
                  activeThread === t.id ? "bg-base-200" : ""
                }`}
                onClick={() => !isRenaming && onSelect(t.id)}
              >
                {isRenaming ? (
                  // Inline rename: enter to commit, escape to cancel,
                  // blur acts as commit too so click-away saves the edit.
                  <input
                    class="input input-bordered input-xs w-full"
                    value={renameDraft}
                    autoFocus
                    onClick={(e) => e.stopPropagation()}
                    onInput={(e) => setRenameDraft((e.target as HTMLInputElement).value)}
                    onKeyDown={(e) => {
                      if (e.key === "Enter") void commitRename(t);
                      if (e.key === "Escape") setRenamingId(null);
                    }}
                    onBlur={() => void commitRename(t)}
                    placeholder="thread title"
                  />
                ) : (
                  <div class="flex items-center gap-1 w-full">
                    <span
                      class={`text-xs truncate flex-1 ${title ? "" : "font-mono text-base-content/70"}`}
                      title={t.id}
                    >
                      {title || t.id}
                    </span>
                    <button
                      class="btn btn-square btn-ghost btn-xs opacity-0 group-hover:opacity-100"
                      style={{ opacity: 0.6 }}
                      onClick={(e) => {
                        e.stopPropagation();
                        beginRename(t);
                      }}
                      title="Rename"
                      aria-label="Rename"
                    >
                      <svg
                        xmlns="http://www.w3.org/2000/svg"
                        width="11"
                        height="11"
                        viewBox="0 0 24 24"
                        fill="none"
                        stroke="currentColor"
                        stroke-width="2"
                        stroke-linecap="round"
                        stroke-linejoin="round"
                      >
                        <path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7" />
                        <path d="M18.5 2.5a2.121 2.121 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5z" />
                      </svg>
                    </button>
                  </div>
                )}
                {!isRenaming && (
                  <span class="text-[10px] text-base-content/50">
                    {title ? <span class="font-mono mr-1">{t.id}</span> : null}
                    {t.messageCount} msg · {formatRelative(new Date(t.lastActiveAt).toISOString())}
                  </span>
                )}
              </div>
            </li>
          );
        })}
      </ul>
      <div class="px-3 py-1 border-t border-base-content/10 text-[10px] text-base-content/40 font-mono truncate">
        agent: {agentId}
      </div>
    </aside>
  );
}

interface PendingAssistant {
  text: string;
  done: boolean;
  error?: string;
  usage?: {
    inputTokens: number;
    outputTokens: number;
    cacheReadTokens?: number;
    cacheWriteTokens?: number;
  };
}

function ChatPane({
  agentId,
  threadId,
  tenant,
  onTurnComplete,
  onInspect,
}: {
  agentId: string;
  threadId: string;
  tenant: Tenant;
  onTurnComplete: () => void;
  onInspect: () => void;
}) {
  const {
    data: messagesResp,
    loading,
    error,
    refresh: refreshMessages,
  } = useFetch<{ threadId: string; messages: Message[] } | { messages: Message[] }>(
    () =>
      api
        .listAgentThreadMessages(agentId, threadId, tenant)
        // 404 is expected for brand-new threads — treat as empty.
        .catch(() => ({ threadId, messages: [] as Message[] })),
    [agentId, threadId, tenant.namespaceId, tenant.resourceId],
  );

  const messages = (messagesResp?.messages ?? []) as Message[];
  const [draft, setDraft] = useState("");
  const [pending, setPending] = useState<PendingAssistant | null>(null);
  // Optimistic user-message bubble — rendered the moment the user hits
  // Send, BEFORE the gateway has persisted it. Cleared as soon as the
  // refreshed history shows the same content as the last user message
  // (see useEffect below). This makes the UI feel responsive even
  // though the SSE round-trip takes a beat.
  const [pendingUser, setPendingUser] = useState<string | null>(null);
  // Inline approval state — set when the agent suspended on a tool call
  // that requires approval. The banner renders right inside the chat
  // scroll area; resolving it (Approve / Reject) re-opens the SSE
  // stream against the resume endpoint and the new deltas splice into
  // the same `pending` bubble.
  const [pendingApproval, setPendingApproval] = useState<{
    toolCallId: string;
    toolName: string;
  } | null>(null);
  // Drawer for the per-thread schedule list. Lives at the ChatPane
  // level so the Schedules button in the header can flip it.
  const [schedulesOpen, setSchedulesOpen] = useState(false);
  const abortRef = useRef<{ abort: () => void } | null>(null);
  const scrollRef = useRef<HTMLDivElement>(null);

  // Drop the optimistic user bubble once the persisted history contains
  // the same text — keeps a brief render window where both would show
  // and produce a duplicate.
  useEffect(() => {
    if (!pendingUser) return;
    for (let i = messages.length - 1; i >= 0; i--) {
      const m = messages[i];
      if (m?.role !== "user") continue;
      if (m.content === pendingUser) setPendingUser(null);
      return;
    }
  }, [messages, pendingUser]);

  // Auto-scroll on new messages or pending updates (including the
  // optimistic user bubble).
  useEffect(() => {
    const el = scrollRef.current;
    if (!el) return;
    el.scrollTop = el.scrollHeight;
  }, [messages.length, pending?.text, pending?.done, pendingUser]);

  /** Shared SSE handlers — used by both the user-task and resume streams. */
  const streamHandlers = (): Parameters<typeof api.streamAgentThread>[3] => ({
    onDelta: (delta) => {
      setPending((p) => (p ? { ...p, text: p.text + delta } : p));
    },
    onApprovalRequested: (info) => {
      setPendingApproval(info);
      // Mark the assistant bubble as "done" so the typing indicator
      // stops and the banner takes the action focus.
      setPending((p) => (p ? { ...p, done: true } : p));
    },
    onSuspended: () => {
      // Suspension confirmation — banner already rendered from the
      // earlier `approval-requested` event, so nothing extra to do.
    },
    onFinish: (info) => {
      setPending((p) => (p ? { ...p, done: true, usage: info.usage } : p));
      setPendingApproval(null);
    },
    onError: (msg) => {
      setPending((p) => (p ? { ...p, error: msg, done: true } : p));
      toast(`Stream error: ${msg}`, { variant: "error" });
    },
  });

  const send = () => {
    const task = draft.trim();
    if (!task || pending) return;
    setDraft("");
    setPendingUser(task);
    setPending({ text: "", done: false });

    const stream = api.streamAgentThread(
      agentId,
      threadId,
      { task, namespaceId: tenant.namespaceId, resourceId: tenant.resourceId },
      streamHandlers(),
    );
    abortRef.current = stream;
    stream.done.then(() => {
      refreshMessages();
      onTurnComplete();
      // Clear pending after the persisted history catches up. The
      // useEffect above will drop pendingUser independently once the
      // refresh's data lands. Don't clear pending while an approval
      // banner is up — the resume stream will reuse it.
      setTimeout(() => {
        setPending((p) => (pendingApproval ? p : null));
      }, 250);
    });
  };

  const decideApproval = (approved: boolean) => {
    if (!pendingApproval) return;
    const { toolCallId } = pendingApproval;
    setPendingApproval(null);
    // Reuse the pending bubble — the resume stream will append more
    // deltas from where it left off (typically the assistant follow-up
    // message after the tool result).
    setPending((p) => (p ? { ...p, done: false } : { text: "", done: false }));

    const stream = api.streamThreadApproval(
      agentId,
      threadId,
      {
        toolCallId,
        approved,
        namespaceId: tenant.namespaceId,
        resourceId: tenant.resourceId,
      },
      streamHandlers(),
    );
    abortRef.current = stream;
    stream.done.then(() => {
      refreshMessages();
      onTurnComplete();
      setTimeout(() => {
        setPending((p) => (pendingApproval ? p : null));
      }, 250);
    });
  };

  const cancel = () => {
    abortRef.current?.abort();
    abortRef.current = null;
    setPending(null);
    setPendingUser(null);
    setPendingApproval(null);
  };

  return (
    // Fade + slight upward slide on every (agent / thread / tenant) swap.
    // The component is keyed on those upstream so React fully remounts on
    // switch — applying `anim-thread-swap` here means each remount replays
    // the animation. Without it, the new pane snaps in instantly which
    // feels jarring when scrolling through threads.
    <section class="card bg-base-100 shadow flex flex-col min-h-0 anim-thread-swap">
      <div class="px-3 py-2 border-b border-base-content/10 flex items-center justify-between gap-2">
        <span class="font-mono text-sm truncate" title={threadId}>
          {threadId}
        </span>
        <div class="flex gap-1 shrink-0">
          <button
            class="btn btn-xs btn-ghost"
            onClick={() => setSchedulesOpen(true)}
            title="Schedules created by this thread — view + cancel cron / interval triggers the agent set up"
          >
            ⏱ Schedules
          </button>
          <button
            class="btn btn-xs btn-ghost"
            onClick={onInspect}
            title="Inspect resolved prompt, namespace/resource/thread cascade, working memory, facts, episodes"
          >
            ⌬ Debug
          </button>
          <button class="btn btn-xs btn-ghost" onClick={() => refreshMessages()} title="Refresh">
            ↻
          </button>
        </div>
      </div>

      <div ref={scrollRef} class="flex-1 overflow-y-auto px-4 py-3 space-y-3 min-h-[40vh]">
        {loading && messages.length === 0 && (
          <div class="space-y-2">
            <Skeleton w="w-3/4" h="h-4" />
            <Skeleton w="w-2/3" h="h-4" />
            <Skeleton w="w-1/2" h="h-4" />
          </div>
        )}
        {error && <div class="alert alert-error text-xs">{error.message}</div>}
        {!loading && messages.length === 0 && !pending && (
          <div class="text-center text-base-content/40 text-sm py-8">
            No messages yet — say hi to start the thread.
          </div>
        )}

        {renderConversation(messages)}

        {/* Optimistic echo of the user's most recent send — appears
            instantly so the UI feels responsive while the gateway
            persists the message and starts the LLM. Removed by the
            useEffect above as soon as the same content shows up in
            the refreshed persisted history. */}
        {pendingUser && <UserBubble content={pendingUser} />}

        {pending && (
          <PendingBubble
            text={pending.text}
            done={pending.done}
            error={pending.error}
            usage={pending.usage}
          />
        )}

        {pendingApproval && (
          <InlineApprovalBanner
            toolName={pendingApproval.toolName}
            onApprove={() => decideApproval(true)}
            onReject={() => decideApproval(false)}
          />
        )}
      </div>

      <div class="border-t border-base-content/10 px-3 py-2 flex gap-2">
        <textarea
          class="textarea textarea-bordered textarea-sm flex-1 resize-none"
          rows={2}
          placeholder="Type a message…"
          value={draft}
          disabled={!!pending}
          onInput={(e) => setDraft((e.target as HTMLTextAreaElement).value)}
          onKeyDown={(e) => {
            if (e.key === "Enter" && !e.shiftKey) {
              e.preventDefault();
              send();
            }
          }}
        />
        {pending ? (
          <button class="btn btn-sm btn-ghost text-error" onClick={cancel}>
            Cancel
          </button>
        ) : (
          <button class="btn btn-sm btn-primary" onClick={send} disabled={!draft.trim()}>
            Send
          </button>
        )}
      </div>
      {schedulesOpen && (
        <ThreadSchedulesDrawer
          threadId={threadId}
          tenant={tenant}
          onClose={() => setSchedulesOpen(false)}
        />
      )}
    </section>
  );
}

// Walks the persisted message history and emits a flat list of bubbles +
// inline tool events. Mirrors the Claude.ai / OpenAI pattern: pure tool-
// call assistant messages don't get a chat bubble — they render as a
// compact "Used X tool" pill paired with the matching tool-result message,
// expandable on click. Assistant messages that DO carry text still render
// as a normal bubble; their tool calls (if any) follow as inline events.
function renderConversation(messages: ReadonlyArray<Message>): preact.JSX.Element[] {
  // Index tool results by toolCallId so we can fold each one into its
  // matching call instead of leaking a bare "tool result …" row.
  const resultsByCallId = new Map<string, string>();
  for (const m of messages) {
    if (m.role === "tool") resultsByCallId.set(m.toolCallId, m.content);
  }

  const out: preact.JSX.Element[] = [];
  for (let i = 0; i < messages.length; i++) {
    const m = messages[i]!;
    if (m.role === "system" || m.role === "tool") continue;
    if (m.role === "user") {
      out.push(<UserBubble key={`u-${i}`} content={m.content} />);
      continue;
    }
    // assistant
    const text = (m.content ?? "").trim();
    const toolCalls = m.toolCalls ?? [];
    if (text) {
      out.push(<AssistantBubble key={`a-${i}`} text={text} />);
    }
    for (let j = 0; j < toolCalls.length; j++) {
      const call = toolCalls[j]!;
      out.push(
        <ToolEvent
          key={`t-${i}-${j}`}
          name={call.name}
          input={call.input}
          result={resultsByCallId.get(call.id)}
        />,
      );
    }
  }
  return out;
}

function UserBubble({ content }: { content: string }) {
  return (
    <div class="chat chat-end">
      <div class="chat-header text-xs text-base-content/50">you</div>
      <div class="chat-bubble chat-bubble-primary whitespace-pre-wrap break-words">{content}</div>
    </div>
  );
}

function AssistantBubble({ text }: { text: string }) {
  return (
    <div class="chat chat-start">
      <div class="chat-header text-xs text-base-content/50">assistant</div>
      <div class="chat-bubble break-words">
        <Markdown text={text} />
      </div>
    </div>
  );
}

function ToolEvent({
  name,
  input,
  result,
}: {
  name: string;
  input: unknown;
  result: string | undefined;
}) {
  const [open, setOpen] = useState(false);
  const summary = compactInput(input);
  const status = result === undefined ? "running" : "done";
  return (
    <details
      class="bg-base-200/60 border border-base-content/10 rounded-md text-xs"
      open={open}
      onToggle={(e) => setOpen((e.target as HTMLDetailsElement).open)}
    >
      <summary class="cursor-pointer select-none px-3 py-1.5 list-none flex items-center gap-2">
        <span
          class={`inline-block w-1.5 h-1.5 rounded-full ${
            status === "running" ? "bg-warning animate-pulse" : "bg-success"
          }`}
        />
        <span class="text-base-content/60">Used</span>
        <span class="font-mono text-base-content/80">{name}</span>
        {summary && (
          <span class="text-base-content/40 font-mono truncate max-w-[40ch]">{summary}</span>
        )}
        <span class="ml-auto text-base-content/40">{open ? "−" : "+"}</span>
      </summary>
      <div class="border-t border-base-content/10 px-3 py-2 space-y-2">
        <div>
          <div class="text-[10px] uppercase tracking-wider text-base-content/40 mb-1">Input</div>
          <pre class="bg-base-300/60 rounded p-2 text-[11px] font-mono whitespace-pre-wrap break-words">
            {formatJson(input)}
          </pre>
        </div>
        <div>
          <div class="text-[10px] uppercase tracking-wider text-base-content/40 mb-1">Output</div>
          <pre class="bg-base-300/60 rounded p-2 text-[11px] font-mono whitespace-pre-wrap break-words">
            {result === undefined ? <em class="opacity-60">(running…)</em> : formatJson(result)}
          </pre>
        </div>
      </div>
    </details>
  );
}

function compactInput(input: unknown): string {
  try {
    const json = JSON.stringify(input);
    if (!json) return "";
    return json.length > 60 ? `${json.slice(0, 57)}…` : json;
  } catch {
    return "";
  }
}

function formatJson(value: unknown): string {
  if (typeof value === "string") {
    // Tool results arrive as strings — try to pretty-print JSON, fall back
    // to the raw string when it isn't structured.
    try {
      return JSON.stringify(JSON.parse(value), null, 2);
    } catch {
      return value;
    }
  }
  try {
    return JSON.stringify(value, null, 2);
  } catch {
    return String(value);
  }
}

/**
 * Thread-scoped schedule list — modal drawer triggered from the ⏱
 * Schedules button in the chat header. Lists every durable schedule
 * the agent created in this thread and lets the user cancel any of
 * them. Filtered client-side from the namespace-wide schedules list:
 * `metadata.threadId === threadId` AND `metadata.agentTrigger === true`.
 */
function ThreadSchedulesDrawer({
  threadId,
  tenant,
  onClose,
}: {
  threadId: string;
  tenant: Tenant;
  onClose: () => void;
}) {
  // Show cancelled (disabled) schedules alongside active ones. Default
  // off so the common case shows just the live ones; flipping the
  // toggle widens the fetch to include `enabled: false` rows so the
  // user can review or restore prior cancels.
  const [showCancelled, setShowCancelled] = useState(false);

  // Server-side metadata containment filter — pushed to storage so we
  // don't fetch the full namespace just to discard everything except
  // this thread's agent schedules. Same predicate the dispatch helper
  // uses (`agentTrigger` + `threadId`), translated to a JSON-path query
  // on backends with native support. `enabled` filter omitted when the
  // user wants to see cancelled rows too.
  const { data, loading, error, refresh } = useFetch(
    () =>
      api.listSchedules({
        namespace: tenant.namespaceId,
        ...(!showCancelled && { enabled: true }),
        metadata: { agentTrigger: true, threadId },
      }),
    [tenant.namespaceId, threadId, showCancelled],
    15_000,
  );

  const schedules = data?.schedules ?? [];

  // Soft cancel — flip `enabled` to false. The schedule persists so
  // the user can review history or restore it from the same drawer.
  // Hard delete via `api.deleteSchedule` would erase the row and
  // there'd be nothing to put behind a "show cancelled" filter.
  const cancel = async (id: string) => {
    try {
      await api.patchSchedule(id, { enabled: false });
      toast(`Cancelled schedule ${id}`);
      refresh();
    } catch (e) {
      toast(`Failed to cancel: ${(e as Error).message}`, { variant: "error" });
    }
  };
  const restore = async (id: string) => {
    try {
      await api.patchSchedule(id, { enabled: true });
      toast(`Restored schedule ${id}`);
      refresh();
    } catch (e) {
      toast(`Failed to restore: ${(e as Error).message}`, { variant: "error" });
    }
  };

  return (
    <div class="modal modal-open" onClick={onClose}>
      <div class="modal-box max-w-2xl" onClick={(e) => e.stopPropagation()}>
        <div class="flex items-center justify-between mb-3">
          <h3 class="font-semibold text-base">Schedules in this thread</h3>
          <button class="btn btn-xs btn-ghost" onClick={onClose} aria-label="Close">
            ✕
          </button>
        </div>
        <p class="text-xs text-base-content/50 mb-3">
          Cron / interval / RRULE triggers the agent created in{" "}
          <span class="font-mono">{threadId}</span>. Each fire re-invokes the agent in this thread
          with the saved task.
        </p>

        <label class="flex items-center gap-2 text-xs text-base-content/70 mb-3 cursor-pointer w-fit">
          <input
            type="checkbox"
            class="checkbox checkbox-xs"
            checked={showCancelled}
            onChange={(e) => setShowCancelled((e.target as HTMLInputElement).checked)}
          />
          Show cancelled
        </label>

        {error && <div class="alert alert-error text-xs mb-3">{error.message}</div>}
        {loading && !data && (
          <div class="space-y-2">
            <Skeleton w="w-3/4" h="h-4" />
            <Skeleton w="w-1/2" h="h-4" />
          </div>
        )}

        {!loading && schedules.length === 0 && (
          <div class="text-sm text-base-content/50 py-6 text-center">
            {showCancelled
              ? "No schedules in this thread."
              : 'No active schedules. Ask the agent to schedule something — e.g. "Every Monday at 9am summarise active runs".'}
          </div>
        )}

        {schedules.length > 0 && (
          <ul class="divide-y divide-base-content/10">
            {schedules.map((s) => (
              <li
                key={s.id}
                class={`py-2 flex items-start justify-between gap-3 ${
                  s.enabled ? "" : "opacity-60"
                }`}
              >
                <div class="min-w-0 flex-1">
                  <div class="flex items-center gap-2">
                    <div class="text-sm font-medium truncate" title={s.name ?? s.id}>
                      {s.name ?? s.id}
                    </div>
                    {!s.enabled && <span class="badge badge-xs badge-ghost">cancelled</span>}
                  </div>
                  <div class="text-xs text-base-content/60 break-words">
                    {(s.metadata?.["task"] as string) ?? "—"}
                  </div>
                  <div class="text-[11px] text-base-content/40 font-mono mt-1">
                    {scheduleTriggerSummary(s)} ·{" "}
                    {s.enabled
                      ? s.nextRunAt
                        ? `next ${formatRelative(s.nextRunAt)}`
                        : "no next run"
                      : "not firing"}{" "}
                    · id: {s.id}
                  </div>
                </div>
                {s.enabled ? (
                  <button
                    class="btn btn-xs btn-ghost text-error shrink-0"
                    onClick={() => cancel(s.id)}
                  >
                    Cancel
                  </button>
                ) : (
                  <button class="btn btn-xs btn-ghost shrink-0" onClick={() => restore(s.id)}>
                    Restore
                  </button>
                )}
              </li>
            ))}
          </ul>
        )}
      </div>
      <div class="modal-backdrop" onClick={onClose} />
    </div>
  );
}

function scheduleTriggerSummary(s: { cron?: string; intervalMs?: number; rrule?: string }): string {
  if (s.cron) return `cron ${s.cron}`;
  if (s.rrule) return `rrule ${s.rrule}`;
  if (s.intervalMs !== undefined) return `every ${(s.intervalMs / 1000).toLocaleString()}s`;
  return "no trigger";
}

/**
 * Inline approval banner — renders right inside the chat scroll area
 * when the agent suspends on a tool call that requires approval. The
 * user resolves it by clicking Approve / Reject; the chat panel POSTs
 * to the resume endpoint and the resumed turn's deltas splice into
 * the same conversation.
 */
function InlineApprovalBanner({
  toolName,
  onApprove,
  onReject,
}: {
  toolName: string;
  onApprove: () => void;
  onReject: () => void;
}) {
  return (
    <div class="alert border border-warning/40 bg-warning/10 text-sm flex items-center gap-3">
      <span class="text-warning text-base">⚠</span>
      <div class="flex-1">
        <div>
          The agent wants to call <span class="font-mono text-xs">{toolName}</span>. Approve to
          continue.
        </div>
      </div>
      <div class="flex gap-1 shrink-0">
        <button class="btn btn-xs btn-ghost" onClick={onReject}>
          Reject
        </button>
        <button class="btn btn-xs btn-primary" onClick={onApprove}>
          Approve
        </button>
      </div>
    </div>
  );
}

function PendingBubble({
  text,
  done,
  error,
  usage,
}: {
  text: string;
  done: boolean;
  error?: string;
  usage?: PendingAssistant["usage"];
}) {
  return (
    <div class="chat chat-start">
      <div class="chat-header text-xs text-base-content/50">
        assistant {!done && <span class="opacity-60">· streaming…</span>}
      </div>
      <div class="chat-bubble break-words">
        {text ? <Markdown text={text} /> : <span class="opacity-50">…</span>}
        {!done && <span class="ml-1 opacity-50 animate-pulse">▍</span>}
      </div>
      {error && <div class="chat-footer text-error text-xs mt-1">{error}</div>}
      {usage && <UsageFooter usage={usage} />}
    </div>
  );
}

/**
 * Tokens-and-cache footer rendered under an assistant bubble. The cache
 * fields surface prompt-cache observability — non-zero `cacheRead` means
 * the system + tools prefix from a prior turn was still warm at the
 * provider; non-zero `cacheWrite` means we just stored it for next time.
 */
function UsageFooter({ usage }: { usage: NonNullable<PendingAssistant["usage"]> }) {
  const cacheParts: string[] = [];
  if (usage.cacheReadTokens !== undefined && usage.cacheReadTokens > 0) {
    cacheParts.push(`cache read ${usage.cacheReadTokens.toLocaleString()}`);
  }
  if (usage.cacheWriteTokens !== undefined && usage.cacheWriteTokens > 0) {
    cacheParts.push(`cache write ${usage.cacheWriteTokens.toLocaleString()}`);
  }
  return (
    <div class="chat-footer text-[10px] text-base-content/50 mt-1 font-mono">
      ↘ {usage.inputTokens.toLocaleString()} in · {usage.outputTokens.toLocaleString()} out
      {cacheParts.length > 0 && <span class="text-success">{` · ${cacheParts.join(" · ")}`}</span>}
    </div>
  );
}
