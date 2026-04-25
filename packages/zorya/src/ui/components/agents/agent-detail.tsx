import { useEffect, useRef, useState } from "preact/hooks";
import { useFetch } from "../../hooks/use-fetch.ts";
import { api } from "../../api/client.ts";
import type { Message, RegisteredAgent, ThreadSummary } from "../../../server/routes/agents.ts";
import { Page } from "../ui/page.tsx";
import { Skeleton } from "../ui/skeleton.tsx";
import { formatRelative } from "../../lib/format.ts";
import { toast } from "../../lib/dialogs.ts";
import { MemoryInspector } from "./memory-inspector.tsx";

interface AgentDetailProps {
  id: string;
  onBack: () => void;
}

const TENANT_KEY = "zorya_agent_tenant";
const ACTIVE_THREAD_KEY = "zorya_agent_active_thread";

interface Tenant {
  namespaceId: string;
  resourceId: string;
}

function loadTenant(): Tenant {
  try {
    const raw = localStorage.getItem(TENANT_KEY);
    if (raw) {
      const parsed = JSON.parse(raw) as Partial<Tenant>;
      if (parsed.namespaceId) {
        return {
          namespaceId: parsed.namespaceId,
          resourceId: parsed.resourceId ?? "alice",
        };
      }
    }
  } catch {
    // fall through
  }
  return { namespaceId: "acme", resourceId: "alice" };
}

function saveTenant(t: Tenant) {
  localStorage.setItem(TENANT_KEY, JSON.stringify(t));
}

export function AgentDetail({ id, onBack }: AgentDetailProps) {
  const [tenant, setTenant] = useState<Tenant>(loadTenant);
  const [activeThread, setActiveThread] = useState<string | null>(() =>
    localStorage.getItem(`${ACTIVE_THREAD_KEY}:${id}`),
  );
  const [inspectorOpen, setInspectorOpen] = useState(false);

  // Persist tenant + active thread.
  useEffect(() => saveTenant(tenant), [tenant]);
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
          {agent && <AgentMetaBadges agent={agent} />}
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
                  Inspect tenant memory →
                </button>
              </div>
            </div>
          )}
        </div>
      </div>

      {inspectorOpen && (
        <MemoryInspector
          namespaceId={tenant.namespaceId}
          resourceId={tenant.resourceId || undefined}
          threadId={activeThread ?? undefined}
          onClose={() => setInspectorOpen(false)}
        />
      )}
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
      <label class="flex items-center gap-1">
        <span class="text-base-content/60">namespaceId</span>
        <input
          class="input input-bordered input-xs font-mono w-32"
          value={tenant.namespaceId}
          onInput={(e) =>
            onChange({ ...tenant, namespaceId: (e.target as HTMLInputElement).value })
          }
        />
      </label>
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
}: {
  agentId: string;
  tenant: Tenant;
  threads: ThreadSummary[];
  activeThread: string | null;
  onSelect: (id: string) => void;
  onNew: (id: string) => void;
}) {
  const [newId, setNewId] = useState("");

  const startNew = () => {
    const trimmed = newId.trim();
    const id = trimmed || `chat-${new Date().toISOString().slice(0, 19).replace(/[:T]/g, "-")}`;
    setNewId("");
    onNew(id);
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
        {threads.map((t) => (
          <li>
            <a
              class={`flex flex-col items-start gap-0 ${activeThread === t.id ? "active" : ""}`}
              onClick={() => onSelect(t.id)}
            >
              <span class="font-mono text-xs truncate w-full" title={t.id}>
                {t.id}
              </span>
              <span class="text-[10px] text-base-content/50">
                {t.messageCount} msg · {formatRelative(new Date(t.lastActiveAt).toISOString())}
              </span>
            </a>
          </li>
        ))}
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
  const abortRef = useRef<{ abort: () => void } | null>(null);
  const scrollRef = useRef<HTMLDivElement>(null);

  // Auto-scroll on new messages or pending updates.
  useEffect(() => {
    const el = scrollRef.current;
    if (!el) return;
    el.scrollTop = el.scrollHeight;
  }, [messages.length, pending?.text, pending?.done]);

  const send = () => {
    const task = draft.trim();
    if (!task || pending) return;
    setDraft("");
    setPending({ text: "", done: false });

    const stream = api.streamAgentThread(
      agentId,
      threadId,
      { task, namespaceId: tenant.namespaceId, resourceId: tenant.resourceId },
      {
        onDelta: (delta) => {
          setPending((p) => (p ? { ...p, text: p.text + delta } : p));
        },
        onFinish: () => {
          setPending((p) => (p ? { ...p, done: true } : p));
        },
        onError: (msg) => {
          setPending((p) => (p ? { ...p, error: msg, done: true } : p));
          toast(`Stream error: ${msg}`, { variant: "error" });
        },
      },
    );
    abortRef.current = stream;
    stream.done.then(() => {
      refreshMessages();
      onTurnComplete();
      // Clear pending after the persisted history catches up.
      setTimeout(() => setPending(null), 250);
    });
  };

  const cancel = () => {
    abortRef.current?.abort();
    abortRef.current = null;
    setPending(null);
  };

  // Build the rendered list: persisted history first, then optional pending.
  const userJustSent =
    pending && messages.length > 0 && messages[messages.length - 1]?.role === "user"
      ? null
      : pending && draft === ""
        ? null
        : null;

  return (
    <section class="card bg-base-100 shadow flex flex-col min-h-0">
      <div class="px-3 py-2 border-b border-base-content/10 flex items-center justify-between gap-2">
        <span class="font-mono text-sm truncate" title={threadId}>
          {threadId}
        </span>
        <div class="flex gap-1 shrink-0">
          <button
            class="btn btn-xs btn-ghost"
            onClick={onInspect}
            title="Inspect resolved system prompt + memory cascade"
          >
            ⌬ Inspect
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

        {messages.map((m, i) => (
          <MessageBubble key={i} message={m} />
        ))}

        {pending && <PendingBubble text={pending.text} done={pending.done} error={pending.error} />}

        {/* Suppress unused-var warning while keeping the variable for future
            "echoed user message" fast-path support. */}
        {userJustSent}
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
    </section>
  );
}

function MessageBubble({ message }: { message: Message }) {
  if (message.role === "system") return null;
  if (message.role === "tool") {
    return (
      <div class="text-xs font-mono text-base-content/60 bg-base-200 rounded p-2">
        <span class="text-warning">tool result</span> {message.content}
      </div>
    );
  }
  const isUser = message.role === "user";
  const content = message.role === "assistant" ? (message.content ?? "") : message.content;
  const toolCalls = message.role === "assistant" ? (message.toolCalls ?? []) : [];

  return (
    <div class={`chat ${isUser ? "chat-end" : "chat-start"}`}>
      <div class="chat-header text-xs text-base-content/50">{isUser ? "you" : "assistant"}</div>
      <div
        class={`chat-bubble whitespace-pre-wrap break-words ${isUser ? "chat-bubble-primary" : ""}`}
      >
        {content || (toolCalls.length > 0 ? <em class="opacity-60">(tool call only)</em> : "")}
      </div>
      {toolCalls.length > 0 && (
        <div class="chat-footer mt-1 space-y-1">
          {toolCalls.map((c) => (
            <div class="text-[10px] font-mono text-base-content/60 bg-base-300 px-2 py-1 rounded inline-block">
              → {c.name}({JSON.stringify(c.input).slice(0, 120)})
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

function PendingBubble({ text, done, error }: { text: string; done: boolean; error?: string }) {
  return (
    <div class="chat chat-start">
      <div class="chat-header text-xs text-base-content/50">
        assistant {!done && <span class="opacity-60">· streaming…</span>}
      </div>
      <div class="chat-bubble whitespace-pre-wrap break-words">
        {text || <span class="opacity-50">…</span>}
        {!done && <span class="ml-1 opacity-50 animate-pulse">▍</span>}
      </div>
      {error && <div class="chat-footer text-error text-xs mt-1">{error}</div>}
    </div>
  );
}
