// ---------------------------------------------------------------------------
// AgentTraceModal — read-only post-hoc trace of one thread.
//
// Renders the turn-tree returned by GET /api/agents/:id/threads/:tid/trace
// as an indented hierarchy:
//
//   Turn 1 — "user said this..."
//     └─ assistant: "let me search..."
//        ├─ tool: search({ q: "cats" })  →  "found 42 results"
//        └─ tool: filter({ ... })        →  "Error: ..."   (failure highlighted)
//     └─ assistant: "I found..."
//   Turn 2 — ...
//
// v0 indented tree only. Real graph layout (react-flow) is Phase 2 of dxnf.
// ---------------------------------------------------------------------------

import { useEffect, useMemo, useState } from "preact/hooks";
import {
  api,
  type AgentTraceDto,
  type TraceChildDto,
  type TraceToolCallDto,
} from "../../api/client.ts";
import { useFetch } from "../../hooks/use-fetch.ts";

interface Props {
  agentId: string;
  threadId: string;
  namespaceId: string;
  resourceId?: string;
  onClose: () => void;
}

export function AgentTraceModal({ agentId, threadId, namespaceId, resourceId, onClose }: Props) {
  const { data, loading, error } = useFetch(
    () => api.getThreadTrace(agentId, threadId, { namespaceId, ...(resourceId && { resourceId }) }),
    [agentId, threadId, namespaceId, resourceId],
    0,
  );

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => e.key === "Escape" && onClose();
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onClose]);

  const trace = data?.trace;
  const summary = trace?.summary;

  const [filterToolCalls, setFilterToolCalls] = useState(false);
  const [filterFailuresOnly, setFilterFailuresOnly] = useState(false);

  const exportJson = () => {
    if (!trace) return;
    const blob = new Blob([JSON.stringify(trace, null, 2)], { type: "application/json" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `${agentId}-${threadId}-trace.json`;
    a.click();
    URL.revokeObjectURL(url);
  };

  return (
    <>
      <div class="fixed inset-0 bg-black/40 z-30 anim-backdrop-in" onClick={onClose} aria-hidden />
      <div
        class="fixed inset-x-4 top-8 bottom-8 mx-auto max-w-5xl bg-base-100 rounded-lg shadow-2xl
               z-40 flex flex-col anim-drawer-in"
        role="dialog"
        aria-label="Agent run trace"
      >
        <header class="flex items-start justify-between gap-2 p-4 border-b border-base-300">
          <div class="min-w-0">
            <div class="text-xs text-base-content/50 uppercase tracking-wider">Run trace</div>
            <div class="font-mono text-sm truncate">
              {agentId} · {threadId}
            </div>
            {summary && (
              <div class="text-[10px] text-base-content/50 font-mono mt-0.5">
                {summary.turns} turns · {summary.assistantMoves} assistant moves ·{" "}
                {summary.toolCalls} tool calls
                {summary.toolFailures > 0 && (
                  <span class="text-error">
                    {" "}
                    · {summary.toolFailures} failure{summary.toolFailures === 1 ? "" : "s"}
                  </span>
                )}
                {(summary.orphanedToolCalls > 0 || summary.orphanedToolResults > 0) && (
                  <span class="text-warning">
                    {" "}
                    · ⚠ {summary.orphanedToolCalls + summary.orphanedToolResults} orphan
                  </span>
                )}
              </div>
            )}
          </div>
          <div class="flex items-center gap-2">
            <button
              class="btn btn-sm btn-ghost"
              onClick={exportJson}
              disabled={!trace}
              title="Download trace as JSON for sharing in bug reports"
            >
              Export JSON
            </button>
            <button class="btn btn-sm btn-ghost" onClick={onClose} title="Close (Esc)">
              ✕
            </button>
          </div>
        </header>

        <div class="px-4 py-2 border-b border-base-300 flex items-center gap-3 text-xs">
          <label class="cursor-pointer flex items-center gap-1">
            <input
              type="checkbox"
              class="checkbox checkbox-xs"
              checked={filterToolCalls}
              onChange={(e) => setFilterToolCalls((e.target as HTMLInputElement).checked)}
            />
            Hide tool calls
          </label>
          <label class="cursor-pointer flex items-center gap-1">
            <input
              type="checkbox"
              class="checkbox checkbox-xs"
              checked={filterFailuresOnly}
              onChange={(e) => setFilterFailuresOnly((e.target as HTMLInputElement).checked)}
            />
            Failures only
          </label>
        </div>

        {error && <div class="alert alert-error m-4 text-xs">{error.message}</div>}

        {loading && !data ? (
          <div class="p-8 text-center text-base-content/50 text-sm">Loading trace…</div>
        ) : !trace ? (
          <div class="p-8 text-center text-base-content/50 text-sm">No data.</div>
        ) : trace.turns.length === 0 && trace.orphanSystem.length === 0 ? (
          <div class="p-8 text-center text-base-content/50 text-sm">
            Empty thread — no turns recorded yet.
          </div>
        ) : (
          <div class="flex-1 overflow-y-auto p-4 space-y-4 font-mono text-xs">
            {trace.orphanSystem.length > 0 && (
              <section>
                <div class="text-[10px] uppercase tracking-wider text-base-content/40 mb-1">
                  System messages
                </div>
                {trace.orphanSystem.map((s) => (
                  <pre class="bg-base-200 p-2 rounded mb-1 whitespace-pre-wrap">{s.content}</pre>
                ))}
              </section>
            )}
            {trace.turns.map((turn) => (
              <TurnView
                turn={turn}
                hideToolCalls={filterToolCalls}
                failuresOnly={filterFailuresOnly}
              />
            ))}
          </div>
        )}
      </div>
    </>
  );
}

function TurnView({
  turn,
  hideToolCalls,
  failuresOnly,
}: {
  turn: AgentTraceDto["turns"][number];
  hideToolCalls: boolean;
  failuresOnly: boolean;
}) {
  // When "failures only" is on, hide the whole turn unless it contains
  // at least one failed tool call.
  const hasFailure = useMemo(() => containsFailure(turn.children), [turn.children]);
  if (failuresOnly && !hasFailure) return null;

  return (
    <div class="border border-base-300 rounded">
      <div class="bg-base-200 px-3 py-1.5 text-[10px] uppercase tracking-wider text-base-content/60 font-sans">
        Turn {turn.turnIndex + 1}
        <span class="text-base-content/40 ml-2 normal-case tracking-normal">
          seq {turn.fromSeq}–{turn.toSeq}
        </span>
      </div>
      <div class="p-3 space-y-2">
        {turn.children.map((c) => (
          <ChildView child={c} hideToolCalls={hideToolCalls} failuresOnly={failuresOnly} />
        ))}
      </div>
    </div>
  );
}

function ChildView({
  child,
  hideToolCalls,
  failuresOnly,
}: {
  child: TraceChildDto;
  hideToolCalls: boolean;
  failuresOnly: boolean;
}) {
  if (child.kind === "user") {
    return (
      <div class="border-l-2 border-info pl-2">
        <div class="text-[10px] uppercase tracking-wider text-info/70 mb-0.5">User</div>
        <pre class="whitespace-pre-wrap break-words">{child.content}</pre>
      </div>
    );
  }
  if (child.kind === "system") {
    return (
      <div class="border-l-2 border-base-content/30 pl-2 text-base-content/60">
        <div class="text-[10px] uppercase tracking-wider mb-0.5">System</div>
        <pre class="whitespace-pre-wrap break-words">{child.content}</pre>
      </div>
    );
  }
  if (child.kind === "tool-call") {
    return <ToolCallView call={child} indent={0} failuresOnly={failuresOnly} />;
  }
  // assistant
  const visibleCalls = hideToolCalls ? [] : child.toolCalls;
  return (
    <div class="border-l-2 border-success pl-2">
      <div class="text-[10px] uppercase tracking-wider text-success/70 mb-0.5">Assistant</div>
      {child.content && <pre class="whitespace-pre-wrap break-words">{child.content}</pre>}
      {visibleCalls.map((tc) => (
        <ToolCallView call={tc} indent={1} failuresOnly={failuresOnly} />
      ))}
    </div>
  );
}

function ToolCallView({
  call,
  indent,
  failuresOnly,
}: {
  call: TraceToolCallDto;
  indent: number;
  failuresOnly: boolean;
}) {
  const failed = call.result?.failed === true;
  const orphan = call.callSeq === -1;
  if (failuresOnly && !failed) return null;
  const [expanded, setExpanded] = useState(failed);
  const colorClass = failed ? "border-error" : orphan ? "border-warning" : "border-base-content/30";
  return (
    <div class={`border-l-2 ${colorClass} pl-2 ml-${indent * 4} mt-1`}>
      <button
        type="button"
        class="text-left w-full hover:bg-base-200 rounded px-1"
        onClick={() => setExpanded((v) => !v)}
      >
        <span class="text-[10px] uppercase tracking-wider text-base-content/50">
          {expanded ? "▼" : "▶"} Tool
        </span>
        <span class="ml-1">{call.name}</span>
        {failed && <span class="badge badge-xs badge-error ml-2">failed</span>}
        {orphan && <span class="badge badge-xs badge-warning ml-2">orphan</span>}
        {!call.result && !orphan && (
          <span class="badge badge-xs badge-warning ml-2">no result</span>
        )}
      </button>
      {expanded && (
        <div class="mt-1 space-y-1">
          <details class="ml-4">
            <summary class="cursor-pointer text-[10px] text-base-content/50">input</summary>
            <pre class="bg-base-200 p-2 rounded text-[11px] whitespace-pre-wrap break-words">
              {JSON.stringify(call.input, null, 2)}
            </pre>
          </details>
          {call.result && (
            <details class="ml-4" open={failed}>
              <summary class="cursor-pointer text-[10px] text-base-content/50">
                result {failed && <span class="text-error">(failed)</span>}
              </summary>
              <pre
                class={`p-2 rounded text-[11px] whitespace-pre-wrap break-words ${
                  failed ? "bg-error/10" : "bg-base-200"
                }`}
              >
                {call.result.content}
              </pre>
            </details>
          )}
        </div>
      )}
    </div>
  );
}

function containsFailure(children: ReadonlyArray<TraceChildDto>): boolean {
  for (const c of children) {
    if (c.kind === "assistant") {
      if (c.toolCalls.some((tc) => tc.result?.failed)) return true;
    } else if (c.kind === "tool-call" && c.result?.failed) {
      return true;
    }
  }
  return false;
}
