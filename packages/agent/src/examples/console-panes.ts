import type { AgentSession } from "../lib/agent-loop.ts";
import type { MemoryStore } from "../lib/memory-store.ts";
import type { TreeNode } from "./common/terminal.ts";
import type { InMemoryWorkflowStorage, InMemoryScheduler, WorkflowRunner } from "@promin/workflow";

export const WORKFLOW_ICON: Record<string, string> = {
  completed: "✓",
  failed: "✗",
  running: "◎",
  pending: "○",
  sleeping: "⏸",
  waiting_for_signal: "⏳",
};

export function buildHistory(session: AgentSession): string[] {
  const msgs = session.messages();
  if (!msgs.length) return ["(no history yet)"];
  const lines: string[] = [];
  for (const m of msgs) {
    if (m.role === "system") {
      lines.push("", "\x1b[2m[system]\x1b[0m", m.content ?? "");
    } else if (m.role === "user") {
      lines.push("", "\x1b[2m[user]\x1b[0m", m.content ?? "");
    } else if (m.role === "assistant") {
      if (m.content) lines.push("", "\x1b[2m[assistant]\x1b[0m", m.content);
      for (const tc of m.toolCalls ?? [])
        lines.push("", `\x1b[2m[tool: ${tc.name}]\x1b[0m`, JSON.stringify(tc.input));
    } else if (m.role === "tool") {
      lines.push("", `\x1b[2m[result: ${m.toolCallId?.slice(0, 8)}]\x1b[0m`, m.content ?? "");
    }
  }
  lines.push("", `\x1b[2m${msgs.length} messages\x1b[0m`);
  return lines;
}

interface LifecycleMachine {
  getState(id: string): Promise<{ current: string; context: unknown } | null>;
  getHistory(
    id: string,
  ): Promise<Array<{ from: string; event: string; to: string; createdAt: Date }>>;
}

export async function buildAgentState(
  machine: LifecycleMachine,
  sessionId: string,
): Promise<string[]> {
  const st = await machine.getState(sessionId);
  const transitions = await machine.getHistory(sessionId);
  if (!st) return ["(no state yet)"];
  const lines: string[] = [
    `state: \x1b[1m${st.current}\x1b[0m   context: ${JSON.stringify(st.context)}`,
    "",
  ];
  for (const t of transitions)
    lines.push(
      `  ${t.from} \x1b[2m──[\x1b[0m${t.event}\x1b[2m]──▶\x1b[0m ${t.to}   \x1b[2m${t.createdAt.toLocaleTimeString()}\x1b[0m`,
    );
  if (!transitions.length) lines.push("  (no transitions yet)");
  return lines;
}

export async function buildStepsTree(
  storage: InMemoryWorkflowStorage,
  runner: WorkflowRunner,
  workflowName: string,
): Promise<TreeNode[]> {
  const runs = await storage.listWorkflows({ name: workflowName });
  return Promise.all(
    runs.map(async (run): Promise<TreeNode> => {
      const info = await runner.getStatus(run.workflowId, { includeStepResults: false });
      if (!info) return { label: `? ${run.workflowId}`, children: [], expanded: false };

      const stepNodes = await Promise.all(
        Object.entries(info.steps).map(async ([name, step]): Promise<TreeNode> => {
          const entries = await storage.loadJournal(run.workflowId, name);
          const entryNodes: TreeNode[] = entries.map((entry): TreeNode => {
            const icon =
              entry.exit?.tag === "Success" ? "✓" : entry.exit?.tag === "Failure" ? "✗" : "○";
            const lbl =
              entry.stepType === "signal" ? `signal: ${entry.activityName}` : entry.activityName;
            const raw = entry.exit?.tag === "Success" ? JSON.stringify(entry.exit.value) : null;

            const children: TreeNode[] = [];
            if (raw) {
              try {
                const lines = JSON.stringify(JSON.parse(raw), null, 2).split("\n");
                children.push(
                  ...lines.map((l): TreeNode => ({ label: l, children: [], expanded: false })),
                );
              } catch {
                children.push({ label: raw, children: [], expanded: false });
              }
            } else if (entry.exit?.tag === "Failure") {
              children.push({ label: entry.exit.error, children: [], expanded: false });
            }

            const preview = raw
              ? `  →  ${raw.length > 60 ? `${raw.slice(0, 60)}…` : raw}`
              : entry.exit?.tag === "Failure"
                ? `  ✗  ${entry.exit.error.slice(0, 60)}`
                : "";
            return { label: `${icon} ${lbl}\x1b[2m${preview}\x1b[0m`, children, expanded: false };
          });
          return {
            label: `${WORKFLOW_ICON[step.status] ?? "?"} ${name}`,
            children: entryNodes,
            expanded: false,
          };
        }),
      );

      return {
        label: `${WORKFLOW_ICON[info.state] ?? "?"} ${run.workflowId}  \x1b[2m(${info.state})\x1b[0m`,
        children: stepNodes,
        expanded: true,
      };
    }),
  );
}

export async function buildMemories(store: MemoryStore, query?: string): Promise<string[]> {
  const entries = query ? await store.search(query, 10) : await store.list(50);
  if (!entries.length) return [query ? `(no memories matching "${query}")` : "(no memories yet)"];
  return entries.map(
    (e) =>
      `  \x1b[2m[${e.createdAt.toLocaleTimeString()}] ${e.id.slice(0, 8)}\x1b[0m  ${e.content}`,
  );
}

export function buildSchedules(scheduler: InMemoryScheduler): string[] {
  const schedules = scheduler.list();
  if (!schedules.length) return ["(no active schedules)"];
  const lines = schedules.map((s) => {
    const trigger = s.cron ?? (s.intervalMs ? `every ${s.intervalMs}ms` : "unknown");
    const status = s.enabled === false ? " \x1b[2m[paused]\x1b[0m" : "";
    const task = s.metadata?.task ?? "(no task)";
    return `  \x1b[1m${s.id}\x1b[0m${status}  ${trigger}  →  "${task}"`;
  });
  lines.push("", "\x1b[2m/cancel-schedule <id>   /pause-schedule <id>\x1b[0m");
  return lines;
}
