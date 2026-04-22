/**
 * Interactive console agent with persistent memory, filesystem access, and multi-LLM delegation.
 *
 * Run:
 *   ANTHROPIC_API_KEY=sk-... bun packages/agent/src/examples/console-chat.ts
 *   AGENT_WORKSPACE=/path/to/project ...    (default: cwd)
 *   RATE_LIMIT_RPM=10 ...                   (max LLM calls per minute)
 *   SESSION_TOKEN_BUDGET=100000 ...         (session token cap; blocks new turns when exhausted)
 *   TOOL_AUTO_APPROVE=true ...              (skip approval prompts; same as /approve-all)
 *
 * Commands:
 *   /history                  — conversation messages
 *   /steps                    — workflow step tree
 *   /state                    — agent lifecycle state machine
 *   /tools                    — loaded tools
 *   /memories [query]         — search memories (omit query to list all)
 *   /remember <text>          — save a memory directly
 *   /schedules                — list active schedules
 *   /cancel-schedule <id>     — immediately cancel a schedule (bypasses agent)
 *   /pause-schedule <id>      — pause a schedule
 *   /approve-all              — toggle auto-approve for all tool calls
 *   exit                      — quit
 */

import { createInterface } from "node:readline";
import { mkdir } from "node:fs/promises";
import { join } from "node:path";
import {
  stateMachine,
  InMemoryStateMachineStorage,
  InMemoryWorkflowStorage,
  createWorkflowRunner,
  InMemoryScheduler,
} from "@promin/workflow";
import { PipelineRateLimiter } from "@promin/core";
import { anthropic } from "../lib/adapters/anthropic.ts";
import { openai } from "../lib/adapters/openai.ts";
import { agentLoop } from "../lib/agent-loop.ts";
import { tool } from "../lib/tool.ts";
import { InMemoryMemoryStore } from "../lib/memory-store.ts";
import { InMemorySecretStore } from "../lib/secret-store.ts";
import { createWriteToolTool } from "../lib/tools/write-tool.ts";
import { createRequireSecretTool } from "../lib/tools/require-secret-tool.ts";
import { createFilesystemTools } from "../lib/tools/filesystem-tools.ts";
import { createShellTool } from "../lib/tools/shell-tool.ts";
import { createMemoryTools } from "../lib/tools/memory-tools.ts";
import { createLlmTool } from "../lib/tools/llm-tool.ts";
import { createSchedulerTools } from "../lib/tools/scheduler-tools.ts";
import { createAgentTool } from "../lib/tools/agent-tool-factory.ts";
import { createFileToolRegistry } from "../lib/tool-registry.ts";
import { Terminal, PROMPT } from "./terminal.ts";
import type { TreeNode } from "./terminal.ts";
import type { LLMProvider, LLMUsage } from "../lib/llm-provider.ts";
import type { ToolRegistry } from "../lib/tool-registry.ts";
import type { AgentTool } from "../lib/tool.ts";
import { z } from "zod";

const apiKey = process.env.ANTHROPIC_API_KEY;
if (!apiKey) {
  console.error("Set ANTHROPIC_API_KEY to run.");
  process.exit(1);
}

const workspace = process.env.AGENT_WORKSPACE ?? process.cwd();

// readline
const rl = createInterface({ input: process.stdin, output: process.stdout, historySize: 100 });
const term = new Terminal(rl);

// ---- token usage tracking ----

type Totals = { input: number; output: number; cacheRead: number; cacheWrite: number };
const sessionUsage: Totals = { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 };
let lastTurnUsage: Totals = { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 };

function withUsageTracking(llm: LLMProvider): LLMProvider {
  const add = (u: LLMUsage) => {
    lastTurnUsage.input += u.inputTokens;
    lastTurnUsage.output += u.outputTokens;
    lastTurnUsage.cacheRead += u.cacheReadTokens ?? 0;
    lastTurnUsage.cacheWrite += u.cacheWriteTokens ?? 0;
    sessionUsage.input += u.inputTokens;
    sessionUsage.output += u.outputTokens;
    sessionUsage.cacheRead += u.cacheReadTokens ?? 0;
    sessionUsage.cacheWrite += u.cacheWriteTokens ?? 0;
  };
  const wrapped: LLMProvider = {
    chat: async (params) => {
      const r = await llm.chat(params);
      if (r.usage) add(r.usage);
      return r;
    },
  };
  if (llm.chatStream) {
    const orig = llm.chatStream.bind(llm);
    wrapped.chatStream = async function* (params) {
      for await (const chunk of orig(params)) {
        if (chunk.usage) add(chunk.usage);
        yield chunk;
      }
    };
  }
  return wrapped;
}

function fmtN(n: number): string {
  return n >= 10_000
    ? `${Math.round(n / 1000)}k`
    : n >= 1000
      ? `${(n / 1000).toFixed(1)}k`
      : String(n);
}

const tokenBudget = process.env.SESSION_TOKEN_BUDGET
  ? Number(process.env.SESSION_TOKEN_BUDGET)
  : null;

function sessionTokensUsed(): number {
  return sessionUsage.input + sessionUsage.output;
}

function printUsage(): void {
  const t = lastTurnUsage;
  if (!t.input && !t.output) return;
  const parts: string[] = [`in ${fmtN(t.input)}`, `out ${fmtN(t.output)}`];
  if (t.cacheRead) parts.push(`cached ${fmtN(t.cacheRead)}`);
  if (t.cacheWrite) parts.push(`wrote ${fmtN(t.cacheWrite)}`);
  const used = sessionTokensUsed();
  const budgetStr = tokenBudget
    ? `${fmtN(used)} / ${fmtN(tokenBudget)} tokens`
    : `${fmtN(used)} tokens`;
  parts.push(`·  session ${budgetStr}`);
  process.stdout.write(`\x1b[2m  ${parts.join("  ")}\x1b[0m\n`);
}

function ask(question: string): Promise<string> {
  return new Promise((resolve) => {
    term.stopSpinner(); // clear status line so the prompt is visible
    rl.question(`\n${question}: `, (v) => {
      process.stdout.write("\n");
      resolve(v.trim());
    });
  });
}

// dynamic tool registry (watches examples/tools/ for hot-loaded tools)
const toolsDir = join(import.meta.dir, "tools");
await mkdir(toolsDir, { recursive: true });

// Batch onLoad notifications with a short debounce so startup prints one
// summary line instead of one line per file.
let _loadBatch: string[] = [];
let _loadTimer: ReturnType<typeof setTimeout> | null = null;
function _flushLoadBatch() {
  if (!_loadBatch.length) return;
  const shown = _loadBatch.slice(0, 5);
  const extra = _loadBatch.length - 5;
  const label = shown.join(", ") + (extra > 0 ? `, …and ${extra} more` : "");
  console.log(`\x1b[2mloaded tools: ${label}\x1b[0m`);
  _loadBatch = [];
  _loadTimer = null;
}

const fileRegistry = await createFileToolRegistry({
  dir: toolsDir,
  onLoad: (name, cat) => {
    _loadBatch.push(cat ? `${cat}/${name}` : name);
    if (_loadTimer) clearTimeout(_loadTimer);
    _loadTimer = setTimeout(_flushLoadBatch, 50);
  },
  onUnload: (name, cat) =>
    console.log(`\x1b[2munloaded tool: ${cat ? `${cat}/` : ""}${name}\x1b[0m`),
  onError: (file, err) => console.error(`tool error: ${file}`, err),
});

// memory
const memoryStore = new InMemoryMemoryStore();

// Consumes source until signal fires, then returns WITHOUT calling iter.return().
// The underlying session.stream() generator is abandoned in-place — the background
// workflow run continues to completion (holding the lock) and the next session.stream()
// call waits for it via runner.runSafe()'s distributed lock before starting.
async function* abortable(
  source: AsyncIterable<string>,
  signal: AbortSignal,
): AsyncGenerator<string> {
  if (signal.aborted) return;
  const iter = source[Symbol.asyncIterator]();
  const abortPromise = new Promise<void>((r) =>
    signal.addEventListener("abort", () => r(), { once: true }),
  );
  while (true) {
    let aborted = false;
    const result = await Promise.race([
      iter.next(),
      abortPromise.then(() => {
        aborted = true;
        return { done: true as const, value: "" };
      }),
    ]);
    if (aborted || result.done) break;
    yield result.value;
  }
  // Intentionally NOT calling iter.return() — lets background workflow finish on its own.
}

// Picks the most human-readable value from a tool's input for spinner display.
const ABBREV_PRIORITY = [
  "command",
  "cmd",
  "path",
  "file",
  "url",
  "query",
  "expression",
  "text",
  "message",
  "content",
  "name",
  "prompt",
  "input",
];
function abbrevInput(input: Record<string, unknown>): string {
  for (const key of ABBREV_PRIORITY) {
    if (key in input && typeof input[key] === "string") {
      const v = input[key] as string;
      return v.length > 42 ? `${v.slice(0, 39)}…` : v;
    }
  }
  for (const v of Object.values(input)) {
    if (typeof v === "string") return v.length > 42 ? `${v.slice(0, 39)}…` : v;
  }
  return "";
}

// Counts completed tool calls in the current turn (reset before each stream()).
let turnStep = 0;

// Tracks concurrently active tool calls: callId -> { name, abbrevDim, startMs }
// Used to show all parallel tool names in the spinner label simultaneously.
const activeToolCalls = new Map<string, { name: string; abbrevDim: string; startMs: number }>();
let _callSeq = 0;

function _refreshSpinner(): void {
  if (activeToolCalls.size === 0) {
    term.startSpinner(`thinking...  \x1b[2mstep ${turnStep + 1}\x1b[0m`);
    return;
  }
  const labels = [...activeToolCalls.values()].map((e) => `→ ${e.name}${e.abbrevDim}`);
  if (labels.length === 1) {
    term.startSpinner(labels[0]);
  } else {
    // Show all parallel tools separated by  ║
    term.startSpinner(labels.join(`  \x1b[2m║\x1b[0m  `));
  }
}

// Wraps every tool's execute to show name + abbreviated input in the spinner,
// then prints a one-line summary above the prompt when the call finishes.
// Supports parallel tool calls: all concurrently active tools appear in the spinner.
// biome-ignore lint/suspicious/noExplicitAny: preserves runtime behavior
function withStatusTracking(registry: ToolRegistry): ToolRegistry {
  return {
    getTools() {
      const tools = registry.getTools();
      return Object.fromEntries(
        Object.entries(tools).map(([name, t]) => [
          name,
          {
            ...t,
            // biome-ignore lint/suspicious/noExplicitAny: runtime-validated by Zod in agentAction
            execute: async (input: any) => {
              const abbrev = abbrevInput(input ?? {});
              const abbrevDim = abbrev ? `  \x1b[2m${abbrev}\x1b[0m` : "";
              const callId = String(++_callSeq);
              const startMs = Date.now();
              activeToolCalls.set(callId, { name, abbrevDim, startMs });
              _refreshSpinner();
              let failed = false;
              try {
                return await t.execute(input);
              } catch (err) {
                failed = true;
                throw err;
              } finally {
                activeToolCalls.delete(callId);
                const ms = Date.now() - startMs;
                const elapsedStr = ms >= 1000 ? `${(ms / 1000).toFixed(1)}s` : `${ms}ms`;
                if (failed) {
                  term.printAbove(`\x1b[31m✗ ${name}${abbrevDim}  failed  (${elapsedStr})\x1b[0m`);
                } else {
                  term.printAbove(`\x1b[2m✓ ${name}${abbrevDim}  (${elapsedStr})\x1b[0m`);
                }
                turnStep++;
                _refreshSpinner();
              }
            },
          } as AgentTool<any, any>,
        ]),
      ) as Record<string, AgentTool<any, any>>;
    },
    close: () => registry.close(),
  };
}

// tools
const openaiKeyStore = new InMemorySecretStore();

// Shared one-shot GPT-4o tool — used by both the main agent and sub-agents.
const chatGptOneShotTool = createLlmTool(
  {
    // provider resolved lazily so the key is only prompted on first use
    chat: async (params) => {
      let key = await openaiKeyStore.get("OPENAI_API_KEY");
      if (!key) {
        key = process.env.OPENAI_API_KEY ?? (await ask("[chatGPT] Enter OPENAI_API_KEY"));
        await openaiKeyStore.set("OPENAI_API_KEY", key);
      }
      return openai("gpt-4o", { apiKey: key }).chat(params);
    },
  },
  {
    name: "chatGPT",
    description: "Consult ChatGPT (GPT-4o) for a second opinion or different perspective.",
  },
);
const scheduler = new InMemoryScheduler();
// Tracks schedule IDs with an in-flight onTick call — prevents pileup.
const activeTicks = new Set<string>();

// forward reference — assigned after session is created below
let sessionRef: { send: (task: string) => Promise<string> } | undefined;

// biome-ignore lint/suspicious/noExplicitAny: tool registry uses runtime Zod validation
const staticTools: Record<string, AgentTool<any, any>> = {
  calculator: tool({
    name: "calculator",
    description: "Evaluate a JS math expression and return the result.",
    parameters: z.object({ expression: z.string() }),
    // biome-ignore lint/security/noEval: example only
    execute: async ({ expression }) => {
      try {
        return String(eval(expression));
      } catch {
        return `Error: ${expression}`;
      }
    },
  }),

  currentTime: tool({
    name: "currentTime",
    description: "Return the current local date and time.",
    parameters: z.object({}),
    execute: async () => new Date().toLocaleString(),
  }),

  // lets the LLM create new tools at runtime, saved to examples/tools/
  writeTool: createWriteToolTool({
    dir: toolsDir,
    requireApproval: false,
    toolImportPath: "../../lib/index.ts",
  }),

  requireSecret: createRequireSecretTool({
    readSecret: (prompt) => ask(`[secret] ${prompt}`),
  }),

  ...createFilesystemTools({ rootDir: workspace }),

  shell: createShellTool({
    cwd: workspace,
    allowedCommands: ["bun", "git", "ls", "cat", "find", "grep", "npm", "npx"],
  }),

  ...createMemoryTools({ store: memoryStore }),

  chatGPT: chatGptOneShotTool,

  ...createSchedulerTools({
    scheduler,
    onTick: (task, tick) => {
      // Skip if cancelled or already processing this schedule.
      if (!scheduler.list().find((s) => s.id === tick.scheduleId)) return;
      if (activeTicks.has(tick.scheduleId)) return;
      activeTicks.add(tick.scheduleId);
      const run = async () => {
        try {
          term.printAbove(`\x1b[2m[scheduler → ${task}]\x1b[0m`);
          const prevSuppress = term.suppress;
          term.suppress = true;
          const answer = await sessionRef!.send(task);
          // Stop any spinner the scheduler turn started before restoring visibility.
          term.stopSpinner();
          term.suppress = prevSuppress;
          term.printAbove(`\x1b[2m[scheduler]\x1b[0m Agent: ${answer}`, "");
        } finally {
          activeTicks.delete(tick.scheduleId);
        }
      };
      run().catch((err) => console.error("[scheduler] tick error:", err));
    },
  }),
};

const mergedRegistry: ToolRegistry = {
  getTools: () => ({ ...fileRegistry.getTools(), ...staticTools }),
  close: () => fileRegistry.close(),
};

// agent lifecycle state machine (tracks turns for /state command)
type AgentStates = {
  idle: { context: { turns: number }; transitions: { message: "thinking" } };
  thinking: {
    context: { turns: number; turn: number; task: string };
    transitions: { done: "idle" };
  };
};

const agentMachine = stateMachine<AgentStates>({
  name: "agent-lifecycle",
  storage: new InMemoryStateMachineStorage(),
})
  .state("idle")
  .state("thinking")
  // biome-ignore lint/suspicious/noExplicitAny: state machine action context is dynamically typed
  .on("message", {
    from: "idle",
    to: "thinking",
    action: (ctx: any, d: any) => ({ turns: ctx.turns, turn: d.turn, task: d.task }),
  })
  // biome-ignore lint/suspicious/noExplicitAny: state machine action context is dynamically typed
  .on("done", { from: "thinking", to: "idle", action: (ctx: any) => ({ turns: ctx.turns + 1 }) })
  .initial("idle")
  .build();

// infrastructure
const storage = new InMemoryWorkflowStorage();
const runner = createWorkflowRunner({ storage });

await agentMachine.start({ id: "session", context: { turns: 0 } });

// Shared tool set given to every sub-agent: workspace access + cross-model consultation.
const subagentTools = {
  ...createFilesystemTools({ rootDir: workspace }),
  shell: createShellTool({
    cwd: workspace,
    allowedCommands: ["bun", "git", "ls", "cat", "find", "grep", "npm", "npx"],
  }),
  ...createMemoryTools({ store: memoryStore }),
  chatGPT: chatGptOneShotTool,
};

const subagentSystemPrompt = `You are a focused sub-agent. Be concise and task-focused.\nWorkspace: ${workspace}`;

// claudeAgent: parallel Claude Sonnet sub-agent — delegates independent subtasks.
staticTools.claudeAgent = createAgentTool({
  runner,
  llm: anthropic("claude-sonnet-4-6", { apiKey }),
  name: "claudeAgent",
  description:
    "Delegate a task to a parallel Claude (Sonnet) sub-agent with filesystem, shell, memory, and chatGPT access. Use to parallelise independent subtasks or run deep research alongside the main thread.",
  tools: subagentTools,
  systemPrompt: subagentSystemPrompt,
});

// gptAgent: parallel GPT-4o sub-agent with the same workspace tools (key prompted on first use).
const lazyOpenAI: LLMProvider = {
  chat: async (params) => {
    let key = await openaiKeyStore.get("OPENAI_API_KEY");
    if (!key) {
      key = process.env.OPENAI_API_KEY ?? (await ask("[gptAgent] Enter OPENAI_API_KEY"));
      await openaiKeyStore.set("OPENAI_API_KEY", key);
    }
    return openai("gpt-4o", { apiKey: key }).chat(params);
  },
  chatStream: async function* (params) {
    let key = await openaiKeyStore.get("OPENAI_API_KEY");
    if (!key) {
      key = process.env.OPENAI_API_KEY ?? (await ask("[gptAgent] Enter OPENAI_API_KEY"));
      await openaiKeyStore.set("OPENAI_API_KEY", key);
    }
    yield* openai("gpt-4o", { apiKey: key }).chatStream!(params);
  },
};

staticTools.gptAgent = createAgentTool({
  runner,
  llm: lazyOpenAI,
  name: "gptAgent",
  description:
    "Delegate a task to a parallel GPT-4o sub-agent with filesystem, shell, memory, and chatGPT access. Use for a second opinion, different reasoning style, or to parallelise work.",
  tools: subagentTools,
  systemPrompt: subagentSystemPrompt,
});

const rateLimitRpm = process.env.RATE_LIMIT_RPM ? Number(process.env.RATE_LIMIT_RPM) : null;
const rateLimiter = rateLimitRpm
  ? PipelineRateLimiter.make({ limit: rateLimitRpm, windowMs: 60_000, strategy: "sliding-window" })
  : undefined;

const SYSTEM_PROMPT = [
  "You are a helpful assistant in an interactive console. Be concise.",
  "IMPORTANT: After every tool call (or sequence of tool calls), always write a brief text",
  "reply confirming what was done. Never end a turn silently — the user cannot see tool results.",
  `Workspace: ${workspace}`,
  "Tools: readFile, writeFile, listDir, statFile (filesystem), shell (run commands),",
  "       searchMemory, saveMemory (long-term memory),",
  "       chatGPT (one-shot GPT-4o query for a quick second opinion),",
  "       claudeAgent (parallel Claude sub-agent with filesystem/shell/memory/chatGPT access),",
  "       gptAgent (parallel GPT-4o sub-agent with filesystem/shell/memory/chatGPT access),",
  "       scheduleTask, listSchedules, cancelSchedule (recurring tasks),",
  "       writeTool (create new tools at runtime), requireSecret (prompt user for API keys).",
  "Use claudeAgent or gptAgent to parallelise independent subtasks or get a different perspective.",
  "Never ask for secrets in chat — always use requireSecret.",
].join("\n");

let autoApprove = process.env.TOOL_AUTO_APPROVE === "true";
let sessionIdSeq = 0;

async function makeAgentSession(id: string) {
  return agentLoop({
    name: "console-agent",
    llm: withUsageTracking(anthropic("claude-sonnet-4-6", { apiKey })),
    toolRegistry: withStatusTracking(mergedRegistry),
    rateLimiter,
    systemPrompt: SYSTEM_PROMPT,
    memory: { store: memoryStore },
    hooks: {
      onApprovalRequired: async (call) => {
        if (autoApprove) return { approved: true };
        term.stopSpinner();
        const input = (call.input as Record<string, unknown>) ?? {};
        const abbrev = abbrevInput(input);
        // Fall back to truncated JSON if no priority key matched.
        const paramStr = abbrev || JSON.stringify(input).slice(0, 80);
        const answer = await ask(
          `Allow tool "${call.name}"${paramStr ? `  \x1b[2m${paramStr}\x1b[0m` : ""}? [y/n/always]`,
        );
        if (answer.toLowerCase() === "always") {
          autoApprove = true;
          term.printAbove("\x1b[2mAuto-approve enabled for this session.\x1b[0m");
        }
        const approved = answer.toLowerCase().startsWith("y") || answer.toLowerCase() === "always";
        if (approved) term.startSpinner(`thinking...  \x1b[2mstep ${turnStep + 1}\x1b[0m`);
        return { approved };
      },
    },
  }).session({ runner, sessionId: id });
}

let session = await makeAgentSession("session");
sessionRef = session;

// ---- command pane builders ----
// Each returns string[] so command handlers can pass to term.showPane().

function buildHistory(): string[] {
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

async function buildAgentState(): Promise<string[]> {
  const state = await agentMachine.getState("session");
  const transitions = await agentMachine.getHistory("session");
  if (!state) return ["(no state yet)"];
  const lines: string[] = [
    `state: \x1b[1m${state.current}\x1b[0m   context: ${JSON.stringify(state.context)}`,
    "",
  ];
  for (const t of transitions)
    lines.push(
      `  ${t.from} \x1b[2m──[\x1b[0m${t.event}\x1b[2m]──▶\x1b[0m ${t.to}   \x1b[2m${t.createdAt.toLocaleTimeString()}\x1b[0m`,
    );
  return lines;
}

const ICON: Record<string, string> = {
  completed: "✓",
  failed: "✗",
  running: "◎",
  pending: "○",
  sleeping: "⏸",
  waiting_for_signal: "⏳",
};

async function buildStepsTree(): Promise<TreeNode[]> {
  const runs = await storage.listWorkflows({ name: "console-agent" });
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

            // Pretty-print value as expandable children (one TreeNode per line).
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
            label: `${ICON[step.status] ?? "?"} ${name}`,
            children: entryNodes,
            expanded: false,
          };
        }),
      );

      return {
        label: `${ICON[info.state] ?? "?"} ${run.workflowId}  \x1b[2m(${info.state})\x1b[0m`,
        children: stepNodes,
        expanded: true,
      };
    }),
  );
}

async function buildMemories(query?: string): Promise<string[]> {
  const entries = query ? await memoryStore.search(query, 10) : await memoryStore.list(50);
  if (!entries.length) return [query ? `(no memories matching "${query}")` : "(no memories yet)"];
  return entries.map(
    (e) =>
      `  \x1b[2m[${e.createdAt.toLocaleTimeString()}] ${e.id.slice(0, 8)}\x1b[0m  ${e.content}`,
  );
}

function buildSchedules(): string[] {
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

// ---- interrupt handling ----
// Ctrl+C while agent is working: immediately returns to prompt (background run finishes on its own).
// Ctrl+C when idle: exits.
let currentAc: AbortController | null = null;

rl.on("SIGINT", () => {
  if (currentAc) {
    if (term.agentHasTextOnLine) process.stdout.write("\n");
    term.stopSpinner();
    process.stdout.write("\x1b[2m(interrupted)\x1b[0m\n");
    term.suppress = true;
    currentAc.abort();
    currentAc = null;
  } else {
    process.stdout.write("\n");
    session
      .close()
      .catch(() => {})
      .finally(() => {
        mergedRegistry.close();
        term.close();
        rl.close();
        process.exit(0);
      });
  }
});

// REPL
let turn = 0;

function prompt() {
  const parts: string[] = [];

  function readLine(isFirst: boolean): void {
    rl.question(isFirst ? PROMPT : "... ", async (line) => {
      if (isFirst) {
        term.inPrompt = false;
        term.stopPromptAnimation();
      }

      // Multi-line continuation: trailing backslash collects more lines.
      if (line.endsWith("\\")) {
        parts.push(line.slice(0, -1));
        readLine(false);
        return;
      }

      parts.push(line);
      const input = parts.join("\n").trim();
      parts.length = 0;

      if (!input) return prompt();

      if (input === "exit" || input === "quit") {
        await session.close();
        mergedRegistry.close();
        term.close();
        return rl.close();
      }

      // Display commands — shown as transient panes that erase on dismiss.
      if (input === "/help") {
        await term.showPane("help", [
          "  /history                  — conversation messages (full session truth)",
          "  /clear                    — start a new conversation (memories persist)",
          "  /steps                    — workflow step tree",
          "  /state                    — agent lifecycle state machine",
          "  /tools                    — loaded tools",
          "  /memories [query]         — search memories (omit query to list all)",
          "  /remember <text>          — save a memory directly",
          "  /schedules                — list active schedules",
          "  /cancel-schedule <id>     — immediately cancel a schedule",
          "  /pause-schedule <id>      — pause a schedule",
          `  /approve-all              — toggle auto-approve (currently: ${autoApprove ? "ON" : "OFF"})`,
          "  /help                     — show this help",
          "  exit                      — quit",
          "",
          "  Multi-line input: end a line with \\ to continue on the next line.",
          "  Ctrl+C during a turn: interrupt (background run continues).",
          "  Ctrl+C at prompt: exit.",
          "  Approval prompt: answer 'always' to enable auto-approve for the session.",
        ]);
        return prompt();
      }
      if (input === "/clear") {
        await session.close();
        session = await makeAgentSession(`session-${++sessionIdSeq}`);
        sessionRef = session;
        turn = 0;
        sessionUsage.input = 0;
        sessionUsage.output = 0;
        sessionUsage.cacheRead = 0;
        sessionUsage.cacheWrite = 0;
        lastTurnUsage = { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 };
        term.printAbove(
          "\x1b[2mConversation cleared — new session started. Memories persist.\x1b[0m",
        );
        return prompt();
      }
      if (input === "/history") {
        await term.showPane("history", buildHistory());
        return prompt();
      }
      if (input === "/steps") {
        await term.showInteractiveTree("steps", await buildStepsTree());
        return prompt();
      }
      if (input === "/state") {
        await term.showPane("state", await buildAgentState());
        return prompt();
      }
      if (input === "/tools") {
        await term.showPane(
          "tools",
          Object.keys(mergedRegistry.getTools()).map((n) => `  ${n}`),
        );
        return prompt();
      }
      if (input === "/schedules") {
        await term.showPane("schedules", buildSchedules());
        return prompt();
      }
      if (input.startsWith("/memories")) {
        const q = input.slice("/memories".length).trim();
        await term.showPane(`memories${q ? ` · "${q}"` : ""}`, await buildMemories(q || undefined));
        return prompt();
      }

      if (input === "/approve-all") {
        autoApprove = !autoApprove;
        console.log(`\n\x1b[2mAuto-approve: ${autoApprove ? "ON" : "OFF"}\x1b[0m\n`);
        return prompt();
      }

      // Action commands — inline confirmation, no pane.
      if (input.startsWith("/remember ")) {
        const text = input.slice("/remember ".length).trim();
        if (text) {
          const id = await memoryStore.save({ content: text });
          console.log(`\n\x1b[2mSaved ${id.slice(0, 8)}: "${text}"\x1b[0m\n`);
        }
        return prompt();
      }
      if (input.startsWith("/cancel-schedule ")) {
        const id = input.slice("/cancel-schedule ".length).trim();
        scheduler.unregister(id);
        activeTicks.delete(id);
        console.log(`\n\x1b[2mCancelled schedule "${id}"\x1b[0m\n`);
        return prompt();
      }
      if (input.startsWith("/pause-schedule ")) {
        const id = input.slice("/pause-schedule ".length).trim();
        scheduler.pause(id);
        console.log(`\n\x1b[2mPaused schedule "${id}"\x1b[0m\n`);
        return prompt();
      }

      await agentMachine.send({ id: "session", event: "message", data: { turn, task: input } });

      if (tokenBudget && sessionTokensUsed() >= tokenBudget) {
        const used = fmtN(sessionTokensUsed());
        process.stdout.write(
          `\n\x1b[33m  Token budget exhausted (${used} / ${fmtN(tokenBudget)}). Start a new session to continue.\x1b[0m\n`,
        );
        return prompt();
      }

      const ac = new AbortController();
      currentAc = ac;
      lastTurnUsage = { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 };
      turnStep = 0;
      activeToolCalls.clear();
      term.suppress = false;
      term.startSpinner("thinking...");
      term.agentHasTextOnLine = false;

      let answer = "";
      let labelShown = false;
      let streamError: Error | undefined;
      try {
        for await (const chunk of abortable(session.stream(input, ac.signal), ac.signal)) {
          term.stopSpinner();
          if (!labelShown) {
            process.stdout.write("Agent: ");
            labelShown = true;
          }
          term.writeChunk(chunk);
          term.agentHasTextOnLine = true;
          answer += chunk;
        }
        term.flushChunks();
      } catch (err) {
        term.flushChunks();
        streamError = err instanceof Error ? err : new Error(String(err));
      }

      currentAc = null;
      term.stopSpinner();

      if (ac.signal.aborted) {
        term.agentHasTextOnLine = false;
        await agentMachine.send({ id: "session", event: "done" }).catch(() => {});
        prompt();
        return;
      }

      if (streamError) {
        if (term.agentHasTextOnLine) process.stdout.write("\n");
        term.agentHasTextOnLine = false;

        // "Prompt too long" errors leave the workflow permanently stuck at the
        // failing think activity — replaying the same poisoned history every time.
        // Auto-clear so the user can continue without having to know about /clear.
        const contextFull =
          streamError.message.includes("prompt is too long") ||
          streamError.message.includes("context_length_exceeded") ||
          streamError.message.includes("maximum context");

        if (contextFull) {
          process.stdout.write(
            "\x1b[31mContext window full — conversation history is too large to continue.\x1b[0m\n",
          );
          await session.close();
          session = await makeAgentSession(`session-${++sessionIdSeq}`);
          sessionRef = session;
          turn = 0;
          sessionUsage.input = 0;
          sessionUsage.output = 0;
          sessionUsage.cacheRead = 0;
          sessionUsage.cacheWrite = 0;
          lastTurnUsage = { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 };
          process.stdout.write(
            "\x1b[2m  Session automatically cleared. Memories persist.\x1b[0m\n",
          );
        } else {
          process.stdout.write(`\x1b[31mError: ${streamError.message}\x1b[0m\n`);
        }

        await agentMachine.send({ id: "session", event: "done" }).catch(() => {});
        prompt();
        return;
      }

      process.stdout.write("\n");
      printUsage();
      if (tokenBudget) {
        const used = sessionTokensUsed();
        const pct = used / tokenBudget;
        if (pct >= 0.8 && pct < 1.0) {
          term.printAbove(
            `\x1b[33m  Token budget ${Math.round(pct * 100)}% used — ${fmtN(tokenBudget - used)} remaining\x1b[0m`,
          );
        }
      }
      term.agentHasTextOnLine = false;
      turn++;
      await agentMachine.send({ id: "session", event: "done" });
      prompt();
    });

    if (isFirst) {
      term.inPrompt = true;
      term.startPromptAnimation();
    }
  }

  readLine(true);
}

console.log(
  `\nConsole agent  workspace=${workspace}  tools=${Object.keys(staticTools).join(", ")}`,
);
console.log(`Type /help for commands. Use \\ at line end for multi-line input.\n`);
prompt();
