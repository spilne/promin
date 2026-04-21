/**
 * Single-session console agent — one persistent workflow for the entire conversation.
 *
 * Each user message arrives as a signal (task-N). The journaled step suspends
 * between turns, so the /steps tree shows all turns nested under one session:
 *
 *   └─ ◎ session  (running)
 *      └─ ◎ conversation
 *         ├─ ✓ signal: task-0  →  {"task":"hello"}
 *         ├─ ✓ think-0-0
 *         ├─ ✓ emit-0
 *         ├─ ✓ signal: task-1  →  {"task":"what time is it?"}
 *         ├─ ✓ think-1-0
 *         ├─ ✓ tool-currentTime-1-0-...
 *         ├─ ✓ think-1-1
 *         ├─ ✓ emit-1
 *         └─ ○ signal: task-2   ← waiting for next input
 *
 * Run:
 *   ANTHROPIC_API_KEY=sk-... bun packages/agent/src/examples/console-chat.ts
 *
 * Commands:
 *   /history          — print conversation messages
 *   /steps            — print full workflow step tree from storage
 *   /state            — print agent lifecycle state machine (current state + transition history)
 *   /tools            — list currently loaded dynamic tools
 *   /memories [query] — search memories (omit query to list all)
 *   /remember <text>  — save a memory entry directly (bypasses LLM)
 *   exit              — quit
 */

import { createInterface } from "node:readline";
import { mkdir } from "node:fs/promises";
import { join } from "node:path";
import { InMemoryMemoryStore } from "../lib/memory-store.ts";
import { stateMachine, InMemoryStateMachineStorage } from "@promin/workflow";
import { InMemoryWorkflowStorage, createWorkflowRunner } from "@promin/workflow";
import { anthropic } from "../lib/adapters/anthropic.ts";
import { agentLoop } from "../lib/agent-loop.ts";
import { tool } from "../lib/tool.ts";
import { createWriteToolTool } from "../lib/tools/write-tool.ts";
import { createRequireSecretTool } from "../lib/tools/require-secret-tool.ts";
import { createFilesystemTools } from "../lib/tools/filesystem-tools.ts";
import { createShellTool } from "../lib/tools/shell-tool.ts";
import { createMemoryTools } from "../lib/tools/memory-tools.ts";
import { createFileToolRegistry } from "../lib/tool-registry.ts";
import type { ToolRegistry } from "../lib/tool-registry.ts";
import type { AgentTool } from "../lib/tool.ts";
import { z } from "zod";

// ---- config ----

const apiKey = process.env.ANTHROPIC_API_KEY;
if (!apiKey) {
  console.error("Set ANTHROPIC_API_KEY to run this example.");
  process.exit(1);
}

// Root directory for filesystem + shell tools. Defaults to the directory from
// which the process is started; override with AGENT_WORKSPACE env var.
const workspace = process.env.AGENT_WORKSPACE ?? process.cwd();

// ---- conversation history (updated after each turn for /history command) ----

import type { Message } from "../lib/message.ts";

let conversationHistory: Message[] = [];

function printHistory() {
  if (conversationHistory.length === 0) {
    console.log("\n(no history yet)\n");
    return;
  }
  console.log("\n--- conversation history ---");
  for (const m of conversationHistory) {
    if (m.role === "user") {
      console.log(`\n[user]\n${m.content}`);
    } else if (m.role === "assistant") {
      if (m.content) console.log(`\n[assistant]\n${m.content}`);
      for (const tc of m.toolCalls ?? []) {
        console.log(`\n[tool call: ${tc.name}]\n${JSON.stringify(tc.input, null, 2)}`);
      }
    } else if (m.role === "tool") {
      console.log(`\n[tool result: ${m.toolCallId}]\n${m.content}`);
    }
  }
  console.log(`\n--- ${conversationHistory.length} messages ---\n`);
}

// ---- readline (created early so tools can use it for out-of-band prompts) ----

const rl = createInterface({ input: process.stdin, output: process.stdout });

// ---- dynamic tool registry ----

const toolsDir = join(import.meta.dir, "tools");
await mkdir(toolsDir, { recursive: true });

const fileRegistry = await createFileToolRegistry({
  dir: toolsDir,
  onLoad: (name, category) =>
    console.log(`[registry] loaded: ${category ? `${category}/` : ""}${name}`),
  onUnload: (name, category) =>
    console.log(`[registry] unloaded: ${category ? `${category}/` : ""}${name}`),
  onError: (file, err) => console.error(`[registry] error in ${file}:`, err),
});

// ---- memory store (declared early; used by tools and REPL commands) ----

const memoryStore = new InMemoryMemoryStore();

async function printMemories(query?: string) {
  const entries = query
    ? await memoryStore.search(query, 10)
    : await memoryStore.list(50);
  if (entries.length === 0) {
    console.log(query ? `\n(no memories matching "${query}")\n` : "\n(no memories saved yet)\n");
    return;
  }
  const label = query ? `memories matching "${query}"` : "all memories";
  console.log(`\n--- ${label} (${entries.length}) ---`);
  for (const e of entries) {
    const ts = e.createdAt.toLocaleTimeString();
    console.log(`  [${ts}] ${e.id.slice(0, 8)}  ${e.content}`);
  }
  console.log("");
}

// ---- tools ----

const calculator = tool({
  name: "calculator",
  description: "Evaluate a mathematical expression and return the numeric result.",
  usage: "Use for arithmetic, exponentiation, and trig. Not for string manipulation.",
  examples: [
    { input: { expression: "2 ** 10" }, output: "1024" },
    { input: { expression: "Math.sqrt(144)" }, output: "12" },
  ],
  parameters: z.object({
    expression: z.string().describe("A valid JS math expression, e.g. '2 ** 10'"),
  }),
  execute: async ({ expression }) => {
    try {
      // biome-ignore lint/security/noEval: example only
      return String(eval(expression));
    } catch {
      return `Error evaluating: ${expression}`;
    }
  },
});

const currentTime = tool({
  name: "currentTime",
  description: "Return the current local date and time.",
  parameters: z.object({}),
  execute: async () => new Date().toLocaleString(),
});

const showHistory = tool({
  name: "showHistory",
  description: "Print the full conversation history stored in memory.",
  parameters: z.object({}),
  execute: async () => {
    printHistory();
    return `Printed ${conversationHistory.length} messages.`;
  },
});

// "../../lib/index.ts" resolves correctly from examples/tools/*.ts → src/lib/index.ts
const writeTool = createWriteToolTool({
  dir: toolsDir,
  requireApproval: false,
  toolImportPath: "../../lib/index.ts",
});

const requireSecret = createRequireSecretTool({
  readSecret: (prompt) =>
    new Promise((resolve) => {
      // Value is captured here and never forwarded to the LLM
      rl.question(`\n[secret needed] ${prompt}: `, (value) => {
        process.stdout.write("\n");
        resolve(value);
      });
    }),
});

const fsTools = createFilesystemTools({ rootDir: workspace });

const shell = createShellTool({
  cwd: workspace,
  allowedCommands: ["bun", "git", "ls", "cat", "find", "grep", "npm", "npx"],
});

const memoryTools = createMemoryTools({ store: memoryStore });

// biome-ignore lint/suspicious/noExplicitAny: tool registry uses runtime Zod validation
const staticTools: Record<string, AgentTool<any, any>> = {
  calculator,
  currentTime,
  showHistory,
  writeTool,
  requireSecret,
  ...fsTools,
  shell,
  ...memoryTools,
};

// Merges static tools with the file registry so both are visible each think step.
// Static tools take precedence; registry tools fill in everything else.
const mergedRegistry: ToolRegistry = {
  getTools: () => ({ ...fileRegistry.getTools(), ...staticTools }),
  close: () => fileRegistry.close(),
};

// ---- agent lifecycle state machine ----

type AgentStates = {
  idle: {
    context: { completedTurns: number };
    transitions: { message: "thinking" };
  };
  thinking: {
    context: { completedTurns: number; turn: number; task: string };
    transitions: { done: "idle" };
  };
};

const smStorage = new InMemoryStateMachineStorage();
// biome-ignore lint/suspicious/noExplicitAny: state machine actions receive runtime data
const agentMachine = stateMachine<AgentStates>({ name: "agent-lifecycle", storage: smStorage })
  .state("idle")
  .state("thinking")
  .on("message", {
    from: "idle",
    to: "thinking",
    action: (ctx: any, data: any) => ({
      completedTurns: ctx.completedTurns,
      turn: data.turn,
      task: data.task,
    }),
  })
  .on("done", {
    from: "thinking",
    to: "idle",
    action: (ctx: any) => ({ completedTurns: ctx.completedTurns + 1 }),
  })
  .initial("idle")
  .build();

// ---- infrastructure ----

const storage = new InMemoryWorkflowStorage();
const runner = createWorkflowRunner({ storage });

await agentMachine.start({ id: "session", context: { completedTurns: 0 } });

// ---- agentLoop session ----

const loop = agentLoop({
  name: "console-agent",
  llm: anthropic("claude-sonnet-4-6", { apiKey }),
  toolRegistry: mergedRegistry,
  // All tools are auto-approved in this interactive example — the user is watching.
  // In production, replace with a function that prompts for approval on write/shell ops.
  autoApprove: true,
  systemPrompt:
    "You are a helpful assistant running in an interactive console. Be concise. " +
    `Your workspace directory is: ${workspace}\n` +
    "You have filesystem tools (readFile, writeFile, listDir, statFile) to read and edit files in the workspace. " +
    "You have a shell tool to run commands like 'bun test', 'git log', or 'grep'. " +
    "You have memory tools (searchMemory, saveMemory) to recall and persist facts across sessions. " +
    "You have a writeTool to create new tools at runtime when no existing tool covers a need. " +
    "If a tool needs an API key, call requireSecret first — never ask for secrets in chat.",
  memory: { store: memoryStore },
});

let turnCounter = 0;

const session = await loop.session({ runner, sessionId: "session" });

// ---- /state: agent lifecycle ----

async function printAgentState() {
  const state = await agentMachine.getState("session");
  const history = await agentMachine.getHistory("session");
  if (!state) {
    console.log("\n(no state yet)\n");
    return;
  }

  console.log(`\n agent lifecycle`);
  console.log(`  current: ${state.current}`);
  console.log(`  context: ${JSON.stringify(state.context, null, 4).replace(/\n/g, "\n  ")}`);

  if (history.length > 0) {
    console.log(`\n  transitions:`);
    for (const t of history) {
      const ts = t.createdAt.toLocaleTimeString();
      console.log(`    ${t.from.padEnd(12)} --[${t.event}]--> ${t.to}  @ ${ts}`);
    }
  }
  console.log("");
}

// ---- /steps tree ----

const STATUS_ICON: Record<string, string> = {
  completed: "✓",
  failed: "✗",
  running: "◎",
  pending: "○",
  sleeping: "⏸",
  waiting_for_signal: "⏳",
  skipped: "—",
};

async function printWorkflowSteps() {
  const runs = await storage.listWorkflows({ name: "console-agent" });
  if (runs.length === 0) {
    console.log("\n(no workflow runs in storage yet)\n");
    return;
  }
  console.log(`\n workflow step history`);
  for (let i = 0; i < runs.length; i++) {
    const run = runs[i]!;
    const isLastRun = i === runs.length - 1;
    const runPrefix = isLastRun ? "└─" : "├─";
    const runIndent = isLastRun ? "   " : "│  ";

    const info = await runner.getStatus(run.workflowId, { includeStepResults: false });
    if (!info) continue;

    console.log(
      `${runPrefix} ${STATUS_ICON[info.state] ?? "?"} ${run.workflowId}  (${info.state})`,
    );

    const stepEntries = Object.entries(info.steps);
    for (let j = 0; j < stepEntries.length; j++) {
      const [stepName, step] = stepEntries[j]!;
      const isLastStep = j === stepEntries.length - 1;
      const stepPrefix = isLastStep ? "└─" : "├─";
      const stepIndent = runIndent + (isLastStep ? "   " : "│  ");

      console.log(`${runIndent}${stepPrefix} ${STATUS_ICON[step.status] ?? "?"} ${stepName}`);

      const entries = await storage.loadJournal(run.workflowId, stepName);
      for (let k = 0; k < entries.length; k++) {
        const entry = entries[k]!;
        const isLastEntry = k === entries.length - 1;
        const entryPrefix = isLastEntry ? "└─" : "├─";
        const exitTag = entry.exit?.tag;
        const entryIcon = exitTag === "Success" ? "✓" : exitTag === "Failure" ? "✗" : "○";

        let resultStr = "";
        if (entry.exit?.tag === "Success") {
          const raw = JSON.stringify(entry.exit.value);
          resultStr = `  →  ${raw.length > 80 ? `${raw.slice(0, 80)}…` : raw}`;
        } else if (entry.exit?.tag === "Failure") {
          resultStr = `  ✗  ${entry.exit.error.slice(0, 80)}`;
        }

        const label =
          entry.stepType === "signal" ? `signal: ${entry.activityName}` : entry.activityName;
        console.log(`${stepIndent}${entryPrefix} ${entryIcon} ${label}${resultStr}`);
      }
    }
  }
  console.log("");
}

// ---- REPL ----

function prompt() {
  rl.question("\nYou: ", async (line) => {
    const task = line.trim();
    if (!task) {
      prompt();
      return;
    }
    if (task === "exit" || task === "quit") {
      await session.close();
      mergedRegistry.close();
      rl.close();
      return;
    }
    if (task === "/history") {
      printHistory();
      prompt();
      return;
    }
    if (task === "/steps") {
      await printWorkflowSteps();
      prompt();
      return;
    }
    if (task === "/state") {
      await printAgentState();
      prompt();
      return;
    }
    if (task === "/tools") {
      const loaded = Object.keys(mergedRegistry.getTools());
      console.log(`\nLoaded tools (${loaded.length}): ${loaded.join(", ")}\n`);
      prompt();
      return;
    }
    if (task.startsWith("/memories")) {
      const query = task.slice("/memories".length).trim() || undefined;
      await printMemories(query);
      prompt();
      return;
    }
    if (task.startsWith("/remember ")) {
      const text = task.slice("/remember ".length).trim();
      if (text) {
        const id = await memoryStore.save({ content: text });
        console.log(`\nSaved memory ${id.slice(0, 8)}: "${text}"\n`);
      } else {
        console.log("\nUsage: /remember <text>\n");
      }
      prompt();
      return;
    }

    const turn = turnCounter++;
    await agentMachine.send({ id: "session", event: "message", data: { turn, task } });
    process.stdout.write("\nAgent: ");
    let streamed = "";
    for await (const chunk of session.stream(task)) {
      process.stdout.write(chunk);
      streamed += chunk;
    }
    process.stdout.write("\n");
    conversationHistory = [
      ...conversationHistory,
      { role: "user", content: task },
      { role: "assistant", content: streamed, toolCalls: undefined },
    ];
    await agentMachine.send({ id: "session", event: "done" });
    prompt();
  });
}

console.log(`Console agent ready.`);
console.log(`  Workspace : ${workspace}`);
console.log(`  Tools dir : ${toolsDir}`);
console.log(`  Tools     : ${Object.keys(staticTools).join(", ")}`);
console.log('  Commands  : /history, /steps, /state, /tools, /memories [query], /remember <text>, exit\n');
prompt();
