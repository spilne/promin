/**
 * Interactive console agent with persistent memory, filesystem access, and multi-LLM delegation.
 *
 * Run:
 *   ANTHROPIC_API_KEY=sk-... bun packages/agent/src/examples/console-chat.ts
 *   AGENT_WORKSPACE=/path/to/project ...   (default: cwd)
 *
 * Commands:
 *   /history          — conversation messages
 *   /steps            — workflow step tree
 *   /state            — agent lifecycle state machine
 *   /tools            — loaded tools
 *   /memories [query] — search memories (omit query to list all)
 *   /remember <text>  — save a memory directly
 *   exit              — quit
 */

import { createInterface } from "node:readline";
import { mkdir } from "node:fs/promises";
import { join } from "node:path";
import { stateMachine, InMemoryStateMachineStorage, InMemoryWorkflowStorage, createWorkflowRunner } from "@promin/workflow";
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
import { createFileToolRegistry } from "../lib/tool-registry.ts";
import type { ToolRegistry } from "../lib/tool-registry.ts";
import type { AgentTool } from "../lib/tool.ts";
import type { Message } from "../lib/message.ts";
import { z } from "zod";

const apiKey = process.env.ANTHROPIC_API_KEY;
if (!apiKey) { console.error("Set ANTHROPIC_API_KEY to run."); process.exit(1); }

const workspace = process.env.AGENT_WORKSPACE ?? process.cwd();

// readline
const rl = createInterface({ input: process.stdin, output: process.stdout });

function ask(question: string): Promise<string> {
  return new Promise((resolve) => {
    rl.question(`\n${question}: `, (v) => { process.stdout.write("\n"); resolve(v.trim()); });
  });
}

// dynamic tool registry (watches examples/tools/ for hot-loaded tools)
const toolsDir = join(import.meta.dir, "tools");
await mkdir(toolsDir, { recursive: true });

const fileRegistry = await createFileToolRegistry({
  dir: toolsDir,
  onLoad:  (name, cat) => console.log(`[registry] +${cat ? `${cat}/` : ""}${name}`),
  onUnload: (name, cat) => console.log(`[registry] -${cat ? `${cat}/` : ""}${name}`),
  onError: (file, err) => console.error(`[registry] error in ${file}:`, err),
});

// memory
const memoryStore = new InMemoryMemoryStore();

async function printMemories(query?: string) {
  const entries = query ? await memoryStore.search(query, 10) : await memoryStore.list(50);
  if (!entries.length) {
    console.log(query ? `\n(no memories matching "${query}")\n` : "\n(no memories yet)\n");
    return;
  }
  console.log(`\n--- ${query ? `"${query}"` : "all memories"} (${entries.length}) ---`);
  for (const e of entries)
    console.log(`  [${e.createdAt.toLocaleTimeString()}] ${e.id.slice(0, 8)}  ${e.content}`);
  console.log("");
}

// tools
const openaiKeyStore = new InMemorySecretStore();

// biome-ignore lint/suspicious/noExplicitAny: tool registry uses runtime Zod validation
const staticTools: Record<string, AgentTool<any, any>> = {
  calculator: tool({
    name: "calculator",
    description: "Evaluate a JS math expression and return the result.",
    parameters: z.object({ expression: z.string() }),
    // biome-ignore lint/security/noEval: example only
    execute: async ({ expression }) => { try { return String(eval(expression)); } catch { return `Error: ${expression}`; } },
  }),

  currentTime: tool({
    name: "currentTime",
    description: "Return the current local date and time.",
    parameters: z.object({}),
    execute: async () => new Date().toLocaleString(),
  }),

  // lets the LLM create new tools at runtime, saved to examples/tools/
  writeTool: createWriteToolTool({ dir: toolsDir, requireApproval: false, toolImportPath: "../../lib/index.ts" }),

  requireSecret: createRequireSecretTool({
    readSecret: (prompt) => ask(`[secret] ${prompt}`),
  }),

  ...createFilesystemTools({ rootDir: workspace }),

  shell: createShellTool({
    cwd: workspace,
    allowedCommands: ["bun", "git", "ls", "cat", "find", "grep", "npm", "npx"],
  }),

  ...createMemoryTools({ store: memoryStore }),

  chatGPT: createLlmTool(
    {
      // provider resolved lazily so the key is only prompted on first use
      chat: async (params) => {
        let key = await openaiKeyStore.get("OPENAI_API_KEY");
        if (!key) {
          key = process.env.OPENAI_API_KEY ?? await ask("[chatGPT] Enter OPENAI_API_KEY");
          await openaiKeyStore.set("OPENAI_API_KEY", key);
        }
        return openai("gpt-4o", { apiKey: key }).chat(params);
      },
    },
    { name: "chatGPT", description: "Consult ChatGPT (GPT-4o) for a second opinion or different perspective." },
  ),
};

const mergedRegistry: ToolRegistry = {
  getTools: () => ({ ...fileRegistry.getTools(), ...staticTools }),
  close: () => fileRegistry.close(),
};

// agent lifecycle state machine (tracks turns for /state command)
type AgentStates = {
  idle:     { context: { turns: number };                            transitions: { message: "thinking" } };
  thinking: { context: { turns: number; turn: number; task: string }; transitions: { done: "idle" } };
};

const agentMachine = stateMachine<AgentStates>({
  name: "agent-lifecycle",
  storage: new InMemoryStateMachineStorage(),
})
  .state("idle")
  .state("thinking")
  // biome-ignore lint/suspicious/noExplicitAny: state machine action context is dynamically typed
  .on("message", { from: "idle",     to: "thinking", action: (ctx: any, d: any) => ({ turns: ctx.turns, turn: d.turn, task: d.task }) })
  // biome-ignore lint/suspicious/noExplicitAny: state machine action context is dynamically typed
  .on("done",    { from: "thinking", to: "idle",     action: (ctx: any)         => ({ turns: ctx.turns + 1 }) })
  .initial("idle")
  .build();

// infrastructure
const storage = new InMemoryWorkflowStorage();
const runner  = createWorkflowRunner({ storage });

await agentMachine.start({ id: "session", context: { turns: 0 } });

const session = await agentLoop({
  name: "console-agent",
  llm: anthropic("claude-sonnet-4-6", { apiKey }),
  toolRegistry: mergedRegistry,
  autoApprove: true,
  systemPrompt: [
    "You are a helpful assistant in an interactive console. Be concise.",
    `Workspace: ${workspace}`,
    "Tools: readFile, writeFile, listDir, statFile (filesystem), shell (run commands),",
    "       searchMemory, saveMemory (long-term memory), chatGPT (delegate to GPT-4o),",
    "       writeTool (create new tools at runtime), requireSecret (prompt user for API keys).",
    "Never ask for secrets in chat — always use requireSecret.",
  ].join("\n"),
  memory: { store: memoryStore },
}).session({ runner, sessionId: "session" });

// debug helpers
let history: Message[] = [];

function printHistory() {
  if (!history.length) { console.log("\n(no history yet)\n"); return; }
  console.log("\n--- history ---");
  for (const m of history) {
    if (m.role === "user") console.log(`\n[user]\n${m.content}`);
    else if (m.role === "assistant") {
      if (m.content) console.log(`\n[assistant]\n${m.content}`);
      for (const tc of m.toolCalls ?? [])
        console.log(`\n[tool: ${tc.name}]\n${JSON.stringify(tc.input, null, 2)}`);
    } else if (m.role === "tool") console.log(`\n[result: ${m.toolCallId}]\n${m.content}`);
  }
  console.log(`\n--- ${history.length} messages ---\n`);
}

async function printAgentState() {
  const state   = await agentMachine.getState("session");
  const transitions = await agentMachine.getHistory("session");
  if (!state) { console.log("\n(no state yet)\n"); return; }
  console.log(`\n state: ${state.current}  context: ${JSON.stringify(state.context)}`);
  for (const t of transitions)
    console.log(`  ${t.from} --[${t.event}]--> ${t.to}  @ ${t.createdAt.toLocaleTimeString()}`);
  console.log("");
}

const ICON: Record<string, string> = {
  completed: "✓", failed: "✗", running: "◎", pending: "○", sleeping: "⏸", waiting_for_signal: "⏳",
};

async function printSteps() {
  const runs = await storage.listWorkflows({ name: "console-agent" });
  if (!runs.length) { console.log("\n(no runs yet)\n"); return; }
  console.log("");
  for (const [ri, run] of runs.entries()) {
    const last = ri === runs.length - 1;
    const info = await runner.getStatus(run.workflowId, { includeStepResults: false });
    if (!info) continue;
    const rp = last ? "└─" : "├─"; const ri2 = last ? "   " : "│  ";
    console.log(`${rp} ${ICON[info.state] ?? "?"} ${run.workflowId}  (${info.state})`);
    const steps = Object.entries(info.steps);
    for (const [si, [name, step]] of steps.entries()) {
      const slast = si === steps.length - 1;
      const sp = slast ? "└─" : "├─"; const si2 = ri2 + (slast ? "   " : "│  ");
      console.log(`${ri2}${sp} ${ICON[step.status] ?? "?"} ${name}`);
      const entries = await storage.loadJournal(run.workflowId, name);
      for (const [ei, entry] of entries.entries()) {
        const elast = ei === entries.length - 1;
        const ep = elast ? "└─" : "├─";
        const icon = entry.exit?.tag === "Success" ? "✓" : entry.exit?.tag === "Failure" ? "✗" : "○";
        const label = entry.stepType === "signal" ? `signal: ${entry.activityName}` : entry.activityName;
        let suffix = "";
        if (entry.exit?.tag === "Success") { const r = JSON.stringify(entry.exit.value); suffix = `  →  ${r.length > 80 ? `${r.slice(0, 80)}…` : r}`; }
        if (entry.exit?.tag === "Failure") suffix = `  ✗  ${entry.exit.error.slice(0, 80)}`;
        console.log(`${si2}${ep} ${icon} ${label}${suffix}`);
      }
    }
  }
  console.log("");
}

// REPL
let turn = 0;

function prompt() {
  rl.question("\nYou: ", async (line) => {
    const input = line.trim();
    if (!input) return prompt();

    if (input === "exit" || input === "quit") {
      await session.close(); mergedRegistry.close(); return rl.close();
    }
    if (input === "/history")  { printHistory();     return prompt(); }
    if (input === "/steps")    { await printSteps(); return prompt(); }
    if (input === "/state")    { await printAgentState(); return prompt(); }
    if (input === "/tools") {
      const names = Object.keys(mergedRegistry.getTools());
      console.log(`\nTools (${names.length}): ${names.join(", ")}\n`);
      return prompt();
    }
    if (input.startsWith("/memories")) {
      await printMemories(input.slice("/memories".length).trim() || undefined);
      return prompt();
    }
    if (input.startsWith("/remember ")) {
      const text = input.slice("/remember ".length).trim();
      if (text) { const id = await memoryStore.save({ content: text }); console.log(`\nSaved ${id.slice(0, 8)}: "${text}"\n`); }
      return prompt();
    }

    await agentMachine.send({ id: "session", event: "message", data: { turn, task: input } });
    process.stdout.write("\nAgent: ");
    let answer = "";
    for await (const chunk of session.stream(input)) { process.stdout.write(chunk); answer += chunk; }
    process.stdout.write("\n");
    history.push({ role: "user", content: input }, { role: "assistant", content: answer, toolCalls: undefined });
    turn++;
    await agentMachine.send({ id: "session", event: "done" });
    prompt();
  });
}

console.log(`\nConsole agent  workspace=${workspace}  tools=${Object.keys(staticTools).join(", ")}`);
console.log(`Commands: /history /steps /state /tools /memories [q] /remember <text> exit\n`);
prompt();
