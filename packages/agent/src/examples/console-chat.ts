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
import { readdir } from "node:fs/promises";
import {
  stateMachine,
  InMemoryStateMachineStorage,
  InMemoryWorkflowStorage,
  createWorkflowRunner,
} from "@promin/workflow";
import { PipelineRateLimiter } from "@promin/core";
import { anthropic } from "../lib/adapters/anthropic.ts";
import { agentLoop } from "../lib/agent-loop.ts";
import { InMemoryMemoryStore } from "../lib/memory-store.ts";
import { CompositeSecretStore, EnvSecretStore, InMemorySecretStore } from "../lib/secret-store.ts";
import { Terminal } from "./common/terminal.ts";
import { ConsoleRunner } from "./common/console-runner.ts";
import { UsageTracker, fmtN } from "./console-usage.ts";
import { createSpinnerTracker, abbrevInput } from "./console-spinner.ts";
import { createToolRegistry } from "./console-tools.ts";
import {
  buildHistory,
  buildAgentState,
  buildStepsTree,
  buildMemories,
  buildSchedules,
} from "./console-panes.ts";
import type { AgentSession } from "../lib/agent-loop.ts";

const apiKey = process.env.ANTHROPIC_API_KEY;
if (!apiKey) {
  console.error("Set ANTHROPIC_API_KEY to run.");
  process.exit(1);
}

const workspace = process.env.AGENT_WORKSPACE ?? process.cwd();

// ---- slash-command completions ----
const SLASH_COMPLETIONS = [
  "/help",
  "/history",
  "/clear",
  "/steps",
  "/state",
  "/tools",
  "/memories",
  "/remember",
  "/schedules",
  "/approve-all",
  "/cancel-schedule",
  "/pause-schedule",
];

// ---- @-file: list workspace files matching a prefix ----
async function listWorkspaceFiles(ws: string, prefix: string, limit = 20): Promise<string[]> {
  try {
    const all = (await readdir(ws, { recursive: true })) as string[];
    const skip = (f: string) =>
      f.split("/").some((p) => p.startsWith(".")) ||
      f.includes("node_modules") ||
      f.includes("/dist/");
    return all.filter((f) => !skip(f) && (!prefix || f.startsWith(prefix))).slice(0, limit);
  } catch {
    return [];
  }
}

// ---- async dropdown completer ----
// term is assigned immediately after createInterface — no Tab can fire before then.
let term!: Terminal;

type CompleterCb = (err: Error | null, result: [string[], string]) => void;

function makeCompleterEntry(line: string, cb: CompleterCb): void {
  (async (): Promise<[string[], string]> => {
    // @-file fuzzy match
    const atMatch = line.match(/@(\S*)$/);
    if (atMatch) {
      const files = await listWorkspaceFiles(workspace, atMatch[1]);
      if (files.length === 0) return [[], line];
      const selected = await term.showInlineMenu(files.map((f) => `@${f}`));
      if (!selected) return [[], line];
      return [[line.slice(0, line.length - atMatch[0].length) + selected], line];
    }
    // slash-command dropdown
    if (line.startsWith("/")) {
      const hits = SLASH_COMPLETIONS.filter((c) => c.startsWith(line));
      if (hits.length === 0) return [[], line];
      const selected = await term.showInlineMenu(hits);
      if (!selected) return [[], line];
      return [[selected], line];
    }
    return [[], line];
  })().then(
    (r) => cb(null, r),
    (e) => cb(e instanceof Error ? e : new Error(String(e)), [[], line]),
  );
}

// ---- terminal ----
const rl = createInterface({
  input: process.stdin,
  output: process.stdout,
  historySize: 100,
  completer: makeCompleterEntry,
});
term = new Terminal(rl);

// ---- infrastructure ----
const memoryStore = new InMemoryMemoryStore();
// Env vars checked first; user-provided secrets (via requireSecret or key prompts) go to in-memory.
const secrets = new CompositeSecretStore([new EnvSecretStore(), new InMemorySecretStore()]);
const storage = new InMemoryWorkflowStorage();
const runner = createWorkflowRunner({ storage });

const rateLimitRpm = process.env.RATE_LIMIT_RPM ? Number(process.env.RATE_LIMIT_RPM) : null;
const rateLimiter = rateLimitRpm
  ? PipelineRateLimiter.make({ limit: rateLimitRpm, windowMs: 60_000, strategy: "sliding-window" })
  : undefined;

// ---- usage + spinner trackers ----
const usage = new UsageTracker();
const spinner = createSpinnerTracker(term);
const consoleRunner = new ConsoleRunner(term, usage);

// ---- tools ----
const sessionRef: { current: { send: (task: string) => Promise<string> } | undefined } = {
  current: undefined,
};

const { registry, scheduler, activeTicks } = await createToolRegistry({
  workspace,
  memoryStore,
  secrets,
  apiKey,
  ask: (q) => term.ask(q),
  runner,
  usage,
  sessionRef,
});

// ---- agent lifecycle state machine ----
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

// ---- agent loop (created once, reused across sessions) ----
const SYSTEM_PROMPT = [
  "You are a helpful assistant in an interactive console. Be concise.",
  "IMPORTANT: After every tool call (or sequence of tool calls), always write a brief text",
  "reply confirming what was done. Never end a turn silently — the user cannot see tool results.",
  `Workspace: ${workspace}`,
  "Tools: readFile, writeFile, listDir, statFile (filesystem), shell (run commands),",
  "       memory (long-term memory — commands: search, save),",
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
let currentSessionId = "session";

const loop = agentLoop({
  name: "console-agent",
  llm: usage.withTracking(anthropic("claude-sonnet-4-6", { apiKey }), "claude-sonnet-4-6"),
  toolRegistry: spinner.withStatusTracking(registry),
  rateLimiter,
  systemPrompt: SYSTEM_PROMPT,
  memory: { store: memoryStore },
  hooks: {
    onApprovalRequired: async (call) => {
      if (autoApprove) return { approved: true };
      term.stopSpinner();
      const input = (call.input as Record<string, unknown>) ?? {};
      const paramStr = abbrevInput(input) || JSON.stringify(input).slice(0, 80);
      const answer = await term.ask(
        `Allow tool "${call.name}"${paramStr ? `  \x1b[2m${paramStr}\x1b[0m` : ""}? [y/n/always]`,
      );
      if (answer.toLowerCase() === "always") {
        autoApprove = true;
        term.printAbove("\x1b[2mAuto-approve enabled for this session.\x1b[0m");
      }
      const approved = answer.toLowerCase().startsWith("y") || answer.toLowerCase() === "always";
      if (approved) term.startSpinner(`thinking...  \x1b[2mstep ${spinner.step + 1}\x1b[0m`);
      return { approved };
    },
  },
  onLifecycle: (e) => {
    if (e.event === "message") {
      agentMachine
        // biome-ignore lint/suspicious/noExplicitAny: lifecycle context is dynamically typed
        .send({ id: e.sessionId, event: "message", data: e.context as any })
        .catch(() => {});
    } else if (e.event === "done") {
      agentMachine.send({ id: e.sessionId, event: "done" }).catch(() => {});
    }
  },
});

// ---- session reset helper ----
async function resetSession(newId: string): Promise<AgentSession> {
  currentSessionId = newId;
  await agentMachine.start({ id: newId, context: { turns: 0 } }).catch(() => {});
  const s = await loop.session({ runner, sessionId: newId });
  sessionRef.current = s;
  usage.resetSession();
  return s;
}

let session = await resetSession("session");

// ---- SIGINT ----
consoleRunner.setupSigInt(rl, {
  onInterrupt: () => {
    term.suppress = true;
  },
  onIdleHint: () => prompt(),
  onExit: () => {
    session
      .close()
      .catch(() => {})
      .finally(() => {
        registry.close();
        term.close();
        rl.close();
        process.exit(0);
      });
  },
});

// ---- REPL ----

function prompt() {
  const parts: string[] = [];

  function readLine(isFirst: boolean): void {
    if (isFirst) term.printRule();
    rl.question(isFirst ? term.promptStr : "... ", async (line) => {
      if (isFirst) {
        term.inPrompt = false;
        term.stopPromptAnimation();
      }

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
        registry.close();
        term.close();
        return rl.close();
      }

      // ---- display commands ----
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
        session = await resetSession(`session-${++sessionIdSeq}`);
        term.printAbove(
          "\x1b[2mConversation cleared — new session started. Memories persist.\x1b[0m",
        );
        return prompt();
      }
      if (input === "/history") {
        await term.showPane("history", buildHistory(session));
        return prompt();
      }
      if (input === "/steps") {
        await term.showInteractiveTree(
          "steps",
          await buildStepsTree(storage, runner, "console-agent"),
        );
        return prompt();
      }
      if (input === "/state") {
        await term.showPane("state", await buildAgentState(agentMachine, currentSessionId));
        return prompt();
      }
      if (input === "/tools") {
        await term.showPane(
          "tools",
          Object.keys(registry.getTools()).map((n) => `  ${n}`),
        );
        return prompt();
      }
      if (input === "/schedules") {
        await term.showPane("schedules", buildSchedules(scheduler));
        return prompt();
      }
      if (input.startsWith("/memories")) {
        const q = input.slice("/memories".length).trim();
        await term.showPane(
          `memories${q ? ` · "${q}"` : ""}`,
          await buildMemories(memoryStore, q || undefined),
        );
        return prompt();
      }
      if (input === "/approve-all") {
        autoApprove = !autoApprove;
        console.log(`\n\x1b[2mAuto-approve: ${autoApprove ? "ON" : "OFF"}\x1b[0m\n`);
        return prompt();
      }

      // ---- action commands ----
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

      // ---- agent turn ----
      if (usage.tokenBudget && usage.sessionTokensUsed() >= usage.tokenBudget) {
        process.stdout.write(
          `\n\x1b[33m  Token budget exhausted (${fmtN(usage.sessionTokensUsed())} / ${fmtN(usage.tokenBudget)}). Start a new session to continue.\x1b[0m\n`,
        );
        return prompt();
      }

      spinner.resetTurn();
      term.suppress = false;

      const { aborted, error: streamError } = await consoleRunner.runTurn(
        (signal) => session.stream(input, signal),
        { label: "Agent", workspace },
      );

      if (aborted) {
        term.agentHasTextOnLine = false;
        prompt();
        return;
      }

      if (streamError) {
        if (term.agentHasTextOnLine) process.stdout.write("\n");
        term.agentHasTextOnLine = false;

        const contextFull =
          streamError.message.includes("prompt is too long") ||
          streamError.message.includes("context_length_exceeded") ||
          streamError.message.includes("maximum context");
        const journalDiverged = streamError.message.includes("diverged at activity");

        if (contextFull || journalDiverged) {
          process.stdout.write(
            contextFull
              ? "\x1b[31mContext window full — conversation history is too large to continue.\x1b[0m\n"
              : `\x1b[31mWorkflow journal diverged — session state is inconsistent.\x1b[0m\n\x1b[2m  ${streamError.message}\x1b[0m\n`,
          );
          await session.close();
          session = await resetSession(`session-${++sessionIdSeq}`);
          process.stdout.write(
            "\x1b[2m  Session automatically cleared. Memories persist.\x1b[0m\n",
          );
        } else {
          process.stdout.write(`\x1b[31mError: ${streamError.message}\x1b[0m\n`);
        }

        prompt();
        return;
      }

      process.stdout.write("\n");
      usage.printUsage();
      if (usage.tokenBudget) {
        const used = usage.sessionTokensUsed();
        const pct = used / usage.tokenBudget;
        if (pct >= 0.8 && pct < 1.0) {
          term.printAbove(
            `\x1b[33m  Token budget ${Math.round(pct * 100)}% used — ${fmtN(usage.tokenBudget - used)} remaining\x1b[0m`,
          );
        }
      }
      term.agentHasTextOnLine = false;
      prompt();
    });

    if (isFirst) {
      if (consoleRunner.savedPlaceholder) {
        rl.write(consoleRunner.savedPlaceholder);
        consoleRunner.clearSavedPlaceholder();
      }
      term.inPrompt = true;
      term.startPromptAnimation();
    }
  }

  readLine(true);
}

console.log(
  `\nConsole agent  workspace=${workspace}  tools=${Object.keys(registry.getTools()).join(", ")}`,
);
console.log(`Type /help for commands. Use \\ at line end for multi-line input.\n`);
prompt();
