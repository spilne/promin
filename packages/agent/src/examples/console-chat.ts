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
 * Type /help for commands.
 */

import {
  stateMachine,
  InMemoryStateMachineStorage,
  InMemoryWorkflowStorage,
  createWorkflowRunner,
} from "@promin/workflow";
import { PipelineRateLimiter } from "@promin/core";
import { anthropic } from "../lib/adapters/anthropic.ts";
import { agentLoop } from "../lib/agent-loop.ts";
import { InMemoryMemoryIndex } from "../lib/memory-index.ts";
import { CompositeSecretStore, EnvSecretStore, InMemorySecretStore } from "../lib/secret-store.ts";
import { ChatTerminal } from "../lib/terminal/chat-terminal.ts";
import { ConsoleRunner } from "../lib/terminal/console-runner.ts";
import { UsageTracker, fmtN } from "./console-usage.ts";
import { InMemorySessionLogger } from "../lib/session-logger.ts";
import { createSpinnerTracker, abbrevInput } from "./console-spinner.ts";
import { createToolRegistry } from "./console-tools.ts";
import {
  buildHistory,
  buildAgentState,
  buildStepsTree,
  buildMemories,
  buildSchedules,
  buildHelp,
  buildEventLog,
} from "./console-panes.ts";
import { executeDirectCall, dispatchCommand, type ReplCommand } from "./console-repl.ts";
import type { AgentSession } from "../lib/agent-loop.ts";

const apiKey = process.env.ANTHROPIC_API_KEY;
if (!apiKey) {
  console.error("Set ANTHROPIC_API_KEY to run.");
  process.exit(1);
}

const workspace = process.env.AGENT_WORKSPACE ?? process.cwd();
const autoApproveRef = { value: process.env.TOOL_AUTO_APPROVE === "true" };

// ---- REPL commands (single source of truth for completions + /help) ----
const COMMANDS: ReplCommand[] = [
  {
    cmd: "/history",
    desc: "conversation messages (full session truth)",
    handle: async () => {
      await term.showPane("history", buildHistory(session));
    },
  },
  {
    cmd: "/clear",
    desc: "start a new conversation (memories persist)",
    handle: async () => {
      await session.close();
      session = await resetSession(`session-${++sessionIdSeq}`);
      term.printAbove(
        "\x1b[2mConversation cleared — new session started. Memories persist.\x1b[0m",
      );
    },
  },
  {
    cmd: "/steps",
    desc: "workflow step tree",
    handle: async () => {
      await term.showInteractiveTree(
        "steps",
        await buildStepsTree(storage, runner, "console-agent"),
      );
    },
  },
  {
    cmd: "/state",
    desc: "agent lifecycle state machine",
    handle: async () => {
      await term.showPane("state", await buildAgentState(agentMachine, currentSessionId));
    },
  },
  {
    cmd: "/tools",
    desc: "loaded tools",
    handle: async () => {
      await term.showPane(
        "tools",
        Object.keys(registry.getTools()).map((n) => `  ${n}`),
      );
    },
  },
  {
    cmd: "/memories",
    args: "[query]",
    desc: "search memories (omit query to list all)",
    handle: async (input) => {
      const q = input.slice("/memories".length).trim();
      await term.showPane(
        `memories${q ? ` · "${q}"` : ""}`,
        await buildMemories(memoryIndex, q || undefined),
      );
    },
  },
  {
    cmd: "/remember",
    args: "<text>",
    desc: "save a memory directly",
    handle: async (input) => {
      const text = input.slice("/remember ".length).trim();
      if (text) {
        const id = await memoryIndex.save({ content: text });
        console.log(`\n\x1b[2mSaved ${id.slice(0, 8)}: "${text}"\x1b[0m\n`);
      }
    },
  },
  {
    cmd: "/schedules",
    desc: "list active schedules",
    handle: async () => {
      await term.showPane("schedules", buildSchedules(scheduler));
    },
  },
  {
    cmd: "/cancel-schedule",
    args: "<id>",
    desc: "immediately cancel a schedule",
    handle: (input) => {
      const id = input.slice("/cancel-schedule ".length).trim();
      scheduler.unregister(id);
      activeTicks.delete(id);
      console.log(`\n\x1b[2mCancelled schedule "${id}"\x1b[0m\n`);
    },
  },
  {
    cmd: "/pause-schedule",
    args: "<id>",
    desc: "pause a schedule",
    handle: (input) => {
      const id = input.slice("/pause-schedule ".length).trim();
      scheduler.pause(id);
      console.log(`\n\x1b[2mPaused schedule "${id}"\x1b[0m\n`);
    },
  },
  {
    cmd: "/compact",
    desc: "compact conversation history now (summarise and drop old messages)",
    handle: async () => {
      term.startSpinner("compacting…");
      try {
        const r = await session.compact();
        term.stopSpinner();
        const summary = r.summary
          ? `\n\x1b[2m  Summary: ${r.summary.slice(0, 120)}${r.summary.length > 120 ? "…" : ""}\x1b[0m`
          : "";
        term.printAbove(
          `\x1b[2mCompacted — kept ${r.kept} messages, dropped ${r.dropped}.${summary}\x1b[0m`,
        );
      } catch (e) {
        term.stopSpinner();
        console.log(`\n\x1b[31m${e instanceof Error ? e.message : String(e)}\x1b[0m\n`);
      }
    },
  },
  {
    cmd: "/approve-all",
    desc: () => `toggle auto-approve (currently: ${autoApproveRef.value ? "ON" : "OFF"})`,
    handle: () => {
      autoApproveRef.value = !autoApproveRef.value;
      console.log(`\n\x1b[2mAuto-approve: ${autoApproveRef.value ? "ON" : "OFF"}\x1b[0m\n`);
    },
  },
  {
    cmd: "/log",
    desc: "session event log",
    handle: async () => {
      const lines = buildEventLog(sessionLogger.events());
      await term.showPane("log", lines.length ? lines : ["  No events recorded yet."]);
    },
  },
  {
    cmd: "/help",
    desc: "show this help",
    handle: async () => {
      await term.showPane("help", buildHelp(COMMANDS, registry.getTools()));
    },
  },
];

// ---- terminal ----
const chatTerm = new ChatTerminal({ commands: COMMANDS, workspace });
const { term, rl } = chatTerm;

// ---- infrastructure ----
const memoryIndex = new InMemoryMemoryIndex();
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
const sessionRef: { current: AgentSession | undefined } = { current: undefined };
const sessionIdRef: { current: string } = { current: "session" };
const sessionLogger = new InMemorySessionLogger();

const { registry, scheduler, activeTicks } = await createToolRegistry({
  workspace,
  memoryIndex,
  secrets,
  apiKey,
  ask: (q) => term.ask(q),
  printAbove: (...lines) => term.printAbove(...lines),
  runner,
  usage,
  sessionRef,
  sessionIdRef,
  autoApproveRef,
  logger: sessionLogger,
});
chatTerm.setRegistry(registry);

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
  "       sessionDebug (diagnose failures — commands: errors, calls, status, log),",
  "       chatGPT (one-shot GPT-4o query for a quick second opinion),",
  "       claudeAgent (parallel Claude sub-agent with filesystem/shell/memory/chatGPT access),",
  "       gptAgent (parallel GPT-4o sub-agent with filesystem/shell/memory/chatGPT access),",
  "       scheduleTask, listSchedules, cancelSchedule (recurring tasks),",
  "       writeTool (create new tools at runtime), requireSecret (prompt user for API keys).",
  "Use claudeAgent or gptAgent to parallelise independent subtasks or get a different perspective.",
  "Never ask for secrets in chat — always use requireSecret.",
].join("\n");

let sessionIdSeq = 0;
let currentSessionId = "session";

const loop = agentLoop({
  name: "console-agent",
  llm: usage.withTracking(anthropic("claude-sonnet-4-6", { apiKey }), "claude-sonnet-4-6"),
  logger: sessionLogger,
  toolRegistry: spinner.withStatusTracking(registry),
  rateLimiter,
  systemPrompt: SYSTEM_PROMPT,
  memory: { store: memoryIndex },
  hooks: {
    onApprovalRequired: async (call) => {
      if (autoApproveRef.value) return { approved: true };
      term.stopSpinner();
      const input = (call.input as Record<string, unknown>) ?? {};
      const paramStr = abbrevInput(input) || JSON.stringify(input).slice(0, 80);
      const answer = await term.ask(
        `Allow tool "${call.name}"${paramStr ? `  \x1b[2m${paramStr}\x1b[0m` : ""}? [y/n/always]`,
      );
      if (answer.toLowerCase() === "always") {
        autoApproveRef.value = true;
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
  sessionIdRef.current = newId;
  sessionLogger.clear();
  await agentMachine.start({ id: newId, context: { turns: 0 } }).catch(() => {});
  const s = await loop.session({ runner, sessionId: newId });
  sessionRef.current = s;
  usage.resetSession();
  return s;
}

let session = await resetSession("session");

// ---- SIGINT ----

let _inMultiLine = false;
let _cancelInput = false;

consoleRunner.setupSigInt(rl, {
  onInterrupt: () => {
    term.suppress = true;
  },
  onIdleHint: () => {
    // Defer rl.write to avoid re-entrant _ttyWrite while the SIGINT handler is still on the stack.
    _cancelInput = true;
    setImmediate(() => rl.write("\n"));
  },
  isMultiLine: () => _inMultiLine,
  onCancelMultiLine: () => {
    _cancelInput = true;
    setImmediate(() => rl.write("\n")); // resolve the pending "... " question so the callback can clean up
  },
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
    _inMultiLine = !isFirst;
    rl.question(isFirst ? term.promptStr : "... ", async (line) => {
      if (isFirst) {
        term.inPrompt = false;
        term.stopPromptAnimation();
      }
      _inMultiLine = false;

      // Ctrl+C fired during first-line or continuation — discard input and restart.
      if (_cancelInput) {
        _cancelInput = false;
        parts.length = 0;
        return prompt();
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

      if (await dispatchCommand(COMMANDS, input)) return prompt();

      // ---- :tool direct-call ----
      const directCall = await executeDirectCall(input, registry);
      if (directCall !== null) {
        if (directCall.ok) await term.showPane(directCall.title, directCall.lines);
        else console.log(`\n\x1b[31m${directCall.error}\x1b[0m\n`);
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
        (signal) => {
          term.setActiveSignal(signal);
          return session.stream(input, signal);
        },
        { label: "Agent", workspace },
      );
      term.setActiveSignal(null);

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
