/**
 * Town Chat — a multi-agent research town built with createAgentTown.
 *
 * Five agents collaborate to answer questions:
 *   director  (mayor) — orchestrates peers; dispatches tasks in parallel
 *   researcher        — searches the web and fetches URLs
 *   analyst           — interprets and synthesises findings (runs in parallel with researcher)
 *   factChecker       — verifies claims and adds caveats
 *   writer            — polishes the combined output into clear prose
 *
 * Run:
 *   ANTHROPIC_API_KEY=sk-... bun packages/agent/src/examples/town-chat.ts
 *   TOOL_AUTO_APPROVE=true      (skip per-tool approval prompts)
 *
 * Slash commands:
 *   /help    — show this list
 *   /history — director conversation history
 *   /steps   — director workflow step tree
 *   /tools   — all agents and their tools
 *   exit     — quit
 *
 * Ctrl+C during a response interrupts the current turn.
 * Ctrl+C at the prompt twice exits.
 */

import { InMemoryWorkflowStorage, createWorkflowRunner } from "@promin/workflow";
import {
  anthropic,
  createAgentTown,
  InMemoryMemoryStore,
  InMemorySessionLogger,
  fetchUrl,
  webSearch,
} from "../lib/index.ts";
import { ChatTerminal } from "../lib/terminal/chat-terminal.ts";
import { ConsoleRunner } from "../lib/terminal/console-runner.ts";
import { UsageTracker } from "./console-usage.ts";
import { buildHistory, buildStepsTree, buildHelp, buildEventLog } from "./console-panes.ts";
import { dispatchCommand, type ReplCommand } from "./console-repl.ts";

// ---- setup ----

const apiKey = process.env.ANTHROPIC_API_KEY;
if (!apiKey) {
  console.error("Set ANTHROPIC_API_KEY to run.");
  process.exit(1);
}

let autoApprove = process.env.TOOL_AUTO_APPROVE === "true";

const COMMANDS: ReplCommand[] = [
  {
    cmd: "/history",
    desc: "director conversation history",
    handle: async () => {
      const mayorSession = await town.getMayorSession();
      await term.showPane("director history", buildHistory(mayorSession));
    },
  },
  {
    cmd: "/steps",
    desc: "director workflow step tree",
    handle: async () => {
      await term.showInteractiveTree(
        "steps",
        await buildStepsTree(storage, runner, "town-director"),
      );
    },
  },
  {
    cmd: "/tools",
    desc: "agents and their tools",
    handle: async () => {
      await term.showPane("tools", [
        "  director    sendMessage, readInbox, memory, searchSharedMemory, saveSharedMemory",
        "  researcher  webSearch, fetchUrl, sendMessage, memory",
        "  analyst     sendMessage, memory",
        "  factChecker webSearch, sendMessage, memory",
        "  writer      sendMessage, memory",
      ]);
    },
  },
  {
    cmd: "/log",
    args: "[agent]",
    desc: "session event log (all agents or one)",
    handle: async (input) => {
      const agentArg = input.slice("/log".length).trim() as AgentName | "";
      const targets: AgentName[] =
        agentArg && agentArg in loggers ? [agentArg as AgentName] : [...agentNames];
      const lines: string[] = [];
      for (const name of targets) {
        const events = loggers[name].events();
        if (events.length === 0) continue;
        lines.push(`\x1b[1m${name}\x1b[0m  (${events.length} events)`);
        lines.push(...buildEventLog(events, 30));
        lines.push("");
      }
      if (lines.length === 0) lines.push("  No events recorded yet.");
      await term.showPane(`log${agentArg ? ` · ${agentArg}` : ""}`, lines);
    },
  },
  {
    cmd: "/approve-all",
    desc: () => `toggle auto-approve (currently: ${autoApprove ? "ON" : "OFF"})`,
    handle: () => {
      autoApprove = !autoApprove;
      term.printAbove(`\x1b[2mAuto-approve: ${autoApprove ? "ON" : "OFF"}\x1b[0m`);
    },
  },
  {
    cmd: "/help",
    desc: "show this list",
    handle: async () => {
      await term.showPane("help", [
        ...buildHelp(COMMANDS, {}),
        "",
        "  Agents: director, researcher, analyst, factChecker, writer",
        "  Ctrl+C during a turn: interrupt (session recovers quickly).",
        "  Ctrl+C at prompt twice: exit.",
        "  Tool approval: y=yes  a=always  N=no",
      ]);
    },
  },
];

const agentNames = ["director", "researcher", "analyst", "factChecker", "writer"] as const;
type AgentName = (typeof agentNames)[number];
const loggers = Object.fromEntries(
  agentNames.map((n) => [n, new InMemorySessionLogger()]),
) as Record<AgentName, InMemorySessionLogger>;

const storage = new InMemoryWorkflowStorage();
const runner = createWorkflowRunner({ storage });

const usage = new UsageTracker();
const claude = usage.withTracking(anthropic("claude-sonnet-4-6", { apiKey }), "claude-sonnet-4-6");

// ---- terminal ----
const { term, rl } = new ChatTerminal({ commands: COMMANDS });
const consoleRunner = new ConsoleRunner(term, usage);

const town = createAgentTown({
  runner,
  mayor: "director",
  sharedMemory: new InMemoryMemoryStore(),

  onAgentActivity: (() => {
    const active = new Set<string>();
    return ({ agent, state }: { agent: string; state: "thinking" | "idle" }) => {
      if (state === "thinking") {
        active.add(agent);
        term.startSpinner(`${agent} thinking...`);
      } else {
        active.delete(agent);
        if (active.size > 0) {
          term.startSpinner(`${[...active].join(", ")} thinking...`);
        }
        // when all daemons are idle the director's own stream loop owns the spinner
      }
    };
  })(),

  onToolApproval: async ({ agent, call }) => {
    if (autoApprove) return { approved: true };
    term.stopSpinner();
    const inputPreview = JSON.stringify(call.input).slice(0, 80);
    return new Promise((resolve) => {
      rl.question(
        `\n[${agent}] Approve tool "${call.name}"(${inputPreview})? [y/a/N]: `,
        (answer) => {
          const a = answer.trim().toLowerCase();
          if (a === "a") {
            autoApprove = true;
            term.startSpinner(`${agent} thinking...`);
            resolve({ approved: true });
          } else if (a === "y") {
            term.startSpinner(`${agent} thinking...`);
            resolve({ approved: true });
          } else {
            resolve({ approved: false });
          }
        },
      );
    });
  },

  agents: {
    director: {
      llm: claude,
      memory: new InMemoryMemoryStore(),
      logger: loggers.director,
      prompt: [
        "You are the director of a research town. Coordinate specialist agents to answer questions.",
        "",
        "For research questions, use this parallel workflow:",
        "1. sendMessage to researcher to gather facts.",
        "2. readInbox once — wait for researcher reply.",
        "3. sendMessage to BOTH analyst AND factChecker simultaneously (two sendMessage calls),",
        "   passing the question + researcher findings to each.",
        "4. readInbox twice to collect both replies (they run in parallel).",
        "5. sendMessage to writer with the question + all findings + analysis + fact-check.",
        "6. readInbox once for the polished answer.",
        "7. Present the polished answer directly to the user.",
        "",
        "For simple/conversational questions, answer directly without delegating.",
        "Always reply with markdown formatting.",
      ].join("\n"),
    },

    researcher: {
      llm: claude,
      memory: new InMemoryMemoryStore(),
      logger: loggers.researcher,
      tools: { fetchUrl, webSearch },
      requireApprovalForAllTools: true,
      prompt: [
        "You are a researcher. You receive tasks from the director.",
        "Use webSearch to find relevant pages and fetchUrl to read details.",
        "Summarize findings as concise bullet points with source URLs.",
        "When finished, send findings to the director via sendMessage.",
      ].join("\n"),
    },

    analyst: {
      llm: claude,
      memory: new InMemoryMemoryStore(),
      logger: loggers.analyst,
      prompt: [
        "You are an analyst. You receive research tasks from the director.",
        "Apply domain knowledge to synthesise, interpret, and add context beyond raw search results.",
        "Identify patterns, implications, and key takeaways. Be concise.",
        "When finished, send your analysis to the director via sendMessage.",
      ].join("\n"),
    },

    factChecker: {
      llm: claude,
      memory: new InMemoryMemoryStore(),
      logger: loggers.factChecker,
      tools: { webSearch },
      requireApprovalForAllTools: true,
      prompt: [
        "You are a fact-checker. You receive a question and research findings from the director.",
        "Verify key claims, flag uncertainties, and add important caveats.",
        "Use webSearch to cross-check critical facts if needed.",
        "When finished, send your fact-check report to the director via sendMessage.",
      ].join("\n"),
    },

    writer: {
      llm: claude,
      memory: new InMemoryMemoryStore(),
      logger: loggers.writer,
      prompt: [
        "You are a writer. You receive a question, research findings, analysis, and a fact-check from the director.",
        "Combine all inputs into clear, well-structured markdown prose.",
        "Be concise — a focused answer, not an essay. Preserve important source links.",
        "When finished, send the polished answer to the director via sendMessage.",
      ].join("\n"),
    },
  },
});

// ---- SIGINT: interrupt turn or exit ----
consoleRunner.setupSigInt(rl, {
  onInterrupt: () => town.interruptMayorInbox(),
  onIdleHint: () => prompt(),
  onExit: () => {
    town.close().finally(() => {
      term.close();
      rl.close();
      process.exit(0);
    });
  },
});

// ---- REPL ----
function prompt(): void {
  term.printRule();
  rl.question(term.promptStr, async (input) => {
    input = input.trim();
    if (!input) return prompt();

    if (input === "exit" || input === "quit") {
      await town.close();
      term.close();
      rl.close();
      return;
    }

    if (await dispatchCommand(COMMANDS, input)) return prompt();

    // ---- normal turn ----
    const { aborted, error } = await consoleRunner.runTurn((signal) => town.stream(input, signal), {
      initialSpinner: "director thinking...",
      label: "Director",
      maxRetries: 4,
      retryOn: (e) => e.message.includes("busy"),
      retrySpinner: (n) => `settling… (${n})`,
    });

    if (!aborted) {
      if (error) {
        if (term.agentHasTextOnLine) process.stdout.write("\n");
        process.stdout.write(`\x1b[31mError: ${error.message}\x1b[0m\n`);
      } else {
        process.stdout.write("\n");
        usage.printUsage();
      }
    }

    term.agentHasTextOnLine = false;

    if (consoleRunner.savedPlaceholder) {
      rl.write(consoleRunner.savedPlaceholder);
      consoleRunner.clearSavedPlaceholder();
    }

    prompt();
  });
}

console.log("\nTown Chat  agents=director,researcher,analyst,factChecker,writer");
console.log("Type /help for commands. Type 'exit' or Ctrl+C twice to quit.\n");
prompt();
