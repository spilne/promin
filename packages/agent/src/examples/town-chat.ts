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

import { createInterface } from "node:readline";
import { InMemoryWorkflowStorage, createWorkflowRunner } from "@promin/workflow";
import { z } from "zod";
import { anthropic, createAgentTown, InMemoryMemoryStore, tool } from "../lib/index.ts";
import { Terminal } from "./common/terminal.ts";
import { ConsoleRunner } from "./common/console-runner.ts";
import { UsageTracker } from "./console-usage.ts";
import { buildHistory, buildStepsTree } from "./console-panes.ts";

// ---- tools ----

const FETCH_TIMEOUT_MS = 15_000;

const fetchUrl = tool({
  name: "fetchUrl",
  description: "Fetch the text content of a URL.",
  parameters: z.object({ url: z.string().url() }),
  execute: async ({ url }) => {
    const resp = await fetch(url, { signal: AbortSignal.timeout(FETCH_TIMEOUT_MS) });
    return resp.text();
  },
});

const webSearch = tool({
  name: "webSearch",
  description: "Search the web using DuckDuckGo and return results.",
  parameters: z.object({ query: z.string() }),
  execute: async ({ query }) => {
    const url = `https://api.duckduckgo.com/?q=${encodeURIComponent(query)}&format=json&no_html=1&skip_disambig=1`;
    const resp = await fetch(url, { signal: AbortSignal.timeout(FETCH_TIMEOUT_MS) });
    const data = (await resp.json()) as Record<string, unknown>;
    const topics = (data.RelatedTopics as { Text?: string }[] | undefined) ?? [];
    const results = [
      data.AbstractText && `**Summary:** ${data.AbstractText}`,
      ...topics
        .slice(0, 5)
        .map((t) => t.Text)
        .filter(Boolean),
    ].filter(Boolean);
    return results.length ? results.join("\n\n") : "No results found.";
  },
});

// ---- setup ----

const apiKey = process.env.ANTHROPIC_API_KEY;
if (!apiKey) {
  console.error("Set ANTHROPIC_API_KEY to run.");
  process.exit(1);
}

const autoApproveEnv = process.env.TOOL_AUTO_APPROVE === "true";

const storage = new InMemoryWorkflowStorage();
const runner = createWorkflowRunner({ storage });

const usage = new UsageTracker();
const claude = usage.withTracking(anthropic("claude-sonnet-4-6", { apiKey }), "claude-sonnet-4-6");

// ---- terminal ----
const rl = createInterface({ input: process.stdin, output: process.stdout, historySize: 100 });
const term = new Terminal(rl);
const consoleRunner = new ConsoleRunner(term, usage);

// ---- approval state ----
let autoApprove = autoApproveEnv;

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

    // ---- slash commands ----
    if (input === "/help") {
      await term.showPane("help", [
        "  /history  — director conversation history",
        "  /steps    — director workflow step tree",
        "  /tools    — agents and their tools",
        `  /approve-all  — toggle auto-approve (currently: ${autoApprove ? "ON" : "OFF"})`,
        "  /help     — show this list",
        "  exit      — quit",
        "",
        "  Ctrl+C during a turn: interrupt (session recovers quickly).",
        "  Ctrl+C at prompt twice: exit.",
        "  Tool approval: y=yes  a=always  N=no",
      ]);
      return prompt();
    }

    if (input === "/approve-all") {
      autoApprove = !autoApprove;
      term.printAbove(`\x1b[2mAuto-approve: ${autoApprove ? "ON" : "OFF"}\x1b[0m`);
      return prompt();
    }

    if (input === "/history") {
      const mayorSession = await town.getMayorSession();
      await term.showPane("director history", buildHistory(mayorSession));
      return prompt();
    }

    if (input === "/steps") {
      await term.showInteractiveTree(
        "director steps",
        await buildStepsTree(storage, runner, "town-director"),
      );
      return prompt();
    }

    if (input === "/tools") {
      const lines: string[] = [
        "  director    sendMessage, readInbox, memory, searchSharedMemory, saveSharedMemory",
        "  researcher  webSearch, fetchUrl, sendMessage, memory",
        "  analyst     sendMessage, memory",
        "  factChecker webSearch, sendMessage, memory",
        "  writer      sendMessage, memory",
      ];
      await term.showPane("tools", lines);
      return prompt();
    }

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
