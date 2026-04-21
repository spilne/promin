/**
 * Interactive console agent — type a task, get a durable answer.
 *
 * Run:
 *   ANTHROPIC_API_KEY=sk-... bun packages/agent/src/examples/console-chat.ts
 *
 * Each turn is a separate agentAction run with the full conversation history
 * passed as seed messages, giving the agent multi-turn memory without an
 * always-on workflow. Crash mid-turn → resume from the last journaled step.
 *
 * Commands:
 *   /history  — print all stored conversation messages
 *   /steps    — print workflow step history from storage for every turn
 *   exit      — quit
 */

import { createInterface } from "node:readline";
import { agentAction } from "../lib/agent-action.ts";
import type { AgentResult } from "../lib/agent-action.ts";
import { anthropic } from "../lib/adapters/anthropic.ts";
import { tool } from "../lib/tool.ts";
import type { Message } from "../lib/message.ts";
import { InMemoryWorkflowStorage, createWorkflowRunner } from "@promin/workflow";
import { z } from "zod";

const apiKey = process.env.ANTHROPIC_API_KEY;
if (!apiKey) {
  console.error("Set ANTHROPIC_API_KEY to run this example.");
  process.exit(1);
}

// --- history state (shared between REPL commands and the showHistory tool) ---

let history: Message[] = [];

function printHistory() {
  if (history.length === 0) {
    console.log("\n(no history yet)\n");
    return;
  }
  console.log("\n--- conversation history ---");
  for (const m of history) {
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
  console.log(`\n--- ${history.length} messages ---\n`);
}

// --- tools ---

const calculator = tool({
  description: "Evaluate a mathematical expression and return the numeric result.",
  parameters: z.object({
    expression: z.string().describe("A valid JS math expression, e.g. '2 ** 10'"),
  }),
  execute: async ({ expression }) => {
    try {
      // biome-ignore lint/security/noEval: example only
      const result = eval(expression);
      return String(result);
    } catch {
      return `Error evaluating: ${expression}`;
    }
  },
});

const currentTime = tool({
  description: "Return the current local date and time.",
  parameters: z.object({}),
  execute: async () => new Date().toLocaleString(),
});

const showHistory = tool({
  description: "Print the full conversation history stored in memory.",
  parameters: z.object({}),
  execute: async () => {
    printHistory();
    return `Printed ${history.length} messages from history.`;
  },
});

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
    const childIndent = isLastRun ? "   " : "│  ";

    const info = await runner.getStatus(run.workflowId, { includeStepResults: true });
    if (!info) continue;

    const runIcon = STATUS_ICON[info.state] ?? "?";
    console.log(`${runPrefix} ${runIcon} ${run.workflowId}  (${info.state})`);

    const stepEntries = Object.entries(info.steps);
    for (let j = 0; j < stepEntries.length; j++) {
      const [stepName, step] = stepEntries[j]!;
      const isLastStep = j === stepEntries.length - 1;
      const stepPrefix = isLastStep ? "└─" : "├─";
      const stepIcon = STATUS_ICON[step.status] ?? "?";

      let resultStr = "";
      if (step.result !== undefined) {
        const raw = JSON.stringify(step.result);
        resultStr = `  →  ${raw.length > 100 ? `${raw.slice(0, 100)}…` : raw}`;
      }

      console.log(`${childIndent}${stepPrefix} ${stepIcon} ${stepName}${resultStr}`);
    }
  }
  console.log("");
}

// --- agent ---

const storage = new InMemoryWorkflowStorage();
const runner = createWorkflowRunner({ storage });

const agent = agentAction({
  name: "console-agent",
  llm: anthropic("claude-sonnet-4-6", { apiKey }),
  systemPrompt: "You are a helpful assistant. Be concise.",
  tools: { calculator, currentTime, showHistory },
  maxSteps: 10,
});

// --- REPL ---

const rl = createInterface({ input: process.stdin, output: process.stdout });

let turn = 0;

function prompt() {
  rl.question("\nYou: ", async (line) => {
    const task = line.trim();
    if (!task) {
      prompt();
      return;
    }
    if (task === "exit" || task === "quit") {
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

    const { data, error } = await runner.runSafe({
      workflow: agent,
      workflowId: `turn-${turn++}`,
      input: { task, messages: history },
    });

    if (error || !data) {
      console.error("\nError:", String(error));
    } else {
      const result = data as AgentResult;
      console.log(`\nAgent: ${result.answer}`);
      // Strip system messages — agentAction prepends its own each turn.
      history = result.messages.filter((m): m is Message => m.role !== "system");
    }

    prompt();
  });
}

console.log("Console agent ready. Tools: calculator, currentTime, showHistory.");
console.log('Type "/history" for messages, "/steps" for workflow step tree, "exit" to quit.\n');
prompt();
