/**
 * Interactive console agent — type a task, get a durable answer.
 *
 * Run:
 *   ANTHROPIC_API_KEY=sk-... bun packages/agent/src/examples/console-chat.ts
 *
 * Each turn is a separate agentAction run with the full conversation history
 * passed as seed messages, giving the agent multi-turn memory without an
 * always-on workflow. Crash mid-turn → resume from the last journaled step.
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

// --- agent ---

const storage = new InMemoryWorkflowStorage();
const runner = createWorkflowRunner({ storage });

const agent = agentAction({
  name: "console-agent",
  llm: anthropic("claude-sonnet-4-6", { apiKey }),
  systemPrompt: "You are a helpful assistant. Be concise.",
  tools: { calculator, currentTime },
  maxSteps: 10,
});

// --- REPL ---

const rl = createInterface({ input: process.stdin, output: process.stdout });

// history excludes system messages — agentAction prepends its own
let history: Message[] = [];
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

console.log('Console agent ready. Tools: calculator, currentTime. Type "exit" to quit.\n');
prompt();
