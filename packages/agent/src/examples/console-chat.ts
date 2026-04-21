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
 *   /history  — print conversation messages
 *   /steps    — print full workflow step tree from storage
 *   /state    — print agent lifecycle state machine (current state + transition history)
 *   exit      — quit
 */

import { createInterface } from "node:readline";
import { workflow, stateMachine, InMemoryStateMachineStorage } from "@promin/workflow";
import { InMemoryWorkflowStorage, createWorkflowRunner } from "@promin/workflow";
import { anthropic } from "../lib/adapters/anthropic.ts";
import { tool } from "../lib/tool.ts";
import { zodToJsonSchema } from "../lib/zod-to-json-schema.ts";
import type { Message, AssistantMessage, ToolResultMessage } from "../lib/message.ts";
import type { LLMToolDefinition } from "../lib/llm-provider.ts";
import { z } from "zod";

// ---- config ----

const apiKey = process.env.ANTHROPIC_API_KEY;
if (!apiKey) {
  console.error("Set ANTHROPIC_API_KEY to run this example.");
  process.exit(1);
}

const MAX_TURNS = 100;
const MAX_STEPS_PER_TURN = 10;

// ---- conversation history (updated after each emit, used by /history and showHistory tool) ----

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

// ---- tools ----

const calculator = tool({
  description: "Evaluate a mathematical expression and return the numeric result.",
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
  description: "Return the current local date and time.",
  parameters: z.object({}),
  execute: async () => new Date().toLocaleString(),
});

const showHistory = tool({
  description: "Print the full conversation history stored in memory.",
  parameters: z.object({}),
  execute: async () => {
    printHistory();
    return `Printed ${conversationHistory.length} messages.`;
  },
});

const toolsMap = { calculator, currentTime, showHistory };

// Pre-compute tool defs for LLM (constant — lives outside the journaled step)
const llmToolDefs: LLMToolDefinition[] = Object.entries(toolsMap).map(([name, t]) => ({
  name,
  description: t.description,
  parameters: zodToJsonSchema(t.parameters),
}));

const llm = anthropic("claude-sonnet-4-6", { apiKey });

// ---- agent lifecycle state machine ----

type AgentStates = {
  idle: {
    context: { completedTurns: number };
    transitions: { message: "thinking" };
  };
  thinking: {
    context: { completedTurns: number; turn: number; step: number; task: string };
    transitions: { tool_calls: "tool_calling"; done: "idle" };
  };
  tool_calling: {
    context: {
      completedTurns: number;
      turn: number;
      step: number;
      task: string;
      activeTools: string[];
    };
    transitions: { tools_done: "thinking" };
  };
};

const smStorage = new InMemoryStateMachineStorage();
// biome-ignore lint/suspicious/noExplicitAny: state machine actions receive runtime data
const agentMachine = stateMachine<AgentStates>({ name: "agent-lifecycle", storage: smStorage })
  .state("idle")
  .state("thinking")
  .state("tool_calling")
  .on("message", {
    from: "idle",
    to: "thinking",
    action: (ctx: any, data: any) => ({
      completedTurns: ctx.completedTurns,
      turn: data.turn,
      step: 0,
      task: data.task,
    }),
  })
  .on("tool_calls", {
    from: "thinking",
    to: "tool_calling",
    action: (ctx: any, data: any) => ({ ...ctx, activeTools: data.tools }),
  })
  .on("tools_done", {
    from: "tool_calling",
    to: "thinking",
    action: (ctx: any) => ({
      completedTurns: ctx.completedTurns,
      turn: ctx.turn,
      step: ctx.step + 1,
      task: ctx.task,
    }),
  })
  .on("done", {
    from: "thinking",
    to: "idle",
    action: (ctx: any) => ({ completedTurns: ctx.completedTurns + 1 }),
  })
  .initial("idle")
  .build();

// ---- in-process response delivery ----
// The journaled step resolves these promises from inside emit-N activities.
// Works in-process; a real system would use SSE/WebSocket.

const pendingResponses = new Map<number, (answer: string) => void>();

// ---- single session workflow ----

const sessionWorkflow = workflow<void>({ name: "console-agent" })
  .journaled("conversation", function* (ctx, _input) {
    let messages: Message[] = [
      { role: "system", content: "You are a helpful assistant. Be concise." },
    ];

    for (let turn = 0; turn < MAX_TURNS; turn++) {
      // Suspend here until the user sends their next message via handle.signal()
      const { task } = yield* ctx.signal<{ task: string }>(`task-${turn}`);
      messages = [...messages, { role: "user", content: task }];

      yield* ctx.activity(`sm-message-${turn}`, () =>
        agentMachine.send({ id: "session", event: "message", data: { turn, task } }),
      );

      let answer = "";

      for (let step = 0; step < MAX_STEPS_PER_TURN; step++) {
        const response = yield* ctx.activity(`think-${turn}-${step}`, () =>
          llm.chat({ messages, tools: llmToolDefs }),
        );

        const assistantMsg: AssistantMessage = {
          role: "assistant",
          content: response.content,
          toolCalls: response.toolCalls,
        };
        messages = [...messages, assistantMsg];

        if (response.finishReason === "stop" || !response.toolCalls?.length) {
          answer = response.content ?? "";
          yield* ctx.activity(`sm-done-${turn}`, () =>
            agentMachine.send({ id: "session", event: "done" }),
          );
          break;
        }

        // Agent decided to use tools
        yield* ctx.activity(`sm-tool-calls-${turn}-${step}`, () =>
          agentMachine.send({
            id: "session",
            event: "tool_calls",
            data: { tools: response.toolCalls!.map((c) => c.name) },
          }),
        );

        const toolResultMsgs: ToolResultMessage[] = [];
        for (const call of response.toolCalls) {
          const toolDef = toolsMap[call.name as keyof typeof toolsMap];
          if (!toolDef) {
            toolResultMsgs.push({
              role: "tool",
              toolCallId: call.id,
              content: `Error: unknown tool "${call.name}"`,
            });
            continue;
          }
          // biome-ignore lint/suspicious/noExplicitAny: Zod validates input at runtime
          const output = yield* ctx.activity(
            `tool-${call.name}-${turn}-${step}-${call.id}`,
            async () => {
              const parsed = toolDef.parameters.parse(call.input);
              return toolDef.execute(parsed as any);
            },
          );
          const content = typeof output === "string" ? output : JSON.stringify(output);
          toolResultMsgs.push({ role: "tool", toolCallId: call.id, content });
        }
        messages = [...messages, ...toolResultMsgs];

        yield* ctx.activity(`sm-tools-done-${turn}-${step}`, () =>
          agentMachine.send({ id: "session", event: "tools_done" }),
        );
      }

      // Journaled — executes exactly once, skipped on replay
      yield* ctx.activity(`emit-${turn}`, async () => {
        conversationHistory = messages.filter((m) => m.role !== "system");
        pendingResponses.get(turn)?.(answer);
        pendingResponses.delete(turn);
        return answer;
      });
    }
  })
  .build();

// ---- infrastructure ----

const storage = new InMemoryWorkflowStorage();
const runner = createWorkflowRunner({ storage });

// Start the state machine instance for this session
await agentMachine.start({ id: "session", context: { completedTurns: 0 } });

// Single persistent session — fire-and-forget, never completes
const handle = await runner.start({
  workflow: sessionWorkflow,
  workflowId: "session",
  input: undefined,
});

let sessionTurn = 0;

async function sendTask(task: string): Promise<string> {
  const turn = sessionTurn++;
  const promise = new Promise<string>((resolve) => pendingResponses.set(turn, resolve));
  await handle.signal(`task-${turn}`, { task });
  return promise;
}

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

const rl = createInterface({ input: process.stdin, output: process.stdout });

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
    if (task === "/state") {
      await printAgentState();
      prompt();
      return;
    }

    const answer = await sendTask(task);
    console.log(`\nAgent: ${answer}`);
    prompt();
  });
}

console.log("Console agent ready. Tools: calculator, currentTime, showHistory.");
console.log('Type "/history", "/steps", "/state", or "exit".\n');
prompt();
