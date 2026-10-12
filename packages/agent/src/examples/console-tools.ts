import { join } from "node:path";
import { mkdir } from "node:fs/promises";
import { openai } from "../lib/adapters/openai.ts";
import { anthropic } from "../lib/adapters/anthropic.ts";
import { gemini } from "../lib/adapters/gemini.ts";
import { tool } from "../lib/tool.ts";
import { SecretScope, type SecretsStorage } from "../lib/secrets/types.ts";
import { createWriteToolTool } from "../lib/tools/write-tool.ts";
import { createFilesystemTools } from "../lib/tools/filesystem-tools.ts";
import { createShellTool } from "../lib/tools/shell-tool.ts";
import { createLlmTool } from "../lib/tools/llm-tool.ts";
import { subAgentTool } from "../lib/delegation/sub-agent-tool.ts";
import { SessionEventBus } from "../lib/session-logger.ts";
import type { ToolCall } from "../lib/message.ts";
import { createLayeredMemoryTool } from "../lib/tools/layered-memory-tools.ts";
import { createFileToolRegistry } from "../lib/tool-registry.ts";
import { InMemoryScheduler, type ScheduleTick } from "@promin/workflow/scheduler";
import type { MemoryStore, ThreadKey } from "../lib/memory/types.ts";
import type { ToolRegistry } from "../lib/tool-registry.ts";
import type { AgentTool, ApprovalDecision } from "../lib/tool.ts";
import type { LLMProvider } from "../lib/llm-provider.ts";
import type { UsageTracker } from "./console-usage.ts";
import type { Message } from "../lib/message.ts";
import type { ConsoleSession } from "./console-session.ts";
import { abbrevInput } from "./console-spinner.ts";
import { z } from "zod";

export interface ToolDeps {
  workspace: string;
  /** Long-term memory the `memory` tool reads and writes. */
  memory: { store: MemoryStore; key: ThreadKey };
  /** Keys the user typed in at a prompt. Env vars are checked first. */
  secrets: SecretsStorage;
  apiKey: string;
  ask: (question: string) => Promise<string>;
  usage: UsageTracker;
  /** Mutable reference to the current session — filled in after session creation. */
  sessionRef: { current: ConsoleSession | undefined };
  /** Shared auto-approve flag — subagents read and write this so "always" propagates globally. */
  autoApproveRef: { value: boolean };
  /** Print a line above the current spinner/prompt — used to surface sub-agent activity. */
  printAbove: (...lines: string[]) => void;
}

export interface ToolSetup {
  registry: ToolRegistry;
  scheduler: InMemoryScheduler;
  activeTicks: Set<string>;
}

const SessionDebugInputSchema = z.discriminatedUnion("command", [
  z.object({
    command: z.literal("errors"),
    limit: z.number().int().min(1).max(50).default(20).describe("Max failures to return"),
  }),
  z.object({
    command: z.literal("calls"),
    limit: z.number().int().min(1).max(30).default(10).describe("Max calls to return"),
    errorsOnly: z.boolean().default(false).describe("Show only failed calls"),
  }),
  z.object({ command: z.literal("status") }),
  z.object({
    command: z.literal("log"),
    limit: z
      .number()
      .int()
      .min(1)
      .max(200)
      .default(100)
      .describe("Max events to return (most recent first)"),
    type: z.string().optional().describe("Filter by event type prefix, e.g. 'tool' or 'step'"),
  }),
]);

interface ToolCallRecord {
  readonly name: string;
  readonly input: unknown;
  readonly result: string;
  readonly failed: boolean;
}

/** Every tool call in `messages` with its result. */
function toolCallRecords(messages: ReadonlyArray<Message>): ToolCallRecord[] {
  const results = new Map<string, { content: string; failed: boolean }>();
  for (const m of messages) {
    if (m.role === "tool") {
      results.set(m.toolCallId, { content: m.content, failed: m.isError === true });
    }
  }
  const records: ToolCallRecord[] = [];
  for (const m of messages) {
    if (m.role !== "assistant") continue;
    for (const call of m.toolCalls ?? []) {
      const result = results.get(call.id);
      if (!result) continue;
      records.push({
        name: call.name,
        input: call.input,
        result: result.content,
        failed: result.failed,
      });
    }
  }
  return records;
}

export function createSessionDebugTool(sessionRef: { current: ConsoleSession | undefined }) {
  async function calls(params: { limit: number; errorsOnly: boolean }): Promise<string> {
    const session = sessionRef.current;
    if (!session) return "No active session.";
    const records = toolCallRecords(await session.messages()).filter(
      (r) => !params.errorsOnly || r.failed,
    );
    if (records.length === 0) {
      return params.errorsOnly ? "No failed tool calls in this session." : "No tool calls found.";
    }
    return records
      .slice(-params.limit)
      .map((r, i) => {
        const marker = r.failed ? "✗" : "✓";
        const param = abbrevInput((r.input as Record<string, unknown>) ?? {});
        const preview = r.result.length > 200 ? `${r.result.slice(0, 197)}…` : r.result;
        return `${i + 1}. ${marker} ${r.name}${param ? `  ${param}` : ""}\n   ${preview}`;
      })
      .join("\n\n");
  }

  async function status(): Promise<string> {
    const session = sessionRef.current;
    if (!session) return "No active session.";
    const messages = await session.messages();
    return [
      `Session:   ${session.id}`,
      `Status:    ${session.busy() ? "thinking" : "idle"}`,
      `Messages:  ${messages.length}`,
    ].join("\n");
  }

  function log(params: { limit: number; type?: string }): string {
    const session = sessionRef.current;
    if (!session) return "No active session.";
    const typeFilter = params.type;
    const events = session.events().filter((e) => !typeFilter || e.type.startsWith(typeFilter));
    if (events.length === 0) return "No session events recorded yet.";
    return events
      .slice(-params.limit)
      .reverse()
      .map((e) => {
        const t = new Date(e.ts).toLocaleTimeString();
        const { type, ts: _ts, ...rest } = e;
        const detail = Object.entries(rest)
          .map(([k, v]) => `${k}=${JSON.stringify(v)?.slice(0, 80)}`)
          .join("  ");
        return `[${t}] ${type}  ${detail}`;
      })
      .join("\n");
  }

  return tool({
    name: "sessionDebug",
    description:
      "Inspect the current agent session to diagnose failures and errors.\n\nCommands:\n" +
      "  errors: List failed tool calls of this session\n" +
      "  calls: Show recent tool calls with their results\n" +
      "  status: Current session id, status and message count\n" +
      "  log: Show the recent run events (tool calls, steps, approvals, finishes)",
    parameters: SessionDebugInputSchema,
    execute: async (input) => {
      switch (input.command) {
        case "errors":
          return calls({ limit: input.limit, errorsOnly: true });
        case "calls":
          return calls(input);
        case "status":
          return status();
        case "log":
          return log(input);
      }
    },
  });
}

/**
 * In-process `scheduleTask` / `listSchedules` / `cancelSchedule` tools over
 * an `InMemoryScheduler`. Each tick replays its task into the live session
 * through `onTick`. Console-only: schedules die with the process. Durable
 * agents use `createDurableSchedulerTools` instead.
 */
function createConsoleSchedulerTools(config: {
  scheduler: InMemoryScheduler;
  onTick: (task: string, tick: ScheduleTick) => void;
}) {
  void (async () => {
    for await (const tick of config.scheduler.subscribe().toAsyncIterable()) {
      const task = tick.metadata?.task;
      if (typeof task === "string") config.onTick(task, tick);
    }
  })();

  return {
    scheduleTask: tool({
      name: "scheduleTask",
      description:
        "Schedule a recurring task. Use a cron expression for calendar-based schedules " +
        "(e.g. '0 9 * * *' for daily at 9am), or intervalMs for fixed intervals " +
        "(e.g. 60000 for every minute). The task string is replayed back to you when " +
        "the schedule fires.",
      parameters: z
        .object({
          id: z
            .string()
            .describe("Unique schedule ID — use a short descriptive name, e.g. 'daily-report'"),
          task: z
            .string()
            .describe("The task to run when the schedule fires (replayed as a new message)"),
          cron: z.string().optional().describe("Cron expression, e.g. '0 9 * * *' (daily 9am UTC)"),
          intervalMs: z
            .number()
            .int()
            .positive()
            .optional()
            .describe("Fixed interval in milliseconds"),
          timezone: z
            .string()
            .optional()
            .describe("IANA timezone for cron evaluation, e.g. 'America/New_York'"),
        })
        .refine((v) => v.cron !== undefined || v.intervalMs !== undefined, {
          message: "Provide either cron or intervalMs",
        }),
      execute: async ({ id, task, cron, intervalMs, timezone }) => {
        await config.scheduler.register({
          id,
          ...(cron ? { cron } : {}),
          ...(intervalMs ? { intervalMs } : {}),
          ...(timezone ? { timezone } : {}),
          metadata: { task },
        });
        const schedule = cron
          ? `cron(${cron}${timezone ? ` ${timezone}` : ""})`
          : `every ${intervalMs}ms`;
        return `Scheduled "${id}": ${schedule}`;
      },
    }),

    listSchedules: tool({
      name: "listSchedules",
      description: "List all registered schedules and their configurations.",
      parameters: z.object({}),
      execute: async () => {
        const schedules = await config.scheduler.list();
        if (!schedules.length) return "No schedules registered.";
        return schedules
          .map((s) => {
            const trigger = s.cron ?? (s.intervalMs ? `every ${s.intervalMs}ms` : "unknown");
            const status = s.enabled === false ? " [paused]" : "";
            const task = s.metadata?.task ?? "(no task)";
            return `${s.id}${status}: ${trigger} → "${task}"`;
          })
          .join("\n");
      },
    }),

    cancelSchedule: tool({
      name: "cancelSchedule",
      description: "Cancel and remove a scheduled task by its ID.",
      parameters: z.object({
        id: z.string().describe("The schedule ID to cancel"),
      }),
      execute: async ({ id }) => {
        await config.scheduler.unregister({ scheduleId: id });
        return `Cancelled schedule "${id}".`;
      },
    }),
  };
}

export async function createToolRegistry(deps: ToolDeps): Promise<ToolSetup> {
  const { workspace, memory, secrets, apiKey, ask, usage, sessionRef, autoApproveRef, printAbove } =
    deps;

  const toolsDir = join(import.meta.dir, "tools");
  await mkdir(toolsDir, { recursive: true });

  // Batch file-registry load notifications with a short debounce.
  let loadBatch: string[] = [];
  let loadTimer: ReturnType<typeof setTimeout> | null = null;
  function flushLoadBatch() {
    if (!loadBatch.length) return;
    const shown = loadBatch.slice(0, 5);
    const extra = loadBatch.length - 5;
    console.log(
      `\x1b[2mloaded tools: ${shown.join(", ")}${extra > 0 ? `, …and ${extra} more` : ""}\x1b[0m`,
    );
    loadBatch = [];
    loadTimer = null;
  }

  const fileRegistry = await createFileToolRegistry({
    dir: toolsDir,
    onLoad: (name, cat) => {
      loadBatch.push(cat ? `${cat}/${name}` : name);
      if (loadTimer) clearTimeout(loadTimer);
      loadTimer = setTimeout(flushLoadBatch, 50);
    },
    onUnload: (name, cat) =>
      console.log(`\x1b[2munloaded tool: ${cat ? `${cat}/` : ""}${name}\x1b[0m`),
    onError: (file, err) => console.error(`tool error: ${file}`, err),
  });

  // Serialize all ask() calls so concurrent tool executions never overlap prompts.
  // Without this, parallel tools (chatGPT + chatGemini) both call rl.question()
  // simultaneously: the second prompt is never shown and its ask() hangs until
  // Ctrl+C aborts the turn with "Tool execution failed: Aborted".
  let askQueue: Promise<void> = Promise.resolve();
  function serialAsk(question: string): Promise<string> {
    const result = askQueue.then(() => ask(question));
    // Advance the queue even if ask() throws (e.g. abort), so the next caller unblocks.
    askQueue = result.then(
      () => {},
      () => {},
    );
    return result;
  }

  /** Resolve a secret: env first, then the secrets storage. Prompt once if absent. */
  async function getSecret(envKey: string, toolName: string): Promise<string> {
    const fromEnv = process.env[envKey];
    if (fromEnv) return fromEnv;
    const scope = SecretScope.global();
    let value = await secrets.get({ scope, key: envKey });
    if (!value) {
      value = await serialAsk(`[${toolName}] Enter ${envKey}`);
      await secrets.set({ scope, key: envKey, value });
    }
    return value;
  }

  const memoryTool = createLayeredMemoryTool({
    store: memory.store,
    namespaceId: memory.key.namespaceId,
    ...(memory.key.resourceId !== undefined && { resourceId: memory.key.resourceId }),
    threadId: memory.key.threadId,
  });

  // One-shot GPT-4o tool shared by main agent and sub-agents.
  const chatGptOneShotTool = createLlmTool(
    {
      chat: async (params) => {
        const key = await getSecret("OPENAI_API_KEY", "chatGPT");
        return openai("gpt-4o", { apiKey: key }).chat(params);
      },
    },
    {
      name: "chatGPT",
      description: "Consult ChatGPT (GPT-4o) for a second opinion or different perspective.",
    },
  );

  const scheduler = new InMemoryScheduler();
  const activeTicks = new Set<string>();

  const staticTools: Record<string, AgentTool<any, any>> = {
    calculator: tool({
      name: "calculator",
      description: "Evaluate a JS math expression and return the result.",
      parameters: z.object({ expression: z.string() }),
      execute: async ({ expression }) => {
        try {
          // eslint-disable-next-line no-eval
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

    writeTool: createWriteToolTool({
      dir: toolsDir,
      requireApproval: false,
      toolImportPath: "../../lib/index.ts",
    }),

    ...createFilesystemTools({ rootDir: workspace }),

    shell: createShellTool({
      cwd: workspace,
      allowedCommands: ["bun", "git", "ls", "cat", "find", "grep", "npm", "npx"],
    }),

    memory: memoryTool,

    sessionDebug: createSessionDebugTool(sessionRef),

    chatGPT: chatGptOneShotTool,

    ...createConsoleSchedulerTools({
      scheduler,
      onTick: (task, tick) => {
        if (activeTicks.has(tick.scheduleId)) return;
        activeTicks.add(tick.scheduleId);
        const run = async () => {
          try {
            const schedules = await scheduler.list();
            if (!schedules.some((s) => s.id === tick.scheduleId)) return;
            const answer = await sessionRef.current!.send(task);
            console.log(`\x1b[2m[scheduler]\x1b[0m Agent: ${answer}`);
          } finally {
            activeTicks.delete(tick.scheduleId);
          }
        };
        run().catch((err) => {
          if (err instanceof Error && err.message.includes("Session is busy")) {
            console.log(
              `\x1b[2m[scheduler] skipped tick "${tick.scheduleId}" — session busy\x1b[0m`,
            );
          } else {
            console.error("[scheduler] tick error:", err);
          }
        });
      },
    }),
  };

  // Sub-agents share filesystem + shell + memory + chatGPT.
  const subagentTools = {
    ...createFilesystemTools({ rootDir: workspace }),
    shell: createShellTool({
      cwd: workspace,
      allowedCommands: ["bun", "git", "ls", "cat", "find", "grep", "npm", "npx"],
    }),
    memory: memoryTool,
    chatGPT: chatGptOneShotTool,
  };

  const subagentSystemPrompt =
    `You are a focused sub-agent. Be concise and task-focused.\nWorkspace: ${workspace}\n\n` +
    `You have access to these tools: ${Object.keys(subagentTools).join(", ")}. ` +
    "Always use tools when they are relevant to the task rather than relying on your own knowledge.";

  // A sub-agent's tool calls print above the prompt; its approvals go
  // through the same [y/n/always] prompt as the main agent's.
  const subagentBus = (prefix: string): SessionEventBus => {
    const bus = new SessionEventBus();
    const inputs = new Map<string, Record<string, unknown>>();
    bus.subscribe((event) => {
      if (event.type === "tool.start") {
        inputs.set(event.toolCallId, (event.input ?? {}) as Record<string, unknown>);
        return;
      }
      if (event.type !== "tool.end") return;
      const param = abbrevInput(inputs.get(event.toolCallId) ?? {});
      inputs.delete(event.toolCallId);
      const elapsed =
        event.durationMs >= 1000
          ? `${(event.durationMs / 1000).toFixed(1)}s`
          : `${event.durationMs}ms`;
      const hint = param ? `  \x1b[2m${param}\x1b[0m` : "";
      printAbove(
        event.failed
          ? `\x1b[31m✗ ${prefix} ${event.name}${hint}  failed  (${elapsed})\x1b[0m`
          : `\x1b[2m✓ ${prefix} ${event.name}${hint}  (${elapsed})\x1b[0m`,
      );
    });
    return bus;
  };
  const subagentApproval =
    (name: string) =>
    async (call: ToolCall): Promise<ApprovalDecision> => {
      if (autoApproveRef.value) return { approved: true };
      const param = abbrevInput((call.input ?? {}) as Record<string, unknown>);
      const hint = param ? `  \x1b[2m${param}\x1b[0m` : "";
      const answer = (
        await serialAsk(`[${name}] Allow tool "${call.name}"${hint}? [y/n/always]`)
      ).toLowerCase();
      if (answer === "always") autoApproveRef.value = true;
      return { approved: answer.startsWith("y") || answer === "always" };
    };

  staticTools.claudeAgent = subAgentTool({
    name: "claudeAgent",
    description:
      "Delegate a task to a parallel Claude (Sonnet) sub-agent with filesystem, shell, memory, and chatGPT access. Use to parallelise independent subtasks or run deep research alongside the main thread.",
    timeoutMs: 120_000,
    agent: {
      name: "claudeAgent",
      llm: usage.withTracking(anthropic("claude-sonnet-4-6", { apiKey }), "claude-sonnet-4-6"),
      tools: subagentTools,
      systemPrompt: subagentSystemPrompt,
      maxSteps: 20,
      onApprovalRequired: subagentApproval("claudeAgent"),
      bus: subagentBus("[claudeAgent]"),
    },
  });

  const lazyOpenAI: LLMProvider = {
    chat: async (params) => {
      const key = await getSecret("OPENAI_API_KEY", "gptAgent");
      return openai("gpt-4o", { apiKey: key }).chat(params);
    },
    chatStream: async function* (params) {
      const key = await getSecret("OPENAI_API_KEY", "gptAgent");
      yield* openai("gpt-4o", { apiKey: key }).chatStream!(params);
    },
  };

  staticTools.gptAgent = subAgentTool({
    name: "gptAgent",
    description:
      "Delegate a task to a parallel GPT-4o sub-agent with filesystem, shell, memory, and chatGPT access. Use for a second opinion, different reasoning style, or to parallelise work.",
    timeoutMs: 120_000,
    agent: {
      name: "gptAgent",
      llm: usage.withTracking(lazyOpenAI, "gpt-4o"),
      tools: subagentTools,
      systemPrompt: subagentSystemPrompt,
      maxSteps: 20,
      onApprovalRequired: subagentApproval("gptAgent"),
      bus: subagentBus("[gptAgent]"),
    },
  });

  staticTools.chatGemini = createLlmTool(
    {
      chat: async (params) => {
        const key = await getSecret("GEMINI_API_KEY", "chatGemini");
        return gemini("gemini-2.0-flash", { apiKey: key }).chat(params);
      },
    },
    {
      name: "chatGemini",
      description:
        "Consult Gemini (gemini-2.0-flash) for a second opinion or different perspective.",
    },
  );

  const registry: ToolRegistry = {
    getTools: () => ({ ...fileRegistry.getTools(), ...staticTools }),
    close: () => fileRegistry.close(),
  };

  return { registry, scheduler, activeTicks };
}
