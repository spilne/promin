import { join } from "node:path";
import { mkdir } from "node:fs/promises";
import { openai } from "../lib/adapters/openai.ts";
import { anthropic } from "../lib/adapters/anthropic.ts";
import { gemini } from "../lib/adapters/gemini.ts";
import { tool } from "../lib/tool.ts";
import type { SecretStore } from "../lib/secret-store.ts";
import { createWriteToolTool } from "../lib/tools/write-tool.ts";
import { createRequireSecretTool } from "../lib/tools/require-secret-tool.ts";
import { createFilesystemTools } from "../lib/tools/filesystem-tools.ts";
import { createShellTool } from "../lib/tools/shell-tool.ts";
import { multiTool, command } from "../lib/multi-tool.ts";
import { createLlmTool } from "../lib/tools/llm-tool.ts";
import { createSchedulerTools } from "../lib/tools/scheduler-tools.ts";
import { createAgentTool } from "../lib/tools/agent-tool-factory.ts";
import { createFileToolRegistry } from "../lib/tool-registry.ts";
import { InMemoryScheduler, isActivityJournalStorage } from "@promin/workflow";
import type { WorkflowRunner } from "@promin/workflow";
import type { MemoryStore } from "../lib/memory-store.ts";
import type { ToolRegistry } from "../lib/tool-registry.ts";
import type { AgentTool } from "../lib/tool.ts";
import type { LLMProvider } from "../lib/llm-provider.ts";
import type { UsageTracker } from "./console-usage.ts";
import type { AgentSession } from "../lib/agent-loop.ts";
import type { SessionLogger } from "../lib/session-logger.ts";
import { abbrevInput } from "./console-spinner.ts";
import { z } from "zod";

export interface ToolDeps {
  workspace: string;
  memoryStore: MemoryStore;
  /** Shared secret store — checked env-first via CompositeSecretStore. */
  secrets: SecretStore;
  apiKey: string;
  ask: (question: string) => Promise<string>;
  runner: WorkflowRunner;
  usage: UsageTracker;
  /** Mutable reference to the current session — filled in after session creation. */
  sessionRef: { current: AgentSession | undefined };
  /** Mutable reference to the current session ID — used to look up the activity journal. */
  sessionIdRef: { current: string };
  /** Shared auto-approve flag — subagents read and write this so "always" propagates globally. */
  autoApproveRef: { value: boolean };
  /** Print a line above the current spinner/prompt — used to surface sub-agent activity. */
  printAbove: (...lines: string[]) => void;
  /** Session event logger — used by sessionDebug and sub-agent tracking. */
  logger?: SessionLogger;
}

export interface ToolSetup {
  registry: ToolRegistry;
  scheduler: InMemoryScheduler;
  activeTicks: Set<string>;
}

function fmtAge(d: Date): string {
  const sec = Math.floor((Date.now() - d.getTime()) / 1000);
  if (sec < 60) return `${sec}s ago`;
  if (sec < 3600) return `${Math.floor(sec / 60)}m ago`;
  if (sec < 86400) return `${Math.floor(sec / 3600)}h ago`;
  return `${Math.floor(sec / 86400)}d ago`;
}

export function createMemoryTool(store: MemoryStore) {
  return multiTool({
    name: "memory",
    description: "Read and write long-term memory that persists across sessions.",
    commands: {
      search: command({
        description: "Search for relevant memories by natural-language query",
        parameters: z.object({
          query: z.string().describe("Natural-language search query"),
          limit: z.number().int().min(1).max(20).default(5).describe("Max entries to return"),
        }),
        execute: async ({ query, limit }) => {
          const [entries, all] = await Promise.all([
            store.search(query, limit),
            store.list(undefined),
          ]);
          if (entries.length === 0) return "No memories found matching that query.";
          const header =
            all.length > entries.length
              ? `Showing ${entries.length} of ${all.length} entries:\n`
              : "";
          return (
            header +
            entries
              .map(
                (e, i) => `${i + 1}. [${e.id.slice(0, 8)}] (${fmtAge(e.createdAt)}) ${e.content}`,
              )
              .join("\n")
          );
        },
      }),
      list: command({
        description: "Browse recent memories without a search query",
        parameters: z.object({
          limit: z.number().int().min(1).max(50).default(10).describe("Max entries to return"),
        }),
        execute: async ({ limit }) => {
          const [entries, all] = await Promise.all([store.list(limit), store.list(undefined)]);
          if (entries.length === 0) return "No memories stored yet.";
          const header =
            all.length > entries.length
              ? `Showing ${entries.length} of ${all.length} entries:\n`
              : "";
          return (
            header +
            entries
              .map(
                (e, i) => `${i + 1}. [${e.id.slice(0, 8)}] (${fmtAge(e.createdAt)}) ${e.content}`,
              )
              .join("\n")
          );
        },
      }),
      save: command({
        description: "Persist a fact or insight so it can be recalled in future sessions",
        parameters: z.object({
          content: z.string().min(1).describe("The fact or insight to remember"),
        }),
        execute: async ({ content }) => {
          const id = await store.save({ content });
          return `Saved to memory (id: ${id.slice(0, 8)}).`;
        },
      }),
      update: command({
        description: "Correct or replace the content of an existing memory entry",
        parameters: z.object({
          id: z.string().describe("First 8+ characters of the memory id"),
          content: z.string().min(1).describe("Replacement content"),
        }),
        execute: async ({ id, content }) => {
          const all = await store.list();
          const entry = all.find((e) => e.id.startsWith(id));
          if (!entry) return `No memory found with id starting "${id}".`;
          await store.update(entry.id, { content });
          return `Updated memory ${entry.id.slice(0, 8)}.`;
        },
      }),
      delete: command({
        description: "Remove a memory entry permanently",
        parameters: z.object({
          id: z.string().describe("First 8+ characters of the memory id"),
        }),
        execute: async ({ id }) => {
          const all = await store.list();
          const entry = all.find((e) => e.id.startsWith(id));
          if (!entry) return `No memory found with id starting "${id}".`;
          await store.delete(entry.id);
          return `Deleted memory ${entry.id.slice(0, 8)}.`;
        },
      }),
    },
  });
}

export function createSessionDebugTool(
  sessionRef: { current: AgentSession | undefined },
  sessionIdRef: { current: string },
  runner: WorkflowRunner,
  logger?: SessionLogger,
) {
  return multiTool({
    name: "sessionDebug",
    description: "Inspect the current agent session to diagnose failures and errors.",
    commands: {
      errors: command({
        description:
          "List activity failures from the workflow journal (survives context compaction)",
        parameters: z.object({
          limit: z.number().int().min(1).max(50).default(20).describe("Max failures to return"),
        }),
        execute: async ({ limit }) => {
          const storage = runner.storage;
          if (!isActivityJournalStorage(storage)) return "Journal storage not available.";
          const entries = await storage.loadJournal(sessionIdRef.current, "conversation");
          const failures = entries.filter((e) => e.exit?.tag === "Failure");
          if (failures.length === 0) return "No errors recorded in current session.";
          return failures
            .slice(-limit)
            .map((e, i) => {
              const t = e.createdAt.toLocaleTimeString();
              return `${i + 1}. [${t}] ${e.activityName}\n   ${(e.exit as { tag: "Failure"; error: string }).error}`;
            })
            .join("\n");
        },
      }),
      calls: command({
        description: "Show recent tool calls with their results (from current context window)",
        parameters: z.object({
          limit: z.number().int().min(1).max(30).default(10).describe("Max calls to return"),
          errorsOnly: z.boolean().default(false).describe("Show only failed calls"),
        }),
        execute: async ({ limit, errorsOnly }) => {
          const session = sessionRef.current;
          if (!session) return "No active session.";
          const messages = session.messages();
          type Pair = { name: string; input: unknown; result: string; failed: boolean };
          const pairs: Pair[] = [];
          for (const msg of messages) {
            if (msg.role !== "assistant" || !msg.toolCalls?.length) continue;
            for (const call of msg.toolCalls) {
              const resultMsg = messages.find(
                (m): m is { role: "tool"; toolCallId: string; content: string } =>
                  m.role === "tool" && (m as any).toolCallId === call.id,
              );
              if (!resultMsg) continue;
              const content = resultMsg.content;
              const failed =
                content.startsWith("Tool execution failed") ||
                content.startsWith("Invalid input") ||
                content.startsWith("Error:");
              if (errorsOnly && !failed) continue;
              pairs.push({ name: call.name, input: call.input, result: content, failed });
            }
          }
          if (pairs.length === 0)
            return errorsOnly ? "No failed tool calls in current context." : "No tool calls found.";
          return pairs
            .slice(-limit)
            .map((p, i) => {
              const marker = p.failed ? "✗" : "✓";
              const param = abbrevInput((p.input as Record<string, unknown>) ?? {});
              const preview = p.result.length > 200 ? `${p.result.slice(0, 197)}…` : p.result;
              return `${i + 1}. ${marker} ${p.name}${param ? `  ${param}` : ""}\n   ${preview}`;
            })
            .join("\n\n");
        },
      }),
      status: command({
        description: "Current session status, cumulative token usage, and lifecycle state",
        parameters: z.object({}),
        execute: async () => {
          const session = sessionRef.current;
          if (!session) return "No active session.";
          const agentStatus = await session.status();
          const usage = session.usage();
          const lc = session.lifecycleState();
          return [
            `Status:    ${agentStatus}`,
            `Tokens:    ${usage.inputTokens.toLocaleString()} in / ${usage.outputTokens.toLocaleString()} out`,
            `Lifecycle: ${lc.current}`,
          ].join("\n");
        },
      }),
      log: command({
        description:
          "Show the structured session event log (turn, llm.call, tool, compact, approval events)",
        parameters: z.object({
          limit: z
            .number()
            .int()
            .min(1)
            .max(200)
            .default(50)
            .describe("Max events to return (most recent first)"),
          type: z
            .string()
            .optional()
            .describe("Filter by event type prefix, e.g. 'tool' or 'turn'"),
          turn: z.number().int().optional().describe("Filter to a specific turn number"),
        }),
        execute: async ({ limit, type: typeFilter, turn: turnFilter }) => {
          const session = sessionRef.current;
          const events = session ? session.eventLog() : (logger?.events() ?? []);
          if (events.length === 0) return "No session events recorded yet.";
          let filtered = events;
          if (typeFilter) filtered = filtered.filter((e) => e.type.startsWith(typeFilter));
          if (turnFilter !== undefined) {
            filtered = filtered.filter(
              (e) => "turn" in e && (e as { turn: number }).turn === turnFilter,
            );
          }
          if (filtered.length === 0) return "No events match the filter.";
          const shown = filtered.slice(-limit).reverse();
          return shown
            .map((e) => {
              const t = new Date(e.ts).toLocaleTimeString();
              const rest = { ...e } as Record<string, unknown>;
              delete rest.type;
              delete rest.ts;
              const detail = Object.entries(rest)
                .map(([k, v]) => {
                  if (k === "task" || k === "answer") {
                    const s = String(v);
                    return `${k}=${s.length > 60 ? `${s.slice(0, 57)}…` : s}`;
                  }
                  if (k === "input" && typeof v === "object" && v !== null) {
                    return `input=${abbrevInput(v as Record<string, unknown>) || JSON.stringify(v).slice(0, 40)}`;
                  }
                  return `${k}=${JSON.stringify(v)}`;
                })
                .join("  ");
              return `[${t}] ${e.type}  ${detail}`;
            })
            .join("\n");
        },
      }),
    },
  });
}

export async function createToolRegistry(deps: ToolDeps): Promise<ToolSetup> {
  const {
    workspace,
    memoryStore,
    secrets,
    apiKey,
    ask,
    runner,
    usage,
    sessionRef,
    sessionIdRef,
    autoApproveRef,
    printAbove,
    logger,
  } = deps;

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

  /** Resolve a secret: CompositeSecretStore checks env first, then in-memory. Prompt once if absent. */
  async function getSecret(envKey: string, toolName: string): Promise<string> {
    let value = await secrets.get(envKey);
    if (!value) {
      value = await serialAsk(`[${toolName}] Enter ${envKey}`);
      await secrets.set(envKey, value);
    }
    return value;
  }

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

  // biome-ignore lint/suspicious/noExplicitAny: tool registry uses runtime Zod validation
  const staticTools: Record<string, AgentTool<any, any>> = {
    calculator: tool({
      name: "calculator",
      description: "Evaluate a JS math expression and return the result.",
      parameters: z.object({ expression: z.string() }),
      execute: async ({ expression }) => {
        try {
          // biome-ignore lint/security/noEval: example only
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

    requireSecret: createRequireSecretTool({
      readSecret: (prompt) => ask(`[secret] ${prompt}`),
      store: (key, value) => secrets.set(key, value),
    }),

    ...createFilesystemTools({ rootDir: workspace }),

    shell: createShellTool({
      cwd: workspace,
      allowedCommands: ["bun", "git", "ls", "cat", "find", "grep", "npm", "npx"],
    }),

    memory: createMemoryTool(memoryStore),

    sessionDebug: createSessionDebugTool(sessionRef, sessionIdRef, runner, logger),

    chatGPT: chatGptOneShotTool,

    ...createSchedulerTools({
      scheduler,
      onTick: (task, tick) => {
        if (!scheduler.list().find((s) => s.id === tick.scheduleId)) return;
        if (activeTicks.has(tick.scheduleId)) return;
        activeTicks.add(tick.scheduleId);
        const run = async () => {
          try {
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
    memory: createMemoryTool(memoryStore),
    chatGPT: chatGptOneShotTool,
  };

  const subagentSystemPrompt = `You are a focused sub-agent. Be concise and task-focused.\nWorkspace: ${workspace}`;

  const makeOnStep =
    (prefix: string) =>
    ({
      tool,
      param,
      durationMs,
      failed,
    }: {
      tool: string;
      param: string;
      durationMs: number;
      failed: boolean;
    }) => {
      const elapsed = durationMs >= 1000 ? `${(durationMs / 1000).toFixed(1)}s` : `${durationMs}ms`;
      const hint = param ? `  \x1b[2m${param}\x1b[0m` : "";
      if (failed) {
        printAbove(`\x1b[31m✗ ${prefix} ${tool}${hint}  failed  (${elapsed})\x1b[0m`);
      } else {
        printAbove(`\x1b[2m✓ ${prefix} ${tool}${hint}  (${elapsed})\x1b[0m`);
      }
    };

  staticTools.claudeAgent = createAgentTool({
    runner,
    llm: usage.withTracking(anthropic("claude-sonnet-4-6", { apiKey }), "claude-sonnet-4-6"),
    name: "claudeAgent",
    description:
      "Delegate a task to a parallel Claude (Sonnet) sub-agent with filesystem, shell, memory, and chatGPT access. Use to parallelise independent subtasks or run deep research alongside the main thread.",
    tools: subagentTools,
    systemPrompt: subagentSystemPrompt,
    ask,
    autoApproveRef,
    onStep: makeOnStep("[claudeAgent]"),
    onSubagentStart: (p) => logger?.emit({ type: "subagent.start", name: "claudeAgent", ...p }),
    onSubagentEnd: (p) => logger?.emit({ type: "subagent.end", name: "claudeAgent", ...p }),
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

  staticTools.gptAgent = createAgentTool({
    runner,
    llm: usage.withTracking(lazyOpenAI, "gpt-4o"),
    name: "gptAgent",
    description:
      "Delegate a task to a parallel GPT-4o sub-agent with filesystem, shell, memory, and chatGPT access. Use for a second opinion, different reasoning style, or to parallelise work.",
    tools: subagentTools,
    systemPrompt: subagentSystemPrompt,
    ask,
    autoApproveRef,
    onStep: makeOnStep("[gptAgent]"),
    onSubagentStart: (p) => logger?.emit({ type: "subagent.start", name: "gptAgent", ...p }),
    onSubagentEnd: (p) => logger?.emit({ type: "subagent.end", name: "gptAgent", ...p }),
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
