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
import { InMemoryScheduler } from "@promin/workflow";
import type { WorkflowRunner } from "@promin/workflow";
import type { MemoryStore } from "../lib/memory-store.ts";
import type { ToolRegistry } from "../lib/tool-registry.ts";
import type { AgentTool } from "../lib/tool.ts";
import type { LLMProvider } from "../lib/llm-provider.ts";
import type { UsageTracker } from "./console-usage.ts";
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
  sessionRef: { current: { send: (task: string) => Promise<string> } | undefined };
}

export interface ToolSetup {
  registry: ToolRegistry;
  scheduler: InMemoryScheduler;
  activeTicks: Set<string>;
}

function createMemoryTool(store: MemoryStore) {
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
          const entries = await store.search(query, limit);
          if (entries.length === 0) return "No memories found matching that query.";
          return entries.map((e, i) => `${i + 1}. [${e.id.slice(0, 8)}] ${e.content}`).join("\n");
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
    },
  });
}

export async function createToolRegistry(deps: ToolDeps): Promise<ToolSetup> {
  const { workspace, memoryStore, secrets, apiKey, ask, runner, usage, sessionRef } = deps;

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

  /** Resolve a secret: CompositeSecretStore checks env first, then in-memory. Prompt once if absent. */
  async function getSecret(envKey: string, toolName: string): Promise<string> {
    let value = await secrets.get(envKey);
    if (!value) {
      value = await ask(`[${toolName}] Enter ${envKey}`);
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

  staticTools.claudeAgent = createAgentTool({
    runner,
    llm: usage.withTracking(anthropic("claude-sonnet-4-6", { apiKey }), "claude-sonnet-4-6"),
    name: "claudeAgent",
    description:
      "Delegate a task to a parallel Claude (Sonnet) sub-agent with filesystem, shell, memory, and chatGPT access. Use to parallelise independent subtasks or run deep research alongside the main thread.",
    tools: subagentTools,
    systemPrompt: subagentSystemPrompt,
    onRequiresApproval: async (call) => {
      const answer = await ask(`[claudeAgent] Approve tool "${call.name}"? [y/N]`);
      return { approved: answer.toLowerCase().startsWith("y") };
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

  staticTools.gptAgent = createAgentTool({
    runner,
    llm: usage.withTracking(lazyOpenAI, "gpt-4o"),
    name: "gptAgent",
    description:
      "Delegate a task to a parallel GPT-4o sub-agent with filesystem, shell, memory, and chatGPT access. Use for a second opinion, different reasoning style, or to parallelise work.",
    tools: subagentTools,
    systemPrompt: subagentSystemPrompt,
    onRequiresApproval: async (call) => {
      const answer = await ask(`[gptAgent] Approve tool "${call.name}"? [y/N]`);
      return { approved: answer.toLowerCase().startsWith("y") };
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
