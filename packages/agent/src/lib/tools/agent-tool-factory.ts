import { z } from "zod";
import { tool } from "../tool.ts";
import { agentAction } from "../agent-action.ts";
import type { AgentTool } from "../tool.ts";
import type { LLMProvider } from "../llm-provider.ts";
import type { WorkflowRunner } from "@promin/workflow";

export interface AgentToolFactoryConfig {
  /** WorkflowRunner used to execute the agent action. */
  runner: WorkflowRunner;
  /** LLM provider the sub-agent will use (openai, anthropic, ollama, etc.). */
  llm: LLMProvider;
  /** Tool name visible to the orchestrator. Default: "agent". */
  name?: string;
  /** Description the orchestrator reads to decide when to delegate. */
  description?: string;
  /**
   * System prompt injected into the sub-agent session.
   * A tools section listing available tool names and descriptions is always
   * appended automatically so the sub-agent knows what it can call.
   */
  systemPrompt?: string;
  /** Tools the sub-agent is allowed to call. */
  // biome-ignore lint/suspicious/noExplicitAny: tool inputs validated at runtime via Zod
  tools?: Record<string, AgentTool<any, any>>;
  /** Maximum think/tool steps per invocation. Default: 20. */
  maxSteps?: number;
  /**
   * Called when a sub-agent tool with `requireApproval: true` needs a decision.
   * Surface this to the user (e.g. via ask()) so approvals reach the terminal.
   * When omitted, the sub-agent hangs waiting for an external signal (likely timing out).
   *
   * If `ask` is provided instead, the factory builds a standard interactive handler
   * that shows the tool name + abbreviated params and supports [y/n/always].
   */
  // biome-ignore lint/suspicious/noExplicitAny: ToolCall input is validated by Zod at runtime
  onRequiresApproval?: (call: {
    id: string;
    name: string;
    input: any;
  }) => Promise<{ approved: boolean; reason?: string }>;
  /**
   * Interactive prompt function. When provided (and `onRequiresApproval` is not),
   * the factory wires up a standard `[name] Allow tool "X"  param? [y/n/always]`
   * approval handler automatically.
   */
  ask?: (question: string) => Promise<string>;
  /**
   * Shared auto-approve flag. When `ask` is used, reading this skips the prompt;
   * typing "always" sets it to true so subsequent approvals are automatic.
   */
  autoApproveRef?: { value: boolean };
  /**
   * Hard wall-clock timeout for the entire sub-agent run in milliseconds.
   * If the sub-agent (including any LLM calls) does not complete within this
   * window, the tool rejects with a timeout error so the parent session is not
   * blocked indefinitely. Default: 120 000 ms (2 min).
   */
  timeoutMs?: number;
}

/**
 * Builds a human-readable tools section from a tool map and appends it to a
 * base system prompt. This makes the sub-agent aware of what tools it has
 * available without relying solely on the LLM schema definitions.
 */
// biome-ignore lint/suspicious/noExplicitAny: tool inputs validated at runtime via Zod
function buildSystemPrompt(base: string, tools: Record<string, AgentTool<any, any>>): string {
  const entries = Object.values(tools);
  if (entries.length === 0) return base;
  const lines = entries.map((t) => `- ${t.name}: ${t.description}`);
  return `${base}\n\nYou have access to the following tools:\n${lines.join("\n")}\n\nAlways use tools when they are relevant to the task rather than relying on your own knowledge.`;
}

/**
 * Creates a tool that delegates tasks to a sub-agent powered by any LLMProvider.
 *
 * The sub-agent runs a full `agentAction` loop: it can call tools, reason over
 * results, and iterate until it reaches a final answer — just like the main agent.
 * Unlike `createLlmTool` (single one-shot call, no tools), this supports multi-step
 * reasoning with a full tool set.
 *
 * The sub-agent's system prompt is automatically enriched with a listing of all
 * available tools so it knows what it can call and when to use them.
 *
 * Works with any adapter: openai(), anthropic(), ollama(), llamacpp(), etc.
 *
 * Usage:
 *
 *   // GPT-4o sub-agent with filesystem access
 *   const gptAgent = createAgentTool({
 *     runner,
 *     llm: openai("gpt-4o"),
 *     name: "gpt_researcher",
 *     tools: { ...createFilesystemTools({ rootDir: process.cwd() }) },
 *     systemPrompt: "You are a helpful research assistant.",
 *   });
 *
 *   // Local Llama sub-agent for cheap summarisation
 *   const llamaAgent = createAgentTool({
 *     runner,
 *     llm: ollama("llama3.2"),
 *     name: "summariser",
 *     description: "Summarise long text cheaply using a local model.",
 *   });
 *
 *   agentLoop({
 *     llm: anthropic("claude-opus-4-5"),
 *     tools: { gpt_researcher: gptAgent, summariser: llamaAgent },
 *   });
 */
function buildApprovalHandler(
  name: string,
  ask: (q: string) => Promise<string>,
  autoApproveRef?: { value: boolean },
  // biome-ignore lint/suspicious/noExplicitAny: ToolCall input is validated by Zod at runtime
): (call: { id: string; name: string; input: any }) => Promise<{ approved: boolean }> {
  return async (call) => {
    if (autoApproveRef?.value) return { approved: true };
    // Grab the first string value from the input as a short hint.
    const raw = (call.input as Record<string, unknown>) ?? {};
    const paramStr = (() => {
      for (const v of Object.values(raw)) {
        if (typeof v === "string") return v.length > 42 ? `${v.slice(0, 39)}…` : v;
      }
      return "";
    })();
    const hint = paramStr ? `  \x1b[2m${paramStr}\x1b[0m` : "";
    const answer = await ask(`[${name}] Allow tool "${call.name}"${hint}? [y/n/always]`);
    if (answer.toLowerCase() === "always" && autoApproveRef) autoApproveRef.value = true;
    return { approved: answer.toLowerCase().startsWith("y") || answer.toLowerCase() === "always" };
  };
}

export function createAgentTool(
  config: AgentToolFactoryConfig,
  // biome-ignore lint/suspicious/noExplicitAny: tool inputs validated at runtime via Zod
): AgentTool<{ task: string }, string> {
  const name = config.name ?? "agent";
  const tools = config.tools ?? {};
  const basePrompt =
    config.systemPrompt ?? "You are a focused sub-agent. Be concise and task-focused.";
  const systemPrompt = buildSystemPrompt(basePrompt, tools);

  return tool({
    name,
    description:
      config.description ??
      "Delegate a task to a sub-agent. The agent has access to tools and can reason over multiple steps to complete the task.",
    parameters: z.object({
      task: z.string().describe("The task or question to send to the sub-agent."),
    }),
    execute: async ({ task }) => {
      const timeoutMs = config.timeoutMs ?? 120_000;
      const startMs = Date.now();
      let pausedMs = 0;
      let askStartMs: number | null = null;

      // Active elapsed excludes time spent waiting for user approval prompts.
      const activeMs = () => {
        const inAsk = askStartMs !== null ? Date.now() - askStartMs : 0;
        return Date.now() - startMs - pausedMs - inAsk;
      };

      // Build per-execution approval handler. When using ask-based approval, wrap
      // ask so the timeout clock stops while the user is reading/responding.
      const onApprovalRequired: typeof config.onRequiresApproval = (() => {
        if (config.onRequiresApproval) return config.onRequiresApproval;
        if (!config.ask) return undefined;
        const pausableAsk = async (q: string): Promise<string> => {
          askStartMs = Date.now();
          try {
            return await config.ask!(q);
          } finally {
            if (askStartMs !== null) {
              pausedMs += Date.now() - askStartMs;
              askStartMs = null;
            }
          }
        };
        return buildApprovalHandler(name, pausableAsk, config.autoApproveRef);
      })();

      const agentWorkflow = agentAction({
        name: `${name}-action`,
        llm: config.llm,
        tools,
        systemPrompt,
        maxSteps: config.maxSteps ?? 20,
        onApprovalRequired,
      });

      const workflowId = `${name}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
      const run = config.runner.run({ workflow: agentWorkflow, workflowId, input: { task } });

      // Poll-based timeout that ignores time spent in approval prompts.
      let tid: ReturnType<typeof setTimeout> | null = null;
      const timeout = new Promise<never>((_, reject) => {
        const check = () => {
          const remaining = timeoutMs - activeMs();
          if (remaining <= 0) {
            reject(new Error(`Sub-agent "${name}" timed out after ${timeoutMs / 1000}s`));
          } else {
            tid = setTimeout(check, Math.min(remaining, 500));
          }
        };
        check();
      });

      try {
        const result = await Promise.race([run, timeout]);
        return (result as { answer: string }).answer;
      } finally {
        if (tid !== null) clearTimeout(tid);
      }
    },
  });
}
