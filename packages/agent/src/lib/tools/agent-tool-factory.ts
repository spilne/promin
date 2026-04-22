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
  /** System prompt injected into the sub-agent session. */
  systemPrompt?: string;
  /** Tools the sub-agent is allowed to call. */
  // biome-ignore lint/suspicious/noExplicitAny: tool inputs validated at runtime via Zod
  tools?: Record<string, AgentTool<any, any>>;
  /** Maximum think/tool steps per invocation. Default: 20. */
  maxSteps?: number;
}

/**
 * Creates a tool that delegates tasks to a sub-agent powered by any LLMProvider.
 *
 * The sub-agent runs a full `agentAction` loop: it can call tools, reason over
 * results, and iterate until it reaches a final answer — just like the main agent.
 * Unlike `createLlmTool` (single one-shot call, no tools), this supports multi-step
 * reasoning with a full tool set.
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
export function createAgentTool(
  config: AgentToolFactoryConfig,
  // biome-ignore lint/suspicious/noExplicitAny: tool inputs validated at runtime via Zod
): AgentTool<{ task: string }, string> {
  const name = config.name ?? "agent";

  const agentWorkflow = agentAction({
    name: `${name}-action`,
    llm: config.llm,
    tools: config.tools ?? {},
    systemPrompt: config.systemPrompt,
    maxSteps: config.maxSteps ?? 20,
  });

  return tool({
    name,
    description:
      config.description ??
      "Delegate a task to a sub-agent. The agent has access to tools and can reason over multiple steps to complete the task.",
    parameters: z.object({
      task: z.string().describe("The task or question to send to the sub-agent."),
    }),
    execute: async ({ task }) => {
      const workflowId = `${name}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
      const result = await config.runner.run({
        workflow: agentWorkflow,
        workflowId,
        input: { task },
      });
      return (result as { answer: string }).answer;
    },
  });
}
