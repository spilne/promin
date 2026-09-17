import { openai } from "../adapters/openai.ts";
import { createAgentTool } from "./agent-tool-factory.ts";
import type { AgentTool } from "../tool.ts";
import type { AgentToolFactoryConfig } from "./agent-tool-factory.ts";
import type { WorkflowRunner } from "@promin/workflow";

export interface ChatGptAgentToolConfig extends Omit<AgentToolFactoryConfig, "llm" | "runner"> {
  /** WorkflowRunner used to execute the agent action. */
  runner: WorkflowRunner;
  /** OpenAI API key. Defaults to process.env.OPENAI_API_KEY. */
  apiKey?: string;
  /** Model to use. Default: "gpt-4o". */
  model?: string;
}

/**
 * Convenience wrapper around `createAgentTool` pre-configured for GPT-4o.
 *
 * Saves you from importing and instantiating the openai() adapter manually.
 * For any other provider use `createAgentTool` directly.
 *
 * Usage:
 *
 *   const gpt = createChatGptAgentTool({
 *     runner,
 *     tools: { ...createFilesystemTools({ rootDir: process.cwd() }) },
 *     systemPrompt: "You are a helpful coding assistant.",
 *   });
 *
 *   agentLoop({
 *     llm: anthropic("claude-opus-4-5"),
 *     tools: { chatgpt: gpt },
 *   });
 */
export function createChatGptAgentTool(
  config: ChatGptAgentToolConfig,
  // biome-ignore lint/suspicious/noExplicitAny: tool inputs validated at runtime via Zod
): AgentTool<{ task: string }, string> {
  const llm = openai(config.model ?? "gpt-4o", {
    apiKey: config.apiKey ?? process.env["OPENAI_API_KEY"],
  });

  return createAgentTool({
    ...config,
    llm,
    name: config.name ?? "chatgpt",
    description:
      config.description ??
      "Delegate a task to a GPT-4o agent. The agent has access to tools and can reason over multiple steps. Use for a second opinion, a different model's perspective, or to parallelise work.",
  });
}
