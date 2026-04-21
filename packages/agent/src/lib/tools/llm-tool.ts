import { z } from "zod";
import { tool } from "../tool.ts";
import type { LLMProvider } from "../llm-provider.ts";

export interface LlmToolConfig {
  /**
   * Tool name shown to the main LLM. Make it descriptive of the capability,
   * not just the model name — e.g. "cheapSummarize", "codeReview", "mathSolve".
   */
  name: string;
  /**
   * Description the main LLM reads to decide when to call this tool.
   * Include cost/quality trade-off hints so it can route intelligently.
   */
  description: string;
  /** The LLM provider that handles the delegated call. */
  llm: LLMProvider;
  /**
   * Optional system prompt injected before the delegated prompt.
   * Use to give the sub-LLM a specialised persona or instructions.
   */
  systemPrompt?: string;
  /** Passed through to the sub-LLM call. */
  temperature?: number;
  maxTokens?: number;
}

/**
 * Creates a tool that delegates a single-turn prompt to a separate LLM provider.
 *
 * The main LLM writes the full prompt and receives the sub-LLM's response as the
 * tool result — no tool use, no multi-step reasoning on the sub side. Use this for
 * cheap one-shot tasks (summarise, classify, translate) where you want the main LLM
 * to remain in control of the overall workflow.
 *
 * For multi-step sub-tasks that need their own tools, use agentTool() instead.
 *
 * Typical setup:
 *
 *   agentLoop({
 *     llm: anthropic("claude-sonnet-4-6"),
 *     tools: {
 *       cheapSummarize: createLlmTool({
 *         name: "cheapSummarize",
 *         description: "Summarise text using a fast, cheap model. Use for long passages where cost matters.",
 *         llm: anthropic("claude-haiku-4-5"),
 *       }),
 *       deepReason: createLlmTool({
 *         name: "deepReason",
 *         description: "Solve hard reasoning problems using a powerful model. Use when quality matters most.",
 *         llm: openai("o3"),
 *       }),
 *       privateProcess: createLlmTool({
 *         name: "privateProcess",
 *         description: "Process sensitive data locally without sending it to the cloud.",
 *         llm: ollama({ model: "llama3.2" }),
 *       }),
 *     },
 *   })
 */
export function createLlmTool(config: LlmToolConfig) {
  return tool({
    name: config.name,
    description: config.description,
    parameters: z.object({
      prompt: z.string().describe("The full prompt to send to this LLM"),
    }),
    execute: async ({ prompt }) => {
      const messages = [
        ...(config.systemPrompt
          ? [{ role: "system" as const, content: config.systemPrompt }]
          : []),
        { role: "user" as const, content: prompt },
      ];
      const response = await config.llm.chat({
        messages,
        ...(config.temperature !== undefined ? { temperature: config.temperature } : {}),
        ...(config.maxTokens !== undefined ? { maxTokens: config.maxTokens } : {}),
      });
      return response.content ?? "";
    },
  });
}
