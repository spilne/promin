import { z } from "zod";
import { tool } from "../tool.ts";
import type { LLMProvider } from "../llm-provider.ts";

export interface LlmToolConfig {
  /** The LLM provider that handles the delegated call. */
  llm: LLMProvider;
  /** Tool name visible to the main LLM. Default: "llm". */
  name?: string;
  /** Description the main LLM reads to decide when to call this tool. */
  description?: string;
  /** System prompt injected before the user prompt in the sub-call. */
  systemPrompt?: string;
  temperature?: number;
  maxTokens?: number;
}

function buildTool(config: LlmToolConfig) {
  return tool({
    name: config.name ?? "llm",
    description:
      config.description ??
      "Send a message to another LLM and get its response. Use when you need a second opinion, a different perspective, or want to delegate a subtask to a specialised model.",
    parameters: z.object({
      prompt: z.string().describe("The message to send"),
    }),
    execute: async ({ prompt }) => {
      const messages = [
        ...(config.systemPrompt ? [{ role: "system" as const, content: config.systemPrompt }] : []),
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

/**
 * Creates a tool that lets the main LLM talk to another LLM provider.
 *
 * Two call forms:
 *
 *   // Simple — just pass a provider; name defaults to "llm"
 *   createLlmTool(ollama({ model: "llama3.2" }))
 *
 *   // With options — override name, description, system prompt, etc.
 *   createLlmTool(openai("o3"), { name: "deepReason", description: "Hard math and logic." })
 *
 * The main LLM writes the prompt; the sub-LLM responds in one turn.
 * For multi-step sub-tasks that need their own tools, use agentTool() instead.
 */
export function createLlmTool(
  llm: LLMProvider,
  options?: Omit<LlmToolConfig, "llm">,
): ReturnType<typeof buildTool>;
export function createLlmTool(config: LlmToolConfig): ReturnType<typeof buildTool>;
export function createLlmTool(
  llmOrConfig: LLMProvider | LlmToolConfig,
  options?: Omit<LlmToolConfig, "llm">,
): ReturnType<typeof buildTool> {
  const config: LlmToolConfig =
    "chat" in llmOrConfig
      ? { llm: llmOrConfig as LLMProvider, ...options }
      : (llmOrConfig as LlmToolConfig);
  return buildTool(config);
}
