import { z } from "zod";
import { tool } from "./tool.ts";
import type { WorkflowRunner, Workflow } from "@promin/workflow";
import type { AgentInput, AgentResult } from "./agent-action.ts";

// ---- types ----

export interface AgentSpec {
  /** The workflow to run — typically the result of agentAction({ ... }). */
  workflow: Workflow<AgentInput, AgentResult>;
  /**
   * What this specialist can do — shown to the orchestrator as the tool description.
   * Be specific: "Searches the web and summarizes findings" beats "research agent".
   */
  description: string;
}

export interface AgentNetworkConfig {
  runner: WorkflowRunner;
  /**
   * Map of specialist name → spec. Each entry becomes a handoff tool.
   * Names must be valid tool names (alphanumeric + underscores).
   */
  agents: Record<string, AgentSpec>;
}

export interface AgentNetwork {
  /**
   * Returns one tool per registered specialist.
   * Pass the result directly into agentLoop or agentAction tools config:
   *
   *   agentLoop({
   *     tools: {
   *       ...network.handoffTools(),
   *       ...otherTools,
   *     },
   *   });
   *
   * The orchestrator calls each tool with { task: "..." } and receives
   * the specialist's final answer as a string.
   */
  handoffTools(): Record<string, ReturnType<typeof tool>>;
}

// ---- core ----

/**
 * Creates a network of specialist agents that an orchestrator can delegate to.
 *
 * Each specialist is an agentAction workflow — it has its own LLM, system prompt,
 * and tool set. The orchestrator sees each specialist as a regular tool call.
 * Under the hood, delegating to a specialist starts a new durable workflow run:
 * crash-safe, journaled, and observable via /steps.
 *
 * Usage:
 *
 *   const network = agentNetwork({
 *     runner,
 *     agents: {
 *       researcher: {
 *         workflow: agentAction({ llm: claude, tools: { search, readUrl }, systemPrompt: "..." }),
 *         description: "Searches the web and returns a researched summary.",
 *       },
 *       coder: {
 *         workflow: agentAction({ llm: claude, tools: { shell, writeFile }, systemPrompt: "..." }),
 *         description: "Writes and runs code. Returns the output or created file path.",
 *       },
 *     },
 *   });
 *
 *   const orchestrator = agentLoop({
 *     llm: claude,
 *     tools: { ...network.handoffTools() },
 *     systemPrompt: "You are an orchestrator. Delegate subtasks to specialist agents.",
 *   });
 */
export function agentNetwork(config: AgentNetworkConfig): AgentNetwork {
  return {
    handoffTools() {
      return Object.fromEntries(
        Object.entries(config.agents).map(([name, spec]) => [
          name,
          tool({
            name,
            description: spec.description,
            parameters: z.object({
              task: z.string().describe("The task to delegate to this specialist."),
            }),
            execute: async ({ task }) => {
              const workflowId = `${name}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
              const handle = await config.runner.start({
                workflow: spec.workflow,
                workflowId,
                input: { task },
              });
              const result = (await handle.result()) as AgentResult;
              return result.answer;
            },
          }),
        ]),
      ) as Record<string, ReturnType<typeof tool>>;
    },
  };
}
