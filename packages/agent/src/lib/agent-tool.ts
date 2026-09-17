import { randomUUID } from "node:crypto";
import { z } from "zod";
import type { Workflow, WorkflowRunner } from "@promin/workflow";
import type { AgentInput, AgentResult } from "./agent-action.ts";
import type { AgentTool } from "./tool.ts";

export interface AgentToolConfig {
  agent: Workflow<AgentInput, AgentResult>;
  runner: WorkflowRunner;
  name: string;
  description: string;
  workflowIdPrefix?: string;
  requireApproval?: boolean;
}

export function agentTool(config: AgentToolConfig): AgentTool<{ task: string }, string> {
  return {
    name: config.name,
    description: config.description,
    requireApproval: config.requireApproval ?? false,
    parameters: z.object({ task: z.string() }),
    execute: async ({ task }) => {
      const prefix = config.workflowIdPrefix ?? config.name;
      const workflowId = `${prefix}-${randomUUID()}`;
      const result = (await config.runner.run({
        workflow: config.agent,
        workflowId,
        input: { task },
      })) as AgentResult;
      return result.answer;
    },
  };
}
