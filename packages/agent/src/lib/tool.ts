import type { z } from "zod";

export interface AgentTool<TInput = unknown, TOutput = unknown> {
  description: string;
  parameters: z.ZodType<TInput>;
  execute: (input: TInput) => Promise<TOutput>;
  requireApproval?: boolean;
  toModelOutput?: (output: TOutput) => string;
}

export function tool<TInput, TOutput>(
  config: AgentTool<TInput, TOutput>,
): AgentTool<TInput, TOutput> {
  return config;
}

export interface ApprovalDecision {
  approved: boolean;
  reason?: string;
}
