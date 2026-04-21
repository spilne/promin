import type { Message } from "./message.ts";
import type { LLMResponse } from "./llm-provider.ts";

export interface ProcessorContext {
  /** Zero-based think-step index within the current turn. */
  step: number;
  /** Turn index (agentLoop only; always 0 for agentAction). */
  turn: number;
  workflowId: string;
}

export interface ProcessorsConfig {
  /**
   * Transform the message list immediately before each LLM call.
   * Return the modified list — or the same list to pass through unchanged.
   * Runs inside the think activity closure (not journaled separately).
   */
  beforeLLM?: (messages: Message[], ctx: ProcessorContext) => Promise<Message[]> | Message[];
  /**
   * Transform the LLM response before it is appended to the message history.
   * Return the modified response — or the same response to pass through unchanged.
   * Runs inside the think activity closure (not journaled separately).
   */
  afterLLM?: (response: LLMResponse, ctx: ProcessorContext) => Promise<LLMResponse> | LLMResponse;
}

/**
 * Compose multiple ProcessorsConfigs into one, running each in series.
 * beforeLLM processors run in order; afterLLM processors run in order.
 */
export function combineProcessors(...processors: ProcessorsConfig[]): ProcessorsConfig {
  return {
    beforeLLM: async (messages, ctx) => {
      let current = messages;
      for (const p of processors) {
        if (p.beforeLLM) current = await p.beforeLLM(current, ctx);
      }
      return current;
    },
    afterLLM: async (response, ctx) => {
      let current = response;
      for (const p of processors) {
        if (p.afterLLM) current = await p.afterLLM(current, ctx);
      }
      return current;
    },
  };
}
