import type { Message } from "./message.ts";
import type { LLMResponse } from "./llm-provider.ts";

export interface ProcessorContext {
  /** Zero-based think-step index within the current turn. */
  step: number;
  /** Turn index (agentLoop only; always 0 for agentAction). */
  turn: number;
  workflowId: string;
  /**
   * `true` when the wrapping workflow body is executing on top of
   * pre-existing journal entries (worker restart, signal-resume).
   *
   * Processors run inside the journaled `think-N` activity, so they
   * fire ONLY when that activity is fresh (journal hits short-circuit
   * the callback). When `isReplay` is `true` your processor is firing
   * fresh inside a body that has been started before — the previous
   * worker pass got further down the body but didn't reach this
   * activity yet. Use it to gate non-idempotent side effects in
   * processors that you want to skip on body restart.
   */
  isReplay: boolean;
}

export interface ProcessorsConfig {
  /**
   * Transform the message list immediately before each LLM call.
   * Return the modified list — or the same list to pass through unchanged.
   *
   * Runs inside the journaled `think-N` activity. On workflow replay the
   * activity result is read from the journal and `beforeLLM` is **not called
   * again**. Do not perform side effects here that must run on every execution
   * (use `hooks.beforeTurn` for that, which has its own journaled activity).
   */
  beforeLLM?: (messages: Message[], ctx: ProcessorContext) => Promise<Message[]> | Message[];
  /**
   * Transform the LLM response before it is appended to the message history.
   * Return the modified response — or the same response to pass through unchanged.
   *
   * Same replay caveat as `beforeLLM`: this runs inside the journaled
   * `think-N` activity and is skipped when the activity result is replayed
   * from the journal.
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
