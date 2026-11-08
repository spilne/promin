// ---------------------------------------------------------------------------
// agentTarget — run any `Agent` as an EvalTarget.
//
// The agent is materialized per case through `agent()` (so per-run state,
// such as a scripted LLM's position, never leaks between cases), invoked
// once with the case mapped to an `AgentInput`, and its run mapped through
// `toEvalOutput` so the trace and token / cost metrics ride along. The
// runner's signal (abort, case timeout) is passed to the invocation.
// ---------------------------------------------------------------------------

import type { Agent, AgentInput, AgentInvokeOpts, Message, ModelCostRates } from "@promin/agent";
import { SystemWallClock, type WallClock } from "@promin/workflow";
import type { EvalCase, EvalTarget } from "../types.ts";
import { toEvalOutput } from "./to-eval-output.ts";

export interface AgentTargetConfig {
  readonly id: string;
  readonly version?: string;
  /** Materialize the agent under test; called once per case. */
  readonly agent: () => Agent | Promise<Agent>;
  /** Map a case into the agent's input. Default: `{ task: <stringified input> }`. */
  readonly toInput?: (evalCase: EvalCase) => AgentInput;
  /** Scope and metadata for every invocation (the signal comes from the runner). */
  readonly invoke?: Omit<AgentInvokeOpts, "signal">;
  /** Per-model cost rates — when supplied, runs report `metrics.costUsd`. */
  readonly rates?: ModelCostRates;
  /** Time source for `latencyMs`. Default `SystemWallClock`. */
  readonly clock?: WallClock;
}

/** Build an `EvalTarget` that invokes an agent once per case. */
export function agentTarget(config: AgentTargetConfig): EvalTarget {
  const clock = config.clock ?? SystemWallClock;
  const toInput = config.toInput ?? defaultAgentInput;
  return {
    id: config.id,
    ...(config.version !== undefined && { version: config.version }),
    async run(evalCase, opts) {
      const startedAt = clock.currentTimeMs();
      try {
        const agent = await config.agent();
        const run = await agent.invoke(toInput(evalCase), {
          ...config.invoke,
          ...(opts?.signal !== undefined && { signal: opts.signal }),
        });
        return await toEvalOutput({
          run,
          latencyMs: clock.currentTimeMs() - startedAt,
          ...(config.rates !== undefined && { rates: config.rates }),
        });
      } catch (err) {
        return {
          output: undefined,
          metrics: { latencyMs: clock.currentTimeMs() - startedAt },
          error: err instanceof Error ? err.message : String(err),
        };
      }
    },
  };
}

/**
 * The default case → agent input mapping: the input as the task text. A
 * case whose `metadata.messages` is a non-empty array replays that
 * conversation before the task (the earlier turns of a thread a case was
 * sampled from).
 */
export function defaultAgentInput(evalCase: EvalCase): AgentInput {
  const { input } = evalCase;
  const history = evalCase.metadata?.messages;
  return {
    task: typeof input === "string" ? input : JSON.stringify(input),
    ...(Array.isArray(history) && history.length > 0 && { messages: history as Message[] }),
  };
}
