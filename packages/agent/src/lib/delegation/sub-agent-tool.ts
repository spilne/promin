// ---------------------------------------------------------------------------
// subAgentTool — the one way an agent delegates to another agent.
//
// Each call runs as a journaled child workflow of the calling turn (see
// `run-delegated-call.ts`). Two shapes:
//
//   - a fixed sub-agent: `agent` is an engine config (`agentAction` config)
//     or an `Agent`; the model passes `{ task }`.
//   - a routed sub-agent: `route` picks the callee per call from the
//     model's input and `load` builds its turn (the agents network's
//     `callAgent` is one).
// ---------------------------------------------------------------------------

import { workflow, type Workflow } from "@promin/workflow";
import { z } from "zod";
import {
  agentAction,
  type AgentActionConfig,
  type AgentFinishReason,
  type AgentInput,
  type AgentResult,
} from "../agent-action.ts";
import type { Agent, FinishReason } from "../agent/types.ts";
import { SessionEventBus } from "../session-logger.ts";
import type { ToolCall } from "../message.ts";
import type { AgentTool, ApprovalDecision } from "../tool.ts";
import type { ChildTurnWiring } from "./child-wiring.ts";
import {
  DEFAULT_MAX_DEPTH,
  isDelegableAgent,
  type SubAgentDelegation,
  type SubAgentRouteResult,
} from "./types.ts";

interface SubAgentToolCommon {
  /** Tool name the model calls. */
  readonly name: string;
  /** What the sub-agent does; the model reads it to decide when to delegate. */
  readonly description: string;
  readonly usage?: string;
  readonly requireApproval?: boolean;
  /**
   * Time limit for one sub-agent run, ms. On expiry the sub-agent's turn is
   * stopped and the call fails. Unset: the calling agent's `toolTimeoutMs`.
   */
  readonly timeoutMs?: number;
  /** Deepest delegation chain a call may start. Default `DEFAULT_MAX_DEPTH`. */
  readonly maxDepth?: number;
}

/** A sub-agent tool bound to one sub-agent; the model passes `{ task }`. */
export interface FixedSubAgentToolConfig extends SubAgentToolCommon {
  /**
   * The sub-agent: an `agentAction` config (a fresh turn workflow is built
   * per call, stopped by the call's signal) or an `Agent` (`LocalAgent`
   * runs its turn as the child; any other agent is invoked inside it).
   */
  readonly agent: Omit<AgentActionConfig, "signal"> | Agent;
}

/** A sub-agent tool that picks its callee per call. */
export interface RoutedSubAgentToolConfig<TInput> extends SubAgentToolCommon {
  readonly parameters: z.ZodType<TInput>;
  /** See `SubAgentDelegation.route`; `input` is validated against `parameters`. */
  readonly route: (
    params: Omit<Parameters<SubAgentDelegation["route"]>[0], "input"> & { readonly input: TInput },
  ) => Promise<SubAgentRouteResult>;
  /** See `SubAgentDelegation.load`. */
  readonly load: SubAgentDelegation["load"];
}

const TASK_PARAMETERS = z.object({
  task: z.string().min(1).describe("The task or question to hand to the sub-agent."),
});

/**
 * Build a tool that delegates to a sub-agent. Each call runs as a child
 * workflow of the calling turn with id `<caller workflow id>.<step>.<toolCallId>`,
 * so a crash or replay of the caller re-attaches to the same child run —
 * the tool is idempotent on that basis. The child's answer is the tool
 * result; a failed, refused or cancelled child is a tool error. Depth and
 * the call chain travel in the child's input: a call deeper than
 * `maxDepth`, or to an agent already waiting on the chain, is refused.
 *
 * Sub-agent tools run inside the durable engines (`agentAction`,
 * `agentLoop`, `LocalAgent`); calling `execute` directly throws.
 *
 * @example
 * ```ts
 * const researcher = subAgentTool({
 *   name: "researcher",
 *   description: "Research a question with web search and return a short report.",
 *   agent: { name: "researcher", llm, tools: { search }, systemPrompt: "You research." },
 *   timeoutMs: 120_000,
 * });
 * const lead = agentAction({ name: "lead", llm, tools: { researcher } });
 * ```
 */
export function subAgentTool(config: FixedSubAgentToolConfig): AgentTool<{ task: string }, string>;
export function subAgentTool<TInput>(
  config: RoutedSubAgentToolConfig<TInput>,
): AgentTool<TInput, string>;
export function subAgentTool<TInput>(
  config: FixedSubAgentToolConfig | RoutedSubAgentToolConfig<TInput>,
): AgentTool<TInput, string> | AgentTool<{ task: string }, string> {
  const maxDepth = config.maxDepth ?? DEFAULT_MAX_DEPTH;
  const common = {
    name: config.name,
    description: config.description,
    ...(config.usage !== undefined && { usage: config.usage }),
    ...(config.requireApproval !== undefined && { requireApproval: config.requireApproval }),
    ...(config.timeoutMs !== undefined && { timeoutMs: config.timeoutMs }),
    idempotent: true,
    execute: async (): Promise<string> => {
      throw new Error(
        `subAgentTool "${config.name}" runs only inside a durable agent turn ` +
          "(agentAction, agentLoop or LocalAgent)",
      );
    },
  };
  if ("agent" in config) {
    return {
      ...common,
      parameters: TASK_PARAMETERS,
      delegation: fixedDelegation({ agent: config.agent, name: config.name, maxDepth }),
    };
  }
  const { route, load } = config;
  return {
    ...common,
    parameters: config.parameters,
    delegation: {
      maxDepth,
      route: (params) => route({ ...params, input: params.input as TInput }),
      load,
    },
  };
}

function fixedDelegation(params: {
  readonly agent: FixedSubAgentToolConfig["agent"];
  /** The tool's name: the agent id of a sub-agent that has none of its own. */
  readonly name: string;
  readonly maxDepth: number;
}): SubAgentDelegation {
  const { agent, name, maxDepth } = params;
  if (!isAgent(agent)) {
    return {
      maxDepth,
      route: async ({ input }) => ({
        ok: true,
        route: { agentId: agent.name, input: { task: (input as { task: string }).task } },
      }),
      load: async ({ signal, scope, wiring }) => {
        // The sub-agent acts for the same caller; its own name is its agent id.
        const subScope = agent.scope ?? (scope && { ...scope, agentId: agent.name });
        const bus = bothBuses({ own: agent.bus, wired: wiring.bus });
        const engine: AgentActionConfig = {
          ...agent,
          signal,
          ...(subScope && { scope: subScope }),
          ...(bus && { bus }),
          ...(wiring.approvalIndex && { approvalIndex: wiring.approvalIndex }),
        };
        // The overloads split on `outputSchema`; this config may carry one.
        return (agentAction as (cfg: AgentActionConfig) => Workflow<AgentInput, AgentResult>)(
          engine,
        );
      },
    };
  }
  return {
    maxDepth,
    route: async ({ input, frame }) => {
      const task = (input as { task: string }).task;
      const callerAgentId = frame.chain[frame.chain.length - 1];
      const runInput = {
        task,
        ...(callerAgentId !== undefined && { source: { kind: "agent" as const, callerAgentId } }),
      };
      if (isDelegableAgent(agent)) {
        const prepared = await agent.prepareTurn(runInput);
        return { ok: true, route: { agentId: prepared.agentId, input: prepared.input } };
      }
      return { ok: true, route: { agentId: name, input: { task } } };
    },
    load: async ({ route, signal, wiring }) =>
      agentTurnWorkflow({ agent, agentId: route.agentId, signal, wiring }),
  };
}

/** A bus emitting to both `own` and `wired`, or whichever is set. */
function bothBuses(params: {
  readonly own: SessionEventBus | undefined;
  readonly wired: SessionEventBus | undefined;
}): SessionEventBus | undefined {
  const { own, wired } = params;
  if (!own || !wired) return own ?? wired;
  const bus = new SessionEventBus();
  bus.subscribe((event) => {
    own.emit(event);
    wired.emit(event);
  });
  return bus;
}

function isAgent(value: Omit<AgentActionConfig, "signal"> | Agent): value is Agent {
  return typeof (value as Partial<Agent>).invoke === "function";
}

/**
 * The turn workflow a delegated call to `agent` runs as its child: the
 * agent's own turn when it is a `DelegableAgent` (`LocalAgent`), otherwise
 * one activity that invokes it. That activity is not idempotent: a crash
 * while the agent runs fails the child (its outcome is unknown) rather
 * than invoking the agent twice.
 */
export function agentTurnWorkflow(params: {
  readonly agent: Agent;
  /** Names the wrapper workflow of an agent that cannot run as a child. */
  readonly agentId: string;
  readonly signal: AbortSignal;
  /** Where the child turn's approvals and events go (a `DelegableAgent` only). */
  readonly wiring?: ChildTurnWiring;
  /** Decides the child turn's approvals instead of suspending (a `DelegableAgent` only). */
  readonly onApprovalRequired?: (call: ToolCall) => Promise<ApprovalDecision>;
}): Workflow<AgentInput, AgentResult> {
  const { agent, agentId, signal, wiring } = params;
  if (isDelegableAgent(agent)) {
    return agent.turnWorkflow({
      signal,
      ...(wiring?.bus && { bus: wiring.bus }),
      ...(wiring?.approvalIndex && { approvalIndex: wiring.approvalIndex }),
      ...(params.onApprovalRequired && { onApprovalRequired: params.onApprovalRequired }),
    });
  }
  return workflow<AgentInput>({ name: `invoke-${agentId}` })
    .journaled("invoke", function* (ctx, input) {
      return yield* ctx.activity("invoke", async (): Promise<AgentResult> => {
        const out = await agent.invoke(
          { task: input.task, ...(input.messages !== undefined && { messages: input.messages }) },
          { signal },
        );
        const [answer, messages, finishReason, usage, steps] = await Promise.all([
          out.text,
          out.messages,
          out.finishReason,
          out.usage,
          out.steps,
        ]);
        return {
          answer,
          messages,
          turnMessages: [],
          steps: steps.filter((s) => s.type === "llm").length,
          finishReason: engineFinishReason(finishReason),
          usage: {
            inputTokens: usage.inputTokens,
            outputTokens: usage.outputTokens,
            ...(usage.cacheReadTokens !== undefined && { cacheReadTokens: usage.cacheReadTokens }),
            ...(usage.cacheWriteTokens !== undefined && {
              cacheWriteTokens: usage.cacheWriteTokens,
            }),
          },
        };
      });
    })
    .build() as Workflow<AgentInput, AgentResult>;
}

function engineFinishReason(reason: FinishReason): AgentFinishReason {
  switch (reason) {
    case "cancelled":
    case "max_steps":
    case "stalled":
      return reason;
    default:
      return "stop";
  }
}
