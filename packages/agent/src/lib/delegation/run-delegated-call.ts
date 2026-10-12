// ---------------------------------------------------------------------------
// runDelegatedCall — how an agent engine runs one call of a sub-agent tool.
//
// Journal layout (under the engine's own name for the call, `<name>`):
//
//   <name>-route   validate the input, check depth, route the call, check
//                  the chain for a cycle; records the route and start time
//   <name>         the child workflow (`ctx.child`), id `childWorkflowId`
//   <name>-end     emits the call's end (tool.end, metrics, callbacks)
//
// The child runs on the caller's runtime and is linked to it by
// `parentWorkflowId`. A replay of the caller reads the child's outcome from
// the journal; a caller that crashed while the child ran re-attaches to the
// same child id and resumes it from its own journal. The child is stopped
// (its turn ends as cancelled) when the caller's signal aborts or the call
// runs past its time limit; a failed child is a tool error for the caller.
//
// The time limit runs from the journaled start of the call, so it holds
// across crashes and resumes — time the child spends suspended (waiting
// for an approval) counts. The child's approvals and tool events are
// carried up to the root turn (`child-wiring.ts`).
// ---------------------------------------------------------------------------

import {
  ChildWorkflowGoneError,
  type ActivityYield,
  type JournaledContext,
  type WallClock,
  type Workflow,
} from "@promin/workflow";
import type { AgentResult, TurnApprovalIndex } from "../agent-action.ts";
import { formatToolError, isEngineControl, truncateToolContent } from "../agent-shared.ts";
import type { ToolCall, ToolResultMessage } from "../message.ts";
import type { SessionEventBus } from "../session-logger.ts";
import type { AgentTool, ToolScope } from "../tool.ts";
import { buildAgentTrace, CHILD_TRACE_META_KEY } from "../trace.ts";
import { childTurnWiring } from "./child-wiring.ts";
import {
  childFrame,
  DelegationCycleError,
  DelegationDepthError,
  type DelegationFrame,
  type SubAgentRoute,
} from "./types.ts";

/** Failures that are the child's outcome, not the engine's control flow. */
const CHILD_OUTCOME_TAGS: ReadonlySet<string> = new Set([
  "WorkflowFailedError",
  "WorkflowCancelledError",
  "WorkflowTripwireError",
  "WorkflowDeadlineError",
]);

/**
 * Whether an error thrown by `ctx.child` is the engine's control flow (the
 * child suspended, a lock was lost, the journal drifted) that must pass
 * through the caller untouched — as opposed to the child's own failure,
 * which the caller handles as the child's outcome.
 */
export function isChildControlFlow(err: unknown): boolean {
  const tag = (err as { _tag?: unknown } | null)?._tag;
  return isEngineControl(err) && !(typeof tag === "string" && CHILD_OUTCOME_TAGS.has(tag));
}

type RouteOutcome =
  | { readonly ok: false; readonly error: string; readonly startedAt: number }
  | {
      readonly ok: true;
      readonly route: SubAgentRoute;
      readonly frame: DelegationFrame;
      readonly startedAt: number;
    };

export interface RunDelegatedCallParams {
  readonly ctx: JournaledContext<any, any>;
  /** Journal name of the call; the route and end entries are suffixed. */
  readonly name: string;
  readonly call: ToolCall;
  /** The live tool set; the call's tool is looked up when it runs. */
  readonly tools: () => Readonly<Record<string, AgentTool<any, any>>>;
  /** The calling turn's own frame. */
  readonly frame: DelegationFrame;
  /** Agent id of the calling turn — appended to the chain. */
  readonly callerId: string;
  /** Child run id: `delegatedRunId` of the caller's workflow, step and tool call. */
  readonly childWorkflowId: string;
  readonly scope?: ToolScope;
  /** The calling turn's bus: the child's approval and tool events go to its root. */
  readonly bus?: SessionEventBus;
  /** The calling turn's approval index: the child's approvals are recorded at its root. */
  readonly approvalIndex?: TurnApprovalIndex;
  /** The calling turn's cancellation. */
  readonly signal?: AbortSignal;
  /** Time limit for the child run from the call's journaled start, ms; 0 / unset: none. */
  readonly timeoutMs?: number;
  readonly clock: WallClock;
  /** Runs inside the route activity, before anything else. */
  readonly onStart?: () => void;
  /** Runs inside the end activity with the call's result. */
  readonly onEnd?: (params: {
    readonly result: ToolResultMessage;
    readonly output: AgentResult | undefined;
    readonly durationMs: number;
  }) => void;
}

/** The child run id of a sub-agent call: caller workflow id, step and tool call id. */
export function delegatedRunId(params: {
  readonly workflowId: string;
  readonly step: number;
  readonly toolCallId: string;
}): string {
  return `${params.workflowId}.${params.step}.${params.toolCallId}`;
}

export function* runDelegatedCall(
  params: RunDelegatedCallParams,
): Generator<ActivityYield, ToolResultMessage, any> {
  const { ctx, name, call, clock } = params;
  const errorResult = (content: string): ToolResultMessage => ({
    role: "tool",
    toolCallId: call.id,
    content,
    isError: true,
  });

  const routed: RouteOutcome = yield* ctx.activity(
    `${name}-route`,
    async (): Promise<RouteOutcome> => {
      params.onStart?.();
      const startedAt = clock.currentTimeMs();
      const refuse = (error: string): RouteOutcome => ({ ok: false, error, startedAt });
      const tool = params.tools()[call.name];
      const delegation = tool?.delegation;
      if (!tool || !delegation) return refuse(`Error: tool "${call.name}" is no longer available`);
      if (call.inputError !== undefined) return refuse(`Invalid input: ${call.inputError}`);
      const parsed = tool.parameters.safeParse(call.input);
      if (!parsed.success) return refuse(`Invalid input: ${formatToolError(parsed.error)}`);
      const frame = childFrame({ frame: params.frame, callerId: params.callerId });
      if (frame.depth > delegation.maxDepth) {
        return refuse(
          new DelegationDepthError(delegation.maxDepth, frame.depth, frame.chain).message,
        );
      }
      // A thrown error (a store that is down) fails this activity rather
      // than being journaled as the model's answer; only refusals are.
      const answer = await delegation.route({
        input: parsed.data,
        frame,
        ...(params.scope && { scope: params.scope }),
      });
      if (!answer.ok) return refuse(answer.error);
      if (frame.chain.includes(answer.route.agentId)) {
        return refuse(new DelegationCycleError(answer.route.agentId, frame.chain).message);
      }
      return { ok: true, route: answer.route, frame, startedAt };
    },
    { idempotent: true },
  );

  function* finish(
    result: ToolResultMessage,
    output?: AgentResult,
  ): Generator<ActivityYield, ToolResultMessage, any> {
    yield* ctx.activity(
      `${name}-end`,
      async () => {
        params.onEnd?.({ result, output, durationMs: clock.currentTimeMs() - routed.startedAt });
        return null;
      },
      { idempotent: true },
    );
    return result;
  }

  if (!routed.ok) return yield* finish(errorResult(routed.error));
  const { route, frame } = routed;

  // The child stops when the caller's turn is cancelled or the call runs
  // out of time. Neither is journaled: they only steer the live child. The
  // deadline counts from the journaled start, so a re-attach after a crash
  // or a resume after an approval does not get a fresh limit.
  const controller = new AbortController();
  const parentSignal = params.signal;
  const onParentAbort = (): void => controller.abort(parentSignal?.reason);
  if (parentSignal?.aborted) controller.abort(parentSignal.reason);
  else parentSignal?.addEventListener("abort", onParentAbort, { once: true });
  const timeoutMs = params.timeoutMs ?? 0;
  const timedOut = (): void =>
    controller.abort(new Error(`sub-agent call timed out after ${timeoutMs}ms`));
  let timer: { clear(): void } | undefined;
  if (timeoutMs > 0) {
    const remaining = routed.startedAt + timeoutMs - clock.currentTimeMs();
    if (remaining <= 0) timedOut();
    else timer = clock.setTimeout(timedOut, remaining);
  }
  const wiring = childTurnWiring({
    childWorkflowId: params.childWorkflowId,
    ...(params.bus && { bus: params.bus }),
    ...(params.approvalIndex && { approvalIndex: params.approvalIndex }),
  });

  let output: AgentResult;
  try {
    output = yield* ctx.child<AgentResult>(
      {
        name,
        load: async () => {
          const delegation = params.tools()[call.name]?.delegation;
          if (!delegation) {
            throw new ChildWorkflowGoneError({
              message: `tool "${call.name}" is no longer available`,
            });
          }
          const child = await delegation.load({
            route,
            signal: controller.signal,
            ...(params.scope && { scope: params.scope }),
            wiring,
          });
          return child as unknown as Workflow<unknown, AgentResult>;
        },
      },
      {
        workflowId: params.childWorkflowId,
        input: { ...route.input, delegation: frame },
      },
    );
  } catch (err) {
    if (isChildControlFlow(err)) throw err;
    return yield* finish(
      errorResult(`Sub-agent "${route.agentId}" failed: ${formatToolError(err)}`),
    );
  } finally {
    timer?.clear();
    parentSignal?.removeEventListener("abort", onParentAbort);
  }

  if (output.finishReason === "cancelled") {
    const limit = timeoutMs > 0 ? ` or ran past its ${timeoutMs}ms time limit` : "";
    return yield* finish(
      errorResult(`Sub-agent "${route.agentId}" did not finish: the call was cancelled${limit}.`),
      output,
    );
  }
  const result: ToolResultMessage = {
    role: "tool",
    toolCallId: call.id,
    content: truncateToolContent(output.answer),
    // From the turn's own messages: `messages` is what the last think step
    // saw, compacted in a long turn.
    metadata: { [CHILD_TRACE_META_KEY]: buildAgentTrace(output.turnMessages) },
  };
  return yield* finish(result, output);
}
