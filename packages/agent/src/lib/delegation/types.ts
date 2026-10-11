// ---------------------------------------------------------------------------
// Durable delegation — one agent turn handing a task to another agent.
//
// A sub-agent call runs as a journaled child workflow of the calling turn
// (`ctx.child`), with a deterministic id derived from the caller's workflow
// id and the tool call id. A crash or replay of the caller re-attaches to
// the same child run instead of starting a second one; the child's own
// journal keeps its LLM steps from running twice.
//
// The call chain travels in the child's workflow input (`DelegationFrame`),
// never in process-local state, so depth and cycle checks hold when the
// child runs on another worker.
// ---------------------------------------------------------------------------

import type { Workflow } from "@promin/workflow";
import type { AgentInput, AgentResult, TurnApprovalIndex } from "../agent-action.ts";
import type { SessionEventBus } from "../session-logger.ts";
import type { ChildTurnWiring } from "./child-wiring.ts";
import type { AgentInput as AgentRunInput } from "../agent/types.ts";
import type { ToolCall } from "../message.ts";
import type { ApprovalDecision, ToolScope } from "../tool.ts";

/** Delegation depth a sub-agent tool allows when it sets none. */
export const DEFAULT_MAX_DEPTH = 3;

/**
 * Where a turn sits in a delegation chain. A turn nobody delegated to has
 * depth 0 and an empty chain; a child turn has its caller's depth + 1 and
 * the caller's chain plus the caller's agent id.
 */
export interface DelegationFrame {
  /** Delegation hops above this turn. */
  readonly depth: number;
  /** Agent ids waiting on this turn, oldest first; ends with the direct caller. */
  readonly chain: ReadonlyArray<string>;
}

/** The frame of a turn nobody delegated to. */
export const ROOT_FRAME: DelegationFrame = { depth: 0, chain: [] };

/**
 * Validate a frame read from a run's input: a non-negative integer depth
 * and a chain of exactly `depth` agent ids. Throws on anything else — the
 * input may come from any caller, and a forged frame must not lift the
 * depth limit or blank the chain.
 */
export function parseDelegationFrame(value: unknown): DelegationFrame {
  const frame = value as Partial<DelegationFrame> | null;
  const depth = frame?.depth;
  const chain = frame?.chain;
  if (
    typeof depth !== "number" ||
    !Number.isInteger(depth) ||
    depth < 0 ||
    !Array.isArray(chain) ||
    chain.length !== depth ||
    !chain.every((id) => typeof id === "string" && id.length > 0)
  ) {
    throw new Error(
      "invalid delegation frame in the run input: expected { depth, chain } with a " +
        "non-negative integer depth and a chain of that many agent ids",
    );
  }
  return { depth, chain: [...chain] };
}

/** The frame a child of `caller` (running in `frame`) runs in. */
export function childFrame(params: {
  readonly frame: DelegationFrame;
  readonly callerId: string;
}): DelegationFrame {
  return { depth: params.frame.depth + 1, chain: [...params.frame.chain, params.callerId] };
}

/**
 * One call resolved to the sub-agent turn it runs. Journaled with the
 * call, so it must be plain JSON.
 */
export interface SubAgentRoute {
  /** Id of the agent the call runs — checked against the call chain. */
  readonly agentId: string;
  /** The sub-agent turn's input; the engine adds the delegation frame. */
  readonly input: AgentInput;
  /**
   * Host data `load` needs to rebuild the same callee (the scope it runs
   * in, the recipe version it was routed to).
   */
  readonly data?: Readonly<Record<string, unknown>>;
}

/** What `route` answers: a sub-agent turn to run, or why the call is refused. */
export type SubAgentRouteResult =
  | { readonly ok: true; readonly route: SubAgentRoute }
  | { readonly ok: false; readonly error: string };

/**
 * The engine contract of a sub-agent tool (`AgentTool.delegation`). The
 * agent engines (`agentAction`, `agentLoop`) run a call of such a tool as
 * a child workflow instead of calling `execute`.
 */
export interface SubAgentDelegation {
  /** Deepest frame a call may start. */
  readonly maxDepth: number;
  /**
   * Decide one call: run checks, pick the callee, build its input. Runs
   * once per call in a journaled activity (again only after a crash
   * inside it), so it may resolve or create records idempotently.
   * `frame` is the frame the child would run in. Answer a refusal the
   * model should hear with `{ ok: false }`; a thrown error (a store that is
   * down) is not journaled as an answer — it fails the activity.
   */
  route(params: {
    readonly input: unknown;
    readonly frame: DelegationFrame;
    readonly scope?: ToolScope;
  }): Promise<SubAgentRouteResult>;
  /**
   * Build the callee's turn workflow for a routed call. Called whenever the
   * child has to run — the first time, and to re-attach to a child a
   * crashed caller left in flight — never when its outcome is journaled.
   * `signal` aborts when the caller's turn is cancelled or the call times
   * out; the returned workflow must stop its turn on it. Throw
   * `ChildWorkflowGoneError` when the callee is no longer the one `route`
   * chose (another recipe version or backend kind, disabled, gone): the
   * call fails and an in-flight child run is cancelled. Any other error is
   * taken as transient: the caller parks and loads the child again later.
   */
  load(params: {
    readonly route: SubAgentRoute;
    readonly signal: AbortSignal;
    readonly scope?: ToolScope;
    /** Wire the child turn to these: its approvals and events reach the root turn. */
    readonly wiring: ChildTurnWiring;
  }): Promise<Workflow<AgentInput, AgentResult>>;
}

/** A delegated turn, prepared: the callee's id and its turn input. */
export interface PreparedTurn {
  readonly agentId: string;
  readonly input: AgentInput;
}

/**
 * An agent whose one-shot turn can run as a child workflow of a
 * delegating turn. `LocalAgent` implements it; other agents (remote ones)
 * run through `invoke` inside the child instead (`agentTurnWorkflow`).
 */
export interface DelegableAgent {
  /** The turn for `input`: this agent's id, the framed task and retrieved context. */
  prepareTurn(input: AgentRunInput): Promise<PreparedTurn>;
  /**
   * This agent's one-shot turn as a workflow, stopped by `signal`; its
   * approvals go to `approvalIndex` and its events to `bus` when given.
   */
  turnWorkflow(params: {
    readonly signal: AbortSignal;
    readonly bus?: SessionEventBus;
    readonly approvalIndex?: TurnApprovalIndex;
    /** Decides this turn's approvals instead of suspending on them. */
    readonly onApprovalRequired?: (call: ToolCall) => Promise<ApprovalDecision>;
  }): Workflow<AgentInput, AgentResult>;
}

/** Whether `agent` can run its turn as a child workflow. */
export function isDelegableAgent(agent: object): agent is DelegableAgent {
  const candidate = agent as Partial<DelegableAgent>;
  return (
    typeof candidate.prepareTurn === "function" && typeof candidate.turnWorkflow === "function"
  );
}

/**
 * A delegation refused because the callee is already waiting on this
 * call: calling it would loop back into an agent blocked on the call.
 */
export class DelegationCycleError extends Error {
  readonly code = "delegation_cycle";
  constructor(
    readonly calleeId: string,
    readonly chain: ReadonlyArray<string>,
  ) {
    super(
      `delegation to ${calleeId} would create a cycle. Chain: ${[...chain, calleeId].join(" → ")}`,
    );
  }
}

/** A delegation refused because the chain would grow past `maxDepth`. */
export class DelegationDepthError extends Error {
  readonly code = "delegation_max_depth_exceeded";
  constructor(
    readonly maxDepth: number,
    readonly attemptedDepth: number,
    readonly chain: ReadonlyArray<string>,
  ) {
    super(
      `delegation depth ${attemptedDepth} exceeds the limit of ${maxDepth}. Chain: ${chain.join(" → ")}`,
    );
  }
}
