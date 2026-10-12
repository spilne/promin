// ---------------------------------------------------------------------------
// agentAction — the durable turn engine. One journaled workflow runs one
// agent turn: think → (approve) → tools → think … until the model answers,
// and every side effect sits behind a journaled activity so a crash, an
// approval suspend or a worker restart resumes the turn from the journal.
//
// Journal layout of one turn (activity names):
//
//   turn-start                         start time; emits turn.start
//   transcript-open                    transcript.open (the turn's user message)
//   before-turn                        hooks.beforeTurn
//   plan-<step>                        tool-set snapshot (name, requireApproval, idempotent)
//                                      and the compaction cut (see turn-compaction.ts)
//   compact-<step>                     summary of the steps before the cut; emits compact
//   think-<step>                       one LLM call, retried per llmRetry
//   step-<step>-hook                   onStep decision
//   step-<step>-approvals              which calls need approval (autoApprove verdicts)
//   step-<step>-approval-<id>-start    approvalIndex.requested; emits approval.requested
//                                      (tool name + input)
//   step-<step>-approval-<id>          onApprovalRequired decision (or the approve:<id> signal)
//   step-<step>-approval-<id>-end      emits approval.decision
//   tool-<name>-<step>-<id>            one tool call, inside ctx.parallel; a sub-agent
//                                      call is a child workflow plus -route / -end
//                                      entries (see delegation/run-delegated-call.ts)
//   stall-<step>                       emits stall.detected
//   budget-<step>                      with a budget: whether the next think step
//                                      fits (spend so far plus its projection)
//   after-turn                         hooks.afterTurn
//   turn-trace                         end time of the turn's trace
//   transcript-commit                  transcript.commit (what the turn produced,
//                                      the final assistant message stamped with
//                                      the turn's trace)
//   turn-usage                         usageRecorder.record (the turn's spend)
//   turn-end                           emits turn.end / turn.aborted / step_limit.hit /
//                                      budget.exceeded
//   turn-failed                        emits turn.failed (only when the turn throws)
//
// Replay contract: control flow between activities depends only on the
// input and journaled values, and every event / hook / metric fires inside
// the activity that produced it, so nothing is emitted twice for one
// journal position. Time comes from the injected WallClock, inside
// activities only.
//
// Budgets: `turn-start` journals the turn's effective budget (config
// tightened by the input's ceiling); each think and compaction journals
// its usage and priced cost; a sub-agent's spend comes back in its
// journaled result. So the turn's spend, and every budget decision made
// from it, replays the same. A turn over budget ends between steps — never
// between a tool call and its result — with `budget_exceeded`.
//
// Tracing: think and tool activities journal their own start time and
// duration next to their result, approval entries their request and
// decision times, so the turn's `TurnTrace` (AgentResult.trace, and the
// metadata of the committed final assistant message) is rebuilt from the
// journal on replay and reports the original timings.
//
// Every activity except a non-idempotent tool call is idempotent: a crash
// inside one re-runs it on recovery. So events, hooks and callbacks run
// once per journal position normally, and at least once across a crash —
// hooks must tolerate running again.
// ---------------------------------------------------------------------------

import { SystemWallClock, workflow } from "@promin/workflow";
import type { ActivityYield, WallClock, Workflow } from "@promin/workflow";
import type { z } from "zod";
import type { RateLimiter, LlmRetryOptions, RunLlmCallResult } from "./agent-shared.ts";
import {
  callLlmWithRetry,
  executeToolCall,
  formatToolError,
  isEngineControl,
  redactPriorThinkingBlocks,
  uniqueToolCallIds,
  resolveTools,
  runLlmCall,
} from "./agent-shared.ts";
import type { LLMProvider, LLMToolChoice, LLMToolDefinition } from "./llm-provider.ts";
import type { Message, AssistantMessage, ToolResultMessage, ToolCall } from "./message.ts";
import { recordApproval, recordChat, recordTool } from "./metrics/instrument.ts";
import type { AgentMetrics, ModelCostRates, ModelCostRegistry } from "./metrics/types.ts";
import {
  addUsage,
  budgetOverrun,
  EMPTY_USAGE,
  hasLimits,
  llmCallUsage,
  DEFAULT_MIN_CALL_TOKENS,
  ModelUnpricedError,
  planBudgetedCall,
  parseTurnBudget,
  remainingBudget,
  shareOfBudget,
  tightenBudget,
  type BudgetOverrun,
  type BudgetUsage,
  type TurnBudget,
} from "./budget/budget.ts";
import type { ProcessorsConfig } from "./processors.ts";
import type { SessionEventBus } from "./session-logger.ts";
import { StallDetector, type StallDetectorOptions } from "./stall-detector.ts";
import type { ToolAuditLogger } from "./tool-audit/types.ts";
import { zodToToolJsonSchema } from "./tool-json-schema.ts";
import { delegatedRunId, runDelegatedCall } from "./delegation/run-delegated-call.ts";
import {
  applyCompaction,
  estimateMessageTokens,
  planCompaction,
  promptTokens,
  summarizeSteps,
  validateCompactionConfig,
  type StepSummary,
  type TurnCompactionConfig,
} from "./turn-compaction.ts";
import { parseDelegationFrame, ROOT_FRAME, type DelegationFrame } from "./delegation/types.ts";
import type { AgentTool, ApprovalDecision, AutoApprove, ToolScope } from "./tool.ts";
import { shouldAutoApprove } from "./tool.ts";
import { TURN_TRACE_META_KEY, type TurnSpan, type TurnTrace } from "./trace.ts";
import type { ToolRegistry } from "./tool-registry.ts";
import { buildToolDefs } from "./tool-registry.ts";

export interface AgentInput {
  task: string;
  messages?: Message[];
  /**
   * Where this turn sits in a delegation chain; set by the engine on a
   * sub-agent's turn (`subAgentTool`). Absent: a turn nobody delegated to.
   */
  delegation?: DelegationFrame;
  /**
   * A ceiling on this turn's spend, tightening `AgentActionConfig.budget`
   * limit by limit. Set by whoever starts the turn on someone's account: a
   * delegating turn passes its remaining budget, a DAG run a node's share,
   * `LocalAgent` what is left of the thread's and namespace's budgets.
   */
  budget?: TurnBudget;
}

/**
 * Why a turn ended:
 * - `stop` — the model answered (or `onStep` stopped the turn).
 * - `max_steps` — `maxSteps` think steps ran without an answer; the answer
 *   is `STEP_LIMIT_ANSWER`.
 * - `stalled` — the stall detector forced a final answer.
 * - `cancelled` — the turn's `signal` aborted.
 * - `budget_exceeded` — the turn's budget ran out; the answer is
 *   `BUDGET_EXCEEDED_ANSWER`.
 */
export type AgentFinishReason = "stop" | "max_steps" | "stalled" | "cancelled" | "budget_exceeded";

export interface AgentResult<TOutput = unknown> {
  answer: string;
  /**
   * The schema-validated `_respond` payload when `outputSchema` is set and
   * the turn finished with `stop`; undefined otherwise.
   */
  output?: TOutput;
  /**
   * The turn's working conversation as the engine last held it: seed plus
   * steps. Always ends with an assistant message and has a tool result for
   * every tool call. With `compaction`, older steps of a long turn are
   * folded into a summary here — `turnMessages` has them all.
   */
  messages: Message[];
  /**
   * What this turn adds to a conversation: the user task, then the
   * assistant and tool messages the turn produced. Excludes the seed
   * (system prompt, `input.messages`, whatever `beforeTurn` returned) and
   * the engine's own guidance messages. Persist this to continue a thread.
   */
  turnMessages: Message[];
  /** Think steps taken. */
  steps: number;
  finishReason: AgentFinishReason;
  usage?: {
    inputTokens: number;
    outputTokens: number;
    cacheReadTokens?: number;
    cacheWriteTokens?: number;
  };
  /**
   * What the turn spent, sub-agents it delegated to included: tokens,
   * priced cost, LLM and tool calls. Set by every `agentAction` turn; a
   * delegating turn charges it to its own budget.
   */
  spend?: BudgetUsage;
  /**
   * The turn's timed trace: think / tool / approval spans and totals. Set
   * by every `agentAction` turn; absent on a result relayed from an agent
   * that is not run by this engine.
   */
  trace?: TurnTrace;
}

export interface StepContext {
  step: number;
  /** The conversation as the next think step sees it (compacted, with `compaction`). */
  messages: Message[];
  workflowId: string;
  /**
   * `true` when the workflow body is executing on top of pre-existing
   * journal entries (worker restart / signal-resume). `onStep` runs in a
   * journaled activity, so it only fires for a step that was not
   * recorded yet.
   */
  isReplay: boolean;
}

/** Lifecycle state of a turn, as seen by `onLifecycle`. */
export type AgentTurnState = "idle" | "thinking" | "waiting_approval";

export interface AgentTurnLifecycleEvent {
  readonly workflowId: string;
  readonly event: "message" | "approval-required" | "approved" | "rejected" | "done" | "failed";
  readonly from: AgentTurnState;
  readonly to: AgentTurnState;
  /** Epoch ms on the engine's clock. */
  readonly at: number;
  readonly toolCallId?: string;
  readonly finishReason?: AgentFinishReason;
}

export interface AgentTurnHooks {
  /**
   * Runs once at the start of the turn, before the first think step.
   * Return a new message list to inject context, or void to keep it.
   * Journaled: the returned list is replayed, the hook is not re-run —
   * except after a crash inside it, when it runs again (make it idempotent).
   */
  beforeTurn?: (params: {
    task: string;
    messages: Message[];
    workflowId: string;
  }) => Promise<Message[] | void>;
  /**
   * Runs once when the turn has its answer, before `turn.end` is emitted.
   * Journaled: not re-run on replay, except after a crash inside it, when
   * it runs again (make it idempotent). A throw fails the turn.
   */
  afterTurn?: (params: {
    task: string;
    answer: string;
    /** `AgentResult.messages`: compacted, with `compaction`. */
    messages: Message[];
    finishReason: AgentFinishReason;
    usage: { inputTokens: number; outputTokens: number };
    workflowId: string;
  }) => Promise<void>;
}

/**
 * Durable transcript of one thread turn. The engine calls `open` with the
 * turn's user message right after the turn starts, and `commit` with the
 * assistant and tool messages the turn produced once it has its answer —
 * each inside its own journaled activity, so a turn that suspends on an
 * approval or fails has still recorded its user message, and a resumed
 * turn commits its output once.
 *
 * Both run again when a crash interrupts them: implementations must be
 * idempotent per `workflowId` (e.g. `MemoryStore.appendMessages` with an
 * idempotency key).
 */
export interface TurnTranscript {
  open(params: { readonly workflowId: string; readonly message: Message }): Promise<void>;
  commit(params: {
    readonly workflowId: string;
    readonly messages: ReadonlyArray<Message>;
  }): Promise<void>;
}

/**
 * Where a turn's spend is recorded once it commits (thread and namespace
 * budgets). Runs inside the journaled `turn-usage` activity, again only
 * when a crash interrupts it: implementations must be idempotent per
 * `workflowId` (`UsageLedger.record` is).
 */
export interface TurnUsageRecorder {
  record(params: { readonly workflowId: string; readonly usage: BudgetUsage }): Promise<void>;
}

/**
 * Index of the approvals a turn suspends on. `requested` runs inside the
 * journaled activity that precedes the `approve:<toolCallId>` suspension
 * (only when no `onApprovalRequired` callback decides instead), so the
 * index row exists before anyone can see the turn suspended. It runs again
 * when a crash interrupts that activity: make it idempotent per
 * `(workflowId, call.id)`.
 */
export interface TurnApprovalIndex {
  requested(params: { readonly workflowId: string; readonly call: ToolCall }): Promise<void>;
  /**
   * The agent thread the indexed approvals belong to — recorded with each
   * approval's start entry so signal routes can send a decider to the
   * agent. Unset: derived from the turn's `scope`.
   */
  readonly thread?: {
    readonly namespaceId: string;
    readonly threadId: string;
    readonly agentId?: string;
  };
}

export interface AgentActionConfig<TOutput = any> {
  name: string;
  llm: LLMProvider;
  /** Inline tools. Exclusive with `toolRegistry`. */
  tools?: Record<string, AgentTool<any, any>>;
  /**
   * Live tool source, read at every step so hot-reloaded tools show up on
   * the next step. Each step journals the set it saw (names,
   * `requireApproval`, `idempotent`), so a replay takes the same path.
   */
  toolRegistry?: ToolRegistry;
  autoApprove?: AutoApprove;
  /**
   * Async callback invoked when a tool with `requireApproval: true` needs a decision.
   * When omitted, approval falls back to `ctx.approval(<toolCallId>)` (the
   * `approve:<id>` signal): the workflow suspends until it is delivered.
   *
   * Runs in a journaled activity: the decision survives replay, the
   * callback is not re-run — except after a crash inside it, when it is
   * asked again.
   */
  onApprovalRequired?: (call: ToolCall) => Promise<ApprovalDecision>;
  /** Think steps per turn. Default 20. Running out ends the turn with `max_steps`. */
  maxSteps?: number;
  /**
   * What one turn may spend, its sub-agents included (see `TurnBudget`).
   * Checked before every LLM call against the spend so far plus the call's
   * prompt — its output is capped to what is left (see `minCallTokens`) —
   * and after it against the spend; running out ends the turn
   * with `budget_exceeded` before the next step. Sub-agent calls get the
   * remaining budget, split evenly between the calls of one step, and are
   * charged what they spent. `maxCostUsd` needs `costs`, `llmProvider`
   * and `llmModel` to price the model. Default: unlimited.
   */
  budget?: TurnBudget;
  /**
   * With a budget: the smallest output allowance worth an LLM call. A call
   * that could produce fewer tokens than this before a limit is not made
   * (the turn ends with `budget_exceeded`); otherwise the call's
   * `maxTokens` is lowered to what the output, total and cost limits leave.
   * Default `DEFAULT_MIN_CALL_TOKENS` (256).
   */
  minCallTokens?: number;
  systemPrompt?: string;
  rateLimiter?: RateLimiter;
  /** Time source for durations, timeouts and retry waits. Default `SystemWallClock`. */
  clock?: WallClock;
  /**
   * When set, the turn's answer is a structured value: the engine offers
   * a `_respond` tool with this schema and forces it (tool_choice) once the
   * model is ready to answer. `result.output` is the parsed value.
   */
  outputSchema?: z.ZodType<TOutput>;
  /**
   * How many times a `_respond` payload that fails `outputSchema` (or a
   * forced round that didn't call `_respond`) is sent back to the model
   * with the validation errors before the turn fails with
   * `StructuredOutputParseError`. Default 2.
   */
  structuredOutputRepairs?: number;
  processors?: ProcessorsConfig;
  /** Extended-thinking budget forwarded to every LLM call (off on forced rounds). */
  thinkingBudgetTokens?: number;
  /** Output-token cap forwarded to every LLM call. */
  maxTokens?: number;
  /**
   * In-turn context compaction: when the messages of the next think step
   * are estimated to exceed `compaction.maxTokens`, older tool steps of the
   * turn are folded into one summary (see `TurnCompactionConfig`). The
   * seed — system prompt, `input.messages`, the task — is never compacted.
   * Default: off.
   */
  compaction?: TurnCompactionConfig;
  /**
   * Strip thinking blocks of earlier turns from the messages sent to the
   * LLM (see `redactPriorThinkingBlocks`). Default `true`.
   */
  redactPriorThinking?: boolean;
  /** LLM retry policy; `false` disables retries. Default: `LlmRetryOptions` defaults. */
  llmRetry?: LlmRetryOptions | false;
  /** Default per-call tool time limit in ms; `AgentTool.timeoutMs` overrides it. */
  toolTimeoutMs?: number;
  /** Stall detection; `false` turns it off. Default: `StallDetectorOptions` defaults. */
  stall?: StallDetectorOptions | false;
  hooks?: AgentTurnHooks;
  /**
   * Lifecycle transitions of the turn (idle → thinking → waiting_approval
   * → thinking → idle, or → idle via `failed`). Fires inside journaled
   * activities: once per transition, again only if a crash interrupted
   * that activity. Advisory: a throw or rejection is logged and swallowed.
   */
  onLifecycle?: (event: AgentTurnLifecycleEvent) => void | Promise<void>;
  /**
   * Per-step callback. Return `{ continue: false }` to end the turn after
   * this step; return `{ continue: true, feedback: '…' }` to add a system
   * message before the next think. Journaled: the decision is replayed,
   * the callback is not re-run (except after a crash inside it).
   */
  onStep?: (ctx: StepContext) => { continue: boolean; feedback?: string } | void;
  /** Fires for each tool call the LLM emits, inside the think activity. */
  onToolCall?: (call: ToolCall) => void;
  /** Fires after a tool produces output, inside the tool activity. */
  onToolResult?: (call: ToolCall, output: unknown) => void;
  /** Text deltas as the LLM streams (needs `llm.chatStream`). Not replayed. */
  onChunk?: (delta: string) => void;
  /** Extended-thinking deltas as the LLM streams. Not replayed. */
  onThinking?: (delta: string) => void;
  /**
   * Event bus. The engine emits turn / llm / tool / approval / stall
   * events on it. Each event is emitted by the activity that produced it,
   * so a replay (crash recovery, approval resume) never re-emits an event
   * of a recorded activity — a resume stream sees only what happens after
   * the resume. An activity a crash interrupted runs, and emits, again.
   */
  bus?: SessionEventBus;
  /** Caller scope, surfaced on every tool's `ctx.scope`. */
  scope?: ToolScope;
  /** Durable transcript sink — see `TurnTranscript`. Thread turns set it. */
  transcript?: TurnTranscript;
  /** Pending-approval index — see `TurnApprovalIndex`. Thread turns set it. */
  approvalIndex?: TurnApprovalIndex;
  /** Where the committed turn's spend goes — see `TurnUsageRecorder`. Thread turns set it. */
  usageRecorder?: TurnUsageRecorder;
  /** Audit sink for elevated tools, surfaced on `ctx.toolAuditLogger`. */
  toolAuditLogger?: ToolAuditLogger;
  /**
   * Cancels the turn. Threaded into every LLM call and tool `ctx.signal`.
   * The engine observes it at the next think step, which it records as
   * cancelled, so a replay ends the turn at the same point.
   */
  signal?: AbortSignal;
  /** Telemetry sink: llm.*, tool.* and approval.* metrics. */
  metrics?: AgentMetrics;
  /** Per-model USD rates: llm.cost.usd and the turn's priced spend (`budget.maxCostUsd`). */
  costs?: ModelCostRegistry;
  /** Provider label for LLM metrics (required with `metrics` for llm.* metrics). */
  llmProvider?: string;
  /** Model label for LLM metrics (required with `metrics` for llm.* metrics). */
  llmModel?: string;
  /** Extra labels on every metric. */
  metricLabels?: Readonly<Record<string, string>>;
}

/**
 * Engine settings a long-lived agent applies to every turn —
 * `AgentActionConfig` minus the per-run wiring (`bus`, `scope`, `signal`,
 * `transcript`, `approvalIndex`, `usageRecorder`).
 */
export type AgentEngineConfig<TOutput = any> = Omit<
  AgentActionConfig<TOutput>,
  "bus" | "scope" | "signal" | "transcript" | "approvalIndex" | "usageRecorder"
>;

export class StructuredOutputParseError extends Error {
  readonly _tag = "StructuredOutputParseError";
  constructor(override readonly cause: unknown) {
    super(`Agent structured output failed schema validation: ${String(cause)}`);
  }
}

/** Name of the synthetic structured-output tool. */
export const RESPOND_TOOL_NAME = "_respond";
/** Final assistant message of a turn that ran out of steps. */
export const STEP_LIMIT_ANSWER = "(step limit reached)";
/** Final assistant message of a cancelled turn that produced no text. */
export const CANCELLED_ANSWER = "(cancelled)";
/** Final assistant message of a stalled turn whose forced round produced no text. */
export const STALLED_ANSWER = "(no answer: the agent stopped making progress)";
/** Final assistant message of a turn that ran out of budget. */
export const BUDGET_EXCEEDED_ANSWER = "(budget exhausted)";

/** Tool result of a call the engine did not run because the budget ran out. */
const BUDGET_SKIPPED = "Not run: the turn's budget is exhausted.";

const STRUCTURED_OUTPUT_INSTRUCTION =
  "When you have gathered all needed information and are ready to respond, " +
  `call the \`${RESPOND_TOOL_NAME}\` tool with your final structured answer. ` +
  `Do not produce a text reply — always use the \`${RESPOND_TOOL_NAME}\` tool for your final answer.`;

const STALL_INSTRUCTION =
  "You are repeating tool calls without making progress. Stop calling tools and give your " +
  "final answer now from what you already have. If you cannot complete the task, say what " +
  "is blocking you.";

const RESPOND_NUDGE = `Call the \`${RESPOND_TOOL_NAME}\` tool with your final answer.`;

const OUTCOME_UNKNOWN_MESSAGE =
  "Tool outcome unknown: the process stopped while this tool was running, and the tool is " +
  "not marked idempotent, so it was not run again. It may or may not have taken effect — " +
  "check before retrying.";

/** One tool as journaled in a step's snapshot. */
interface ToolSnapshotEntry {
  readonly name: string;
  readonly requireApproval: boolean;
  readonly idempotent: boolean;
  /** A sub-agent tool: its calls run as child workflows. */
  readonly delegated: boolean;
}

type RoundMode = "normal" | "force-respond" | "force-answer";

/** One think step's response with what it spent. */
type ThinkResult = RunLlmCallResult & { readonly spend: BudgetUsage };

interface TurnOutcome {
  readonly answer: string;
  readonly finishReason: AgentFinishReason;
  readonly output?: unknown;
}

/**
 * Build the durable agent turn engine as a workflow.
 *
 * Runs think → tool → think up to `maxSteps` times and returns the agent's
 * answer (plus structured output when `outputSchema` is set). Journaled
 * via `@promin/workflow`: a crash resumes the turn, an approval suspends
 * it until the `approve:<toolCallId>` signal arrives.
 *
 * Durability contract:
 * - LLM calls are idempotent activities: a crash mid-call re-runs the call
 *   on recovery. Transient and rate-limit failures are retried per
 *   `llmRetry` (honoring `Retry-After`); permanent ones fail the turn.
 * - Tool calls run once. After a crash mid-call, a tool marked
 *   `idempotent` runs again with the same `ctx.toolCallId`; any other
 *   tool is not re-run and the model gets an "outcome unknown" error.
 * - The step limit, a stall and a cancellation end the turn with a
 *   sentinel answer and a consistent transcript instead of throwing.
 *
 * Errors: `StructuredOutputParseError` after `structuredOutputRepairs`
 * failed repairs; a permanent LLM error; a throwing hook. A pending
 * approval rejects the run with `WorkflowSuspendedError`.
 *
 * @example
 * ```ts
 * const agent = agentAction({
 *   name: "researcher",
 *   llm: anthropic("claude-sonnet-4-6", { apiKey }),
 *   tools: { search: webSearchTool, fetch: fetchUrlTool },
 *   systemPrompt: "You are a research assistant.",
 * });
 *
 * const result = await runner.run({ workflow: agent, workflowId: "r1", input: { task } });
 * console.log(result.answer);
 * ```
 */
export function agentAction<TOutput>(
  config: AgentActionConfig<TOutput> & { outputSchema: z.ZodType<TOutput> },
): Workflow<AgentInput, AgentResult<TOutput>>;
export function agentAction(
  config: AgentActionConfig<never> & { outputSchema?: undefined },
): Workflow<AgentInput, AgentResult>;
export function agentAction(config: AgentActionConfig<any>): Workflow<AgentInput, AgentResult> {
  if (config.compaction) validateCompactionConfig(config.compaction);
  const configBudget = config.budget === undefined ? undefined : parseTurnBudget(config.budget);
  const maxSteps = config.maxSteps ?? 20;
  const minCallTokens = config.minCallTokens ?? DEFAULT_MIN_CALL_TOKENS;
  const clock = config.clock ?? SystemWallClock;
  const bus = config.bus;
  const repairBudget = config.structuredOutputRepairs ?? 2;
  const redactPriorThinking = config.redactPriorThinking ?? true;
  const schema = config.outputSchema;
  const respondDef: LLMToolDefinition | undefined = schema
    ? {
        name: RESPOND_TOOL_NAME,
        description:
          "Provide your final structured response. Call this when you have all the information needed.",
        parameters: zodToToolJsonSchema(schema),
      }
    : undefined;

  type EventBody = Parameters<SessionEventBus["emit"]>[0];
  const emit = (event: EventBody): void => {
    bus?.emit({ ...event, ts: clock.currentTimeMs() } as EventBody);
  };
  const lifecycle = (event: Omit<AgentTurnLifecycleEvent, "at">): void => {
    if (!config.onLifecycle) return;
    try {
      Promise.resolve(config.onLifecycle({ ...event, at: clock.currentTimeMs() })).catch((err) =>
        console.error("[agentAction] onLifecycle error:", err),
      );
    } catch (err) {
      console.error("[agentAction] onLifecycle error:", err);
    }
  };
  const metricLabels = config.metricLabels ?? {};

  return workflow<AgentInput>({ name: config.name })
    .journaled("agent", function* (ctx, input) {
      const workflowId = ctx.workflowId;
      // The frame arrives in the run's input, which a caller may forge:
      // a malformed one fails the run before anything happens.
      const frame: DelegationFrame =
        input.delegation === undefined ? ROOT_FRAME : parseDelegationFrame(input.delegation);
      // The turn's trace spans, rebuilt from journaled values on replay.
      const spans: TurnSpan[] = [];
      // The turn's limits, journaled: a config edited while the turn is
      // suspended can't change the decisions a replay makes.
      const started = yield* ctx.activity(
        "turn-start",
        async () => {
          const ceiling = input.budget === undefined ? undefined : parseTurnBudget(input.budget);
          const effective = tightenBudget(configBudget, ceiling);
          emit({ type: "turn.start", turn: 0, task: input.task });
          lifecycle({ workflowId, event: "message", from: "idle", to: "thinking" });
          return {
            startedAt: clock.currentTimeMs(),
            budget: hasLimits(effective) ? effective : null,
            // Whether the turn's model is priced, journaled with the
            // budget: a replay doesn't re-read the live `costs`.
            priced: mainRates() !== undefined,
          };
        },
        { idempotent: true },
      );
      const startedAt = started.startedAt;
      const budget: TurnBudget | undefined = started.budget ?? undefined;
      // What the turn has spent so far, from journaled values only.
      let spend: BudgetUsage = EMPTY_USAGE;
      let budgetHit: (BudgetOverrun & { readonly step: number }) | undefined;

      try {
        return yield* turnBody();
      } catch (err) {
        if (isEngineControl(err)) throw err;
        const error = err instanceof Error ? err.message : String(err);
        const failedSpend = spend;
        // Recorded as a failure, not a success: when the step is discarded
        // and run again, the journal drops it with the turn's other failed
        // entries instead of leaving a "turn-failed" slot the re-run can't match.
        yield* ctx.activity(
          "turn-failed",
          async (): Promise<void> => {
            // A failed turn spent too (the steps before the failure): record
            // it, so failing turns can't run past thread / namespace budgets.
            // The recorder keeps the larger spend per turn, so a turn run
            // again after this records what it adds.
            if (config.usageRecorder) {
              try {
                await config.usageRecorder.record({ workflowId, usage: failedSpend });
              } catch (recordErr) {
                console.error("[agentAction] usage record of a failed turn failed:", recordErr);
              }
            }
            emit({ type: "turn.failed", turn: 0, error });
            lifecycle({ workflowId, event: "failed", from: "thinking", to: "idle" });
            throw err;
          },
          { idempotent: true },
        );
        throw err;
      }

      function* turnBody(): Generator<ActivityYield, AgentResult, any> {
        if (budget?.maxCostUsd !== undefined && !started.priced) {
          throw new ModelUnpricedError(config.llmProvider, config.llmModel);
        }
        const transcript = config.transcript;
        if (transcript) {
          yield* ctx.activity(
            "transcript-open",
            () => transcript.open({ workflowId, message: { role: "user", content: input.task } }),
            { idempotent: true },
          );
        }
        let messages: Message[] = [
          ...(config.systemPrompt
            ? [{ role: "system" as const, content: config.systemPrompt }]
            : []),
          ...(input.messages ?? []),
          ...(schema ? [{ role: "system" as const, content: STRUCTURED_OUTPUT_INSTRUCTION }] : []),
          { role: "user" as const, content: input.task },
        ];

        if (config.hooks?.beforeTurn) {
          const beforeTurn = config.hooks.beforeTurn;
          const seeded = messages;
          const modified = yield* ctx.activity(
            "before-turn",
            () => beforeTurn({ task: input.task, messages: seeded, workflowId }),
            { idempotent: true },
          );
          if (modified) messages = modified;
        }
        // The turn's own transcript: the task, then what the turn produces.
        let produced: Message[] = [{ role: "user", content: input.task }];
        const record = (...added: Message[]): void => {
          messages = [...messages, ...added];
          produced = [...produced, ...added];
        };

        const usage = { inputTokens: 0, outputTokens: 0, cacheReadTokens: 0, cacheWriteTokens: 0 };
        const stall = config.stall === false ? undefined : new StallDetector(config.stall ?? {});
        let mode: RoundMode = "normal";
        let repairsLeft = repairBudget;
        let outcome: TurnOutcome | undefined;
        let stepsTaken = 0;

        // Compaction state: the turn's steps start after the seed; the
        // summary of compacted steps (when any) sits at `stepStart`.
        const stepStart = messages.length;
        let summary: string | undefined;
        // What the provider reported for the last think step, while the
        // list it measured is still the list being extended.
        let reported: { readonly tokens: number; readonly sent: number } | undefined;
        // A compaction that couldn't bring the step under budget (the seed
        // or the kept steps alone are too big) backs off for a few steps
        // instead of summarising again on every one.
        let ineffectiveAt: number | undefined;

        for (let step = 0; step < maxSteps && outcome === undefined; step++) {
          stepsTaken = step + 1;

          // ---- plan: control flow below reads only this ----
          // The tool-set snapshot and where to compact are journaled, so a
          // tool or compaction config edited mid-turn can't alter a replay.
          const planned = messages;
          const reportedTokens = reported
            ? reported.tokens + estimateMessageTokens(planned.slice(reported.sent + 1))
            : undefined;
          const plan = yield* ctx.activity(
            `plan-${step}`,
            async () => ({
              tools: Object.entries(resolveTools(config)).map(
                ([name, t]): ToolSnapshotEntry => ({
                  name,
                  requireApproval: t.requireApproval === true,
                  idempotent: t.idempotent === true,
                  delegated: t.delegation !== undefined,
                }),
              ),
              compaction:
                config.compaction &&
                (ineffectiveAt === undefined ||
                  step - ineffectiveAt >= ineffectiveBackoff(config.compaction))
                  ? planCompaction({
                      messages: planned,
                      stepStart,
                      config: config.compaction,
                      ...(reportedTokens !== undefined && { reportedTokens }),
                    })
                  : null,
            }),
            { idempotent: true },
          );
          const snapshot = plan.tools;
          const snapshotByName = new Map(snapshot.map((e) => [e.name, e]));

          // ---- budget: does the next LLM call fit, and with what output cap? ----
          let callMaxTokens = config.maxTokens;
          if (budget) {
            const projectedInput = plan.compaction
              ? plan.compaction.after
              : estimateMessageTokens(planned);
            const summarizes = plan.compaction !== null && config.compaction?.summarize !== false;
            const spent = spend;
            const call = yield* ctx.activity(
              `budget-${step}`,
              async () =>
                planBudgetedCall({
                  budget,
                  spent,
                  promptTokens: projectedInput,
                  llmCalls: summarizes ? 2 : 1,
                  rates: mainRates(),
                  maxTokens: config.maxTokens,
                  minOutputTokens: minCallTokens,
                }),
              { idempotent: true },
            );
            callMaxTokens = call.maxTokens ?? undefined;
            const overrun = call.overrun;
            if (overrun) {
              budgetHit = { ...overrun, step };
              record({ role: "assistant", content: BUDGET_EXCEEDED_ANSWER });
              outcome = { answer: BUDGET_EXCEEDED_ANSWER, finishReason: "budget_exceeded" };
              break;
            }
          }

          // ---- compaction: fold the older steps into one summary ----
          if (plan.compaction) {
            const { cut, tokens, after, ineffective } = plan.compaction;
            const previousSummary = summary;
            const steps = planned.slice(stepStart + (previousSummary === undefined ? 0 : 1), cut);
            const firstIneffective = ineffective && ineffectiveAt === undefined;
            const compacted = yield* ctx.activity(
              `compact-${step}`,
              () =>
                timed(() =>
                  compactSteps({
                    step,
                    steps,
                    previousSummary,
                    tokens,
                    after,
                    kept: planned.length - cut,
                    firstIneffective,
                    fallbackMaxTokens: tokens,
                  }),
                ),
              { idempotent: true },
            );
            const result = compacted.value;
            summary = result.text;
            spans.push({
              kind: "compaction",
              step,
              startedAt: compacted.startedAt,
              durationMs: compacted.durationMs,
              compacted: steps.length,
              summarized: result.summarized,
              ...(result.usage && { usage: result.usage }),
            });
            if (result.usage) {
              usage.inputTokens += result.usage.inputTokens;
              usage.outputTokens += result.usage.outputTokens;
              usage.cacheReadTokens += result.usage.cacheReadTokens ?? 0;
              usage.cacheWriteTokens += result.usage.cacheWriteTokens ?? 0;
            }
            spend = addUsage(spend, result.spend);
            if (ineffective) ineffectiveAt = step;
            messages = applyCompaction({ messages: planned, stepStart, cut, summary });
            reported = undefined;
          }

          // ---- think ----
          const roundMode: RoundMode =
            mode === "normal" && respondDef && snapshot.length === 0 ? "force-respond" : mode;
          const conversation = messages;
          const thought = yield* ctx.activity(
            `think-${step}`,
            () =>
              timed(() =>
                think({
                  step,
                  messages: conversation,
                  snapshot,
                  roundMode,
                  ...(callMaxTokens !== undefined && { maxTokens: callMaxTokens }),
                  isReplay: ctx.isReplay,
                  workflowId,
                }),
              ),
            { idempotent: true },
          );
          const response = thought.value;
          spans.push({
            kind: "think",
            step,
            startedAt: thought.startedAt,
            durationMs: thought.durationMs,
            ...(response.usage && { usage: response.usage }),
          });
          spend = addUsage(spend, response.spend);
          if (response.usage) {
            usage.inputTokens += response.usage.inputTokens;
            usage.outputTokens += response.usage.outputTokens;
            usage.cacheReadTokens += response.usage.cacheReadTokens ?? 0;
            usage.cacheWriteTokens += response.usage.cacheWriteTokens ?? 0;
            // The prompt plus the reply this step adds as its assistant message.
            reported = {
              tokens: promptTokens(response.usage) + response.usage.outputTokens,
              sent: conversation.length,
            };
          } else {
            reported = undefined;
          }

          if (response.aborted) {
            const text = response.content?.trim() ? response.content : null;
            record({ role: "assistant", content: text ?? CANCELLED_ANSWER });
            outcome = { answer: text ?? CANCELLED_ANSWER, finishReason: "cancelled" };
            break;
          }

          const calls = response.toolCalls ?? [];
          const assistant: AssistantMessage = {
            role: "assistant",
            content: response.content,
            ...(calls.length > 0 && { toolCalls: calls }),
            ...(response.thinkingBlocks && { thinkingBlocks: response.thinkingBlocks }),
          };
          record(assistant);

          // ---- structured output: a `_respond` call ends or repairs the turn ----
          const respondCall = respondDef
            ? calls.find((c) => c.name === RESPOND_TOOL_NAME)
            : undefined;
          if (schema && respondCall) {
            const others = calls.filter((c) => c !== respondCall);
            const parsed =
              respondCall.inputError !== undefined
                ? { success: false as const, error: new Error(respondCall.inputError) }
                : schema.safeParse(respondCall.input);
            if (parsed.success) {
              const answer = JSON.stringify(respondCall.input);
              record(
                ...skippedResults(others, `Not run: ${RESPOND_TOOL_NAME} gave the final answer.`),
                { role: "tool", toolCallId: respondCall.id, content: "Response recorded." },
                { role: "assistant", content: answer },
              );
              outcome = { answer, finishReason: "stop", output: parsed.data };
              break;
            }
            if (repairsLeft === 0) throw new StructuredOutputParseError(parsed.error);
            repairsLeft--;
            record(
              ...skippedResults(
                others,
                `Not run: ${RESPOND_TOOL_NAME} was called in the same response; call it again if still needed.`,
              ),
              {
                role: "tool",
                toolCallId: respondCall.id,
                content:
                  `Invalid response: ${formatToolError(parsed.error)}\n` +
                  `Call ${RESPOND_TOOL_NAME} again with output that matches the schema.`,
                isError: true,
              },
            );
            // The repair round keeps the agent's tools available; a prose
            // answer after it still gets a forced `_respond` round.
            continue;
          }

          // ---- onStep ----
          let feedback: string | undefined;
          if (config.onStep) {
            const onStep = config.onStep;
            const stepMessages = messages;
            const decision = yield* ctx.activity(
              `step-${step}-hook`,
              async () => {
                const d = onStep({
                  step,
                  messages: stepMessages,
                  workflowId,
                  isReplay: ctx.isReplay,
                });
                return d ? { continue: d.continue, feedback: d.feedback ?? null } : null;
              },
              { idempotent: true },
            );
            if (decision && !decision.continue) {
              const text = response.content ?? "";
              if (calls.length > 0) {
                record(
                  ...skippedResults(calls, "Not run: the turn was stopped."),
                  {
                    role: "assistant",
                    content: text || "(stopped)",
                  },
                );
              }
              outcome = { answer: text, finishReason: "stop" };
              break;
            }
            feedback = decision?.feedback ?? undefined;
          }

          // ---- over budget: answer instead of running more tools ----
          // A text answer still ends the turn normally; tool calls are
          // closed with results that say why they did not run.
          const callsOverrun =
            calls.length > 0 && budget
              ? (budgetOverrun({ budget, usage: spend }) ??
                budgetOverrun({
                  budget,
                  usage: addUsage(spend, { ...EMPTY_USAGE, toolCalls: calls.length }),
                }))
              : undefined;
          if (callsOverrun) {
            budgetHit = { ...callsOverrun, step };
            record(
              ...skippedResults(calls, BUDGET_SKIPPED),
              {
                role: "assistant",
                content: BUDGET_EXCEEDED_ANSWER,
              },
            );
            outcome = { answer: BUDGET_EXCEEDED_ANSWER, finishReason: "budget_exceeded" };
            break;
          }

          // ---- a text answer ----
          if (calls.length === 0) {
            if (schema) {
              // The model answered in prose; the answer must come through `_respond`.
              if (roundMode === "force-respond") {
                if (repairsLeft === 0) {
                  throw new StructuredOutputParseError(
                    new Error(`the model did not call ${RESPOND_TOOL_NAME}`),
                  );
                }
                repairsLeft--;
              }
              // Never end a request on an assistant turn: the forced round
              // gets a user nudge (guidance only, not part of the transcript).
              messages = [...messages, { role: "user", content: RESPOND_NUDGE }];
              mode = "force-respond";
              continue;
            }
            const text = response.content ?? "";
            if (roundMode === "force-answer") {
              if (!text.trim()) {
                const sentinel: Message = { role: "assistant", content: STALLED_ANSWER };
                messages = [...messages.slice(0, -1), sentinel];
                produced = [...produced.slice(0, -1), sentinel];
                outcome = { answer: STALLED_ANSWER, finishReason: "stalled" };
              } else {
                outcome = { answer: text, finishReason: "stalled" };
              }
              break;
            }
            outcome = { answer: text, finishReason: "stop" };
            break;
          }

          // ---- the model ignored a forced text round: close it out ----
          if (roundMode === "force-answer") {
            const text = response.content?.trim() ? response.content : STALLED_ANSWER;
            record(
              ...skippedResults(calls, "Not run: tools are disabled for the final answer."),
              {
                role: "assistant",
                content: text,
              },
            );
            outcome = { answer: text, finishReason: "stalled" };
            break;
          }

          // ---- approvals (sequential), then tools (parallel) ----
          const results: ToolResultMessage[] = [];
          const toRun: Array<{ index: number; call: ToolCall; entry: ToolSnapshotEntry }> = [];
          // The autoApprove policy reads live config; journal its verdicts so
          // a policy changed while the turn is suspended can't alter replay.
          const needsApproval = calls.some((c) => snapshotByName.get(c.name)?.requireApproval)
            ? yield* ctx.activity(
                `step-${step}-approvals`,
                async () =>
                  calls.map((c) => {
                    const entry = snapshotByName.get(c.name);
                    return entry?.requireApproval === true && !autoApproved(c, entry);
                  }),
                { idempotent: true },
              )
            : calls.map(() => false);
          for (const [index, call] of calls.entries()) {
            const entry = snapshotByName.get(call.name);
            if (!entry) {
              results[index] = errorResult(call, `Error: unknown tool "${call.name}"`);
              continue;
            }
            if (needsApproval[index]) {
              const decision = yield* approve({ step, call });
              if (!decision.approved) {
                results[index] = errorResult(
                  call,
                  `Rejected: ${decision.reason ?? "user rejected"}`,
                );
                continue;
              }
            }
            toRun.push({ index, call, entry });
          }
          if (toRun.length > 0) {
            spend = addUsage(spend, { ...EMPTY_USAGE, toolCalls: toRun.length });
            // Sub-agent calls of this step share what is left of the budget.
            const delegatedCount = toRun.filter(({ entry }) => entry.delegated).length;
            const childBudget =
              budget && delegatedCount > 0
                ? shareOfBudget({
                    budget: remainingBudget({ budget, usage: spend }),
                    parts: delegatedCount,
                  })
                : undefined;
            const outputs = yield* ctx.parallel(
              toRun.map(({ call, entry }) =>
                runTool({ step, call, entry, ...(childBudget && { childBudget }) }),
              ),
            );
            toRun.forEach(({ index }, k) => {
              const ran = outputs[k]!;
              results[index] = ran.result;
              if (ran.spend) spend = addUsage(spend, ran.spend);
            });
          }
          record(...results);

          // ---- progress checks ----
          if (stall && mode === "normal") {
            const verdict = stall.observe({ content: response.content, calls });
            if (verdict.stalled) {
              yield* ctx.activity(
                `stall-${step}`,
                async () => {
                  emit({
                    type: "stall.detected",
                    turn: 0,
                    step,
                    reason: verdict.reason,
                    toolName: verdict.toolName,
                  });
                  return verdict.reason;
                },
                { idempotent: true },
              );
              messages = [...messages, { role: "system", content: STALL_INSTRUCTION }];
              mode = schema ? "force-respond" : "force-answer";
            }
          }
          if (feedback) messages = [...messages, { role: "system", content: feedback }];
        }

        if (outcome === undefined) {
          record({ role: "assistant", content: STEP_LIMIT_ANSWER });
          outcome = { answer: STEP_LIMIT_ANSWER, finishReason: "max_steps" };
        }
        const final = outcome;

        if (config.hooks?.afterTurn) {
          const afterTurn = config.hooks.afterTurn;
          const transcript = messages;
          yield* ctx.activity(
            "after-turn",
            () =>
              afterTurn({
                task: input.task,
                answer: final.answer,
                messages: transcript,
                finishReason: final.finishReason,
                usage: { inputTokens: usage.inputTokens, outputTokens: usage.outputTokens },
                workflowId,
              }),
            { idempotent: true },
          );
        }

        const tokens = {
          inputTokens: usage.inputTokens,
          outputTokens: usage.outputTokens,
          ...(usage.cacheReadTokens > 0 && { cacheReadTokens: usage.cacheReadTokens }),
          ...(usage.cacheWriteTokens > 0 && { cacheWriteTokens: usage.cacheWriteTokens }),
        };
        const endedAt = yield* ctx.activity("turn-trace", async () => clock.currentTimeMs(), {
          idempotent: true,
        });
        const trace: TurnTrace = {
          workflowId,
          startedAt,
          endedAt,
          durationMs: endedAt - startedAt,
          finishReason: final.finishReason,
          steps: stepsTaken,
          usage: tokens,
          spend,
          ...(budget && {
            budget: { limits: budget, ...(budgetHit && { exceeded: budgetHit }) },
          }),
          spans: orderSpans(spans),
        };

        if (transcript) {
          // Everything after the user message `transcript-open` recorded,
          // the final assistant message carrying the turn's trace.
          const output = stampTrace({ messages: produced.slice(1), trace });
          yield* ctx.activity(
            "transcript-commit",
            () => transcript.commit({ workflowId, messages: output }),
            { idempotent: true },
          );
        }
        if (config.usageRecorder) {
          const recorder = config.usageRecorder;
          const turnSpend = spend;
          yield* ctx.activity(
            "turn-usage",
            () => recorder.record({ workflowId, usage: turnSpend }),
            { idempotent: true },
          );
        }
        const hit = budgetHit;
        yield* ctx.activity(
          "turn-end",
          async () => {
            if (final.finishReason === "max_steps") {
              emit({ type: "step_limit.hit", turn: 0, maxSteps });
            }
            if (hit) {
              emit({
                type: "budget.exceeded",
                turn: 0,
                step: hit.step,
                dimension: hit.dimension,
                limit: hit.limit,
                used: hit.used,
              });
            }
            if (final.finishReason === "cancelled") {
              emit({ type: "turn.aborted", turn: 0, reason: "signal" });
            } else {
              emit({
                type: "turn.end",
                turn: 0,
                answer: final.answer,
                durationMs: clock.currentTimeMs() - startedAt,
                tokens,
              });
            }
            lifecycle({
              workflowId,
              event: "done",
              from: "thinking",
              to: "idle",
              finishReason: final.finishReason,
            });
          },
          { idempotent: true },
        );

        const hasUsage = usage.inputTokens > 0 || usage.outputTokens > 0;
        const result: AgentResult = {
          answer: final.answer,
          ...(final.output !== undefined && { output: final.output }),
          messages,
          turnMessages: produced,
          steps: stepsTaken,
          finishReason: final.finishReason,
          ...(hasUsage && { usage: tokens }),
          spend,
          trace,
        };
        return result;
      }

      // ---- step helpers (closures over ctx) ----

      function* approve(params: {
        step: number;
        call: ToolCall;
      }): Generator<ActivityYield, ApprovalDecision, any> {
        const { step, call } = params;
        const prefix = `step-${step}-approval-${call.id}`;
        // The `-start` record carries the tool name + input:
        // `listPendingSignals` reads it to describe the pending approval.
        const onApprovalRequired = config.onApprovalRequired;
        const approvalIndex = onApprovalRequired ? undefined : config.approvalIndex;
        const requested = yield* ctx.activity(
          `${prefix}-start`,
          async () => {
            const requestedAt = clock.currentTimeMs();
            await approvalIndex?.requested({ workflowId, call });
            emit({
              type: "approval.requested",
              turn: 0,
              toolCallId: call.id,
              toolName: call.name,
              input: call.input,
            });
            if (config.metrics) {
              recordApproval({
                metrics: config.metrics,
                outcome: "requested",
                extraLabels: metricLabels,
              });
            }
            lifecycle({
              workflowId,
              event: "approval-required",
              from: "thinking",
              to: "waiting_approval",
              toolCallId: call.id,
            });
            // A thread turn names its thread, so signal routes can tell an
            // agent approval (decided through the agent) from a plain one.
            const scope = config.scope;
            const thread =
              approvalIndex?.thread ??
              (approvalIndex && scope?.namespaceId !== undefined && scope.threadId !== undefined
                ? {
                    namespaceId: scope.namespaceId,
                    threadId: scope.threadId,
                    ...(scope.agentId !== undefined && { agentId: scope.agentId }),
                  }
                : undefined);
            return {
              toolName: call.name,
              toolInput: call.input,
              requestedAt,
              ...(thread && { thread }),
            };
          },
          { idempotent: true },
        );
        const decision: ApprovalDecision = onApprovalRequired
          ? yield* ctx.activity(prefix, () => onApprovalRequired(call), { idempotent: true })
          : yield* ctx.approval(call.id);
        const decidedAt = yield* ctx.activity(
          `${prefix}-end`,
          async () => {
            emit({
              type: "approval.decision",
              turn: 0,
              toolCallId: call.id,
              approved: decision.approved,
            });
            if (config.metrics) {
              recordApproval({
                metrics: config.metrics,
                outcome: decision.approved ? "approved" : "rejected",
                extraLabels: metricLabels,
              });
            }
            lifecycle({
              workflowId,
              event: decision.approved ? "approved" : "rejected",
              from: "waiting_approval",
              to: "thinking",
              toolCallId: call.id,
            });
            return clock.currentTimeMs();
          },
          { idempotent: true },
        );
        spans.push({
          kind: "approval",
          step,
          toolCallId: call.id,
          toolName: call.name,
          approved: decision.approved,
          startedAt: requested.requestedAt,
          durationMs: decidedAt - requested.requestedAt,
        });
        return decision;
      }

      function* runTool(params: {
        step: number;
        call: ToolCall;
        entry: ToolSnapshotEntry;
        /** A sub-agent call's share of the turn's remaining budget. */
        childBudget?: TurnBudget;
      }): Generator<ActivityYield, { result: ToolResultMessage; spend?: BudgetUsage }, any> {
        const { step, call, entry } = params;
        if (entry.delegated) {
          const timeoutMs = timeoutFor(call);
          const delegated = yield* runDelegatedCall({
            ctx,
            name: `tool-${call.name}-${step}-${call.id}`,
            call,
            tools: () => resolveTools(config),
            frame,
            callerId: config.scope?.agentId ?? config.name,
            childWorkflowId: delegatedRunId({ workflowId, step, toolCallId: call.id }),
            ...(config.bus && { bus: config.bus }),
            ...(config.approvalIndex && { approvalIndex: config.approvalIndex }),
            ...(config.scope && { scope: config.scope }),
            ...(config.signal && { signal: config.signal }),
            ...(timeoutMs !== undefined && { timeoutMs }),
            ...(params.childBudget && { budget: params.childBudget }),
            clock,
            onStart: () => toolStarted({ step, call }),
            onEnd: ({ result, output, durationMs }) => {
              if (output !== undefined) config.onToolResult?.(call, output);
              toolEnded({ step, call, durationMs, failed: result.isError === true });
            },
          });
          spans.push({
            kind: "tool",
            step,
            name: call.name,
            toolCallId: call.id,
            failed: delegated.result.isError === true,
            delegated: true,
          });
          return delegated;
        }
        try {
          const ran = yield* ctx.activity(
            `tool-${call.name}-${step}-${call.id}`,
            () => timed(() => executeTool({ step, call })),
            { idempotent: entry.idempotent },
          );
          spans.push({
            kind: "tool",
            step,
            name: call.name,
            toolCallId: call.id,
            failed: ran.value.isError === true,
            startedAt: ran.startedAt,
            durationMs: ran.durationMs,
          });
          return { result: ran.value };
        } catch (err) {
          // A non-idempotent tool interrupted by a crash: don't re-run it,
          // tell the model the outcome is unknown. Replays land here again,
          // so the transcript is the same on every pass.
          if ((err as { _tag?: unknown } | null)?._tag === "AmbiguousActivityOutcome") {
            spans.push({ kind: "tool", step, name: call.name, toolCallId: call.id, failed: true });
            return { result: errorResult(call, OUTCOME_UNKNOWN_MESSAGE) };
          }
          throw err;
        }
      }
    })
    .build();

  /** Run `fn`, journaling its start time and duration next to its value. */
  /**
   * Summarise the steps being compacted (inside the `compact-<step>`
   * activity) and emit what the summary call cost: `llm.call` with
   * `purpose: "compaction"`, its metrics, the `compact` event, and — once
   * per turn — `compaction.ineffective`.
   */
  async function compactSteps(params: {
    step: number;
    steps: ReadonlyArray<Message>;
    previousSummary: string | undefined;
    tokens: number;
    after: number;
    kept: number;
    firstIneffective: boolean;
    fallbackMaxTokens: number;
  }): Promise<StepSummary & { readonly spend: BudgetUsage }> {
    const { step } = params;
    // A compaction recorded by a plan whose config is gone since: digest.
    const compaction = config.compaction ?? {
      maxTokens: params.fallbackMaxTokens,
      summarize: false,
    };
    const start = clock.currentTimeMs();
    const result = await summarizeSteps({
      steps: params.steps,
      previousSummary: params.previousSummary,
      config: compaction,
      llm: config.llm,
      retry: config.llmRetry ?? {},
      clock,
      ...(config.rateLimiter && { rateLimiter: config.rateLimiter }),
      ...(config.signal && { signal: config.signal }),
    });
    const durationMs = clock.currentTimeMs() - start;
    const provider = compaction.llmProvider ?? config.llmProvider;
    const model = compaction.llmModel ?? config.llmModel;
    if (result.summarized) {
      emit({
        type: "llm.call",
        turn: 0,
        step,
        purpose: "compaction",
        durationMs,
        ...(result.usage && { tokens: result.usage }),
      });
      if (config.metrics && provider && model) {
        recordChat({
          metrics: config.metrics,
          ...(config.costs && { costs: config.costs }),
          provider,
          model,
          response: {
            content: result.text,
            finishReason: "stop",
            ...(result.usage && { usage: result.usage }),
          },
          durationMs,
          extraLabels: { ...metricLabels, purpose: "compaction" },
        });
      }
    }
    emit({
      type: "compact",
      turn: 0,
      step,
      reason: "token_limit",
      tokens: params.tokens,
      compacted: params.steps.length,
      kept: params.kept,
    });
    if (params.firstIneffective) {
      emit({
        type: "compaction.ineffective",
        turn: 0,
        step,
        tokens: params.after,
        maxTokens: compaction.maxTokens,
      });
    }
    // Priced here, inside the journaled step, like a think step — at the
    // summary model's own rates. An unpriced summary model counts its
    // tokens and calls but no cost.
    const rates = ratesOf({ provider, model });
    if (result.summarized && !rates && configBudget?.maxCostUsd !== undefined) {
      console.warn(
        `[agentAction] compaction model ${provider ?? "?"}/${model ?? "?"} has no price: ` +
          "its summaries are not counted against maxCostUsd",
      );
    }
    const spend = result.summarized ? llmCallUsage({ usage: result.usage, rates }) : EMPTY_USAGE;
    return { ...result, spend };
  }

  /** Rates of `provider` / `model` in `config.costs`, when all three are known. */
  function ratesOf(params: {
    provider: string | undefined;
    model: string | undefined;
  }): ModelCostRates | undefined {
    const { provider, model } = params;
    return config.costs && provider && model ? config.costs.rates(provider, model) : undefined;
  }

  /** Rates of the turn's own model. */
  function mainRates(): ModelCostRates | undefined {
    return ratesOf({ provider: config.llmProvider, model: config.llmModel });
  }

  async function timed<T>(
    fn: () => Promise<T>,
  ): Promise<{ value: T; startedAt: number; durationMs: number }> {
    const startedAt = clock.currentTimeMs();
    const value = await fn();
    return { value, startedAt, durationMs: clock.currentTimeMs() - startedAt };
  }

  function autoApproved(call: ToolCall, entry: ToolSnapshotEntry): boolean {
    const live = resolveTools(config)[call.name];
    const toolDef: AgentTool<any, any> = live ?? {
      name: entry.name,
      description: "",
      parameters: undefined as never,
      requireApproval: entry.requireApproval,
      execute: async () => undefined,
    };
    return shouldAutoApprove(config.autoApprove, call, toolDef);
  }

  async function think(params: {
    step: number;
    messages: Message[];
    snapshot: ReadonlyArray<ToolSnapshotEntry>;
    roundMode: RoundMode;
    /** Output cap of this call: `maxTokens`, lowered to what the budget leaves. */
    maxTokens?: number;
    isReplay: boolean;
    workflowId: string;
  }): Promise<ThinkResult> {
    const { step, roundMode } = params;
    if (config.signal?.aborted) {
      return { content: null, finishReason: "stop", aborted: true, spend: EMPTY_USAGE };
    }
    const live = resolveTools(config);
    const offered = Object.fromEntries(
      params.snapshot.flatMap((e) => (live[e.name] ? [[e.name, live[e.name]!]] : [])),
    );
    const tools = [...buildToolDefs(offered), ...(respondDef ? [respondDef] : [])];
    const toolChoice: LLMToolChoice | undefined =
      tools.length === 0
        ? undefined
        : roundMode === "force-respond"
          ? { name: RESPOND_TOOL_NAME }
          : roundMode === "force-answer"
            ? "none"
            : undefined;
    // Anthropic rejects a forced tool choice with extended thinking on.
    const thinkingBudgetTokens =
      roundMode === "force-respond" ? undefined : config.thinkingBudgetTokens;
    const llmMessages = redactPriorThinking
      ? redactPriorThinkingBlocks(params.messages)
      : params.messages;

    const start = clock.currentTimeMs();
    const result = await callLlmWithRetry<RunLlmCallResult>({
      retry: config.llmRetry ?? {},
      onAborted: () => ({ content: null, finishReason: "stop", aborted: true }),
      clock,
      ...(config.signal && { signal: config.signal }),
      onRetry: (info) =>
        emit({
          type: "llm.retry",
          turn: 0,
          step,
          attempt: info.attempt,
          delayMs: info.delayMs,
          errorClass: info.errorClass,
          error: info.error instanceof Error ? info.error.message : String(info.error),
        }),
      run: () =>
        runLlmCall({
          llm: config.llm,
          messages: llmMessages,
          tools: tools.length > 0 ? tools : undefined,
          ...(toolChoice !== undefined && { toolChoice }),
          rateLimiter: config.rateLimiter,
          processors: config.processors,
          processorCtx: { step, turn: 0, workflowId: params.workflowId, isReplay: params.isReplay },
          thinkingBudgetTokens,
          ...(params.maxTokens !== undefined && { maxTokens: params.maxTokens }),
          ...(config.signal && { signal: config.signal }),
          clock,
          ...(config.metrics && { metrics: config.metrics }),
          ...(config.costs && { costs: config.costs }),
          ...(config.llmProvider && { provider: config.llmProvider }),
          ...(config.llmModel && { model: config.llmModel }),
          ...(config.metricLabels && { metricLabels: config.metricLabels }),
          onChunk: (delta) => {
            config.onChunk?.(delta);
            emit({ type: "token.delta", turn: 0, delta });
          },
          ...(config.onThinking && { onThinking: config.onThinking }),
        }),
    });
    emit({
      type: "llm.call",
      turn: 0,
      step,
      durationMs: clock.currentTimeMs() - start,
      ...(result.usage && { tokens: result.usage }),
    });
    // Tool-call ids name journal entries, approvals and child runs: a
    // provider that sends none or repeats them gets deterministic ones here,
    // inside the journaled think step.
    const normalized: RunLlmCallResult = result.toolCalls
      ? {
          ...result,
          toolCalls: uniqueToolCallIds({
            calls: result.toolCalls,
            earlier: params.messages,
            prefix: `call-${step}`,
          }),
        }
      : result;
    if (!normalized.aborted) {
      for (const call of normalized.toolCalls ?? []) config.onToolCall?.(call);
    }
    // Priced here, inside the journaled step: a later change of rates
    // can't change what a replay of this turn has spent.
    return { ...normalized, spend: llmCallUsage({ usage: result.usage, rates: mainRates() }) };
  }

  /** Per-call time limit: the tool's own, else the agent's default. */
  function timeoutFor(call: ToolCall): number | undefined {
    return resolveTools(config)[call.name]?.timeoutMs ?? config.toolTimeoutMs;
  }

  function toolStarted(params: { step: number; call: ToolCall }): void {
    const { step, call } = params;
    emit({
      type: "tool.start",
      turn: 0,
      step,
      toolCallId: call.id,
      name: call.name,
      input: call.input,
    });
  }

  function toolEnded(params: {
    step: number;
    call: ToolCall;
    durationMs: number;
    failed: boolean;
  }): void {
    const { step, call, durationMs, failed } = params;
    if (config.metrics) {
      recordTool({
        metrics: config.metrics,
        tool: call.name,
        durationMs,
        ok: !failed,
        extraLabels: metricLabels,
      });
    }
    emit({
      type: "tool.end",
      turn: 0,
      step,
      toolCallId: call.id,
      name: call.name,
      durationMs,
      failed,
    });
  }

  async function executeTool(params: { step: number; call: ToolCall }): Promise<ToolResultMessage> {
    const { step, call } = params;
    toolStarted({ step, call });
    const start = clock.currentTimeMs();
    // The live tool at execution time: a hot-reloaded tool runs its new code.
    const tool = resolveTools(config)[call.name];
    const result = tool
      ? await executeToolCall({
          call,
          tool,
          ...(config.scope && { scope: config.scope }),
          ...(config.toolAuditLogger && { toolAuditLogger: config.toolAuditLogger }),
          ...(config.signal && { signal: config.signal }),
          ...(timeoutFor(call) !== undefined && { timeoutMs: timeoutFor(call) }),
          clock,
          onProgress: (payload) =>
            emit({
              type: "tool.progress",
              turn: 0,
              step,
              toolCallId: call.id,
              name: call.name,
              payload,
            }),
          ...(config.onToolResult && { onResult: config.onToolResult }),
        })
      : errorResult(call, `Error: tool "${call.name}" is no longer available`);
    toolEnded({
      step,
      call,
      durationMs: clock.currentTimeMs() - start,
      failed: result.isError === true,
    });
    return result;
  }
}

/**
 * Steps a turn waits after a compaction that left it over budget before
 * compacting again: one summary per few steps instead of one per step.
 */
function ineffectiveBackoff(compaction: TurnCompactionConfig): number {
  return Math.max(4, (compaction.keepRecentSteps ?? 2) + 1);
}

function errorResult(call: ToolCall, content: string): ToolResultMessage {
  return { role: "tool", toolCallId: call.id, content, isError: true };
}

/** Error results for tool calls the engine chose not to run. */
function skippedResults(calls: ReadonlyArray<ToolCall>, reason: string): ToolResultMessage[] {
  return calls.map((call) => errorResult(call, reason));
}

/**
 * Spans in start order, independent of the order they were recorded in
 * (calls of one step finish in any order, and a replay records them in
 * another): by start time, then step, then compaction / think / approval /
 * tool, then
 * tool call id. A span without a start time sorts at its step's think.
 */
function orderSpans(spans: ReadonlyArray<TurnSpan>): TurnSpan[] {
  const thinkStart = new Map<number, number>();
  for (const span of spans) {
    if (span.kind === "think") thinkStart.set(span.step, span.startedAt);
  }
  const rank = { compaction: 0, think: 1, approval: 2, tool: 3 } as const;
  const at = (span: TurnSpan): number => span.startedAt ?? thinkStart.get(span.step) ?? 0;
  const callId = (span: TurnSpan): string => ("toolCallId" in span ? span.toolCallId : "");
  return [...spans].sort(
    (a, b) =>
      at(a) - at(b) ||
      a.step - b.step ||
      rank[a.kind] - rank[b.kind] ||
      (callId(a) < callId(b) ? -1 : callId(a) > callId(b) ? 1 : 0),
  );
}

/** The turn's output with its final assistant message stamped with `trace`. */
function stampTrace(params: {
  readonly messages: ReadonlyArray<Message>;
  readonly trace: TurnTrace;
}): Message[] {
  const out = [...params.messages];
  for (let i = out.length - 1; i >= 0; i--) {
    const m = out[i]!;
    if (m.role === "assistant") {
      out[i] = { ...m, metadata: { ...m.metadata, [TURN_TRACE_META_KEY]: params.trace } };
      break;
    }
  }
  return out;
}
