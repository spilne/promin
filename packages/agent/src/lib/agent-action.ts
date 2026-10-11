// ---------------------------------------------------------------------------
// agentAction — the durable turn engine. One journaled workflow runs one
// agent turn: think → (approve) → tools → think … until the model answers,
// and every side effect sits behind a journaled activity so a crash, an
// approval suspend or a worker restart resumes the turn from the journal.
//
// Journal layout of one turn (activity names):
//
//   turn-start                         start time; emits turn.start
//   before-turn                        hooks.beforeTurn
//   tools-<step>                       tool-set snapshot (name, requireApproval, idempotent)
//   think-<step>                       one LLM call, retried per llmRetry
//   step-<step>-hook                   onStep decision
//   step-<step>-approvals              which calls need approval (autoApprove verdicts)
//   step-<step>-approval-<id>-start    emits approval.requested (tool name + input)
//   step-<step>-approval-<id>          onApprovalRequired decision (or the approve:<id> signal)
//   step-<step>-approval-<id>-end      emits approval.decision
//   tool-<name>-<step>-<id>            one tool call, inside ctx.parallel
//   stall-<step>                       emits stall.detected
//   after-turn                         hooks.afterTurn
//   turn-end                           emits turn.end / turn.aborted / step_limit.hit
//   turn-failed                        emits turn.failed (only when the turn throws)
//
// Replay contract: control flow between activities depends only on the
// input and journaled values, and every event / hook / metric fires inside
// the activity that produced it, so nothing is emitted twice for one
// journal position. Time comes from the injected WallClock, inside
// activities only.
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
  redactPriorThinkingBlocks,
  resolveTools,
  runLlmCall,
} from "./agent-shared.ts";
import type { LLMProvider, LLMToolChoice, LLMToolDefinition } from "./llm-provider.ts";
import type { Message, AssistantMessage, ToolResultMessage, ToolCall } from "./message.ts";
import { recordApproval, recordTool } from "./metrics/instrument.ts";
import type { AgentMetrics, ModelCostRegistry } from "./metrics/types.ts";
import type { ProcessorsConfig } from "./processors.ts";
import type { SessionEventBus } from "./session-logger.ts";
import { StallDetector, type StallDetectorOptions } from "./stall-detector.ts";
import type { ToolAuditLogger } from "./tool-audit/types.ts";
import { zodToToolJsonSchema } from "./tool-json-schema.ts";
import type { AgentTool, ApprovalDecision, AutoApprove, ToolScope } from "./tool.ts";
import { shouldAutoApprove } from "./tool.ts";
import type { ToolRegistry } from "./tool-registry.ts";
import { buildToolDefs } from "./tool-registry.ts";

export interface AgentInput {
  task: string;
  messages?: Message[];
}

/**
 * Why a turn ended:
 * - `stop` — the model answered (or `onStep` stopped the turn).
 * - `max_steps` — `maxSteps` think steps ran without an answer; the answer
 *   is `STEP_LIMIT_ANSWER`.
 * - `stalled` — the stall detector forced a final answer.
 * - `cancelled` — the turn's `signal` aborted.
 */
export type AgentFinishReason = "stop" | "max_steps" | "stalled" | "cancelled";

export interface AgentResult<TOutput = unknown> {
  answer: string;
  /**
   * The schema-validated `_respond` payload when `outputSchema` is set and
   * the turn finished with `stop`; undefined otherwise.
   */
  output?: TOutput;
  /**
   * The turn's transcript. Always ends with an assistant message and has a
   * tool result for every tool call, so it can seed the next turn.
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
}

export interface StepContext {
  step: number;
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
    messages: Message[];
    finishReason: AgentFinishReason;
    usage: { inputTokens: number; outputTokens: number };
    workflowId: string;
  }) => Promise<void>;
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
  /** Per-model USD rates for llm.cost.usd. */
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
 * `AgentActionConfig` minus the per-run wiring (`bus`, `scope`, `signal`).
 */
export type AgentEngineConfig<TOutput = any> = Omit<
  AgentActionConfig<TOutput>,
  "bus" | "scope" | "signal"
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

const STRUCTURED_OUTPUT_INSTRUCTION =
  "When you have gathered all needed information and are ready to respond, " +
  `call the \`${RESPOND_TOOL_NAME}\` tool with your final structured answer. ` +
  `Do not produce a text reply — always use the \`${RESPOND_TOOL_NAME}\` tool for your final answer.`;

const STALL_INSTRUCTION =
  "You are repeating tool calls without making progress. Stop calling tools and give your " +
  "final answer now from what you already have. If you cannot complete the task, say what " +
  "is blocking you.";

const RESPOND_NUDGE = `Call the \`${RESPOND_TOOL_NAME}\` tool with your final answer.`;

/**
 * Engine control flow (suspension, continue-as-new, lock loss, journal
 * drift, ambiguous outcome) passes through a turn untouched: it is not a
 * failed turn.
 */
function isEngineControl(err: unknown): boolean {
  const tag = (err as { _tag?: unknown } | null)?._tag;
  return (
    typeof tag === "string" &&
    /^(Workflow|Journal|FenceToken|AmbiguousActivityOutcome|Storage|Checkpoint)/.test(tag)
  );
}

const OUTCOME_UNKNOWN_MESSAGE =
  "Tool outcome unknown: the process stopped while this tool was running, and the tool is " +
  "not marked idempotent, so it was not run again. It may or may not have taken effect — " +
  "check before retrying.";

/** One tool as journaled in a step's snapshot. */
interface ToolSnapshotEntry {
  readonly name: string;
  readonly requireApproval: boolean;
  readonly idempotent: boolean;
}

type RoundMode = "normal" | "force-respond" | "force-answer";

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
  const maxSteps = config.maxSteps ?? 20;
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
      const startedAt = yield* ctx.activity(
        "turn-start",
        async () => {
          emit({ type: "turn.start", turn: 0, task: input.task });
          lifecycle({ workflowId, event: "message", from: "idle", to: "thinking" });
          return clock.currentTimeMs();
        },
        { idempotent: true },
      );

      try {
        return yield* turnBody();
      } catch (err) {
        if (isEngineControl(err)) throw err;
        const error = err instanceof Error ? err.message : String(err);
        // Recorded as a failure, not a success: when the step is discarded
        // and run again, the journal drops it with the turn's other failed
        // entries instead of leaving a "turn-failed" slot the re-run can't match.
        yield* ctx.activity(
          "turn-failed",
          async (): Promise<void> => {
            emit({ type: "turn.failed", turn: 0, error });
            lifecycle({ workflowId, event: "failed", from: "thinking", to: "idle" });
            throw err;
          },
          { idempotent: true },
        );
        throw err;
      }

      function* turnBody(): Generator<ActivityYield, AgentResult, any> {
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

        for (let step = 0; step < maxSteps && outcome === undefined; step++) {
          stepsTaken = step + 1;

          // ---- tool-set snapshot: control flow below reads only this ----
          const snapshot = yield* ctx.activity(
            `tools-${step}`,
            async () =>
              Object.entries(resolveTools(config)).map(
                ([name, t]): ToolSnapshotEntry => ({
                  name,
                  requireApproval: t.requireApproval === true,
                  idempotent: t.idempotent === true,
                }),
              ),
            { idempotent: true },
          );
          const snapshotByName = new Map(snapshot.map((e) => [e.name, e]));

          // ---- think ----
          const roundMode: RoundMode =
            mode === "normal" && respondDef && snapshot.length === 0 ? "force-respond" : mode;
          const conversation = messages;
          const response = yield* ctx.activity(
            `think-${step}`,
            () =>
              think({
                step,
                messages: conversation,
                snapshot,
                roundMode,
                isReplay: ctx.isReplay,
                workflowId,
              }),
            { idempotent: true },
          );
          if (response.usage) {
            usage.inputTokens += response.usage.inputTokens;
            usage.outputTokens += response.usage.outputTokens;
            usage.cacheReadTokens += response.usage.cacheReadTokens ?? 0;
            usage.cacheWriteTokens += response.usage.cacheWriteTokens ?? 0;
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
            const outputs = yield* ctx.parallel(
              toRun.map(({ call, entry }) => runTool({ step, call, entry })),
            );
            toRun.forEach(({ index }, k) => {
              results[index] = outputs[k]!;
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
        yield* ctx.activity(
          "turn-end",
          async () => {
            if (final.finishReason === "max_steps") {
              emit({ type: "step_limit.hit", turn: 0, maxSteps });
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
        yield* ctx.activity(
          `${prefix}-start`,
          async () => {
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
            return { toolName: call.name, toolInput: call.input };
          },
          { idempotent: true },
        );
        const onApprovalRequired = config.onApprovalRequired;
        const decision: ApprovalDecision = onApprovalRequired
          ? yield* ctx.activity(prefix, () => onApprovalRequired(call), { idempotent: true })
          : yield* ctx.approval(call.id);
        yield* ctx.activity(
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
          },
          { idempotent: true },
        );
        return decision;
      }

      function* runTool(params: {
        step: number;
        call: ToolCall;
        entry: ToolSnapshotEntry;
      }): Generator<ActivityYield, ToolResultMessage, ToolResultMessage> {
        const { step, call, entry } = params;
        try {
          return yield* ctx.activity(
            `tool-${call.name}-${step}-${call.id}`,
            () => executeTool({ step, call }),
            { idempotent: entry.idempotent },
          );
        } catch (err) {
          // A non-idempotent tool interrupted by a crash: don't re-run it,
          // tell the model the outcome is unknown. Replays land here again,
          // so the transcript is the same on every pass.
          if ((err as { _tag?: unknown } | null)?._tag === "AmbiguousActivityOutcome") {
            return errorResult(call, OUTCOME_UNKNOWN_MESSAGE);
          }
          throw err;
        }
      }
    })
    .build();

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
    isReplay: boolean;
    workflowId: string;
  }): Promise<RunLlmCallResult> {
    const { step, roundMode } = params;
    if (config.signal?.aborted) {
      return { content: null, finishReason: "stop", aborted: true };
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
          ...(config.maxTokens !== undefined && { maxTokens: config.maxTokens }),
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
    if (!result.aborted) {
      for (const call of result.toolCalls ?? []) config.onToolCall?.(call);
    }
    return result;
  }

  async function executeTool(params: { step: number; call: ToolCall }): Promise<ToolResultMessage> {
    const { step, call } = params;
    emit({
      type: "tool.start",
      turn: 0,
      step,
      toolCallId: call.id,
      name: call.name,
      input: call.input,
    });
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
          ...((tool.timeoutMs ?? config.toolTimeoutMs) !== undefined && {
            timeoutMs: tool.timeoutMs ?? config.toolTimeoutMs,
          }),
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
    const durationMs = clock.currentTimeMs() - start;
    const failed = result.isError === true;
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
    return result;
  }
}

function errorResult(call: ToolCall, content: string): ToolResultMessage {
  return { role: "tool", toolCallId: call.id, content, isError: true };
}

/** Error results for tool calls the engine chose not to run. */
function skippedResults(calls: ReadonlyArray<ToolCall>, reason: string): ToolResultMessage[] {
  return calls.map((call) => errorResult(call, reason));
}
