import { workflow } from "@promin/workflow";
import type { Workflow } from "@promin/workflow";
import type { RateLimiter, Clock } from "@promin/core";
import { z } from "zod";
import type { LLMProvider } from "./llm-provider.ts";
import type { AgentTool, ApprovalDecision, AutoApprove } from "./tool.ts";
import { shouldAutoApprove } from "./tool.ts";
import type { ToolRegistry } from "./tool-registry.ts";
import { buildToolDefs } from "./tool-registry.ts";
import { zodToJsonSchema } from "./zod-to-json-schema.ts";
import type { MemoryIndex, MemoryScope } from "./memory-index.ts";
import type { ProcessorsConfig } from "./processors.ts";
import type { Message, AssistantMessage, ToolResultMessage, ToolCall } from "./message.ts";
import {
  executeToolCall,
  runLlmCall,
  resolveTools,
  searchRelevantMemories,
} from "./agent-shared.ts";
import type { SessionEventBus } from "./session-logger.ts";

export interface AgentInput {
  task: string;
  messages?: Message[];
}

export interface AgentResult {
  answer: string;
  /** Populated when outputSchema is set. The parsed, schema-validated output object. */
  output?: unknown;
  messages: Message[];
  steps: number;
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
}

export interface AgentActionMemoryConfig {
  store: MemoryIndex;
  /**
   * Scope for all read and write operations on this store.
   * Omit for the global namespace (same as pre-scoping behaviour).
   */
  scope?: MemoryScope;
  /**
   * How many memories to retrieve and inject before the first think step.
   * Default: 5. Set to 0 to disable injection.
   */
  injectLimit?: number;
  /**
   * Query for memory retrieval. Defaults to the task text.
   */
  searchQuery?: string;
  /**
   * Save the final answer as a memory entry. Default: false.
   */
  saveOnComplete?: boolean;
}

// biome-ignore lint/suspicious/noExplicitAny: outputSchema generic
export interface AgentActionConfig<TOutput = any> {
  name: string;
  llm: LLMProvider;
  // biome-ignore lint/suspicious/noExplicitAny: tool inputs are validated at runtime via Zod
  tools?: Record<string, AgentTool<any, any>>;
  toolRegistry?: ToolRegistry;
  autoApprove?: AutoApprove;
  /**
   * Async callback invoked when a tool with `requireApproval: true` needs a decision.
   * When omitted, approval falls back to `ctx.signal("approve:<id>")`.
   *
   * @replay
   * The decision is awaited inside a journaled activity — the approve/reject
   * RESULT survives replay. The CALLBACK ITSELF is skipped on replay, so
   * side effects (audit logs, notifications) do NOT re-fire. For durable
   * audit trails, use the persistent approval-storage primitive
   * (promin-2nh2) instead of relying on side effects in this callback.
   */
  onApprovalRequired?: (call: ToolCall) => Promise<ApprovalDecision>;
  maxSteps?: number;
  systemPrompt?: string;
  rateLimiter?: RateLimiter;
  clock?: Clock;
  memory?: AgentActionMemoryConfig;
  /**
   * When set, forces the LLM to return a structured response matching this Zod schema.
   * Uses tool-calling under the hood for maximum compatibility across providers.
   * result.output will be the parsed, type-safe value.
   */
  outputSchema?: z.ZodType<TOutput>;
  processors?: ProcessorsConfig;
  /**
   * Per-step callback. Return `{ continue: false }` to abort the agent's
   * step loop early; return `{ continue: true, feedback: '…' }` to inject
   * a system message before the next think.
   *
   * @replay
   * Runs inside a journaled activity — SKIPPED on replay. The decision
   * to continue/stop survives via the journaled result, but any side
   * effects in the callback body do NOT re-fire.
   */
  onStep?: (ctx: StepContext) => { continue: boolean; feedback?: string } | void;
  /**
   * Fires when the LLM emits a tool call.
   *
   * @replay
   * Runs inside the journaled think activity — SKIPPED on replay. Use for
   * UI updates and progress logging only.
   */
  onToolCall?: (call: ToolCall) => void;
  /**
   * Fires after a tool produces output.
   *
   * @replay
   * Runs inside the journaled tool-execute activity — SKIPPED on replay.
   * Same UI-only constraint as `onToolCall`.
   */
  onToolResult?: (call: ToolCall, output: unknown) => void;
  /**
   * Called with each text delta as the LLM streams its response. Requires the provider to
   * support chatStream.
   *
   * @replay
   * Streams are not re-emitted on replay — the journaled think activity
   * returns the complete response without re-streaming. Use for live UI
   * only; reconstruct full transcripts from the journaled response.
   */
  onChunk?: (delta: string) => void;
  /**
   * Multi-subscriber event bus. When provided, the agent emits structured
   * events (turn / llm.call / tool.start / tool.end / token.delta /
   * tool.progress / step_limit.hit) onto the bus as it runs. Multiple
   * subscribers can attach via bus.subscribe() — useful for cross-process
   * forwarders (e.g. WS relay shipping events from a Zorya worker to the
   * dashboard) plus in-process loggers / metrics simultaneously.
   *
   * The legacy onChunk / onToolCall / onToolResult callbacks still fire
   * when set; the bus is additive. Pass `new SessionEventBus()` from
   * `@promin/agent` and subscribe to it before calling
   * `runner.run({ workflow: agentAction({ bus, ... }), ... })`.
   *
   * agentAction is single-task / single-turn so all events carry turn=0.
   *
   * @replay
   * The journaled body re-runs from the journal on worker restart, but
   * activities are SKIPPED (their results come from the journal). The
   * bus does NOT re-emit lifecycle events on replay — only the first
   * execution emits.
   */
  bus?: SessionEventBus;
}

export class MaxStepsError extends Error {
  readonly _tag = "MaxStepsError";
  constructor(readonly maxSteps: number) {
    super(`Agent exceeded maximum steps (${maxSteps})`);
  }
}

export class StructuredOutputParseError extends Error {
  readonly _tag = "StructuredOutputParseError";
  constructor(override readonly cause: unknown) {
    super(`Agent structured output failed schema validation: ${String(cause)}`);
  }
}

// ---- overloads for type narrowing ----

/**
 * Build a single-shot agent workflow.
 *
 * Runs a think → tool → think loop up to `maxSteps` times and returns the agent's
 * final answer (plus structured output when `outputSchema` is set). Journaled via
 * `@promin/workflow`, so it is crash-safe and replayable.
 *
 * Use `agentLoop` instead when you need a persistent interactive session with
 * multiple user turns, streaming, or manual context compaction.
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
): Workflow<AgentInput, AgentResult & { output: TOutput }>;
export function agentAction(
  config: AgentActionConfig<never> & { outputSchema?: undefined },
): Workflow<AgentInput, AgentResult>;
export function agentAction(
  // biome-ignore lint/suspicious/noExplicitAny: overload implementation accepts both forms
  config: AgentActionConfig<any>,
): Workflow<AgentInput, AgentResult> {
  const maxSteps = config.maxSteps ?? 20;

  const bus = config.bus;

  return workflow<AgentInput>({ name: config.name })
    .journaled("agent", function* (ctx, input) {
      bus?.emit({ type: "turn.start", turn: 0, task: input.task });
      const startMs = Date.now();
      let messages: Message[] = [
        ...(config.systemPrompt ? [{ role: "system" as const, content: config.systemPrompt }] : []),
        ...(input.messages ?? []),
      ];

      // Inject structured output instruction when outputSchema is set
      if (config.outputSchema) {
        messages = [
          ...messages,
          {
            role: "system" as const,
            content:
              "When you have gathered all needed information and are ready to respond, " +
              "call the `_respond` tool with your final structured answer. " +
              "Do not produce a text reply — always use the `_respond` tool for your final answer.",
          },
        ];
      }

      // Inject relevant memories before the first think step
      if (config.memory && (config.memory.injectLimit ?? 5) > 0) {
        const memMsgs = yield* ctx.activity("inject-memories", () =>
          searchRelevantMemories(config.memory!, input.task),
        );
        messages = [...messages, ...memMsgs];
      }

      messages = [...messages, { role: "user" as const, content: input.task }];

      let totalInputTokens = 0;
      let totalOutputTokens = 0;
      let totalCacheReadTokens = 0;
      let totalCacheWriteTokens = 0;
      let structuredOutput: unknown;

      function* maybeSaveMemory(answer: string) {
        if (config.memory?.saveOnComplete) {
          yield* ctx.activity("save-memory", () =>
            config.memory!.store.save(
              { content: answer, metadata: { task: input.task } },
              config.memory!.scope,
            ),
          );
        }
      }

      for (let step = 0; step < maxSteps; step++) {
        // Resolve tools fresh each step so a toolRegistry update is visible immediately
        const toolMap = resolveTools(config);
        const toolDefs = buildToolDefs(toolMap);

        // Inject synthetic _respond tool for structured output
        if (config.outputSchema) {
          toolDefs.push({
            name: "_respond",
            description:
              "Provide your final structured response. Call this when you have all the information needed.",
            parameters: zodToJsonSchema(config.outputSchema),
          });
        }

        const response = yield* ctx.activity(`think-${step}`, async () => {
          const llmStart = Date.now();
          // Tee tokens to both the legacy onChunk callback (back-compat)
          // and the bus as token.delta events (transient — never journaled).
          const result = await runLlmCall({
            llm: config.llm,
            messages,
            tools: toolDefs.length > 0 ? toolDefs : undefined,
            rateLimiter: config.rateLimiter,
            processors: config.processors,
            processorCtx: { step, turn: 0, workflowId: ctx.workflowId },
            onChunk: (delta) => {
              config.onChunk?.(delta);
              bus?.emit({ type: "token.delta", turn: 0, delta });
            },
          });
          bus?.emit({
            type: "llm.call",
            turn: 0,
            step,
            durationMs: Date.now() - llmStart,
            tokens: result.usage,
          });
          return result;
        });

        if (response.usage) {
          totalInputTokens += response.usage.inputTokens;
          totalOutputTokens += response.usage.outputTokens;
          totalCacheReadTokens += response.usage.cacheReadTokens ?? 0;
          totalCacheWriteTokens += response.usage.cacheWriteTokens ?? 0;
        }

        // Intercept _respond tool call for structured output
        if (config.outputSchema && response.toolCalls) {
          const respondCall = response.toolCalls.find((c) => c.name === "_respond");
          if (respondCall) {
            try {
              structuredOutput = config.outputSchema.parse(respondCall.input);
            } catch (err) {
              throw new StructuredOutputParseError(err);
            }
            const answer = JSON.stringify(respondCall.input);
            messages = [
              ...messages,
              { role: "assistant" as const, content: null, toolCalls: response.toolCalls },
            ];
            yield* maybeSaveMemory(answer);
            bus?.emit({
              type: "turn.end",
              turn: 0,
              answer,
              durationMs: Date.now() - startMs,
              tokens: {
                inputTokens: totalInputTokens,
                outputTokens: totalOutputTokens,
                cacheReadTokens: totalCacheReadTokens,
                cacheWriteTokens: totalCacheWriteTokens,
              },
            });
            return buildResult(
              answer,
              messages,
              step + 1,
              totalInputTokens,
              totalOutputTokens,
              totalCacheReadTokens,
              totalCacheWriteTokens,
              structuredOutput,
            );
          }
        }

        const assistantMsg: AssistantMessage = {
          role: "assistant",
          content: response.content,
          toolCalls: response.toolCalls,
        };
        messages = [...messages, assistantMsg];

        if (config.onStep) {
          const decision = config.onStep({ step, messages, workflowId: ctx.workflowId });
          if (decision && !decision.continue) {
            const answer = response.content ?? "";
            yield* maybeSaveMemory(answer);
            bus?.emit({
              type: "turn.end",
              turn: 0,
              answer,
              durationMs: Date.now() - startMs,
              tokens: {
                inputTokens: totalInputTokens,
                outputTokens: totalOutputTokens,
                cacheReadTokens: totalCacheReadTokens,
                cacheWriteTokens: totalCacheWriteTokens,
              },
            });
            return buildResult(
              answer,
              messages,
              step + 1,
              totalInputTokens,
              totalOutputTokens,
              totalCacheReadTokens,
              totalCacheWriteTokens,
            );
          }
        }

        if (
          response.finishReason === "stop" ||
          !response.toolCalls ||
          response.toolCalls.length === 0
        ) {
          const answer = response.content ?? "";
          yield* maybeSaveMemory(answer);
          bus?.emit({
            type: "turn.end",
            turn: 0,
            answer,
            durationMs: Date.now() - startMs,
            tokens: {
              inputTokens: totalInputTokens,
              outputTokens: totalOutputTokens,
              cacheReadTokens: totalCacheReadTokens,
              cacheWriteTokens: totalCacheWriteTokens,
            },
          });
          return buildResult(
            answer,
            messages,
            step + 1,
            totalInputTokens,
            totalOutputTokens,
            totalCacheReadTokens,
            totalCacheWriteTokens,
          );
        }

        const toolResultMsgs: ToolResultMessage[] = [];
        // biome-ignore lint/suspicious/noExplicitAny: Zod validates input at runtime
        const toExecute: Array<{ call: ToolCall; toolDef: AgentTool<any, any> }> = [];

        // Phase 1: resolve approvals sequentially (each may need a user signal)
        for (const call of response.toolCalls) {
          config.onToolCall?.(call);

          const toolDef = toolMap[call.name];
          if (!toolDef) {
            toolResultMsgs.push({
              role: "tool",
              toolCallId: call.id,
              content: `Error: unknown tool "${call.name}"`,
            });
            continue;
          }

          if (toolDef.requireApproval && !shouldAutoApprove(config.autoApprove, call, toolDef)) {
            bus?.emit({
              type: "approval.requested",
              turn: 0,
              toolCallId: call.id,
              toolName: call.name,
            });
            const decision: ApprovalDecision = config.onApprovalRequired
              ? yield* ctx.activity(`approval-${call.name}-${step}-${call.id}`, () =>
                  config.onApprovalRequired!(call),
                )
              : yield* ctx.signal<ApprovalDecision>(`approve:${call.id}`);
            bus?.emit({
              type: "approval.decision",
              turn: 0,
              toolCallId: call.id,
              approved: decision.approved,
            });
            if (!decision.approved) {
              toolResultMsgs.push({
                role: "tool",
                toolCallId: call.id,
                content: `Rejected: ${decision.reason ?? "user rejected"}`,
              });
              continue;
            }
          }

          toExecute.push({ call, toolDef });
        }

        // Phase 2: run all approved tools in parallel, each individually journaled
        if (toExecute.length > 0) {
          const results = yield* ctx.parallel(
            toExecute.map(({ call, toolDef }) =>
              ctx.activity(`tool-${call.name}-${step}-${call.id}`, async () => {
                bus?.emit({
                  type: "tool.start",
                  turn: 0,
                  step,
                  name: call.name,
                  input: call.input,
                });
                const toolStart = Date.now();
                let failed = false;
                try {
                  const out = await executeToolCall(call, toolDef, config.onToolResult);
                  return out;
                } catch (err) {
                  failed = true;
                  throw err;
                } finally {
                  bus?.emit({
                    type: "tool.end",
                    turn: 0,
                    step,
                    name: call.name,
                    durationMs: Date.now() - toolStart,
                    failed,
                  });
                }
              }),
            ),
          );
          toolResultMsgs.push(...results);
        }

        messages = [...messages, ...toolResultMsgs];
      }

      bus?.emit({ type: "step_limit.hit", turn: 0, maxSteps });
      throw new MaxStepsError(maxSteps);
    })
    .build();
}

function buildResult(
  answer: string,
  messages: Message[],
  steps: number,
  inputTokens: number,
  outputTokens: number,
  cacheReadTokens: number,
  cacheWriteTokens: number,
  output?: unknown,
): AgentResult {
  const hasUsage = inputTokens > 0 || outputTokens > 0;
  return {
    answer,
    ...(output !== undefined ? { output } : {}),
    messages,
    steps,
    usage: hasUsage
      ? {
          inputTokens,
          outputTokens,
          ...(cacheReadTokens > 0 ? { cacheReadTokens } : {}),
          ...(cacheWriteTokens > 0 ? { cacheWriteTokens } : {}),
        }
      : undefined,
  };
}
