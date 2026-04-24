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
import type { MemoryStore, MemoryScope } from "./memory-store.ts";
import type { ProcessorsConfig } from "./processors.ts";
import type { Message, AssistantMessage, ToolResultMessage, ToolCall } from "./message.ts";
import {
  executeToolCall,
  runLlmCall,
  resolveTools,
  searchRelevantMemories,
} from "./agent-shared.ts";

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
  usage?: { inputTokens: number; outputTokens: number };
}

export interface StepContext {
  step: number;
  messages: Message[];
  workflowId: string;
}

export interface AgentActionMemoryConfig {
  store: MemoryStore;
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
   * Wraps the call in an activity so it is journaled (skipped on replay).
   * When omitted, approval falls back to `ctx.signal("approve:<id>")`.
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
  onStep?: (ctx: StepContext) => { continue: boolean; feedback?: string } | void;
  onToolCall?: (call: ToolCall) => void;
  onToolResult?: (call: ToolCall, output: unknown) => void;
  /** Called with each text delta as the LLM streams its response. Requires the provider to support chatStream. */
  onChunk?: (delta: string) => void;
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

  return workflow<AgentInput>({ name: config.name })
    .journaled("agent", function* (ctx, input) {
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

        const response = yield* ctx.activity(`think-${step}`, () =>
          runLlmCall({
            llm: config.llm,
            messages,
            tools: toolDefs.length > 0 ? toolDefs : undefined,
            rateLimiter: config.rateLimiter,
            processors: config.processors,
            processorCtx: { step, turn: 0, workflowId: ctx.workflowId },
            onChunk: config.onChunk,
          }),
        );

        if (response.usage) {
          totalInputTokens += response.usage.inputTokens;
          totalOutputTokens += response.usage.outputTokens;
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
            return buildResult(
              answer,
              messages,
              step + 1,
              totalInputTokens,
              totalOutputTokens,
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
            return buildResult(answer, messages, step + 1, totalInputTokens, totalOutputTokens);
          }
        }

        if (
          response.finishReason === "stop" ||
          !response.toolCalls ||
          response.toolCalls.length === 0
        ) {
          const answer = response.content ?? "";
          yield* maybeSaveMemory(answer);
          return buildResult(answer, messages, step + 1, totalInputTokens, totalOutputTokens);
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
            const decision: ApprovalDecision = config.onApprovalRequired
              ? yield* ctx.activity(`approval-${call.name}-${step}-${call.id}`, () =>
                  config.onApprovalRequired!(call),
                )
              : yield* ctx.signal<ApprovalDecision>(`approve:${call.id}`);
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
              ctx.activity(`tool-${call.name}-${step}-${call.id}`, () =>
                executeToolCall(call, toolDef, config.onToolResult),
              ),
            ),
          );
          toolResultMsgs.push(...results);
        }

        messages = [...messages, ...toolResultMsgs];
      }

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
  output?: unknown,
): AgentResult {
  return {
    answer,
    ...(output !== undefined ? { output } : {}),
    messages,
    steps,
    usage: inputTokens > 0 || outputTokens > 0 ? { inputTokens, outputTokens } : undefined,
  };
}
