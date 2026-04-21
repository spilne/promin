import { workflow } from "@promin/workflow";
import type { Workflow } from "@promin/workflow";
import type { RateLimiter, Clock } from "@promin/core";
import type { LLMProvider } from "./llm-provider.ts";
import type { AgentTool, ApprovalDecision, AutoApprove } from "./tool.ts";
import { shouldAutoApprove } from "./tool.ts";
import type { ToolRegistry } from "./tool-registry.ts";
import { buildToolDefs } from "./tool-registry.ts";
import type { MemoryStore, MemoryScope } from "./memory-store.ts";
import type { Message, AssistantMessage, ToolResultMessage, ToolCall } from "./message.ts";

export interface AgentInput {
  task: string;
  messages?: Message[];
}

export interface AgentResult {
  answer: string;
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

export interface AgentActionConfig {
  name: string;
  llm: LLMProvider;
  // biome-ignore lint/suspicious/noExplicitAny: tool inputs are validated at runtime via Zod
  tools?: Record<string, AgentTool<any, any>>;
  toolRegistry?: ToolRegistry;
  autoApprove?: AutoApprove;
  maxSteps?: number;
  systemPrompt?: string;
  rateLimiter?: RateLimiter;
  clock?: Clock;
  memory?: AgentActionMemoryConfig;
  onStep?: (ctx: StepContext) => { continue: boolean; feedback?: string } | void;
  onToolCall?: (call: ToolCall) => void;
  onToolResult?: (call: ToolCall, output: unknown) => void;
}

export class MaxStepsError extends Error {
  readonly _tag = "MaxStepsError";
  constructor(readonly maxSteps: number) {
    super(`Agent exceeded maximum steps (${maxSteps})`);
  }
}

export function agentAction(config: AgentActionConfig): Workflow<AgentInput, AgentResult> {
  const maxSteps = config.maxSteps ?? 20;

  return workflow<AgentInput>({ name: config.name })
    .journaled("agent", function* (ctx, input) {
      let messages: Message[] = [
        ...(config.systemPrompt ? [{ role: "system" as const, content: config.systemPrompt }] : []),
        ...(input.messages ?? []),
      ];

      // Inject relevant memories before the first think step
      if (config.memory && (config.memory.injectLimit ?? 5) > 0) {
        const memories = yield* ctx.activity("inject-memories", () =>
          config.memory!.store.search(
            config.memory!.searchQuery ?? input.task,
            config.memory!.injectLimit ?? 5,
            config.memory!.scope,
          ),
        );
        if (memories.length > 0) {
          const block = memories.map((m) => `- ${m.content}`).join("\n");
          messages = [
            ...messages,
            { role: "system" as const, content: `Relevant context from memory:\n${block}` },
          ];
        }
      }

      messages = [...messages, { role: "user" as const, content: input.task }];

      let totalInputTokens = 0;
      let totalOutputTokens = 0;

      for (let step = 0; step < maxSteps; step++) {
        // Resolve tools fresh each step so a toolRegistry update is visible immediately
        const toolMap = config.toolRegistry?.getTools() ?? config.tools ?? {};
        const toolDefs = buildToolDefs(toolMap);

        const response = yield* ctx.activity(`think-${step}`, () => {
          const call = () =>
            config.llm.chat({
              messages,
              tools: toolDefs.length > 0 ? toolDefs : undefined,
            });
          return config.rateLimiter ? config.rateLimiter.withLimitAsync(call) : call();
        });

        if (response.usage) {
          totalInputTokens += response.usage.inputTokens;
          totalOutputTokens += response.usage.outputTokens;
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
            if (config.memory?.saveOnComplete) {
              yield* ctx.activity("save-memory", () =>
                config.memory!.store.save(
                  { content: answer, metadata: { task: input.task } },
                  config.memory!.scope,
                ),
              );
            }
            return buildResult(answer, messages, step + 1, totalInputTokens, totalOutputTokens);
          }
        }

        if (
          response.finishReason === "stop" ||
          !response.toolCalls ||
          response.toolCalls.length === 0
        ) {
          const answer = response.content ?? "";
          if (config.memory?.saveOnComplete) {
            yield* ctx.activity("save-memory", () =>
              config.memory!.store.save(
                { content: answer, metadata: { task: input.task } },
                config.memory!.scope,
              ),
            );
          }
          return buildResult(answer, messages, step + 1, totalInputTokens, totalOutputTokens);
        }

        const toolResultMsgs: ToolResultMessage[] = [];

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
            const decision = yield* ctx.signal<ApprovalDecision>(`approve:${call.id}`);
            if (!decision.approved) {
              toolResultMsgs.push({
                role: "tool",
                toolCallId: call.id,
                content: `Rejected: ${decision.reason ?? "user rejected"}`,
              });
              continue;
            }
          }

          const output = yield* ctx.activity(`tool-${call.name}-${step}-${call.id}`, () => {
            const parsed = toolDef.parameters.parse(call.input);
            // biome-ignore lint/suspicious/noExplicitAny: Zod validates input at runtime
            return toolDef.execute(parsed as any);
          });

          config.onToolResult?.(call, output);

          const content = toolDef.toModelOutput
            ? toolDef.toModelOutput(output)
            : typeof output === "string"
              ? output
              : JSON.stringify(output);

          toolResultMsgs.push({ role: "tool", toolCallId: call.id, content });
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
): AgentResult {
  return {
    answer,
    messages,
    steps,
    usage: inputTokens > 0 || outputTokens > 0 ? { inputTokens, outputTokens } : undefined,
  };
}
