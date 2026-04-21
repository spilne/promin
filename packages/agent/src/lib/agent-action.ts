import { workflow } from "@promin/workflow";
import type { Workflow } from "@promin/workflow";
import type { RateLimiter, Clock } from "@promin/core";
import type { LLMProvider, LLMToolDefinition } from "./llm-provider.ts";
import type { AgentTool, ApprovalDecision } from "./tool.ts";
import type { Message, AssistantMessage, ToolResultMessage, ToolCall } from "./message.ts";
import { zodToJsonSchema } from "./zod-to-json-schema.ts";

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

export interface AgentActionConfig {
  name: string;
  llm: LLMProvider;
  // biome-ignore lint/suspicious/noExplicitAny: tool inputs are validated at runtime via Zod
  tools?: Record<string, AgentTool<any, any>>;
  maxSteps?: number;
  systemPrompt?: string;
  rateLimiter?: RateLimiter;
  clock?: Clock;
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
  // biome-ignore lint/suspicious/noExplicitAny: tool inputs are validated at runtime via Zod
  const tools: Record<string, AgentTool<any, any>> = config.tools ?? {};

  const llmToolDefs: LLMToolDefinition[] = Object.entries(tools).map(([name, t]) => ({
    name,
    description: t.description,
    parameters: zodToJsonSchema(t.parameters),
  }));

  return workflow<AgentInput>({ name: config.name })
    .journaled("agent", function* (ctx, input) {
      let messages: Message[] = [
        ...(config.systemPrompt ? [{ role: "system" as const, content: config.systemPrompt }] : []),
        ...(input.messages ?? []),
        { role: "user" as const, content: input.task },
      ];

      let totalInputTokens = 0;
      let totalOutputTokens = 0;

      for (let step = 0; step < maxSteps; step++) {
        // Agent thinks — response is journaled, so this IS the branch decision on replay
        const response = yield* ctx.activity(`think-${step}`, () => {
          const call = () =>
            config.llm.chat({
              messages,
              tools: llmToolDefs.length > 0 ? llmToolDefs : undefined,
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
            return buildResult(
              response.content ?? "",
              messages,
              step + 1,
              totalInputTokens,
              totalOutputTokens,
            );
          }
        }

        // Agent decided: done
        if (
          response.finishReason === "stop" ||
          !response.toolCalls ||
          response.toolCalls.length === 0
        ) {
          return buildResult(
            response.content ?? "",
            messages,
            step + 1,
            totalInputTokens,
            totalOutputTokens,
          );
        }

        // Agent decided: use tools
        const toolResultMsgs: ToolResultMessage[] = [];

        for (const call of response.toolCalls) {
          config.onToolCall?.(call);

          const toolDef = tools[call.name];
          if (!toolDef) {
            toolResultMsgs.push({
              role: "tool",
              toolCallId: call.id,
              content: `Error: unknown tool "${call.name}"`,
            });
            continue;
          }

          if (toolDef.requireApproval) {
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
            return toolDef.execute(parsed);
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
