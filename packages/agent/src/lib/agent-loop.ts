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
  iterations: number;
  usage?: { inputTokens: number; outputTokens: number };
}

export interface IterationContext {
  iteration: number;
  messages: Message[];
  workflowId: string;
}

export interface AgentConfig {
  name: string;
  llm: LLMProvider;
  tools?: Record<string, AgentTool<unknown, unknown>>;
  maxIterations?: number;
  systemPrompt?: string;
  rateLimiter?: RateLimiter;
  clock?: Clock;
  onIteration?: (ctx: IterationContext) => { continue: boolean; feedback?: string } | void;
  onToolCall?: (call: ToolCall) => void;
  onToolResult?: (call: ToolCall, output: unknown) => void;
}

export class MaxIterationsError extends Error {
  readonly _tag = "MaxIterationsError";
  constructor(readonly maxIterations: number) {
    super(`Agent exceeded maximum iterations (${maxIterations})`);
  }
}

export function agentLoop(config: AgentConfig): Workflow<AgentInput, AgentResult> {
  const maxIterations = config.maxIterations ?? 20;
  const tools = config.tools ?? {};

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

      for (let iteration = 0; iteration < maxIterations; iteration++) {
        const response = yield* ctx.activity(`llm-${iteration}`, () => {
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

        if (config.onIteration) {
          const decision = config.onIteration({ iteration, messages, workflowId: ctx.workflowId });
          if (decision && !decision.continue) {
            return buildResult(
              response.content ?? "",
              messages,
              iteration + 1,
              totalInputTokens,
              totalOutputTokens,
            );
          }
        }

        if (!response.toolCalls || response.toolCalls.length === 0) {
          return buildResult(
            response.content ?? "",
            messages,
            iteration + 1,
            totalInputTokens,
            totalOutputTokens,
          );
        }

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

          const output = yield* ctx.activity(`tool-${call.name}-${iteration}-${call.id}`, () => {
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

      throw new MaxIterationsError(maxIterations);
    })
    .build();
}

function buildResult(
  answer: string,
  messages: Message[],
  iterations: number,
  inputTokens: number,
  outputTokens: number,
): AgentResult {
  return {
    answer,
    messages,
    iterations,
    usage: inputTokens > 0 || outputTokens > 0 ? { inputTokens, outputTokens } : undefined,
  };
}
