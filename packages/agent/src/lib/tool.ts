import type { z } from "zod";

/**
 * Free-form progress sink available to long-running tools. When the
 * agent loop runs the tool, it injects a writer that turns each
 * `write(payload)` into a `tool.progress` SessionEvent labeled with
 * the current turn / step / toolCallId. Live consumers (SSE forwarders,
 * dashboards) get incremental progress without the tool having to
 * know about the event bus.
 *
 * Tools that don't care about progress just ignore the second
 * argument — the writer is always optional and `payload` is opaque.
 */
export interface ToolWriter {
  write(payload: unknown): void;
}

/** Per-call execution context handed to `tool.execute` as an optional second arg. */
export interface ToolExecuteContext {
  /**
   * Progress sink. Always present when the tool is invoked through the
   * agent loop with an event bus; absent in unit tests that call
   * `execute` directly. Tool authors should treat as optional via `?.`.
   */
  readonly writer?: ToolWriter;
}

export interface AgentTool<TInput = unknown, TOutput = unknown> {
  /** Stable identifier — used as the LLM tool name and registry key. */
  name: string;
  /** What the tool does. Sent verbatim to the LLM. */
  description: string;
  /** When and how to use this tool vs alternatives. Appended to the LLM description. */
  usage?: string;
  /** Few-shot examples shown to the LLM alongside the description. */
  examples?: Array<{ input: TInput; output: string }>;
  parameters: z.ZodType<TInput>;
  /**
   * Run the tool. The optional `ctx` carries per-call wiring like a
   * progress writer; tools that ignore `ctx` keep working unchanged
   * — backwards-compat with the original single-arg signature.
   */
  execute: (input: TInput, ctx?: ToolExecuteContext) => Promise<TOutput>;
  requireApproval?: boolean;
  toModelOutput?: (output: TOutput) => string;
}

/**
 * Identity helper that infers `TInput` and `TOutput` from the Zod `parameters`
 * schema and the `execute` return type, giving full type-safety on the callback.
 *
 * @example
 * ```ts
 * const myTool = tool({
 *   name: "add",
 *   description: "Add two numbers.",
 *   parameters: z.object({ a: z.number(), b: z.number() }),
 *   execute: async ({ a, b }) => a + b,
 * });
 * ```
 */
export function tool<TInput, TOutput>(
  config: AgentTool<TInput, TOutput>,
): AgentTool<TInput, TOutput> {
  return config;
}

export interface ApprovalDecision {
  approved: boolean;
  reason?: string;
}

// biome-ignore lint/suspicious/noExplicitAny: tool inputs are validated at runtime via Zod
export type AutoApprove =
  | boolean
  | string[]
  | ((call: { id: string; name: string; input: unknown }, tool: AgentTool<any, any>) => boolean);

// biome-ignore lint/suspicious/noExplicitAny: tool inputs are validated at runtime via Zod
export function shouldAutoApprove(
  policy: AutoApprove | undefined,
  call: { id: string; name: string; input: unknown },
  toolDef: AgentTool<any, any>,
): boolean {
  if (policy === undefined || policy === false) return false;
  if (policy === true) return true;
  if (Array.isArray(policy)) return policy.includes(call.name);
  return policy(call, toolDef);
}
