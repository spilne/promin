import type { z } from "zod";

/**
 * Free-form progress sink for long-running tools. Without it, a tool that
 * takes 30s emits `tool.start`, then nothing until `tool.end` — consumers
 * see a frozen spinner. With it, the tool emits incremental payloads that
 * surface as `tool.progress` SessionEvents (labeled with the current
 * turn / step / toolCallId), so the UI can render "downloading… 50%".
 *
 * The writer is injected by the agent loop when running through a session.
 * Unit tests that call `tool.execute(input)` directly get `ctx === undefined`
 * — always access via the optional chain.
 *
 * Example:
 *
 *   tool({
 *     name: "download",
 *     parameters: z.object({ url: z.string() }),
 *     execute: async ({ url }, ctx) => {
 *       ctx?.writer?.write({ phase: "connecting" });
 *       const res = await fetch(url);
 *       ctx?.writer?.write({ phase: "downloading", percent: 0 });
 *       // ... emit { percent: 25 }, { percent: 50 } as bytes arrive
 *       ctx?.writer?.write({ phase: "decoding" });
 *       return await decode(res);
 *     },
 *   });
 *
 * `payload` is opaque — producer and consumer agree on shape. Common
 * shapes: `{ percent: number }`, `{ phase: string }`, `{ stepName, ok }`.
 *
 * Sibling concern: `tool.progress` covers the "what's happening" channel.
 * For "the result references a binary / external resource" (image, file,
 * URL embedded in the assistant text), an artifact-marker event is the
 * right shape — deferred until a concrete consumer needs it, since the
 * `ArtifactRef` shape depends on whether refs are signed URLs, storage
 * handles, blob refs, or MCP resource ids.
 */
export interface ToolWriter {
  write(payload: unknown): void;
}

/**
 * Caller scope visible to tool execute bodies — populated by the agent
 * runtime per-call. Lets scope-aware tools (durable scheduler, secrets,
 * audit) act on behalf of the live caller without each tool needing
 * its own per-call construction wrapper.
 */
export interface ToolScope {
  readonly namespaceId?: string;
  readonly resourceId?: string;
  readonly threadId?: string;
  /** Recipe id of the agent the tool is executing under. */
  readonly agentId?: string;
}

/** Per-call execution context handed to `tool.execute` as an optional second arg. */
export interface ToolExecuteContext {
  /**
   * Progress sink. Always present when the tool is invoked through the
   * agent loop with an event bus; absent in unit tests that call
   * `execute` directly. Tool authors should treat as optional via `?.`.
   */
  readonly writer?: ToolWriter;
  /**
   * Caller scope (namespace, resource, thread, agentId). Populated by
   * the agent runtime — present when invoked through `agentAction` /
   * `agentLoop`; absent in unit tests that call `execute` directly.
   * Tool authors that need scope should treat it as optional and fall
   * back to a sensible default or throw a clear error.
   */
  readonly scope?: ToolScope;
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
