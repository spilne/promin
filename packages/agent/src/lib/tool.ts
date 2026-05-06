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
  /**
   * Tool isolation kind — set by `createScopedTool` / `createElevatedTool`
   * factories. Undefined for tools built with the bare `tool()` factory
   * (no scope contract, ctx.scope is best-effort).
   *
   *   - "scoped"   — runtime guarantees a complete (namespaceId, resourceId)
   *                  scope is injected; tool body cannot forge or override
   *                  it. Use for any tool that reads/writes per-user state.
   *   - "elevated" — cross-scope tool (admin, billing, system). Requires
   *                  `ctx.audit()` to be called per invocation; missing
   *                  audit fails the call. Optionally gated by `requires`.
   */
  readonly kind?: "scoped" | "elevated";
  /**
   * For elevated tools: the agent capability needed to expose this tool.
   * `buildTools` should filter out elevated tools whose `requires` is
   * not in the agent's `metadata.capabilities`. Capability filtering is
   * not yet wired in; tracked as a follow-up to `promin-3paw`.
   */
  readonly requires?: string;
  /**
   * Operator-toggleable flag. `enabled !== false` (default) means the
   * tool is wired and pickable by recipes. Setting `enabled: false`
   * keeps the source file (or in-process binding) intact while:
   *   - Hiding the tool from the agent's resolved tool list
   *   - Surfacing it in the catalog with a disabled badge
   *   - Treating recipe references as broken (reconciliation surfaces
   *     them in the orphans panel)
   *
   * Lets operators kill-switch a tool without code changes — useful
   * during incident response or when a tool's external dependency is
   * down and you want the agent to stop trying.
   */
  readonly enabled?: boolean;
}

// ---- scoped + elevated tool factories ----------------------------------
//
// `tool()` is the bare factory: no scope contract, ctx is fully optional,
// the tool body is responsible for whatever scope handling it wants.
//
// `createScopedTool()` and `createElevatedTool()` add type-safe contracts
// on top — the user's `execute` callback receives a context with required
// fields (no optional chains), and the runtime guarantees those fields
// were sourced from the dispatching task's envelope. A tool body cannot
// forge or override its own scope; the factory's adapter wraps the user
// callback so only the runtime can supply the context.

/**
 * Per-call context for scoped tools. All identity fields are guaranteed
 * non-empty when this context is passed — runtime enforces it.
 */
export interface ScopedToolContext {
  readonly namespaceId: string;
  readonly resourceId: string;
  readonly threadId?: string;
  readonly agentId?: string;
  readonly writer?: ToolWriter;
}

/**
 * Per-call context for elevated tools. Extends `ScopedToolContext` with
 * a mandatory `audit` callback — the tool MUST call it at least once
 * per invocation. The factory tracks whether audit was called; if not,
 * the call resolves to an error result so the missing-audit case is
 * loud, not silent.
 */
export interface ElevatedToolContext extends ScopedToolContext {
  /**
   * Audit hook — call once per invocation describing what cross-scope
   * action was taken. Missing audit fails the tool call.
   */
  audit(entry: {
    readonly action: string;
    readonly target?: string;
    readonly meta?: Readonly<Record<string, unknown>>;
  }): void;
}

/**
 * Configuration for `createScopedTool`. Same shape as `AgentTool` minus
 * the kind/requires markers (factory sets them) and with a typed
 * `execute` signature requiring a complete `ScopedToolContext`.
 */
export interface ScopedToolConfig<TInput, TOutput> {
  name: string;
  description: string;
  usage?: string;
  examples?: Array<{ input: TInput; output: string }>;
  parameters: z.ZodType<TInput>;
  requireApproval?: boolean;
  toModelOutput?: (output: TOutput) => string;
  execute: (input: TInput, ctx: ScopedToolContext) => Promise<TOutput>;
}

/**
 * Configuration for `createElevatedTool`. Like `ScopedToolConfig`, plus
 * an optional `requires` capability marker and an `execute` callback
 * that receives `ElevatedToolContext`.
 */
export interface ElevatedToolConfig<TInput, TOutput> extends Omit<
  ScopedToolConfig<TInput, TOutput>,
  "execute"
> {
  /**
   * Capability required on the agent template's `metadata.capabilities`
   * for this tool to be exposed. When omitted, any agent with the
   * implicit "elevated" capability sees the tool. Filtering is wired
   * by `buildTools`; currently a no-op (see follow-up).
   */
  readonly requires?: string;
  execute: (input: TInput, ctx: ElevatedToolContext) => Promise<TOutput>;
}

/**
 * Build a scoped tool — the runtime guarantees `ctx.namespaceId` and
 * `ctx.resourceId` are non-empty when `execute` runs. If the runtime
 * dispatches without a complete scope, the call fails before reaching
 * the user callback.
 */
export function createScopedTool<TInput, TOutput>(
  config: ScopedToolConfig<TInput, TOutput>,
): AgentTool<TInput, TOutput> {
  const { execute: userExecute, ...rest } = config;
  return {
    ...rest,
    kind: "scoped",
    execute: async (input: TInput, ctx?: ToolExecuteContext): Promise<TOutput> => {
      const scoped = scopedContextFrom(ctx);
      if (!scoped) {
        throw new Error(
          `Scoped tool '${config.name}' invoked without complete (namespaceId, resourceId) scope. ` +
            "This indicates a runtime bug — scoped tools must only be dispatched through " +
            "the agent loop / agentAction with a populated ToolScope.",
        );
      }
      return userExecute(input, scoped);
    },
  };
}

/**
 * Build an elevated tool — same scope guarantees as `createScopedTool`,
 * plus the user's `execute` callback receives an `audit()` callable.
 * The audit hook MUST be invoked at least once per call; if `execute`
 * returns without calling audit, the factory throws so the missing-audit
 * case surfaces as a tool error (not a silent gap in the audit log).
 */
export function createElevatedTool<TInput, TOutput>(
  config: ElevatedToolConfig<TInput, TOutput>,
): AgentTool<TInput, TOutput> {
  const { execute: userExecute, requires, ...rest } = config;
  return {
    ...rest,
    kind: "elevated",
    ...(requires !== undefined && { requires }),
    execute: async (input: TInput, ctx?: ToolExecuteContext): Promise<TOutput> => {
      const scoped = scopedContextFrom(ctx);
      if (!scoped) {
        throw new Error(
          `Elevated tool '${config.name}' invoked without complete (namespaceId, resourceId) scope.`,
        );
      }
      let auditCalled = false;
      const audit: ElevatedToolContext["audit"] = (_entry) => {
        auditCalled = true;
        // Audit log emission is host-injectable in a follow-up; for now
        // the factory only tracks that the tool DID call audit().
      };
      const elevated: ElevatedToolContext = { ...scoped, audit };
      const output = await userExecute(input, elevated);
      if (!auditCalled) {
        throw new Error(
          `Elevated tool '${config.name}' completed without calling ctx.audit(). ` +
            "Every elevated invocation must record an audit entry.",
        );
      }
      return output;
    },
  };
}

function scopedContextFrom(ctx: ToolExecuteContext | undefined): ScopedToolContext | null {
  const scope = ctx?.scope;
  if (!scope) return null;
  if (typeof scope.namespaceId !== "string" || scope.namespaceId.length === 0) return null;
  if (typeof scope.resourceId !== "string" || scope.resourceId.length === 0) return null;
  return {
    namespaceId: scope.namespaceId,
    resourceId: scope.resourceId,
    ...(scope.threadId !== undefined && { threadId: scope.threadId }),
    ...(scope.agentId !== undefined && { agentId: scope.agentId }),
    ...(ctx?.writer !== undefined && { writer: ctx.writer }),
  };
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
