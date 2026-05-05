// ---------------------------------------------------------------------------
// Agent gateway routes — single entry point external apps call to invoke a
// registered agent.
//
// Routes:
//   GET  /api/agents                          — list registered agents
//   GET  /api/agents/:id                      — get one (latest version)
//   POST /api/agents/:id/invoke               — one-shot invocation
//   POST /api/agents/:id/stream               — one-shot, SSE response
//   POST /api/agents/:id/threads/:threadId    — conversational turn (one-shot per call)
//   POST /api/agents/:id/threads/:threadId/stream — conversational, SSE
//   GET  /api/agents/:id/threads              — list threads (requires namespaceId query)
//   GET  /api/agents/:id/threads/:threadId/messages — read message history
//
// Tenant binding:
//   Every invocation requires `namespaceId` AND ONE OF `resourceId | ownerId`
//   in the body — cross-tenant leak is prevented by-construction, not by
//   policy. `resourceId` = raw scope key; `ownerId` = "resolve via
//   AgentInstance, use its id as the scope key". Both at once is rejected
//   (`conflicting_identity`); neither is rejected (`missing_scope_identity`).
//   Use the exported `AgentInvokeBody` discriminated-union type to get the
//   constraint enforced at compile time.
//
//   The (namespaceId, resourceId) tuple flows through `agent.withScope()`
//   per request — the resolved agent template is shared across requests
//   but each call sees its own tenant + user scope.
//
// Resolver injection:
//   The route doesn't know how to construct an Agent from a recipe — that's
//   the host's job. Pass `resolve(recipe) → Agent` in `AgentGatewayDeps`.
//   For LocalAgent backends, that's `(r) => resolveLocalAgent(r, deps)`.
// ---------------------------------------------------------------------------

import type {
  Agent,
  AgentInstanceRegistry,
  AgentRegistry,
  AgentRunOutput,
  AgentThreadSummary,
  Message,
  RegisteredAgent,
} from "@promin/agent";
import { WorkflowSuspendedError } from "@promin/workflow";
import { json, jsonError, readJson } from "../router.ts";

// ---------------------------------------------------------------------------
// Response DTOs — UI imports these for typing API responses. Re-export the
// underlying agent types so the UI bundle doesn't import @promin/agent
// directly (keeps the import surface narrow and matches schedules.ts).
// `ThreadSummary` here is the agent-facing shape (with `id`), not the
// memory-store shape (which uses `threadId`).
// ---------------------------------------------------------------------------

export type { AgentThreadSummary as ThreadSummary, RegisteredAgent, Message } from "@promin/agent";

export interface AgentsListResponse {
  agents: RegisteredAgent[];
}

export interface AgentThreadsResponse {
  threads: AgentThreadSummary[];
}

export interface ThreadMessagesResponse {
  threadId: string;
  messages: Message[];
}

export interface InvokeResponse {
  text: string;
  finishReason: string;
  usage: {
    inputTokens: number;
    outputTokens: number;
    cacheReadTokens?: number;
    cacheWriteTokens?: number;
  };
  /** Set when the gateway resolved an AgentInstance from `ownerId`. */
  instanceId?: string;
}

export interface ThreadInvokeResponse extends InvokeResponse {
  threadId: string;
  isNew: boolean;
}

export interface AgentGatewayDeps {
  readonly registry: AgentRegistry;
  /**
   * Materialize a live `Agent` from a registered recipe. The gateway calls
   * this per request, then `.withScope()` for tenancy. The result is unscoped —
   * the gateway handles tenant scoping itself.
   */
  readonly resolve: (recipe: RegisteredAgent) => Agent;
  /**
   * Optional. When set, callers may pass `ownerId` instead of
   * `resourceId` in the invoke body. The gateway resolves-or-creates an
   * AgentInstance via this registry and uses `instance.id` as the
   * resourceId for the cascade. The response carries the resolved
   * `instanceId` so clients can skip the lookup on subsequent calls.
   *
   * When unset, `ownerId` in the body is rejected — the server can't
   * honor the implied semantics without somewhere to record the row.
   */
  readonly instanceRegistry?: AgentInstanceRegistry;
}

/** Body shape for invoke / stream / thread send. */
interface InvokeRequest {
  readonly task?: unknown;
  readonly namespaceId?: unknown;
  readonly resourceId?: unknown;
  readonly ownerId?: unknown;
  readonly metadata?: unknown;
}

interface ParsedInvoke {
  readonly task: string;
  readonly namespaceId: string;
  readonly resourceId?: string;
  readonly ownerId?: string;
}

/**
 * Typed shape callers can use to construct an invoke body with the
 * `(namespaceId, resourceId | ownerId)` constraint enforced at the type
 * level. The discriminated union refuses bodies that omit both
 * identity-scope fields — TypeScript clients catch the mistake at
 * compile time; the runtime parser catches anyone bypassing TS.
 *
 * ```ts
 * const body: AgentInvokeBody = {
 *   task: "summarize",
 *   namespaceId: "acme",
 *   ownerId: "u-9",   // OR resourceId; never both, never neither
 * };
 * ```
 */
export type AgentInvokeBody =
  | {
      readonly task: string;
      readonly namespaceId: string;
      readonly resourceId: string;
      readonly ownerId?: never;
      readonly metadata?: Record<string, unknown>;
    }
  | {
      readonly task: string;
      readonly namespaceId: string;
      readonly resourceId?: never;
      readonly ownerId: string;
      readonly metadata?: Record<string, unknown>;
    };

interface ScopeFields {
  readonly namespaceId: string;
  readonly resourceId?: string;
  readonly ownerId?: string;
}

/**
 * Parse + validate the `(namespaceId, resourceId | ownerId)` scope
 * triple from any agent-invocation body. Cross-tenant leak prevention is
 * by-construction: the parser rejects bodies that omit the user-scope
 * field, so a caller can never accidentally invoke "as namespace acme"
 * without saying "for which user" — there's no implicit "default user."
 *
 * Errors:
 *   missing_namespaceId        — namespaceId absent or empty.
 *   missing_scope_identity     — neither resourceId nor ownerId set.
 *   conflicting_identity       — both resourceId and ownerId set.
 *
 * @internal — exported for tests; not part of the public route surface.
 */
export function parseScopeFields(body: {
  readonly namespaceId?: unknown;
  readonly resourceId?: unknown;
  readonly ownerId?: unknown;
}): ScopeFields | { error: string } {
  if (typeof body.namespaceId !== "string" || body.namespaceId.length === 0) {
    return { error: "missing_namespaceId" };
  }
  const resourceId =
    typeof body.resourceId === "string" && body.resourceId.length > 0 ? body.resourceId : undefined;
  const ownerId =
    typeof body.ownerId === "string" && body.ownerId.length > 0 ? body.ownerId : undefined;
  // Both unset — by-construction tenancy means we never let a caller invoke
  // "as namespace X" without naming the user. resourceId = raw scope key,
  // ownerId = resolved-via-AgentInstance.
  if (resourceId === undefined && ownerId === undefined) {
    return { error: "missing_scope_identity" };
  }
  // Conflicting identity signals — caller has to pick one model.
  if (resourceId !== undefined && ownerId !== undefined) {
    return { error: "conflicting_identity" };
  }
  return {
    namespaceId: body.namespaceId,
    ...(resourceId !== undefined && { resourceId }),
    ...(ownerId !== undefined && { ownerId }),
  };
}

function parseInvokeBody(body: InvokeRequest | null): ParsedInvoke | { error: string } {
  if (!body) return { error: "missing_body" };
  if (typeof body.task !== "string" || body.task.length === 0) {
    return { error: "missing_task" };
  }
  const scope = parseScopeFields(body);
  if ("error" in scope) return scope;
  return { task: body.task, ...scope };
}

/**
 * Resolve the effective `resourceId` for the agent scope:
 *   - If `parsed.ownerId` is set, look up (or create) the AgentInstance
 *     and use `instance.id`. Fail if the gateway has no registry.
 *   - Otherwise, pass through `parsed.resourceId` (today's behaviour).
 *
 * Returns either `{ resourceId, instanceId? }` for the success path or
 * `{ error }` for the structured error.
 */
async function resolveScope(
  deps: AgentGatewayDeps,
  registeredAgentId: string,
  parsed: ParsedInvoke,
): Promise<{ resourceId?: string; instanceId?: string } | { error: string }> {
  if (parsed.ownerId !== undefined) {
    if (!deps.instanceRegistry) {
      return { error: "ownerId_unsupported" };
    }
    const instance = await deps.instanceRegistry.resolveOrCreate({
      registeredAgentId,
      namespaceId: parsed.namespaceId,
      ownerId: parsed.ownerId,
    });
    return { resourceId: instance.id, instanceId: instance.id };
  }
  return { resourceId: parsed.resourceId };
}

// ---------------------------------------------------------------------------
// Discovery — list + get
// ---------------------------------------------------------------------------

export function listAgents(deps: AgentGatewayDeps) {
  return async (req: Request): Promise<Response> => {
    const url = new URL(req.url);
    const capability = url.searchParams.get("capability") ?? undefined;
    const tag = url.searchParams.get("tag") ?? undefined;
    const limit = parseIntParam(url.searchParams.get("limit"));
    const cursor = url.searchParams.get("cursor") ?? undefined;
    const agents = await deps.registry.list({ capability, tag, limit, cursor });
    return json(200, { agents });
  };
}

export function getAgent(deps: AgentGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    if (!id) return jsonError(400, "missing_id");
    const url = new URL(req.url);
    const version = url.searchParams.get("version") ?? undefined;
    const recipe = await deps.registry.get(id, version);
    if (!recipe) return jsonError(404, "agent_not_found", `Agent "${id}" is not registered.`);
    return json(200, recipe);
  };
}

// ---------------------------------------------------------------------------
// One-shot invoke + stream
// ---------------------------------------------------------------------------

export function invokeAgent(deps: AgentGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    if (!id) return jsonError(400, "missing_id");

    const recipe = await deps.registry.get(id);
    if (!recipe) return jsonError(404, "agent_not_found", `Agent "${id}" is not registered.`);

    const body = await readJson<InvokeRequest>(req);
    const parsed = parseInvokeBody(body);
    if ("error" in parsed) return jsonError(400, parsed.error);

    const scope = await resolveScope(deps, id, parsed);
    if ("error" in scope) return jsonError(400, scope.error);

    let agent: Agent;
    try {
      agent = deps.resolve(recipe).withScope({
        namespaceId: parsed.namespaceId,
        resourceId: scope.resourceId,
      });
    } catch (err) {
      return jsonError(500, "resolve_failed", asMessage(err));
    }

    try {
      const out = await agent.invoke({ task: parsed.task });
      const response: InvokeResponse = {
        text: await out.text,
        finishReason: await out.finishReason,
        usage: await out.usage,
      };
      if (scope.instanceId !== undefined) response.instanceId = scope.instanceId;
      return json(200, response);
    } catch (err) {
      return jsonError(500, "invoke_failed", asMessage(err));
    }
  };
}

export function streamAgent(deps: AgentGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    if (!id) return jsonError(400, "missing_id");

    const recipe = await deps.registry.get(id);
    if (!recipe) return jsonError(404, "agent_not_found", `Agent "${id}" is not registered.`);

    const body = await readJson<InvokeRequest>(req);
    const parsed = parseInvokeBody(body);
    if ("error" in parsed) return jsonError(400, parsed.error);

    const scope = await resolveScope(deps, id, parsed);
    if ("error" in scope) return jsonError(400, scope.error);

    let agent: Agent;
    try {
      agent = deps.resolve(recipe).withScope({
        namespaceId: parsed.namespaceId,
        resourceId: scope.resourceId,
      });
    } catch (err) {
      return jsonError(500, "resolve_failed", asMessage(err));
    }

    const out = agent.stream({ task: parsed.task });
    const stream = new ReadableStream<Uint8Array>({
      async start(controller) {
        const enc = new TextEncoder();
        try {
          for await (const delta of out.textStream) {
            controller.enqueue(enc.encode(`data: ${JSON.stringify({ delta })}\n\n`));
          }
          const text = await out.text;
          const finishReason = await out.finishReason;
          const usage = await out.usage;
          const finishPayload: Record<string, unknown> = { text, finishReason, usage };
          if (scope.instanceId !== undefined) finishPayload.instanceId = scope.instanceId;
          controller.enqueue(
            enc.encode(`event: finish\ndata: ${JSON.stringify(finishPayload)}\n\n`),
          );
        } catch (err) {
          controller.enqueue(
            enc.encode(`event: error\ndata: ${JSON.stringify({ message: asMessage(err) })}\n\n`),
          );
        } finally {
          controller.close();
        }
      },
      cancel: async () => {
        await out.cancel().catch(() => {});
      },
    });

    return new Response(stream, {
      status: 200,
      headers: {
        "content-type": "text/event-stream",
        "cache-control": "no-cache, no-transform",
        connection: "keep-alive",
      },
    });
  };
}

// ---------------------------------------------------------------------------
// Threaded conversation
// ---------------------------------------------------------------------------

export function sendThreadMessage(deps: AgentGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    const threadId = params.threadId;
    if (!id) return jsonError(400, "missing_id");
    if (!threadId) return jsonError(400, "missing_threadId");

    const recipe = await deps.registry.get(id);
    if (!recipe) return jsonError(404, "agent_not_found", `Agent "${id}" is not registered.`);

    const body = await readJson<InvokeRequest>(req);
    const parsed = parseInvokeBody(body);
    if ("error" in parsed) return jsonError(400, parsed.error);

    const scope = await resolveScope(deps, id, parsed);
    if ("error" in scope) return jsonError(400, scope.error);

    let agent: Agent;
    try {
      agent = deps.resolve(recipe).withScope({
        namespaceId: parsed.namespaceId,
        resourceId: scope.resourceId,
      });
    } catch (err) {
      return jsonError(500, "resolve_failed", asMessage(err));
    }

    try {
      const thread = await agent.thread(threadId);
      const out = await thread.send({ task: parsed.task });
      const response: ThreadInvokeResponse = {
        threadId: thread.id,
        isNew: thread.isNew,
        text: await out.text,
        finishReason: await out.finishReason,
        usage: await out.usage,
      };
      if (scope.instanceId !== undefined) response.instanceId = scope.instanceId;
      return json(200, response);
    } catch (err) {
      return jsonError(500, "send_failed", asMessage(err));
    }
  };
}

export function streamThreadMessage(deps: AgentGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    const threadId = params.threadId;
    if (!id) return jsonError(400, "missing_id");
    if (!threadId) return jsonError(400, "missing_threadId");

    const recipe = await deps.registry.get(id);
    if (!recipe) return jsonError(404, "agent_not_found", `Agent "${id}" is not registered.`);

    const body = await readJson<InvokeRequest>(req);
    const parsed = parseInvokeBody(body);
    if ("error" in parsed) return jsonError(400, parsed.error);

    const scope = await resolveScope(deps, id, parsed);
    if ("error" in scope) return jsonError(400, scope.error);

    let agent: Agent;
    try {
      agent = deps.resolve(recipe).withScope({
        namespaceId: parsed.namespaceId,
        resourceId: scope.resourceId,
      });
    } catch (err) {
      return jsonError(500, "resolve_failed", asMessage(err));
    }

    const thread = await agent.thread(threadId);
    const out = thread.stream({ task: parsed.task });
    const threadMeta = {
      threadId: thread.id,
      isNew: thread.isNew,
      ...(scope.instanceId !== undefined && { instanceId: scope.instanceId }),
    };
    return streamAgentRunResponse(out, threadMeta);
  };
}

/**
 * Continue a turn that suspended on `approve:<callId>`. Body shape:
 *   { toolCallId: string, approved: boolean, reason?: string }
 *
 * Resumes via `LocalAgentThread.resume` (or any backend that exposes it),
 * then streams the resumed turn's events back in the same SSE shape as
 * the user-task stream — so the chat client can splice the resumed
 * deltas into the same conversation panel.
 */
export function streamThreadApproval(deps: AgentGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    const threadId = params.threadId;
    if (!id) return jsonError(400, "missing_id");
    if (!threadId) return jsonError(400, "missing_threadId");

    const recipe = await deps.registry.get(id);
    if (!recipe) return jsonError(404, "agent_not_found", `Agent "${id}" is not registered.`);

    const body = await readJson<ApprovalRequest>(req);
    const parsed = parseApprovalBody(body);
    if ("error" in parsed) return jsonError(400, parsed.error);

    const scope = await resolveScope(deps, id, {
      task: "_resume",
      namespaceId: parsed.namespaceId,
      ...(parsed.resourceId !== undefined && { resourceId: parsed.resourceId }),
      ...(parsed.ownerId !== undefined && { ownerId: parsed.ownerId }),
    });
    if ("error" in scope) return jsonError(400, scope.error);

    let agent: Agent;
    try {
      agent = deps.resolve(recipe).withScope({
        namespaceId: parsed.namespaceId,
        resourceId: scope.resourceId,
      });
    } catch (err) {
      return jsonError(500, "resolve_failed", asMessage(err));
    }

    const thread = await agent.thread(threadId);
    if (!thread.resumeStream) {
      return jsonError(
        500,
        "resume_unsupported",
        "This agent backend doesn't support approval resume.",
      );
    }
    let out: AgentRunOutput;
    try {
      out = thread.resumeStream(parsed.toolCallId, {
        approved: parsed.approved,
        ...(parsed.reason !== undefined && { reason: parsed.reason }),
      });
    } catch (err) {
      return jsonError(404, "resume_failed", asMessage(err));
    }

    const threadMeta = {
      threadId: thread.id,
      isNew: thread.isNew,
      ...(scope.instanceId !== undefined && { instanceId: scope.instanceId }),
    };
    return streamAgentRunResponse(out, threadMeta);
  };
}

interface ApprovalRequest {
  readonly toolCallId?: unknown;
  readonly approved?: unknown;
  readonly reason?: unknown;
  readonly namespaceId?: unknown;
  readonly resourceId?: unknown;
  readonly ownerId?: unknown;
}

interface ParsedApproval {
  readonly toolCallId: string;
  readonly approved: boolean;
  readonly reason?: string;
  readonly namespaceId: string;
  readonly resourceId?: string;
  readonly ownerId?: string;
}

function parseApprovalBody(body: ApprovalRequest | null): ParsedApproval | { error: string } {
  if (!body) return { error: "missing_body" };
  if (typeof body.toolCallId !== "string" || body.toolCallId.length === 0) {
    return { error: "missing_toolCallId" };
  }
  if (typeof body.approved !== "boolean") return { error: "missing_approved" };
  const scope = parseScopeFields(body);
  if ("error" in scope) return scope;
  const reason = typeof body.reason === "string" ? body.reason : undefined;
  return {
    toolCallId: body.toolCallId,
    approved: body.approved,
    ...scope,
    ...(reason !== undefined && { reason }),
  };
}

/**
 * Shared SSE writer for thread.stream / thread.resumeStream — fans the
 * agent's `fullStream` into typed SSE events so the client sees text
 * deltas, approval requests, and finish/suspend signals in one channel.
 *
 * Suspension semantics: when the workflow suspends on an approval gate
 * the underlying `out.text` promise rejects with `WorkflowSuspendedError`.
 * The `approval-requested` event already fired through `fullStream`, so
 * we map the rejection to a clean `event: suspended` and let the client
 * react (render the approval banner). Any other error becomes
 * `event: error`.
 */
function streamAgentRunResponse(
  out: AgentRunOutput,
  threadMeta: Record<string, unknown>,
): Response {
  const stream = new ReadableStream<Uint8Array>({
    async start(controller) {
      const enc = new TextEncoder();
      const send = (event: string | null, data: unknown) => {
        const prefix = event ? `event: ${event}\n` : "";
        try {
          controller.enqueue(enc.encode(`${prefix}data: ${JSON.stringify(data)}\n\n`));
        } catch {
          // Controller already closed — best-effort.
        }
      };

      send("thread", threadMeta);

      let suspended: { toolCallId: string; toolName: string } | null = null;
      try {
        for await (const event of out.fullStream) {
          switch (event.type) {
            case "text-delta":
              send(null, { delta: event.delta });
              break;
            case "approval-requested":
              suspended = { toolCallId: event.toolCallId, toolName: event.toolName };
              send("approval-requested", suspended);
              break;
            case "finish":
              // `finish` lands when the workflow runs to completion.
              // We surface usage + reason in the dedicated `finish`
              // SSE event after the loop in case downstream code needs
              // post-loop work; nothing to do here.
              break;
            case "error":
              send("error", { message: event.error.message });
              break;
            default:
              break;
          }
        }
        // No suspension — workflow completed normally.
        if (!suspended) {
          const text = await out.text;
          const finishReason = await out.finishReason;
          const usage = await out.usage;
          send("finish", { ...threadMeta, text, finishReason, usage });
        } else {
          // The workflow suspended at the approval gate. The runner
          // rejects out.text with WorkflowSuspendedError — swallow it,
          // signal the suspension cleanly so the client renders the
          // banner instead of an error.
          out.text.catch(() => {});
          send("suspended", { ...threadMeta, ...suspended });
        }
      } catch (err) {
        // Suspension shows up here too if it propagated through
        // fullStream's settle path before we caught it.
        if (err instanceof WorkflowSuspendedError) {
          send("suspended", { ...threadMeta, ...(suspended ?? {}) });
        } else {
          send("error", { message: asMessage(err) });
        }
      } finally {
        controller.close();
      }
    },
    cancel: async () => {
      await out.cancel().catch(() => {});
    },
  });

  return new Response(stream, {
    status: 200,
    headers: {
      "content-type": "text/event-stream",
      "cache-control": "no-cache, no-transform",
      connection: "keep-alive",
    },
  });
}

export function listAgentThreads(deps: AgentGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    if (!id) return jsonError(400, "missing_id");

    const url = new URL(req.url);
    const namespaceId = url.searchParams.get("namespaceId") ?? undefined;
    const resourceId = url.searchParams.get("resourceId") ?? undefined;
    if (!namespaceId) return jsonError(400, "missing_namespaceId");

    const recipe = await deps.registry.get(id);
    if (!recipe) return jsonError(404, "agent_not_found", `Agent "${id}" is not registered.`);

    let agent: Agent;
    try {
      agent = deps.resolve(recipe).withScope({ namespaceId, resourceId });
    } catch (err) {
      return jsonError(500, "resolve_failed", asMessage(err));
    }

    try {
      const limit = parseIntParam(url.searchParams.get("limit"));
      const cursor = url.searchParams.get("cursor") ?? undefined;
      const q = url.searchParams.get("q") ?? undefined;
      const threads = await agent.listThreads({ resourceId, limit, cursor, q });
      return json(200, { threads });
    } catch (err) {
      return jsonError(500, "list_threads_failed", asMessage(err));
    }
  };
}

export function listThreadMessages(deps: AgentGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    const threadId = params.threadId;
    if (!id) return jsonError(400, "missing_id");
    if (!threadId) return jsonError(400, "missing_threadId");

    const url = new URL(req.url);
    const namespaceId = url.searchParams.get("namespaceId") ?? undefined;
    const resourceId = url.searchParams.get("resourceId") ?? undefined;
    if (!namespaceId) return jsonError(400, "missing_namespaceId");

    const recipe = await deps.registry.get(id);
    if (!recipe) return jsonError(404, "agent_not_found", `Agent "${id}" is not registered.`);

    let agent: Agent;
    try {
      agent = deps.resolve(recipe).withScope({ namespaceId, resourceId });
    } catch (err) {
      return jsonError(500, "resolve_failed", asMessage(err));
    }

    try {
      const thread = await agent.thread(threadId, { createIfMissing: false });
      const limit = parseIntParam(url.searchParams.get("limit"));
      const fromSeq = parseIntParam(url.searchParams.get("fromSeq"));
      const toSeq = parseIntParam(url.searchParams.get("toSeq"));
      const messages = await thread.messages({ limit, fromSeq, toSeq });
      return json(200, { threadId: thread.id, messages });
    } catch (err) {
      return jsonError(404, "thread_not_found", asMessage(err));
    }
  };
}

// ---------------------------------------------------------------------------
// Rename thread — stash a display title in the thread's metadata bag so
// the dashboard can show it in place of the opaque threadId. We
// read-modify-write the metadata so existing keys (working-memory hints,
// app-specific tags) survive the rename.
// ---------------------------------------------------------------------------

interface RenameThreadRequest {
  readonly namespaceId?: unknown;
  readonly resourceId?: unknown;
  readonly title?: unknown;
}

export function renameAgentThread(deps: AgentGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    const threadId = params.threadId;
    if (!id) return jsonError(400, "missing_id");
    if (!threadId) return jsonError(400, "missing_threadId");

    const body = (await readJson<RenameThreadRequest>(req)) ?? {};
    const namespaceId = typeof body.namespaceId === "string" ? body.namespaceId : undefined;
    const resourceId = typeof body.resourceId === "string" ? body.resourceId : undefined;
    if (!namespaceId) return jsonError(400, "missing_namespaceId");
    // Empty / undefined title clears the rename — back to the threadId default.
    const title =
      typeof body.title === "string" && body.title.trim().length > 0 ? body.title.trim() : null;

    const recipe = await deps.registry.get(id);
    if (!recipe) return jsonError(404, "agent_not_found", `Agent "${id}" is not registered.`);

    let agent: Agent;
    try {
      agent = deps.resolve(recipe).withScope({ namespaceId, resourceId });
    } catch (err) {
      return jsonError(500, "resolve_failed", asMessage(err));
    }

    try {
      const thread = await agent.thread(threadId, { createIfMissing: false });
      await thread.setTitle(title);
      return json(200, { threadId: thread.id, title });
    } catch (err) {
      return jsonError(404, "thread_not_found", asMessage(err));
    }
  };
}

// ---------------------------------------------------------------------------
// Memory consolidation — delegates to the agent's Consolidator.
// ---------------------------------------------------------------------------

interface ConsolidateRequest {
  readonly namespaceId?: unknown;
  readonly resourceId?: unknown;
  readonly force?: unknown;
}

export function distillThread(deps: AgentGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    const threadId = params.threadId;
    if (!id) return jsonError(400, "missing_id");
    if (!threadId) return jsonError(400, "missing_threadId");

    const body = await readJson<ConsolidateRequest>(req);
    const namespaceId = typeof body?.namespaceId === "string" ? body.namespaceId : undefined;
    const resourceId = typeof body?.resourceId === "string" ? body.resourceId : undefined;
    const force = body?.force === true;
    if (!namespaceId) return jsonError(400, "missing_namespaceId");
    if (!resourceId) return jsonError(400, "missing_resourceId");

    const recipe = await deps.registry.get(id);
    if (!recipe) return jsonError(404, "agent_not_found", `Agent "${id}" is not registered.`);

    let agent: Agent;
    try {
      agent = deps.resolve(recipe).withScope({ namespaceId, resourceId });
    } catch (err) {
      return jsonError(500, "resolve_failed", asMessage(err));
    }

    try {
      const episode = await agent.distillThread(threadId, { force });
      return json(200, { episode });
    } catch (err) {
      return jsonError(500, "distill_failed", asMessage(err));
    }
  };
}

export function compactThread(deps: AgentGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    const threadId = params.threadId;
    if (!id) return jsonError(400, "missing_id");
    if (!threadId) return jsonError(400, "missing_threadId");

    const body = await readJson<ConsolidateRequest & { keepRecent?: unknown }>(req);
    const namespaceId = typeof body?.namespaceId === "string" ? body.namespaceId : undefined;
    const resourceId = typeof body?.resourceId === "string" ? body.resourceId : undefined;
    const keepRecent = typeof body?.keepRecent === "number" ? body.keepRecent : undefined;
    if (!namespaceId) return jsonError(400, "missing_namespaceId");

    const recipe = await deps.registry.get(id);
    if (!recipe) return jsonError(404, "agent_not_found", `Agent "${id}" is not registered.`);

    let agent: Agent;
    try {
      agent = deps.resolve(recipe).withScope({ namespaceId, resourceId });
    } catch (err) {
      return jsonError(500, "resolve_failed", asMessage(err));
    }

    try {
      const episode = await agent.compactThread(threadId, { keepRecent });
      return json(200, { episode });
    } catch (err) {
      return jsonError(500, "compact_failed", asMessage(err));
    }
  };
}

// ---------------------------------------------------------------------------
// Archive / unarchive a thread — sets archivedAt on the thread row so
// the sidebar hides it from the default view without deleting it.
// ---------------------------------------------------------------------------

interface ArchiveThreadRequest {
  readonly namespaceId?: unknown;
  readonly resourceId?: unknown;
  /** Unix ms timestamp to archive at, or null to restore. Defaults to Date.now() when omitted. */
  readonly archivedAt?: unknown;
}

export function archiveAgentThread(deps: AgentGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    const threadId = params.threadId;
    if (!id) return jsonError(400, "missing_id");
    if (!threadId) return jsonError(400, "missing_threadId");

    const body = (await readJson<ArchiveThreadRequest>(req)) ?? {};
    const namespaceId = typeof body.namespaceId === "string" ? body.namespaceId : undefined;
    const resourceId = typeof body.resourceId === "string" ? body.resourceId : undefined;
    if (!namespaceId) return jsonError(400, "missing_namespaceId");
    const archivedAt =
      body.archivedAt === null
        ? null
        : typeof body.archivedAt === "number"
          ? body.archivedAt
          : Date.now();

    const recipe = await deps.registry.get(id);
    if (!recipe) return jsonError(404, "agent_not_found", `Agent "${id}" is not registered.`);

    let agent: Agent;
    try {
      agent = deps.resolve(recipe).withScope({ namespaceId, resourceId });
    } catch (err) {
      return jsonError(500, "resolve_failed", asMessage(err));
    }

    try {
      const thread = await agent.thread(threadId, { createIfMissing: false });
      await thread.setArchived(archivedAt);
      return json(200, { threadId: thread.id, archivedAt });
    } catch (err) {
      return jsonError(404, "thread_not_found", asMessage(err));
    }
  };
}

// ---------------------------------------------------------------------------

function parseIntParam(s: string | null): number | undefined {
  if (s === null) return undefined;
  const n = Number.parseInt(s, 10);
  return Number.isFinite(n) ? n : undefined;
}

function asMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}
