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
//   Every invocation requires `namespaceId` in the body. `resourceId` is
//   optional — when set, the resource layer participates in the memory
//   cascade. Both are passed through `agent.bind()` per request, so the
//   resolved agent template is shared across requests but each call sees
//   its own tenant scope.
//
// Resolver injection:
//   The route doesn't know how to construct an Agent from a recipe — that's
//   the host's job. Pass `resolve(recipe) → Agent` in `AgentGatewayDeps`.
//   For LocalAgent backends, that's `(r) => resolveLocalAgent(r, deps)`.
// ---------------------------------------------------------------------------

import type {
  Agent,
  AgentRegistry,
  AgentThreadSummary,
  Message,
  RegisteredAgent,
} from "@promin/agent";
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
  usage: { inputTokens: number; outputTokens: number };
}

export interface ThreadInvokeResponse extends InvokeResponse {
  threadId: string;
  isNew: boolean;
}

export interface AgentGatewayDeps {
  readonly registry: AgentRegistry;
  /**
   * Materialize a live `Agent` from a registered recipe. The gateway calls
   * this per request, then `.bind()` for tenancy. The result is unbound —
   * the gateway handles tenant binding itself.
   */
  readonly resolve: (recipe: RegisteredAgent) => Agent;
}

/** Body shape for invoke / stream / thread send. */
interface InvokeRequest {
  readonly task?: unknown;
  readonly namespaceId?: unknown;
  readonly resourceId?: unknown;
  readonly metadata?: unknown;
}

interface ParsedInvoke {
  readonly task: string;
  readonly namespaceId: string;
  readonly resourceId?: string;
}

function parseInvokeBody(body: InvokeRequest | null): ParsedInvoke | { error: string } {
  if (!body) return { error: "missing_body" };
  if (typeof body.task !== "string" || body.task.length === 0) {
    return { error: "missing_task" };
  }
  if (typeof body.namespaceId !== "string" || body.namespaceId.length === 0) {
    return { error: "missing_namespaceId" };
  }
  const resourceId =
    typeof body.resourceId === "string" && body.resourceId.length > 0 ? body.resourceId : undefined;
  return { task: body.task, namespaceId: body.namespaceId, resourceId };
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

    let agent: Agent;
    try {
      agent = deps.resolve(recipe).bind({
        namespaceId: parsed.namespaceId,
        resourceId: parsed.resourceId,
      });
    } catch (err) {
      return jsonError(500, "resolve_failed", asMessage(err));
    }

    try {
      const out = await agent.invoke({ task: parsed.task });
      return json(200, {
        text: await out.text,
        finishReason: await out.finishReason,
        usage: await out.usage,
      });
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

    let agent: Agent;
    try {
      agent = deps.resolve(recipe).bind({
        namespaceId: parsed.namespaceId,
        resourceId: parsed.resourceId,
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
          controller.enqueue(
            enc.encode(`event: finish\ndata: ${JSON.stringify({ text, finishReason, usage })}\n\n`),
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

    let agent: Agent;
    try {
      agent = deps.resolve(recipe).bind({
        namespaceId: parsed.namespaceId,
        resourceId: parsed.resourceId,
      });
    } catch (err) {
      return jsonError(500, "resolve_failed", asMessage(err));
    }

    try {
      const thread = await agent.thread(threadId);
      const out = await thread.send({ task: parsed.task });
      return json(200, {
        threadId: thread.id,
        isNew: thread.isNew,
        text: await out.text,
        finishReason: await out.finishReason,
        usage: await out.usage,
      });
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

    let agent: Agent;
    try {
      agent = deps.resolve(recipe).bind({
        namespaceId: parsed.namespaceId,
        resourceId: parsed.resourceId,
      });
    } catch (err) {
      return jsonError(500, "resolve_failed", asMessage(err));
    }

    const thread = await agent.thread(threadId);
    const out = thread.stream({ task: parsed.task });

    const stream = new ReadableStream<Uint8Array>({
      async start(controller) {
        const enc = new TextEncoder();
        controller.enqueue(
          enc.encode(
            `event: thread\ndata: ${JSON.stringify({ threadId: thread.id, isNew: thread.isNew })}\n\n`,
          ),
        );
        try {
          for await (const delta of out.textStream) {
            controller.enqueue(enc.encode(`data: ${JSON.stringify({ delta })}\n\n`));
          }
          const text = await out.text;
          const finishReason = await out.finishReason;
          const usage = await out.usage;
          controller.enqueue(
            enc.encode(`event: finish\ndata: ${JSON.stringify({ text, finishReason, usage })}\n\n`),
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
      agent = deps.resolve(recipe).bind({ namespaceId, resourceId });
    } catch (err) {
      return jsonError(500, "resolve_failed", asMessage(err));
    }

    try {
      const limit = parseIntParam(url.searchParams.get("limit"));
      const cursor = url.searchParams.get("cursor") ?? undefined;
      const threads = await agent.listThreads({ resourceId, limit, cursor });
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
      agent = deps.resolve(recipe).bind({ namespaceId, resourceId });
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

function parseIntParam(s: string | null): number | undefined {
  if (s === null) return undefined;
  const n = Number.parseInt(s, 10);
  return Number.isFinite(n) ? n : undefined;
}

function asMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}
