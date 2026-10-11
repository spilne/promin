// ---------------------------------------------------------------------------
// Agent turns: one-shot invoke and stream, threaded send and stream, and
// resuming a turn suspended on a tool approval. Thread-bound turns hold the
// thread's turn lease while they run.
// ---------------------------------------------------------------------------

import {
  ApprovalAlreadyDecidedError,
  ApprovalExpiredError,
  ApprovalNotFoundError,
  ApprovalNotReadyError,
  ThreadAwaitingApprovalError,
  TurnKeyConflictError,
  type AgentThread,
  type PendingApproval,
} from "@promin/agent";
import { WorkflowSuspendedError } from "@promin/workflow";
import { json, jsonError, readJson } from "../../router.ts";
import { internalError } from "../agent-gateway-kit.ts";
import type {
  AgentGatewayDeps,
  InvokeResponse,
  PendingApprovalDto,
  ThreadApprovalsResponse,
  ThreadInvokeResponse,
  ThreadSuspendedResponse,
} from "./agent-gateway-deps.ts";
import {
  acquireTurnLease,
  loadScopedAgent,
  openOwnedThread,
  openThreadFromQuery,
  parseInvokeBody,
  parseScopeFields,
  parseTurnKey,
  type InvokeRequest,
  type ScopeFields,
  type ScopedAgent,
} from "./agent-scope.ts";
import {
  createSseChannel,
  silenceOutput,
  sseResponse,
  streamAgentRunResponse,
  streamErrorPayload,
} from "./agent-sse.ts";

// ---------------------------------------------------------------------------
// One-shot invoke + stream
// ---------------------------------------------------------------------------

export function invokeAgent(deps: AgentGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    if (!id) return jsonError(400, "missing_id");

    const parsed = parseInvokeBody(await readJson<InvokeRequest>(req));
    if ("error" in parsed) return jsonError(400, parsed.error);
    const scoped = await loadScopedAgent({ deps, req, id, scope: parsed, invoking: true });
    if (scoped instanceof Response) return scoped;

    try {
      const out = await scoped.agent.invoke({ task: parsed.task });
      const response: InvokeResponse = {
        text: await out.text,
        finishReason: await out.finishReason,
        usage: await out.usage,
      };
      if (scoped.instanceId !== undefined) response.instanceId = scoped.instanceId;
      return json(200, response);
    } catch (err) {
      return internalError({ code: "invoke_failed", err });
    }
  };
}

export function streamAgent(deps: AgentGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    if (!id) return jsonError(400, "missing_id");

    const parsed = parseInvokeBody(await readJson<InvokeRequest>(req));
    if ("error" in parsed) return jsonError(400, parsed.error);
    const scoped = await loadScopedAgent({ deps, req, id, scope: parsed, invoking: true });
    if (scoped instanceof Response) return scoped;

    const out = scoped.agent.stream({ task: parsed.task });
    silenceOutput(out);
    const sse = createSseChannel();
    const stream = new ReadableStream<Uint8Array>({
      async start(controller) {
        sse.attach(controller);
        try {
          for await (const delta of out.textStream) {
            if (sse.closed) break;
            sse.send(null, { delta });
          }
          if (!sse.closed) {
            const finishPayload: Record<string, unknown> = {
              text: await out.text,
              finishReason: await out.finishReason,
              usage: await out.usage,
            };
            if (scoped.instanceId !== undefined) finishPayload.instanceId = scoped.instanceId;
            sse.send("finish", finishPayload);
          }
        } catch (err) {
          sse.send("error", streamErrorPayload(err));
        } finally {
          sse.close();
        }
      },
      cancel: async () => {
        sse.markClosed();
        await out.cancel().catch(() => {});
      },
    });
    return sseResponse(stream);
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

    const body = await readJson<InvokeRequest>(req);
    const parsed = parseInvokeBody(body);
    if ("error" in parsed) return jsonError(400, parsed.error);
    const turn = parseTurnKey({ req, body });
    if ("error" in turn) return jsonError(400, turn.error);
    const scoped = await loadScopedAgent({ deps, req, id, scope: parsed, invoking: true });
    if (scoped instanceof Response) return scoped;

    const lease = await acquireTurnLease({
      deps,
      key: { namespaceId: scoped.namespaceId, threadId },
    });
    if (lease instanceof Response) return lease;
    try {
      const thread = await openOwnedThread({ scoped, threadId, create: true });
      if (thread instanceof Response) return thread;
      const out = await thread.send({ task: parsed.task }, turn);
      const response: ThreadInvokeResponse = {
        threadId: thread.id,
        isNew: thread.isNew,
        text: await out.text,
        finishReason: await out.finishReason,
        usage: await out.usage,
      };
      if (scoped.instanceId !== undefined) response.instanceId = scoped.instanceId;
      return json(200, response);
    } catch (err) {
      if (err instanceof WorkflowSuspendedError) {
        // The turn suspended on a tool approval: like the stream's
        // `suspended` frame, answer with what it waits for.
        const thread = await openOwnedThread({ scoped, threadId, create: false });
        const pending = thread instanceof Response ? [] : await thread.pendingApprovals?.();
        const suspended: ThreadSuspendedResponse = {
          threadId,
          suspended: true,
          approvals: (pending ?? []).map(toPendingApprovalDto),
          ...(scoped.instanceId !== undefined && { instanceId: scoped.instanceId }),
        };
        return json(202, suspended);
      }
      return approvalError(err) ?? internalError({ code: "send_failed", err });
    } finally {
      await lease.release();
    }
  };
}

export function streamThreadMessage(deps: AgentGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    const threadId = params.threadId;
    if (!id) return jsonError(400, "missing_id");
    if (!threadId) return jsonError(400, "missing_threadId");

    const body = await readJson<InvokeRequest>(req);
    const parsed = parseInvokeBody(body);
    if ("error" in parsed) return jsonError(400, parsed.error);
    const turn = parseTurnKey({ req, body });
    if ("error" in turn) return jsonError(400, turn.error);
    const scoped = await loadScopedAgent({ deps, req, id, scope: parsed, invoking: true });
    if (scoped instanceof Response) return scoped;

    // The lease outlives this handler: the stream is consumed after the
    // response returns. Every exit before the stream takes ownership of
    // the lease releases it here; afterwards the stream releases it when
    // it finishes, fails or the client disconnects.
    const lease = await acquireTurnLease({
      deps,
      key: { namespaceId: scoped.namespaceId, threadId },
    });
    if (lease instanceof Response) return lease;
    try {
      const thread = await openOwnedThread({ scoped, threadId, create: true });
      if (thread instanceof Response) {
        await lease.release();
        return thread;
      }
      // A turn waiting for approval blocks the thread: answer before
      // streaming. `prepareTurn` is the backend's own check (it also closes
      // approvals whose turn ended and finishes expired ones).
      try {
        await thread.prepareTurn?.();
      } catch (err) {
        await lease.release();
        const refused = approvalError(err);
        if (refused) return refused;
        throw err;
      }
      const out = thread.stream({ task: parsed.task }, turn);
      return streamAgentRunResponse({
        out,
        threadMeta: threadMetaOf({ thread, scoped }),
        onClose: () => lease.release(),
      });
    } catch (err) {
      await lease.release();
      return internalError({ code: "stream_failed", err });
    }
  };
}

function threadMetaOf(params: {
  thread: AgentThread;
  scoped: ScopedAgent;
}): Record<string, unknown> {
  const { thread, scoped } = params;
  return {
    threadId: thread.id,
    isNew: thread.isNew,
    ...(scoped.instanceId !== undefined && { instanceId: scoped.instanceId }),
  };
}

/**
 * Continue a turn that suspended on `approve:<callId>`. Body shape:
 *   { toolCallId: string, approved: boolean, reason?: string,
 *     namespaceId, resourceId | ownerId }
 *
 * Takes the thread's turn lease like a send (so an approve can't race a
 * new turn), answers 404 when the thread or the pending approval (looked
 * up in the pending-approval index by exact thread + call) is unknown,
 * then streams the resumed turn's events in the same SSE shape as the
 * user-task stream; an approval decided meanwhile, or approved after it
 * expired, ends the stream with an `error` frame naming why. The decision
 * is recorded with the identity the request was authorized as
 * (`ownerId`, else `resourceId`).
 */
export function streamThreadApproval(deps: AgentGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    const threadId = params.threadId;
    if (!id) return jsonError(400, "missing_id");
    if (!threadId) return jsonError(400, "missing_threadId");

    const parsed = parseApprovalBody(await readJson<ApprovalRequest>(req));
    if ("error" in parsed) return jsonError(400, parsed.error);
    const scoped = await loadScopedAgent({ deps, req, id, scope: parsed, invoking: true });
    if (scoped instanceof Response) return scoped;

    const lease = await acquireTurnLease({
      deps,
      key: { namespaceId: scoped.namespaceId, threadId },
    });
    if (lease instanceof Response) return lease;
    try {
      const thread = await openOwnedThread({ scoped, threadId, create: false });
      if (thread instanceof Response) {
        await lease.release();
        return thread;
      }
      if (!thread.resumeStream) {
        await lease.release();
        return jsonError(
          501,
          "resume_unsupported",
          "This agent backend doesn't support approval resume.",
        );
      }
      if (thread.pendingApprovals) {
        const pending = await thread.pendingApprovals();
        if (!pending.some((p) => p.toolCallId === parsed.toolCallId)) {
          await lease.release();
          return jsonError(404, "approval_not_found");
        }
      }
      const out = thread.resumeStream(parsed.toolCallId, {
        approved: parsed.approved,
        by: parsed.ownerId ?? scoped.resourceId,
        ...(parsed.reason !== undefined && { reason: parsed.reason }),
      });
      return streamAgentRunResponse({
        out,
        threadMeta: threadMetaOf({ thread, scoped }),
        onClose: () => lease.release(),
      });
    } catch (err) {
      await lease.release();
      return internalError({ code: "resume_failed", err });
    }
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

interface ParsedApproval extends ScopeFields {
  readonly toolCallId: string;
  readonly approved: boolean;
  readonly reason?: string;
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
 * `GET /api/agents/:id/threads/:threadId/approvals` — the approvals the
 * thread's turns are suspended on (from the pending-approval index), so a
 * client that reconnects can show the approval prompt again.
 */
export function listThreadApprovals(deps: AgentGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const opened = await openThreadFromQuery({ deps, req, params });
    if (opened instanceof Response) return opened;
    const pending = (await opened.thread.pendingApprovals?.()) ?? [];
    const body: ThreadApprovalsResponse = { approvals: pending.map(toPendingApprovalDto) };
    return json(200, body);
  };
}

function toPendingApprovalDto(p: PendingApproval): PendingApprovalDto {
  return {
    toolCallId: p.toolCallId,
    toolName: p.toolName,
    input: p.input,
    requestedAt: p.requestedAt,
    expiresAt: p.expiresAt,
    status: p.status,
  };
}

function awaitingApproval409(pending: ReadonlyArray<PendingApproval>): Response {
  return json(409, {
    error: "approval_pending",
    message: "A turn on this thread is waiting for approval; approve or reject it first.",
    approvals: pending.map(toPendingApprovalDto),
  });
}

/** The answer for an approval-flow error, or null for any other error. */
function approvalError(err: unknown): Response | null {
  if (err instanceof ThreadAwaitingApprovalError) return awaitingApproval409(err.pending);
  if (err instanceof ApprovalNotFoundError) return jsonError(404, "approval_not_found");
  if (err instanceof ApprovalAlreadyDecidedError) return jsonError(409, "approval_already_decided");
  if (err instanceof ApprovalExpiredError) return jsonError(410, "approval_expired");
  if (err instanceof ApprovalNotReadyError) {
    return jsonError(409, "approval_not_ready", "The turn is not ready for a decision yet; retry.");
  }
  if (err instanceof TurnKeyConflictError) {
    return jsonError(
      422,
      "idempotency_key_mismatch",
      "This turn key was already used for a different message.",
    );
  }
  return null;
}
