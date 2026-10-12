// ---------------------------------------------------------------------------
// Agent turns: one-shot invoke and stream, threaded send and stream,
// resuming a turn suspended on a tool approval, reattaching to a thread
// turn's event stream and cancelling a running turn. Thread-bound turns
// hold the thread's turn lease while they run. See `agent-sse.ts` for the
// event ids, reattach, backpressure and heartbeat rules of the streams.
// ---------------------------------------------------------------------------

import {
  ApprovalAlreadyDecidedError,
  ApprovalExpiredError,
  ApprovalNotFoundError,
  ApprovalNotReadyError,
  threadTurnWorkflowId,
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
  openThreadFromBody,
  openThreadFromQuery,
  parseInvokeBody,
  parseScopeFields,
  parseTurnKey,
  refusalResponse,
  type InvokeRequest,
  type ScopeFields,
  type ScopedAgent,
} from "./agent-scope.ts";
import { parseEventId, type TurnStream, type TurnStreamOwner } from "./agent-sse.ts";

// ---------------------------------------------------------------------------
// One-shot invoke + stream
// ---------------------------------------------------------------------------

export function invokeAgent(deps: AgentGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    if (!id) return jsonError(400, "missing_id");

    const parsed = parseInvokeBody(await readJson<InvokeRequest>(req));
    if ("error" in parsed) return refusalResponse(parsed);
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

/**
 * One-shot stream. Not durable across requests (there is no thread to
 * commit to), so a client that goes away cancels the run.
 */
export function streamAgent(deps: AgentGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    if (!id) return jsonError(400, "missing_id");

    const parsed = parseInvokeBody(await readJson<InvokeRequest>(req));
    if ("error" in parsed) return refusalResponse(parsed);
    const scoped = await loadScopedAgent({ deps, req, id, scope: parsed, invoking: true });
    if (scoped instanceof Response) return scoped;

    const out = scoped.agent.stream({ task: parsed.task });
    const stream = deps.turnStreams.start({
      id: `invoke-${crypto.randomUUID()}`,
      out,
      meta: scoped.instanceId !== undefined ? { instanceId: scoped.instanceId } : {},
      announce: false,
    });
    return deps.turnStreams.respond({ stream, fromSeq: 0, onDisconnect: "cancel" });
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
    if ("error" in parsed) return refusalResponse(parsed);
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

/**
 * `POST /api/agents/:id/threads/:threadId/stream` — run a thread turn and
 * stream its events. The turn's id is its workflow id (from the turn key;
 * one is generated when the client sends none) and every event carries
 * `<turnId>:<seq>`. A client that goes away detaches: the turn runs on,
 * commits, and stays reattachable (`GET .../stream`). Sending the same turn
 * key again while that turn runs here attaches to it (from `Last-Event-ID`
 * when it names this turn) instead of starting it twice; with a different
 * task it answers 422 `idempotency_key_mismatch`.
 */
export function streamThreadMessage(deps: AgentGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    const threadId = params.threadId;
    if (!id) return jsonError(400, "missing_id");
    if (!threadId) return jsonError(400, "missing_threadId");

    const body = await readJson<InvokeRequest>(req);
    const parsed = parseInvokeBody(body);
    if ("error" in parsed) return refusalResponse(parsed);
    const turn = parseTurnKey({ req, body });
    if ("error" in turn) return jsonError(400, turn.error);
    const scoped = await loadScopedAgent({ deps, req, id, scope: parsed, invoking: true });
    if (scoped instanceof Response) return scoped;

    const turnKey = turn.turnKey ?? crypto.randomUUID();
    const turnId = threadTurnWorkflowId({ namespaceId: scoped.namespaceId, threadId, turnKey });
    const owner = turnOwner({ scoped, threadId });
    const running = deps.turnStreams.find({ id: turnId, owner });
    if (running && !running.done) {
      if (!running.isTask(parsed.task)) return turnKeyConflict();
      return deps.turnStreams.respond({
        stream: running,
        fromSeq: resumeSeq({ req, turnId }),
        onDisconnect: "detach",
      });
    }

    // The lease outlives this handler: the turn runs after the response
    // returns. Every exit before the turn stream takes ownership of the
    // lease releases it here; afterwards the stream releases it when the
    // run settles, whether or not a client is still attached.
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
      const out = thread.stream({ task: parsed.task }, { turnKey });
      const stream = deps.turnStreams.start({
        id: turnId,
        owner,
        out,
        meta: { ...threadMetaOf({ thread, scoped }), turnId },
        announce: true,
        task: parsed.task,
        onSettled: () => lease.release(),
      });
      return deps.turnStreams.respond({ stream, fromSeq: 0, onDisconnect: "detach" });
    } catch (err) {
      await lease.release();
      return internalError({ code: "stream_failed", err });
    }
  };
}

/**
 * `GET /api/agents/:id/threads/:threadId/stream` — reattach to a thread
 * turn's event stream (query scope as for other thread reads). The turn is
 * the one `Last-Event-ID` (`<turnId>:<seq>`) names, else `?turnId=`, else
 * the thread's latest turn on this server; the stream resumes after the
 * named event, from `?from=<seq>`, or from the start. A turn that is no
 * longer kept (finished more than the retention window ago, run by another
 * replica, or from before a restart) answers 404 `turn_stream_not_found`:
 * read the thread's messages instead.
 */
export function reattachThreadStream(deps: AgentGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const opened = await openThreadFromQuery({ deps, req, params });
    if (opened instanceof Response) return opened;
    const { scoped, url } = opened;
    const owner = turnOwner({ scoped, threadId: opened.thread.id });
    const lastEvent = parseEventId(req.headers.get("last-event-id"));
    const turnId = lastEvent?.turnId ?? url.searchParams.get("turnId") ?? undefined;
    const stream =
      turnId !== undefined
        ? deps.turnStreams.find({ id: turnId, owner })
        : deps.turnStreams.latest(owner);
    if (!stream) return jsonError(404, "turn_stream_not_found");
    return deps.turnStreams.respond({
      stream,
      fromSeq: resumeSeq({ req, turnId: stream.id }),
      onDisconnect: "detach",
    });
  };
}

/**
 * `POST /api/agents/:id/threads/:threadId/cancel` — cancel a running turn
 * (body scope as for other thread writes, optional `turnId`; default: the
 * thread's latest turn). The turn ends as `cancelled` and commits what it
 * has; attached clients get its `finish` frame. Answers once the run has
 * stopped; 404 `turn_not_running` when no such turn runs on this server.
 * Without a turn gate several turns can run on one thread at once: then
 * `turnId` is required (400 `turnId_required`) rather than guessed.
 */
export function cancelThreadTurn(deps: AgentGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const body = await readJson<{
      readonly namespaceId?: unknown;
      readonly resourceId?: unknown;
      readonly ownerId?: unknown;
      readonly turnId?: unknown;
    }>(req);
    if (!body) return jsonError(400, "missing_body");
    if (body.turnId !== undefined && typeof body.turnId !== "string") {
      return jsonError(400, "invalid_turnId");
    }
    const opened = await openThreadFromBody({ deps, req, params, body });
    if (opened instanceof Response) return opened;
    const owner = turnOwner({ scoped: opened.scoped, threadId: opened.thread.id });
    if (
      typeof body.turnId !== "string" &&
      !deps.turnGate &&
      deps.turnStreams.running(owner).length > 1
    ) {
      return jsonError(
        400,
        "turnId_required",
        "Several turns run on this thread; name the one to cancel with turnId.",
      );
    }
    const stream: TurnStream | undefined =
      typeof body.turnId === "string"
        ? deps.turnStreams.find({ id: body.turnId, owner })
        : deps.turnStreams.latest(owner);
    if (!stream || stream.done) return jsonError(404, "turn_not_running");
    await stream.cancel();
    return json(200, { turnId: stream.id, cancelled: true });
  };
}

/** The frame a (re)attaching client starts from: after `Last-Event-ID` of `turnId`, else `?from=`, else 0. */
function resumeSeq(params: { readonly req: Request; readonly turnId: string }): number {
  const lastEvent = parseEventId(params.req.headers.get("last-event-id"));
  if (lastEvent && lastEvent.turnId === params.turnId) return lastEvent.seq + 1;
  const from = Number(new URL(params.req.url).searchParams.get("from") ?? "0");
  return Number.isSafeInteger(from) && from > 0 ? from : 0;
}

function turnOwner(params: {
  readonly scoped: ScopedAgent;
  readonly threadId: string;
}): TurnStreamOwner {
  return {
    agentId: params.scoped.recipe.id,
    namespaceId: params.scoped.namespaceId,
    resourceId: params.scoped.resourceId,
    threadId: params.threadId,
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
      // The resumed turn's stream is named after the suspended turn and
      // the call it waited on.
      let turnId = `approve-${crypto.randomUUID()}`;
      if (thread.pendingApprovals) {
        const pending = await thread.pendingApprovals();
        const entry = pending.find((p) => p.toolCallId === parsed.toolCallId);
        if (!entry) {
          await lease.release();
          return jsonError(404, "approval_not_found");
        }
        turnId = `${entry.workflowId}/approve/${entry.toolCallId}`;
      }
      const out = thread.resumeStream(parsed.toolCallId, {
        approved: parsed.approved,
        by: parsed.ownerId ?? scoped.resourceId,
        ...(parsed.reason !== undefined && { reason: parsed.reason }),
      });
      const stream = deps.turnStreams.start({
        id: turnId,
        owner: turnOwner({ scoped, threadId }),
        out,
        meta: { ...threadMetaOf({ thread, scoped }), turnId },
        announce: true,
        onSettled: () => lease.release(),
      });
      return deps.turnStreams.respond({ stream, fromSeq: 0, onDisconnect: "detach" });
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
  if (err instanceof TurnKeyConflictError) return turnKeyConflict();
  return null;
}

function turnKeyConflict(): Response {
  return jsonError(
    422,
    "idempotency_key_mismatch",
    "This turn key was already used for a different message.",
  );
}
