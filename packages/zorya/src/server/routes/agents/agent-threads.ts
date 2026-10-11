// ---------------------------------------------------------------------------
// Thread reads and housekeeping: list, messages, trace, rename, distill,
// compact and archive. Allowed on a disabled agent, so operators can inspect
// a kill-switched one.
// ---------------------------------------------------------------------------

import {
  buildAgentTrace,
  ConsolidatorRateLimitError,
  NothingToConsolidateError,
  THREAD_AGENT_METADATA_KEY,
} from "@promin/agent";
import { SystemWallClock } from "@promin/workflow";
import { json, jsonError, parseIntParam, readJson } from "../../router.ts";
import { internalError } from "../agent-gateway-kit.ts";
import type {
  AgentGatewayDeps,
  AgentThreadsResponse,
  RenameThreadResponse,
  ThreadMessagesResponse,
} from "./agent-gateway-deps.ts";
import {
  loadScopedAgent,
  openThreadFromBody,
  openThreadFromQuery,
  parseLimitParam,
  scopeFromQuery,
} from "./agent-scope.ts";

export function listAgentThreads(deps: AgentGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    if (!id) return jsonError(400, "missing_id");
    const url = new URL(req.url);
    const scope = scopeFromQuery(url);
    if ("error" in scope) return jsonError(400, scope.error);
    const scoped = await loadScopedAgent({ deps, req, id, scope, invoking: false });
    if (scoped instanceof Response) return scoped;

    const threads = await scoped.agent.listThreads({
      resourceId: scoped.resourceId,
      ...(scoped.recipe.backend.type === "local" && {
        metadataFilter: { [THREAD_AGENT_METADATA_KEY]: id },
      }),
      limit: parseLimitParam(url.searchParams.get("limit")),
      cursor: url.searchParams.get("cursor") ?? undefined,
      q: url.searchParams.get("q") ?? undefined,
    });
    return json(200, { threads } satisfies AgentThreadsResponse);
  };
}

export function getThreadTrace(deps: AgentGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const opened = await openThreadFromQuery({ deps, req, params });
    if (opened instanceof Response) return opened;
    const messages = await opened.thread.messages({ limit: TRACE_MESSAGE_LIMIT, order: "desc" });
    const trace = buildAgentTrace([...messages].reverse());
    return json(200, { threadId: opened.thread.id, trace });
  };
}

/** Newest messages a trace is built from. */
const TRACE_MESSAGE_LIMIT = 1_000;

export function listThreadMessages(deps: AgentGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const opened = await openThreadFromQuery({ deps, req, params });
    if (opened instanceof Response) return opened;
    const { thread, url } = opened;
    const limit = parseLimitParam(url.searchParams.get("limit"));
    const fromSeq = parseIntParam({
      raw: url.searchParams.get("fromSeq"),
      min: 0,
      max: Number.MAX_SAFE_INTEGER,
    });
    const toSeq = parseIntParam({
      raw: url.searchParams.get("toSeq"),
      min: 0,
      max: Number.MAX_SAFE_INTEGER,
    });
    const messages = await thread.messages({ limit, fromSeq, toSeq });
    return json(200, { threadId: thread.id, messages } satisfies ThreadMessagesResponse);
  };
}

// ---------------------------------------------------------------------------
// Rename thread — sets the thread's first-class display title. The
// response carries the thread's metadata bag, which the title doesn't touch.
// ---------------------------------------------------------------------------

interface RenameThreadRequest {
  readonly namespaceId?: unknown;
  readonly resourceId?: unknown;
  readonly ownerId?: unknown;
  readonly title?: unknown;
}

export function renameAgentThread(deps: AgentGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const body = await readJson<RenameThreadRequest>(req);
    if (!body) return jsonError(400, "missing_body");
    if (body.title !== undefined && body.title !== null && typeof body.title !== "string") {
      return jsonError(400, "invalid_title");
    }
    // Empty / null title clears the rename — back to the threadId default.
    const title =
      typeof body.title === "string" && body.title.trim().length > 0 ? body.title.trim() : null;
    const opened = await openThreadFromBody({ deps, req, params, body });
    if (opened instanceof Response) return opened;
    await opened.thread.setTitle(title);
    const metadata = { ...(await opened.thread.metadata()) };
    return json(200, {
      threadId: opened.thread.id,
      title,
      metadata,
    } satisfies RenameThreadResponse);
  };
}

// ---------------------------------------------------------------------------
// Memory consolidation — delegates to the agent's Consolidator.
// ---------------------------------------------------------------------------

interface ConsolidateRequest {
  readonly namespaceId?: unknown;
  readonly resourceId?: unknown;
  readonly ownerId?: unknown;
  readonly force?: unknown;
  readonly keepRecent?: unknown;
}

function nothingToConsolidate409(err: unknown): Response | null {
  if (err instanceof NothingToConsolidateError) {
    return json(409, {
      error: err.operation === "compact" ? "nothing_to_compact" : "nothing_to_distill",
      messageCount: err.messageCount,
    });
  }
  return null;
}

export function distillThread(deps: AgentGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const body = (await readJson<ConsolidateRequest>(req)) ?? {};
    const opened = await openThreadFromBody({ deps, req, params, body });
    if (opened instanceof Response) return opened;

    try {
      const episode = await opened.scoped.agent.distillThread(opened.thread.id, {
        force: body.force === true,
      });
      return json(200, { episode });
    } catch (err) {
      if (err instanceof ConsolidatorRateLimitError) {
        const retryAfter = err.retryAfterSeconds((deps.clock ?? SystemWallClock).currentTimeMs());
        return new Response(
          JSON.stringify({
            error: "distill_rate_limited",
            message: err.message,
            scope: err.scope,
            windowMs: err.windowMs,
            max: err.max,
            seen: err.seen,
            retryAfterSeconds: retryAfter,
          }),
          {
            status: 429,
            headers: {
              "content-type": "application/json",
              "retry-after": String(retryAfter),
            },
          },
        );
      }
      const nothing = nothingToConsolidate409(err);
      if (nothing) return nothing;
      return internalError({ code: "distill_failed", err });
    }
  };
}

export function compactThread(deps: AgentGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const body = (await readJson<ConsolidateRequest>(req)) ?? {};
    let keepRecent: number | undefined;
    if (body.keepRecent !== undefined) {
      if (
        typeof body.keepRecent !== "number" ||
        !Number.isInteger(body.keepRecent) ||
        body.keepRecent < 0
      ) {
        return jsonError(400, "invalid_keepRecent", "keepRecent must be a non-negative integer.");
      }
      keepRecent = body.keepRecent;
    }
    const opened = await openThreadFromBody({ deps, req, params, body });
    if (opened instanceof Response) return opened;

    try {
      const episode = await opened.scoped.agent.compactThread(opened.thread.id, { keepRecent });
      return json(200, { episode });
    } catch (err) {
      const nothing = nothingToConsolidate409(err);
      if (nothing) return nothing;
      return internalError({ code: "compact_failed", err });
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
  readonly ownerId?: unknown;
  /** Unix ms timestamp to archive at, or null to restore. Defaults to the gateway clock's now when omitted. */
  readonly archivedAt?: unknown;
}

export function archiveAgentThread(deps: AgentGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const body = (await readJson<ArchiveThreadRequest>(req)) ?? {};
    let archivedAt: number | null;
    if (body.archivedAt === null) {
      archivedAt = null;
    } else if (body.archivedAt === undefined) {
      archivedAt = (deps.clock ?? SystemWallClock).currentTimeMs();
    } else if (
      typeof body.archivedAt === "number" &&
      Number.isSafeInteger(body.archivedAt) &&
      body.archivedAt >= 0
    ) {
      archivedAt = body.archivedAt;
    } else {
      return jsonError(
        400,
        "invalid_archivedAt",
        "archivedAt must be a unix-ms timestamp, null, or omitted.",
      );
    }
    const opened = await openThreadFromBody({ deps, req, params, body });
    if (opened instanceof Response) return opened;
    await opened.thread.setArchived(archivedAt);
    return json(200, { threadId: opened.thread.id, archivedAt });
  };
}
