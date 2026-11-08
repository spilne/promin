import { TurnInProgressError } from "@promin/agent";
import { z } from "zod";
import { json, jsonError, readJson, type Router } from "../../router.ts";
import { internalError } from "../agent-gateway-kit.ts";
import {
  RetainedSubagentError,
  type RetainedParent,
  type RetainedSubagents,
} from "../../services/agents/retained-subagents.ts";
import type { AgentGatewayDeps } from "./agent-gateway-deps.ts";
import { openThreadFromBody, openThreadFromQuery } from "./agent-scope.ts";

const ScopeBody = z.object({
  namespaceId: z.string(),
  resourceId: z.string().optional(),
  ownerId: z.string().optional(),
});
const SpawnBody = ScopeBody.extend({
  agentId: z.string().min(1),
  task: z.string().min(1).max(100_000),
  childId: z.string().min(1).max(200).optional(),
});
const MessageBody = ScopeBody.extend({
  task: z.string().min(1).max(100_000),
  mode: z.enum(["queued", "follow-up", "steer"]).optional(),
  messageId: z.string().min(1).max(200).optional(),
});

function parentOf(opened: {
  thread: { id: string };
  scoped: {
    namespaceId: string;
    resourceId: string;
    recipe: { id: string };
  };
}): RetainedParent {
  return {
    namespaceId: opened.scoped.namespaceId,
    resourceId: opened.scoped.resourceId,
    agentId: opened.scoped.recipe.id,
    threadId: opened.thread.id,
  };
}

function failure(error: unknown): Response {
  if (error instanceof RetainedSubagentError) {
    switch (error.code) {
      case "not_found":
        return jsonError(404, "child_not_found");
      case "forbidden":
        return jsonError(403, "delegation_forbidden");
      case "invalid":
        return jsonError(400, "invalid_child_request");
      case "conflict":
        return jsonError(409, "child_conflict");
    }
  }
  if (error instanceof TurnInProgressError) return jsonError(409, "child_conflict");
  return internalError({ code: "retained_agent_failed", err: error });
}

export function mountRetainedAgentRoutes(
  deps: AgentGatewayDeps,
  retained: RetainedSubagents,
  router: Router,
) {
  const base = "/api/agents/:id/threads/:threadId/children";
  router.post(base, async (req, params) => {
    const parsed = SpawnBody.safeParse(await readJson<unknown>(req));
    if (!parsed.success) return jsonError(400, "invalid_child_request");
    const opened = await openThreadFromBody({ deps, req, params, body: parsed.data });
    if (opened instanceof Response) return opened;
    // The child agent is authorized like any agent the caller invokes.
    if (deps.authorizeScope) {
      const allowed = await deps.authorizeScope({
        req,
        scope: {
          namespaceId: opened.scoped.namespaceId,
          resourceId: opened.scoped.resourceId,
          agentId: parsed.data.agentId,
        },
      });
      if (!allowed) return jsonError(403, "scope_forbidden");
    }
    try {
      return json(
        202,
        await retained.spawnAgent({
          parent: parentOf(opened),
          agentId: parsed.data.agentId,
          task: parsed.data.task,
          ...(parsed.data.childId && { childId: parsed.data.childId }),
        }),
      );
    } catch (error) {
      return failure(error);
    }
  });
  router.get(base, async (req, params) => {
    const opened = await openThreadFromQuery({ deps, req, params });
    if (opened instanceof Response) return opened;
    try {
      return json(200, { children: await retained.listChildAgents(parentOf(opened)) });
    } catch (error) {
      return failure(error);
    }
  });
  router.get(`${base}/:childId/messages`, async (req, params) => {
    const opened = await openThreadFromQuery({ deps, req, params });
    if (opened instanceof Response) return opened;
    if (!params.childId) return jsonError(400, "missing_childId");
    try {
      return json(200, await retained.readAgentMessages(parentOf(opened), params.childId));
    } catch (error) {
      return failure(error);
    }
  });
  router.post(`${base}/:childId/messages`, async (req, params) => {
    const parsed = MessageBody.safeParse(await readJson<unknown>(req));
    if (!parsed.success || !params.childId) return jsonError(400, "invalid_child_message");
    const opened = await openThreadFromBody({ deps, req, params, body: parsed.data });
    if (opened instanceof Response) return opened;
    try {
      return json(
        202,
        await retained.messageAgent({
          parent: parentOf(opened),
          childId: params.childId,
          task: parsed.data.task,
          ...(parsed.data.mode && { mode: parsed.data.mode }),
          ...(parsed.data.messageId && { messageId: parsed.data.messageId }),
        }),
      );
    } catch (error) {
      return failure(error);
    }
  });
  router.post(`${base}/:childId/stop`, async (req, params) => {
    const parsed = ScopeBody.safeParse(await readJson<unknown>(req));
    if (!parsed.success || !params.childId) return jsonError(400, "invalid_child_request");
    const opened = await openThreadFromBody({ deps, req, params, body: parsed.data });
    if (opened instanceof Response) return opened;
    try {
      await retained.stopAgent(parentOf(opened), params.childId);
      return json(200, { stopped: true });
    } catch (error) {
      return failure(error);
    }
  });
  router.delete(`${base}/:childId`, async (req, params) => {
    const opened = await openThreadFromQuery({ deps, req, params });
    if (opened instanceof Response) return opened;
    if (!params.childId) return jsonError(400, "missing_childId");
    try {
      await retained.deleteChildAgent(parentOf(opened), params.childId);
      return json(200, { deleted: true });
    } catch (error) {
      return failure(error);
    }
  });
}
