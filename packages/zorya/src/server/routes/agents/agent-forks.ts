import {
  THREAD_AGENT_METADATA_KEY,
  ThreadAwaitingApprovalError,
  ThreadExistsError,
  type MemoryStore,
} from "@promin/agent";
import { z } from "zod";
import { json, jsonError, readJson } from "../../router.ts";
import { internalError } from "../agent-gateway-kit.ts";
import type { AgentGatewayDeps } from "./agent-gateway-deps.ts";
import { acquireTurnLease, openThreadFromBody, openThreadFromQuery } from "./agent-scope.ts";

const ForkBody = z.object({
  namespaceId: z.string(),
  resourceId: z.string().optional(),
  ownerId: z.string().optional(),
  newThreadId: z.string().min(1).max(200),
  atSeq: z.number().int().min(0).optional(),
});

export function forkAgentThread(deps: AgentGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const parsed = ForkBody.safeParse(await readJson<unknown>(req));
    if (!parsed.success) return jsonError(400, "invalid_fork");
    const opened = await openThreadFromBody({ deps, req, params, body: parsed.data });
    if (opened instanceof Response) return opened;
    if (!opened.scoped.agent.forkThread) return jsonError(501, "fork_unsupported");
    const lease = await acquireTurnLease({
      deps,
      key: { namespaceId: opened.scoped.namespaceId, threadId: opened.thread.id },
    });
    if (lease instanceof Response) return lease;
    try {
      const child = await opened.scoped.agent.forkThread(
        opened.thread.id,
        parsed.data.newThreadId,
        parsed.data.atSeq === undefined ? {} : { atSeq: parsed.data.atSeq },
      );
      return json(201, {
        threadId: child.id,
        parentThreadId: opened.thread.id,
        lineage: await child.metadata(),
      });
    } catch (error) {
      if (error instanceof RangeError) return jsonError(400, "invalid_branch_seq");
      if (error instanceof ThreadAwaitingApprovalError) return jsonError(409, "approval_pending");
      if (error instanceof ThreadExistsError) return jsonError(409, "thread_exists");
      return internalError({ code: "fork_failed", err: error });
    } finally {
      await lease.release();
    }
  };
}

export function getAgentThreadLineage(deps: AgentGatewayDeps, memory: MemoryStore) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const opened = await openThreadFromQuery({ deps, req, params });
    if (opened instanceof Response) return opened;
    const metadata = await opened.thread.metadata();
    const children = await memory.listThreads({
      namespaceId: opened.scoped.namespaceId,
      resourceId: opened.scoped.resourceId,
      metadataFilter: {
        [THREAD_AGENT_METADATA_KEY]: opened.scoped.recipe.id,
        branchParentThreadId: opened.thread.id,
      },
      limit: 1_000,
    });
    return json(200, {
      threadId: opened.thread.id,
      parentThreadId: metadata.branchParentThreadId ?? null,
      branchAtSeq: metadata.branchAtSeq ?? null,
      branchDepth: metadata.branchDepth ?? 0,
      children: children.map((child) => ({
        threadId: child.threadId,
        branchAtSeq: child.metadata.branchAtSeq,
      })),
    });
  };
}
