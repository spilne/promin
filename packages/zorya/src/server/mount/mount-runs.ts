// ---------------------------------------------------------------------------
// Runs and workflows API: run listing and detail, run actions, triggers,
// signals and signal tokens, workflow versions, streams, run events, worker
// queries, the Workers page and metrics.
// ---------------------------------------------------------------------------

import type {
  NamespaceRegistry,
  Workflow,
  WallClock,
  WorkflowStorage,
  WorkflowVersionRegistry,
} from "@promin/workflow";
import type { WorkflowAdvertisementRegistry } from "@promin/workflow/distributed";
import type { Handler, RouteAccess, RouteTargetRequest, Router } from "../router.ts";
import type { RunWatcherPool } from "../run-poll-watcher.ts";
import type { AgentStreamHub } from "../services/agent-stream-hub.ts";
import type { WorkerWebSocketServer } from "../services/worker-ws-server.ts";
import type { ZoryaWorkflows } from "../services/workflows/index.ts";
import { RunsService } from "../services/runs-service.ts";
import { streamAgentEvents } from "../routes/agent-stream.ts";
import { getSparklines, getWorkflowGrid, getWorkflowHistory } from "../routes/grid.ts";
import { getMetrics, type MetricsProvider } from "../routes/metrics.ts";
import { queryRun } from "../routes/query.ts";
import {
  getRunAttempts,
  getRunChildren,
  getRunHistory,
  getRunSignals,
  getRunStepJournal,
  rerunRun,
  resumeRun,
  type RunResumer,
} from "../routes/run-extras.ts";
import {
  cancelRun,
  getRun,
  getUpdate,
  listRuns,
  listWorkflowNames,
  markRunFailed,
  markRunSuccess,
  sendSignal,
  sendUpdate,
  triggerRun,
  type RunRoutesDeps,
} from "../routes/runs.ts";
import {
  completeSignalToken,
  describeSignalToken,
  listSignalTokensForRun,
  mintSignalToken,
} from "../routes/signal-tokens.ts";
import { listSignals } from "../routes/signals.ts";
import { streamRunEvents } from "../routes/sse.ts";
import { getStreamChunks, sendStreamChunk, streamChunks } from "../routes/streams.ts";
import { getWorkflowDef, listWorkflowDefs } from "../routes/workflow-defs.ts";
import {
  createVersionSeeder,
  getActiveWorkflowVersion,
  listWorkflowVersions,
  promoteWorkflowVersion,
  rollbackWorkflow,
  type WorkflowVersionsRoutesDeps,
} from "../routes/workflow-versions.ts";
import { listWorkers, type WorkersProvider } from "../routes/workers.ts";
import { ORG_WIDE_NAMESPACE, type SecurityResourceRef } from "../security/index.ts";
import { orgTarget, protectedAccess, queryNamespaceTarget } from "./route-security.ts";

export interface MountRunsParams {
  readonly router: Router;
  readonly workflows: ZoryaWorkflows;
  readonly storage: WorkflowStorage;
  readonly clock: WallClock;
  /** In-process definitions found on the workflows chain. */
  readonly definitions: Readonly<Record<string, Workflow<unknown, unknown>>> | undefined;
  /** Worker advertisements found on the workflows chain. */
  readonly advertisements: WorkflowAdvertisementRegistry | undefined;
  /** The registry the workflows chain dispatches versions through. */
  readonly versionRegistry: WorkflowVersionRegistry | undefined;
  readonly runWatchers: RunWatcherPool;
  readonly agentStreamHub: AgentStreamHub;
  readonly workerWs: WorkerWebSocketServer;
  readonly workers: WorkersProvider;
  readonly metrics: MetricsProvider;
  readonly sampleInput: ((workflowName: string) => unknown) | undefined;
  readonly sseIntervalMs: number | undefined;
  readonly signalTokenMaxTtlMs: number | undefined;
  readonly publicBaseUrl: string | undefined;
  /** Registry an explicit run namespace is checked against. */
  readonly namespaces: NamespaceRegistry | undefined;
}

export function mountRuns(params: MountRunsParams): void {
  const { router, workflows, storage, clock, definitions, advertisements } = params;
  const { runWatchers, sampleInput, sseIntervalMs } = params;

  const deps: RunRoutesDeps = {
    storage,
    clock,
    trigger: (name, input, opts) => workflows.trigger(name, input, opts),
    ...(definitions !== undefined && { workflows: definitions }),
    ...(advertisements !== undefined && { advertisements }),
    ...(params.namespaces !== undefined && { namespaces: params.namespaces }),
    authorizeRunContinuation: (request) => workflows.checkStoredRunSecurity(request),
  };
  const runActions = new RunsService({
    storage,
    clock,
    ...(definitions !== undefined && { workflows: definitions }),
    ...(advertisements !== undefined && { advertisements }),
  });
  const resume: RunResumer = (p) => workflows.resume(p);
  const defDeps = {
    ...(definitions !== undefined && { workflows: definitions }),
    isInternal: (name: string) => workflows.isInternal(name),
    ...(sampleInput !== undefined && { sampleInput }),
    ...(advertisements !== undefined && { advertisements }),
  };
  const declared = (params: {
    readonly method: string;
    readonly path: string;
    readonly access: RouteAccess;
    readonly handler: Handler;
  }): void => {
    router.route(params);
  };
  const storedRun = async ({
    params: route,
  }: RouteTargetRequest): Promise<SecurityResourceRef<"run">> => {
    const workflowId = route.id!;
    const run = await storage.loadWorkflow(workflowId);
    return {
      kind: "run",
      namespaceId: run?.namespace ?? ORG_WIDE_NAMESPACE,
      id: workflowId,
    };
  };
  const workflow = orgTarget({ kind: "workflow", idParam: "name" });
  const triggerTarget = async ({ request, params: route }: RouteTargetRequest) => {
    let namespaceId = ORG_WIDE_NAMESPACE;
    try {
      const body = (await request.clone().json()) as Record<string, unknown>;
      if (typeof body.namespace === "string" && body.namespace.length > 0) {
        namespaceId = body.namespace;
      }
    } catch {
      // Handler-owned schema validation preserves the existing 4xx answer.
    }
    return { kind: "run" as const, namespaceId, id: route.name };
  };
  const runListTarget = queryNamespaceTarget({
    kind: "run",
    query: "namespace",
  });
  const workflowListTarget = queryNamespaceTarget({
    kind: "workflow",
    query: "namespace",
  });
  const tokenDeps = {
    storage,
    clock,
    authorizeRunContinuation: deps.authorizeRunContinuation,
    ...(params.signalTokenMaxTtlMs !== undefined && {
      maxTtlMs: params.signalTokenMaxTtlMs,
    }),
    ...(params.publicBaseUrl !== undefined && {
      publicBaseUrl: params.publicBaseUrl,
    }),
  };

  // The version routes manage the registry the workflows layer dispatches
  // through. Its versioned definitions are registered on first use, so a
  // version the server runs can be promoted without a separate step.
  const versionDeps: WorkflowVersionsRoutesDeps = params.versionRegistry
    ? {
        registry: params.versionRegistry,
        prepare: createVersionSeeder({
          registry: params.versionRegistry,
          definitions: Object.values(definitions ?? {}),
        }),
      }
    : {};

  declared({
    method: "GET",
    path: "/api/runs",
    access: protectedAccess({
      resource: "run",
      action: "read",
      target: runListTarget,
    }),
    handler: listRuns(deps),
  });
  declared({
    method: "GET",
    path: "/api/workflows",
    access: protectedAccess({
      resource: "workflow",
      action: "read",
      target: workflowListTarget,
    }),
    handler: listWorkflowNames(deps),
  });
  declared({
    method: "GET",
    path: "/api/runs/:id",
    access: protectedAccess({
      resource: "run",
      action: "read",
      target: storedRun,
    }),
    handler: getRun(deps),
  });
  declared({
    method: "GET",
    path: "/api/runs/:id/signals",
    access: protectedAccess({
      resource: "run",
      action: "read",
      target: storedRun,
    }),
    handler: getRunSignals(storage),
  });
  declared({
    method: "GET",
    path: "/api/runs/:id/attempts",
    access: protectedAccess({
      resource: "run",
      action: "read",
      target: storedRun,
    }),
    handler: getRunAttempts(storage),
  });
  declared({
    method: "GET",
    path: "/api/runs/:id/history",
    access: protectedAccess({
      resource: "run",
      action: "read",
      target: storedRun,
    }),
    handler: getRunHistory(storage),
  });
  declared({
    method: "GET",
    path: "/api/runs/:id/children",
    access: protectedAccess({
      resource: "run",
      action: "read",
      target: storedRun,
    }),
    handler: getRunChildren(storage),
  });
  declared({
    method: "GET",
    path: "/api/runs/:id/journal/:stepName",
    access: protectedAccess({
      resource: "run",
      action: "read",
      target: storedRun,
    }),
    handler: getRunStepJournal(storage),
  });
  declared({
    method: "GET",
    path: "/api/signals",
    access: protectedAccess({
      resource: "run",
      action: "read",
      target: runListTarget,
    }),
    handler: listSignals(storage),
  });
  declared({
    method: "POST",
    path: "/api/runs/:id/mark-success",
    access: protectedAccess({
      resource: "run",
      action: "cancel",
      target: storedRun,
    }),
    handler: markRunSuccess(runActions),
  });
  declared({
    method: "POST",
    path: "/api/runs/:id/mark-failed",
    access: protectedAccess({
      resource: "run",
      action: "cancel",
      target: storedRun,
    }),
    handler: markRunFailed(runActions),
  });
  declared({
    method: "POST",
    path: "/api/runs/:id/rerun",
    access: protectedAccess({
      resource: "run",
      action: "cancel",
      target: storedRun,
    }),
    handler: rerunRun({ storage, rerun: (id) => workflows.rerun(id) }),
  });
  declared({
    method: "POST",
    path: "/api/runs/:id/resume",
    access: protectedAccess({
      resource: "run",
      action: "cancel",
      target: storedRun,
    }),
    handler: resumeRun({ storage, runs: runActions, resume }),
  });
  // Start a run. Under the workflow (not `/api/runs/...`) so no workflow
  // name can collide with a run action.
  declared({
    method: "POST",
    path: "/api/workflows/:name/runs",
    access: protectedAccess({
      resource: "run",
      action: "trigger",
      target: triggerTarget,
    }),
    handler: triggerRun(deps),
  });
  declared({
    method: "GET",
    path: "/api/workflows/sparklines",
    access: protectedAccess({
      resource: "workflow",
      action: "read",
      target: workflowListTarget,
    }),
    handler: getSparklines(storage),
  });
  declared({
    method: "GET",
    path: "/api/workflows/definitions",
    access: protectedAccess({ resource: "workflow", action: "read" }),
    handler: listWorkflowDefs(defDeps),
  });
  declared({
    method: "GET",
    path: "/api/workflows/:name/grid",
    access: protectedAccess({
      resource: "workflow",
      action: "read",
      target: workflow,
    }),
    handler: getWorkflowGrid(storage),
  });
  declared({
    method: "GET",
    path: "/api/workflows/:name/history",
    access: protectedAccess({
      resource: "workflow",
      action: "read",
      target: workflow,
    }),
    handler: getWorkflowHistory(storage),
  });
  declared({
    method: "GET",
    path: "/api/workflows/:name/definition",
    access: protectedAccess({
      resource: "workflow",
      action: "read",
      target: workflow,
    }),
    handler: getWorkflowDef(defDeps),
  });
  declared({
    method: "POST",
    path: "/api/runs/:id/cancel",
    access: protectedAccess({
      resource: "run",
      action: "cancel",
      target: storedRun,
    }),
    handler: cancelRun(deps),
  });
  declared({
    method: "POST",
    path: "/api/runs/:id/signal",
    access: protectedAccess({
      resource: "run",
      action: "signal",
      target: storedRun,
    }),
    handler: sendSignal(deps),
  });
  declared({
    method: "POST",
    path: "/api/runs/:id/updates",
    access: protectedAccess({
      resource: "run",
      action: "signal",
      target: storedRun,
    }),
    handler: sendUpdate(deps),
  });
  declared({
    method: "GET",
    path: "/api/runs/:id/updates/:updateId",
    access: protectedAccess({
      resource: "run",
      action: "read",
      target: storedRun,
    }),
    handler: getUpdate(deps),
  });
  // Signal tokens — public-bearer auth for deliverSignal. Mint and list
  // are auth-gated through the regular /api auth layer; /describe and
  // /complete are reachable without Zorya credentials and validate the
  // token bearer themselves.
  declared({
    method: "POST",
    path: "/api/runs/:id/signals/:name/token",
    access: protectedAccess({
      resource: "run",
      action: "signal",
      target: storedRun,
    }),
    handler: mintSignalToken(tokenDeps),
  });
  declared({
    method: "GET",
    path: "/api/runs/:id/signal-tokens",
    access: protectedAccess({
      resource: "run",
      action: "signal",
      target: storedRun,
    }),
    handler: listSignalTokensForRun(tokenDeps),
  });
  declared({
    method: "GET",
    path: "/api/signal-tokens/:tokenId/describe",
    access: { kind: "signal-token" },
    handler: describeSignalToken(tokenDeps),
  });
  declared({
    method: "POST",
    path: "/api/signal-tokens/:tokenId/complete",
    access: { kind: "signal-token" },
    handler: completeSignalToken(tokenDeps),
  });
  // Workflow versions — thin layer over WorkflowVersionRegistry's
  // lifecycle methods. Versions are a property of a workflow, so the
  // routes nest under /api/workflows/:name/...
  declared({
    method: "GET",
    path: "/api/workflows/:name/versions",
    access: protectedAccess({
      resource: "workflow",
      action: "read",
      target: workflow,
    }),
    handler: listWorkflowVersions(versionDeps),
  });
  declared({
    method: "GET",
    path: "/api/workflows/:name/versions/active",
    access: protectedAccess({
      resource: "workflow",
      action: "read",
      target: workflow,
    }),
    handler: getActiveWorkflowVersion(versionDeps),
  });
  declared({
    method: "POST",
    path: "/api/workflows/:name/versions/:version/promote",
    access: protectedAccess({
      resource: "workflow",
      action: "update",
      target: workflow,
    }),
    handler: promoteWorkflowVersion(versionDeps),
  });
  declared({
    method: "POST",
    path: "/api/workflows/:name/rollback",
    access: protectedAccess({
      resource: "workflow",
      action: "update",
      target: workflow,
    }),
    handler: rollbackWorkflow(versionDeps),
  });
  // Streams — generic typed channels per run. Output (workflow →
  // subscribers) reads via the SSE handler; input (subscribers →
  // workflow) appends via the POST handler.
  declared({
    method: "GET",
    path: "/api/runs/:id/streams/:streamId",
    access: protectedAccess({
      resource: "run",
      action: "read",
      target: storedRun,
    }),
    handler: streamChunks({
      storage,
      clock,
      watchers: runWatchers,
      ...(sseIntervalMs !== undefined && { pollIntervalMs: sseIntervalMs }),
    }),
  });
  declared({
    method: "GET",
    path: "/api/runs/:id/streams/:streamId/chunks",
    access: protectedAccess({
      resource: "run",
      action: "read",
      target: storedRun,
    }),
    handler: getStreamChunks({ storage }),
  });
  declared({
    method: "POST",
    path: "/api/runs/:id/streams/:streamId",
    access: protectedAccess({
      resource: "run",
      action: "signal",
      target: storedRun,
    }),
    handler: sendStreamChunk({ storage }),
  });
  declared({
    method: "GET",
    path: "/api/runs/:id/events",
    access: protectedAccess({
      resource: "run",
      action: "read",
      target: storedRun,
    }),
    handler: streamRunEvents({ storage, watchers: runWatchers, clock }),
  });
  declared({
    method: "GET",
    path: "/api/runs/:id/agent-stream",
    access: protectedAccess({
      resource: "run",
      action: "read",
      target: storedRun,
    }),
    handler: streamAgentEvents(params.agentStreamHub),
  });
  declared({
    method: "POST",
    path: "/api/runs/:id/query",
    access: protectedAccess({
      resource: "run",
      action: "signal",
      target: storedRun,
    }),
    handler: queryRun({ workerWs: params.workerWs, clock }),
  });
  declared({
    method: "GET",
    path: "/api/workers",
    access: protectedAccess({
      resource: "worker",
      action: "read",
      target: orgTarget({ kind: "worker" }),
    }),
    handler: listWorkers(params.workers),
  });
  declared({
    method: "GET",
    path: "/api/metrics",
    access: protectedAccess({
      resource: "usage",
      action: "read",
      target: orgTarget({ kind: "usage" }),
    }),
    handler: getMetrics(params.metrics),
  });
}
