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
import type { Router } from "../router.ts";
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
  listRuns,
  listWorkflowNames,
  markRunFailed,
  markRunSuccess,
  sendSignal,
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
    trigger: (name, input, opts) => workflows.trigger(name, input, opts),
    ...(definitions !== undefined && { workflows: definitions }),
    ...(advertisements !== undefined && { advertisements }),
    ...(params.namespaces !== undefined && { namespaces: params.namespaces }),
  };
  const runActions = new RunsService({
    storage,
    ...(definitions !== undefined && { workflows: definitions }),
    ...(advertisements !== undefined && { advertisements }),
  });
  const resume: RunResumer = (p) => workflows.resume(p);
  const defDeps = {
    ...(definitions !== undefined && { workflows: definitions }),
    ...(sampleInput !== undefined && { sampleInput }),
    ...(advertisements !== undefined && { advertisements }),
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

  router
    .get("/api/runs", listRuns(deps))
    .get("/api/workflows", listWorkflowNames(deps))
    .get("/api/runs/:id", getRun(deps))
    .get("/api/runs/:id/signals", getRunSignals(storage))
    .get("/api/runs/:id/attempts", getRunAttempts(storage))
    .get("/api/runs/:id/history", getRunHistory(storage))
    .get("/api/runs/:id/children", getRunChildren(storage))
    .get("/api/runs/:id/journal/:stepName", getRunStepJournal(storage))
    .get("/api/signals", listSignals(storage))
    .post("/api/runs/:id/mark-success", markRunSuccess(runActions))
    .post("/api/runs/:id/mark-failed", markRunFailed(runActions))
    .post("/api/runs/:id/rerun", rerunRun({ storage, rerun: (id) => workflows.rerun(id) }))
    .post("/api/runs/:id/resume", resumeRun({ storage, runs: runActions, resume }))
    // Start a run. Under the workflow (not `/api/runs/...`) so no workflow
    // name can collide with a run action.
    .post("/api/workflows/:name/runs", triggerRun(deps))
    .get("/api/workflows/sparklines", getSparklines(storage))
    .get("/api/workflows/definitions", listWorkflowDefs(defDeps))
    .get("/api/workflows/:name/grid", getWorkflowGrid(storage))
    .get("/api/workflows/:name/history", getWorkflowHistory(storage))
    .get("/api/workflows/:name/definition", getWorkflowDef(defDeps))
    .post("/api/runs/:id/cancel", cancelRun(deps))
    .post("/api/runs/:id/signal", sendSignal(deps))
    // Signal tokens — public-bearer auth for deliverSignal. Mint and list
    // are auth-gated through the regular /api auth layer; /describe and
    // /complete are reachable without Zorya credentials and validate the
    // token bearer themselves.
    .post(
      "/api/runs/:id/signals/:name/token",
      mintSignalToken({
        storage,
        clock,
        ...(params.signalTokenMaxTtlMs !== undefined && { maxTtlMs: params.signalTokenMaxTtlMs }),
        ...(params.publicBaseUrl !== undefined && { publicBaseUrl: params.publicBaseUrl }),
      }),
    )
    .get("/api/runs/:id/signal-tokens", listSignalTokensForRun({ storage }))
    .get("/api/signal-tokens/:tokenId/describe", describeSignalToken({ storage, clock }))
    .post("/api/signal-tokens/:tokenId/complete", completeSignalToken({ storage, clock }))
    // Workflow versions — thin layer over WorkflowVersionRegistry's
    // lifecycle methods. Versions are a property of a workflow, so the
    // routes nest under /api/workflows/:name/...
    .get("/api/workflows/:name/versions", listWorkflowVersions(versionDeps))
    .get("/api/workflows/:name/versions/active", getActiveWorkflowVersion(versionDeps))
    .post("/api/workflows/:name/versions/:version/promote", promoteWorkflowVersion(versionDeps))
    .post("/api/workflows/:name/rollback", rollbackWorkflow(versionDeps))
    // Streams — generic typed channels per run. Output (workflow →
    // subscribers) reads via the SSE handler; input (subscribers →
    // workflow) appends via the POST handler.
    .get(
      "/api/runs/:id/streams/:streamId",
      streamChunks({
        storage,
        clock,
        watchers: runWatchers,
        ...(sseIntervalMs !== undefined && { pollIntervalMs: sseIntervalMs }),
      }),
    )
    .get("/api/runs/:id/streams/:streamId/chunks", getStreamChunks({ storage }))
    .post("/api/runs/:id/streams/:streamId", sendStreamChunk({ storage }))
    .get("/api/runs/:id/events", streamRunEvents({ storage, watchers: runWatchers, clock }))
    .get("/api/runs/:id/agent-stream", streamAgentEvents(params.agentStreamHub))
    .post("/api/runs/:id/query", queryRun({ workerWs: params.workerWs, clock }))
    .get("/api/workers", listWorkers(params.workers))
    .get("/api/metrics", getMetrics(params.metrics));
}
