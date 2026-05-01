// ---------------------------------------------------------------------------
// ZoryaServer — HTTP server that wraps a ZoryaWorkflows service (plus
// optional scheduler / agents).
//
// Usage:
// ```ts
// const workflows = new LocalWorkflows({ storage, runner, definitions });
// const server = new ZoryaServer({
//   workflows,
//   apiKeys: ["secret"],
// });
// server.listen({ port: 4000 });
// ```
// ---------------------------------------------------------------------------

import type { StepQueue, WorkerRegistry, WorkflowStorage } from "@promin/workflow";
import { InMemoryStepQueue, InMemoryWorkerRegistry } from "@promin/workflow";
import { createWorkerApiHandler, createWorkflowStorageHandler } from "@promin/workflow-remote";
import { Auth, type AuthConfig } from "./auth.ts";
import { Router, jsonError } from "./router.ts";
import type { WorkflowStartQueue } from "./workflow-starts.ts";
import { WorkerWebSocketServer } from "./services/worker-ws-server.ts";
import {
  listAdvertisements,
  removeAdvertisements,
  upsertAdvertisements,
} from "./routes/advertisements.ts";
import {
  claimWorkflowStarts,
  completeWorkflowStart,
  listWorkflowStarts,
} from "./routes/workflow-starts.ts";
import { RunEventBus } from "./run-event-bus.ts";
import {
  cancelRun,
  getRun,
  listRuns,
  listWorkflowNames,
  sendSignal,
  triggerRun,
} from "./routes/runs.ts";
import { streamRunEvents } from "./routes/sse.ts";
import { streamAgentEvents } from "./routes/agent-stream.ts";
import { AgentStreamHub } from "./services/agent-stream-hub.ts";
import { queryRun } from "./routes/query.ts";
import { getMetrics, StorageMetricsProvider, type MetricsProvider } from "./routes/metrics.ts";
import {
  RegistryBackedWorkersProvider,
  emptyWorkersProvider,
  listWorkers,
  type WorkersProvider,
} from "./routes/workers.ts";
import {
  createSchedule,
  deleteSchedule,
  emitScheduleNow,
  getSchedule,
  getScheduleHistory,
  getScheduleUpcoming,
  listSchedules,
  patchSchedule,
} from "./routes/schedules.ts";
import {
  getRunAttempts,
  getRunChildren,
  getRunHistory,
  getRunSignals,
  getRunStepJournal,
  markRunFailed,
  markRunSuccess,
  rerunRun,
} from "./routes/run-extras.ts";
import { listApprovals } from "./routes/approvals.ts";
import { getSparklines, getWorkflowGrid, getWorkflowHistory } from "./routes/grid.ts";
import { getWorkflowDef, listWorkflowDefs } from "./routes/workflow-defs.ts";
import {
  compactThread,
  archiveAgentThread,
  distillThread,
  getAgent,
  invokeAgent,
  listAgentThreads,
  listAgents,
  listThreadMessages,
  renameAgentThread,
  sendThreadMessage,
  streamAgent,
  streamThreadApproval,
  streamThreadMessage,
  type AgentGatewayDeps,
} from "./routes/agents.ts";
import {
  addNamespaceFact,
  deleteNamespaceFact,
  inspectMemory,
  patchNamespace,
} from "./routes/memory.ts";
import {
  deleteAgentInstance,
  getAgentInstance,
  listAgentInstances,
  listInstancesAcrossAgents,
  resolveAgentInstance,
  updateAgentInstance,
} from "./routes/instances.ts";
import {
  ZoryaWorkflows,
  DistributedWorkflows,
  QueuedWorkflows,
} from "./services/workflows/index.ts";
import { ZoryaScheduler } from "./services/scheduler/index.ts";
import { ZoryaAgents } from "./services/agents/index.ts";

export interface Logger {
  log(message: string, ...args: unknown[]): void;
  warn(message: string, ...args: unknown[]): void;
  error(message: string, ...args: unknown[]): void;
}

/**
 * Mounts the remote-worker RPC surface (`/rpc/storage`, `/rpc/worker`,
 * `/api/advertisements`, `/api/worker-protocol/*`). The `stepQueue`,
 * `workerRegistry`, `advertisements`, and `workflowStarts` are pulled from
 * the configured WorkflowsService (typed as DistributedWorkflows or
 * QueuedWorkflows). An error is thrown at construction if the configured
 * service doesn't expose what's needed.
 */
export interface RemoteWorkersConfig {
  /** API keys gating access to the worker-protocol endpoints. Open when omitted. */
  apiKeys?: ReadonlyArray<string>;
}

export interface ZoryaServerConfig extends AuthConfig {
  /** Workflow execution + state. Required. */
  workflows: ZoryaWorkflows;
  /** Optional scheduler service (CRUD + tick loop + emit-now). */
  scheduler?: ZoryaScheduler;
  /** Optional agent gateway service. */
  agents?: ZoryaAgents;
  /**
   * Mount the remote-worker HTTP surface. Pulls stepQueue / workerRegistry /
   * advertisements / workflowStarts from the workflows service.
   */
  remoteWorkers?: RemoteWorkersConfig;

  /** Override metrics provider. Default: StorageMetricsProvider(workflows.storage). */
  metrics?: MetricsProvider;
  /** Plug a workers provider. Default: derived from workflows when distributed. */
  workers?: WorkersProvider;
  /** Sample input for the dashboard's trigger form, by workflow name. */
  sampleInput?: (workflowName: string) => unknown;
  /** Custom logger. Default: console. */
  logger?: Logger;
  /** Static-asset directory (compiled UI). */
  uiDir?: string;
  /** SSE poll interval in ms. Default 1000. */
  sseIntervalMs?: number;
}

export interface ListenOptions {
  port?: number;
  hostname?: string;
}

export class ZoryaServer {
  readonly workflows: ZoryaWorkflows;
  readonly scheduler?: ZoryaScheduler;
  readonly agents?: ZoryaAgents;
  /**
   * Worker → server WebSocket multiplexer. Always present — workers in
   * step / agent mode connect on `/ws/worker` to receive server-pushed
   * commands and push frames back.
   */
  readonly workerWs: WorkerWebSocketServer;
  /**
   * Bridges WS frames from workers to SSE clients on
   * `/api/runs/:id/agent-stream`. Always present.
   */
  readonly agentStreamHub: AgentStreamHub;

  private readonly auth: Auth;
  private readonly workerAuth: Auth;
  private readonly bus: RunEventBus;
  private readonly router: Router;
  private readonly logger: Logger;
  private serverHandle?: { stop(): void; port: number; hostname: string };

  constructor(config: ZoryaServerConfig) {
    this.workflows = config.workflows;
    if (config.scheduler) this.scheduler = config.scheduler;
    if (config.agents) this.agents = config.agents;

    this.logger = config.logger ?? console;
    this.auth = new Auth(config);
    this.workerAuth = new Auth({ apiKeys: config.remoteWorkers?.apiKeys });
    this.bus = new RunEventBus();
    this.workerWs = new WorkerWebSocketServer({
      authorize: (req) => this.workerAuth.check(req),
    });
    this.agentStreamHub = new AgentStreamHub(this.workerWs);

    const storage: WorkflowStorage = this.workflows.storage;
    const advertisements = this.workflows.advertisements;
    const definitions = this.workflows.definitions;

    const metrics = config.metrics ?? new StorageMetricsProvider(storage);
    // Workers provider: explicit > derived from DistributedWorkflows.workerRegistry > empty
    const workers =
      config.workers ??
      (this.workflows instanceof DistributedWorkflows && this.workflows.workerRegistry
        ? new RegistryBackedWorkersProvider(this.workflows.workerRegistry)
        : emptyWorkersProvider);

    const trigger = (
      name: string,
      input: unknown,
      opts?: Parameters<typeof this.workflows.trigger>[2],
    ) => this.workflows.trigger(name, input, opts);

    const deps = {
      storage,
      trigger,
      ...(definitions !== undefined && { workflows: definitions }),
      ...(advertisements !== undefined && { advertisements }),
    };

    this.router = new Router()
      .get(
        "/api/health",
        () =>
          new Response(JSON.stringify({ ok: true }), {
            headers: { "content-type": "application/json" },
          }),
      )
      .get("/api/runs", listRuns(deps))
      .get("/api/workflows", listWorkflowNames(deps))
      .get("/api/runs/:id", getRun(deps))
      .get("/api/runs/:id/signals", getRunSignals(storage))
      .get("/api/runs/:id/attempts", getRunAttempts(storage))
      .get("/api/runs/:id/history", getRunHistory(storage))
      .get("/api/runs/:id/children", getRunChildren(storage))
      .get("/api/runs/:id/journal/:stepName", getRunStepJournal(storage))
      .get("/api/approvals", listApprovals(storage))
      .post("/api/runs/:id/mark-success", markRunSuccess(storage))
      .post("/api/runs/:id/mark-failed", markRunFailed(storage))
      .post(
        "/api/runs/:id/rerun",
        rerunRun(storage, (id) => this.workflows.rerun(id)),
      )
      .get("/api/workflows/sparklines", getSparklines(storage))
      .get(
        "/api/workflows/definitions",
        listWorkflowDefs({
          ...(definitions !== undefined && { workflows: definitions }),
          ...(config.sampleInput !== undefined && { sampleInput: config.sampleInput }),
          ...(advertisements !== undefined && { advertisements }),
        }),
      )
      .get("/api/workflows/:name/grid", getWorkflowGrid(storage))
      .get("/api/workflows/:name/history", getWorkflowHistory(storage))
      .get(
        "/api/workflows/:name/definition",
        getWorkflowDef({
          ...(definitions !== undefined && { workflows: definitions }),
          ...(config.sampleInput !== undefined && { sampleInput: config.sampleInput }),
          ...(advertisements !== undefined && { advertisements }),
        }),
      )
      .post("/api/runs/trigger/:name", triggerRun(deps))
      .post("/api/runs/:id/cancel", cancelRun(deps))
      .post("/api/runs/:id/signal", sendSignal(deps))
      .get(
        "/api/runs/:id/events",
        streamRunEvents({
          storage,
          bus: this.bus,
          ...(config.sseIntervalMs !== undefined && { pollIntervalMs: config.sseIntervalMs }),
        }),
      )
      .get("/api/runs/:id/agent-stream", streamAgentEvents(this.agentStreamHub))
      .post("/api/runs/:id/query", queryRun(this.workerWs))
      .get("/api/workers", listWorkers(workers))
      .get("/api/metrics", getMetrics(metrics));

    if (this.agents) {
      const agentDeps: AgentGatewayDeps = {
        registry: this.agents.registry,
        resolve: this.agents.resolve,
        ...(this.agents.instances !== undefined && { instanceRegistry: this.agents.instances }),
      };
      this.router
        .get("/api/agents", listAgents(agentDeps))
        .get("/api/agents/:id", getAgent(agentDeps))
        .post("/api/agents/:id/invoke", invokeAgent(agentDeps))
        .post("/api/agents/:id/stream", streamAgent(agentDeps))
        .get("/api/agents/:id/threads", listAgentThreads(agentDeps))
        .patch("/api/agents/:id/threads/:threadId", renameAgentThread(agentDeps))
        .post("/api/agents/:id/threads/:threadId", sendThreadMessage(agentDeps))
        .post("/api/agents/:id/threads/:threadId/stream", streamThreadMessage(agentDeps))
        .post("/api/agents/:id/threads/:threadId/approve", streamThreadApproval(agentDeps))
        .get("/api/agents/:id/threads/:threadId/messages", listThreadMessages(agentDeps))
        .post("/api/agents/:id/threads/:threadId/distill", distillThread(agentDeps))
        .post("/api/agents/:id/threads/:threadId/compact", compactThread(agentDeps))
        .post("/api/agents/:id/threads/:threadId/archive", archiveAgentThread(agentDeps));

      if (this.agents.memory) {
        const mem = { memory: this.agents.memory };
        this.router
          .get("/api/memory/inspect", inspectMemory(mem))
          .patch("/api/memory/namespace/:namespaceId", patchNamespace(mem))
          .post("/api/memory/namespace/:namespaceId/facts", addNamespaceFact(mem))
          .delete("/api/memory/namespace/:namespaceId/facts/:factId", deleteNamespaceFact(mem));
      }

      if (this.agents.instances && this.agents.memory) {
        const inDeps = { registry: this.agents.instances, memory: this.agents.memory };
        this.router
          .get("/api/agents/:id/instances", listAgentInstances(inDeps))
          .post("/api/agents/:id/instances", resolveAgentInstance(inDeps))
          .get("/api/agents/:id/instances/:instanceId", getAgentInstance(inDeps))
          .patch("/api/agents/:id/instances/:instanceId", updateAgentInstance(inDeps))
          .delete("/api/agents/:id/instances/:instanceId", deleteAgentInstance(inDeps))
          .get("/api/instances", listInstancesAcrossAgents(inDeps));
      }
    }

    if (this.scheduler) {
      const sch = this.scheduler.storage;
      const sched = this.scheduler;
      this.router
        .get("/api/schedules", listSchedules(sch))
        .post("/api/schedules", createSchedule(sch))
        .get("/api/schedules/:id", getSchedule(sch))
        .patch("/api/schedules/:id", patchSchedule(sch))
        .delete("/api/schedules/:id", deleteSchedule(sch))
        .get("/api/schedules/:id/history", getScheduleHistory(sch, storage))
        .get("/api/schedules/:id/upcoming", getScheduleUpcoming(sch))
        .post(
          "/api/schedules/:id/emit",
          emitScheduleNow(sch, (id) => sched.fireOnce(id)),
        );
    } else {
      // Stub so the UI can tell "scheduler not configured" from "configured but empty".
      this.router.get(
        "/api/schedules",
        () =>
          new Response(JSON.stringify({ schedules: [], total: 0, configured: false }), {
            headers: { "content-type": "application/json" },
          }),
      );
    }

    if (config.remoteWorkers) {
      const remoteDeps = this.extractRemoteWorkerDeps();
      const storageHandler = createWorkflowStorageHandler(storage);
      this.router.post("/rpc/storage", (req) => storageHandler(req));

      // /rpc/worker covers BOTH the step-queue claim path (DistributedWorkflows)
      // AND the worker-registry / heartbeat path used by every connected
      // worker regardless of mode. Always mount it; supply an in-memory
      // step queue when the workflows chain doesn't expose one (workflow-mode
      // workers don't poll it but their heartbeats / list calls still need
      // a working endpoint).
      const stepQueueForRpc = remoteDeps.stepQueue ?? new InMemoryStepQueue();
      const workerRegistryForRpc = remoteDeps.workerRegistry ?? new InMemoryWorkerRegistry();
      const workerHandler = createWorkerApiHandler({
        stepQueue: stepQueueForRpc,
        storage,
        workerRegistry: workerRegistryForRpc,
      });
      this.router.post("/rpc/worker", (req) => workerHandler(req));

      if (advertisements) {
        this.router
          .post("/api/advertisements", upsertAdvertisements(advertisements))
          .delete("/api/advertisements/:workerId", removeAdvertisements(advertisements))
          .get("/api/advertisements", listAdvertisements(advertisements));
      }

      if (remoteDeps.workflowStarts) {
        const wfs = remoteDeps.workflowStarts;
        this.router
          .post("/api/worker-protocol/claim-starts", claimWorkflowStarts(wfs))
          .post("/api/worker-protocol/complete-start/:id", completeWorkflowStart(wfs))
          .get("/api/worker-protocol/starts", listWorkflowStarts(wfs));
      }
    }

    if (config.uiDir) {
      this.router.setStatic(this.buildStaticHandler(config.uiDir));
    }
  }

  /** Expose the router for tests or embedding. */
  async handle(req: Request): Promise<Response> {
    const url = new URL(req.url);
    const path = url.pathname;

    const isWorkerPath =
      path.startsWith("/rpc/") ||
      path.startsWith("/api/advertisements") ||
      path.startsWith("/api/worker-protocol/");

    if (isWorkerPath) {
      if (!this.workerAuth.check(req)) return jsonError(401, "unauthorized_worker");
    } else if (path.startsWith("/api/")) {
      if (!this.auth.check(req)) return jsonError(401, "unauthorized");
    }

    return this.router.handle(req);
  }

  listen(opts: ListenOptions = {}): { port: number; hostname: string; stop: () => void } {
    const port = opts.port ?? 4000;
    const hostname = opts.hostname ?? "0.0.0.0";
    const handle = this.handle.bind(this);
    const BunGlobal = (globalThis as { Bun?: { serve: typeof Bun.serve } }).Bun;
    if (!BunGlobal) {
      throw new Error("ZoryaServer.listen requires Bun runtime");
    }
    const wsHandlers = this.workerWs.websocketHandlers();
    const srv = BunGlobal.serve({
      port,
      hostname,
      fetch: (req, server) => {
        const upgradeResult = this.workerWs.upgradeIfWorkerWs(req, server);
        if (upgradeResult !== undefined) return upgradeResult;
        const url = new URL(req.url);
        if (url.pathname === "/ws/worker") return undefined;
        return handle(req);
      },
      websocket: wsHandlers,
    });
    const resolvedPort = typeof srv.port === "number" ? srv.port : port;
    const resolvedHost = typeof srv.hostname === "string" ? srv.hostname : hostname;
    this.serverHandle = { stop: () => srv.stop(), port: resolvedPort, hostname: resolvedHost };

    this.workerWs.start();
    this.agentStreamHub.start();

    // Fire and forget — services own their own logging on errors.
    void Promise.all([this.workflows.start(), this.scheduler?.start(), this.agents?.start()]).catch(
      (err) => this.logger.error("[zorya] service start error:", err),
    );

    return {
      port: resolvedPort,
      hostname: resolvedHost,
      stop: () => {
        this.stop();
      },
    };
  }

  async stop(): Promise<void> {
    this.serverHandle?.stop();
    this.serverHandle = undefined;
    this.agentStreamHub.stop();
    this.workerWs.stop();
    await Promise.all([this.workflows.stop(), this.scheduler?.stop(), this.agents?.stop()]).catch(
      () => {},
    );
  }

  /**
   * Pull the remote-worker deps from the configured workflows service.
   * DistributedWorkflows exposes stepQueue + workerRegistry for /rpc/worker;
   * QueuedWorkflows exposes workflowStarts for /api/worker-protocol/*. Either
   * mode (or both, in a chain) is valid; LocalWorkflows alone is not.
   */
  private extractRemoteWorkerDeps(): {
    stepQueue?: StepQueue;
    workerRegistry?: WorkerRegistry;
    workflowStarts?: WorkflowStartQueue;
  } {
    const out: {
      stepQueue?: StepQueue;
      workerRegistry?: WorkerRegistry;
      workflowStarts?: WorkflowStartQueue;
    } = {};

    // Walk the chain so a Local + Queued fallback exposes the queue.
    let layer: ZoryaWorkflows | undefined = this.workflows;
    while (layer) {
      if (layer instanceof DistributedWorkflows) {
        if (out.stepQueue === undefined) out.stepQueue = layer.stepQueue;
        if (out.workerRegistry === undefined && layer.workerRegistry !== undefined) {
          out.workerRegistry = layer.workerRegistry;
        }
      }
      if (layer instanceof QueuedWorkflows) {
        if (out.workflowStarts === undefined) out.workflowStarts = layer.workflowStarts;
      }
      layer = (layer as unknown as { fallback?: ZoryaWorkflows }).fallback;
    }

    if (!out.stepQueue && !out.workflowStarts) {
      throw new Error(
        "ZoryaServer: remoteWorkers requires the workflows chain to include " +
          "DistributedWorkflows (for stepQueue) or QueuedWorkflows (for workflowStarts)",
      );
    }
    return out;
  }

  private buildStaticHandler(dir: string) {
    return async (req: Request): Promise<Response> => {
      const url = new URL(req.url);
      const path = url.pathname === "/" ? "/index.html" : url.pathname;
      const file = Bun.file(`${dir}${path}`);
      if (await file.exists()) return new Response(file);
      const indexFile = Bun.file(`${dir}/index.html`);
      if (await indexFile.exists()) return new Response(indexFile);
      return new Response("Not Found", { status: 404 });
    };
  }
}
