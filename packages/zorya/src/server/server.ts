// ---------------------------------------------------------------------------
// ZoryaServer — HTTP server that wraps a ZoryaWorkflows service (plus
// optional scheduler / agents / registries).
//
// Usage:
// ```ts
// const workflows = new LocalWorkflows({ storage, runner, definitions });
// const server = new ZoryaServer({
//   workflows,
//   apiKeys: [process.env.ZORYA_API_KEY!],
//   remoteWorkers: { apiKeys: [process.env.ZORYA_WORKER_API_KEY!] },
// });
// server.listen({ port: 4000 });
// await server.ready();
// process.on("SIGTERM", () => void server.stop().then(() => process.exit(0)));
// ```
//
// The constructor resolves the stores the workflows chain carries, then
// mounts each surface from `./mount/`: probes, runs and workflows, agents,
// registries, ingress, schedules, the worker RPC plane and the static UI.
// Which credentials each path needs is decided in `mount/mount-auth.ts`.
// ---------------------------------------------------------------------------

import type { SecretsStorage } from "@promin/agent";
import type { WallClock, WorkflowVersionRegistry } from "@promin/workflow";
import { SystemWallClock } from "@promin/workflow";
import { InMemoryWorkerRegistry, type WorkerRegistry } from "@promin/workflow/distributed";
import { Router, jsonError } from "./router.ts";
import { RunEventBus } from "./run-event-bus.ts";
import { RunWatcherPool } from "./run-poll-watcher.ts";
import {
  DEFAULT_READINESS_TIMEOUT_MS,
  DEFAULT_SHUTDOWN_TIMEOUT_MS,
  type ListenOptions,
  type Logger,
  type ZoryaServerConfig,
} from "./server-config.ts";
import { ServiceLifecycle, type ServiceStatus } from "./service-lifecycle.ts";
import { StorageMetricsProvider } from "./routes/metrics.ts";
import type { RemoteDeploymentSweeper } from "./routes/remote-deployments.ts";
import { RegistryBackedWorkersProvider } from "./routes/workers.ts";
import { AgentStreamHub } from "./services/agent-stream-hub.ts";
import type { ZoryaAgents } from "./services/agents/index.ts";
import type { ZoryaDags } from "./services/dags/index.ts";
import type { ZoryaFragments } from "./services/fragments/index.ts";
import type { ZoryaKnowledgeBases } from "./services/knowledge/index.ts";
import type { ZoryaScheduler } from "./services/scheduler/index.ts";
import type { ZoryaSkills } from "./services/skills/index.ts";
import { WorkerWakeups } from "./services/worker-wakeups.ts";
import { WorkerWebSocketServer } from "./services/worker-ws-server.ts";
import { RunRetention } from "./services/workflows/run-retention.ts";
import {
  QueuedWorkflows,
  ZoryaWorkflows,
  assertSharedStores,
  type ZoryaCluster,
} from "./services/workflows/index.ts";
import { mountAgents } from "./mount/mount-agents.ts";
import { AuthPlanes, isProbePath } from "./mount/mount-auth.ts";
import { mountRemoteDeployments, mountWebhooks } from "./mount/mount-ingress.ts";
import {
  InFlightRequests,
  LiveStreams,
  managedServices,
  mountProbes,
} from "./mount/mount-lifecycle.ts";
import { serveListener, type ListeningServer } from "./mount/mount-listener.ts";
import {
  mountDags,
  mountFragments,
  mountKnowledgeBases,
  mountSecrets,
  mountSkills,
} from "./mount/mount-registries.ts";
import { mountRpc } from "./mount/mount-rpc.ts";
import { mountRuns } from "./mount/mount-runs.ts";
import { mountSchedules } from "./mount/mount-schedules.ts";
import { mountStatic } from "./mount/mount-static.ts";
import { ZoryaServerBuilder } from "./server-builder.ts";

export class ZoryaServer {
  readonly workflows: ZoryaWorkflows;
  readonly scheduler?: ZoryaScheduler;
  readonly agents?: ZoryaAgents;
  readonly skills?: ZoryaSkills;
  readonly fragments?: ZoryaFragments;
  readonly dags?: ZoryaDags;
  readonly knowledgeBases?: ZoryaKnowledgeBases;
  /**
   * The workflows layer's version registry (`LocalWorkflowsConfig.versionRegistry`),
   * which `/api/workflows/:name/versions/*` manage. One registry: a promote
   * here changes the version new runs dispatch to. Absent when the layer
   * has none; the version routes then list nothing and refuse changes.
   */
  readonly versionRegistry?: WorkflowVersionRegistry;
  /**
   * The one worker registry: remote workers register and heartbeat into it
   * over `/rpc/worker`, and the Workers page lists it.
   */
  readonly workerRegistry: WorkerRegistry;
  /** Set when the workflows chain runs clustered: several instances share the stores. */
  readonly cluster?: ZoryaCluster;
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
  /**
   * "Work available" messages to connected workers when the chain's
   * `QueuedWorkflows` queues a start they can run. Present with
   * `remoteWorkers` and a queued layer, unless `remoteWorkers.wakeups` is
   * false.
   */
  readonly workerWakeups?: WorkerWakeups;
  /** Optional secrets vault — populated when ZoryaServerConfig provides one. */
  readonly secrets?: SecretsStorage;
  /** Deletes ended runs past their retention age. Present with `config.retention`. */
  readonly retention?: RunRetention;

  private readonly planes: AuthPlanes;
  private readonly lifecycle: ServiceLifecycle;
  private readonly shutdownTimeoutMs: number;
  private readonly liveStreams = new LiveStreams();
  private readonly inFlight: InFlightRequests;
  private stopping = false;
  private stopPromise?: Promise<void>;
  private readonly runWatchers: RunWatcherPool;
  private readonly router: Router;
  private readonly logger: Logger;
  private remoteDeploymentSweeper?: RemoteDeploymentSweeper;
  private bunServer?: ListeningServer;
  private unsubscribeWakeups?: () => void;

  /** Start a fluent `ZoryaServer` configuration (see `ZoryaServerBuilder`). */
  static builder(): ZoryaServerBuilder {
    return new ZoryaServerBuilder();
  }

  constructor(config: ZoryaServerConfig) {
    this.workflows = config.workflows;
    if (config.scheduler) this.scheduler = config.scheduler;
    if (config.agents) this.agents = config.agents;
    if (config.skills) this.skills = config.skills;
    if (config.fragments) this.fragments = config.fragments;
    if (config.dags) this.dags = config.dags;
    if (config.knowledgeBases) this.knowledgeBases = config.knowledgeBases;
    if (config.secrets) this.secrets = config.secrets;
    const clock: WallClock = config.clock ?? SystemWallClock;
    this.inFlight = new InFlightRequests(clock);

    // The workflows chain (top → fallback) carries the stores: the outer
    // layer may not own advertisements or definitions while a fallback does.
    const layers = this.workflows.layers();
    const firstInChain = <T>(pick: (layer: ZoryaWorkflows) => T | undefined): T | undefined =>
      layers.map(pick).find((v) => v !== undefined);
    const versionRegistry = firstInChain((l) => l.versionRegistry);
    if (versionRegistry) this.versionRegistry = versionRegistry;
    const advertisements = firstInChain((l) => l.advertisements);
    const definitions = firstInChain((l) => l.definitions);
    const queued = layers.find((l): l is QueuedWorkflows => l instanceof QueuedWorkflows);

    this.logger = config.logger ?? console;
    this.planes = new AuthPlanes({
      apiKeys: config.apiKeys,
      workerApiKeys: config.remoteWorkers?.apiKeys,
    });
    this.planes.assertConfigured({
      remoteWorkers: config.remoteWorkers !== undefined,
      insecure: config.insecure === true,
      logger: this.logger,
    });
    const readinessTimeoutMs = config.readinessTimeoutMs ?? DEFAULT_READINESS_TIMEOUT_MS;
    this.shutdownTimeoutMs = config.shutdownTimeoutMs ?? DEFAULT_SHUTDOWN_TIMEOUT_MS;
    this.workerWs = new WorkerWebSocketServer({
      authorize: (req) => this.planes.worker.check(req),
      clock,
      onError: (message, err) => this.logger.error(message, err),
    });
    this.agentStreamHub = new AgentStreamHub(this.workerWs);
    const wakeups = config.remoteWorkers?.wakeups;
    if (queued && config.remoteWorkers && wakeups !== false) {
      const workerWakeups = new WorkerWakeups({
        sockets: this.workerWs,
        advertisements: queued.advertisements,
        clock,
        ...(wakeups?.coalesceMs !== undefined && { coalesceMs: wakeups.coalesceMs }),
        ...(wakeups?.followUpMs !== undefined && { followUpMs: wakeups.followUpMs }),
        onError: (err) => this.logger.error("[zorya] worker wake-up failed", err),
      });
      this.workerWakeups = workerWakeups;
      this.unsubscribeWakeups = queued.onStartQueued((notice) => workerWakeups.notify(notice));
    }
    const storage = this.workflows.storage;
    const cluster = firstInChain((l) => l.cluster);
    if (config.retention) {
      this.retention = new RunRetention({
        storage,
        config: config.retention,
        cluster,
        clock,
        onSwept: ({ deleted }) => {
          if (deleted > 0) this.logger.log(`[zorya] retention: deleted ${deleted} ended runs`);
        },
        onError: (err) => this.logger.warn("[zorya] retention sweep failed", err),
      });
    }
    this.lifecycle = new ServiceLifecycle({
      services: managedServices({
        workflows: this.workflows,
        scheduler: this.scheduler,
        agents: this.agents,
        skills: this.skills,
        fragments: this.fragments,
        knowledgeBases: this.knowledgeBases,
        retention: this.retention,
      }),
      clock,
      onError: (message, err) => this.logger.error(message, err),
      ...(config.startRetry?.initialDelayMs !== undefined && {
        initialRetryDelayMs: config.startRetry.initialDelayMs,
      }),
      ...(config.startRetry?.maxDelayMs !== undefined && {
        maxRetryDelayMs: config.startRetry.maxDelayMs,
      }),
    });

    this.workerRegistry =
      config.remoteWorkers?.workerRegistry ??
      queued?.workerRegistry ??
      new InMemoryWorkerRegistry({ clock });
    if (cluster) {
      this.cluster = cluster;
      // A clustered deployment needs every store this server holds shared
      // too: the version registry (a promote on one instance routes starts
      // on all), the worker registry (every instance shows and routes to
      // the same workers) and the scheduler's storage.
      assertSharedStores({
        owner: "ZoryaServer",
        stores: {
          versionRegistry: this.versionRegistry,
          workerRegistry: this.workerRegistry,
          schedulerStorage: config.scheduler?.storage,
        },
      });
    }

    this.runWatchers = new RunWatcherPool({
      storage,
      bus: new RunEventBus(),
      clock,
      onError: ({ workflowId, error }) =>
        this.logger.error(`[zorya] run watch ${workflowId}: storage poll failed`, error),
      ...(config.sseIntervalMs !== undefined && { intervalMs: config.sseIntervalMs }),
    });

    const router = (this.router = new Router({ logger: this.logger }));
    mountProbes({
      router,
      storage,
      lifecycle: this.lifecycle,
      clock,
      readinessTimeoutMs,
      isStopping: () => this.stopping,
    });
    mountRuns({
      router,
      workflows: this.workflows,
      storage,
      clock,
      definitions,
      advertisements,
      versionRegistry: this.versionRegistry,
      runWatchers: this.runWatchers,
      agentStreamHub: this.agentStreamHub,
      workerWs: this.workerWs,
      // One registry behind /rpc/worker and the Workers page, in every mode.
      workers: config.workers ?? new RegistryBackedWorkersProvider(this.workerRegistry, { clock }),
      metrics: config.metrics ?? new StorageMetricsProvider(storage),
      sampleInput: config.sampleInput,
      sseIntervalMs: config.sseIntervalMs,
      signalTokenMaxTtlMs: config.signalTokenMaxTtlMs,
      publicBaseUrl: config.publicBaseUrl,
    });
    if (this.agents) {
      mountAgents({
        router,
        agents: this.agents,
        skills: this.skills,
        fragments: this.fragments,
        secrets: this.secrets,
        clock,
      });
    }
    if (this.skills) mountSkills({ router, skills: this.skills, agents: this.agents });
    if (this.fragments) mountFragments({ router, fragments: this.fragments, agents: this.agents });
    if (this.secrets) mountSecrets({ router, secrets: this.secrets });
    if (config.webhooks && this.agents) {
      mountWebhooks({ router, webhooks: config.webhooks, agents: this.agents, storage, clock });
    }
    if (config.remoteDeployments && this.agents) {
      this.remoteDeploymentSweeper = mountRemoteDeployments({
        router,
        remoteDeployments: config.remoteDeployments,
        agents: this.agents,
        clock,
        logger: this.logger,
      });
    }
    if (this.dags) mountDags({ router, dags: this.dags });
    if (this.knowledgeBases) {
      mountKnowledgeBases({ router, knowledgeBases: this.knowledgeBases, agents: this.agents });
    }
    mountSchedules({ router, scheduler: this.scheduler, storage, clock });
    if (config.remoteWorkers) {
      mountRpc({
        router,
        storage,
        workerRegistry: this.workerRegistry,
        advertisements,
        workflowStarts: queued?.workflowStarts,
        storageCaller: (req) => this.planes.storageCaller(req),
        ...(this.workerWakeups && {
          onStartsClaimed: (starts: ReadonlyArray<{ readonly workflowId: string }>) =>
            this.workerWakeups?.claimed(starts),
        }),
      });
    }
    if (config.uiDir) mountStatic({ router, uiDir: config.uiDir });
  }

  /**
   * Answer one request: authenticate it for its plane, route it, and track
   * event-stream responses so `stop()` can close them. `listen()` serves
   * through this; embedders and tests call it directly.
   */
  async handle(req: Request): Promise<Response> {
    const path = new URL(req.url).pathname;
    if (this.stopping && !isProbePath(path)) return jsonError(503, "shutting_down");

    const denied = this.planes.authenticate({ req, path });
    if (denied) return denied;

    return this.liveStreams.track(await this.router.handle(req));
  }

  /**
   * Start the background services (workflows, scheduler, agents, skills,
   * fragments, knowledge bases) and the worker socket. A service whose start fails is retried
   * with backoff; `/api/ready` and `ready()` reflect progress. `listen()`
   * calls this. Idempotent.
   */
  start(): void {
    if (this.stopping) return;
    this.workerWs.start();
    this.agentStreamHub.start();
    this.remoteDeploymentSweeper?.start();
    this.lifecycle.start();
  }

  /** Resolves once every background service has started; rejects if the server stops first. */
  ready(): Promise<void> {
    return this.lifecycle.ready();
  }

  /** Start state of each background service. */
  serviceStatuses(): ServiceStatus[] {
    return this.lifecycle.statuses();
  }

  listen(opts: ListenOptions = {}): {
    port: number;
    hostname: string;
    stop: () => Promise<void>;
  } {
    if (!(globalThis as { Bun?: unknown }).Bun) {
      throw new Error("ZoryaServer.listen requires Bun runtime");
    }
    if (this.stopping) throw new Error("ZoryaServer.listen: the server was stopped");
    const srv = serveListener({
      options: opts,
      workerWs: this.workerWs,
      inFlight: this.inFlight,
      handle: (req) => this.handle(req),
      logger: this.logger,
    });
    this.bunServer = srv;
    this.start();
    return { port: srv.port, hostname: srv.hostname, stop: () => this.stop() };
  }

  /**
   * Graceful shutdown: refuse new requests (503, probes excepted), stop
   * accepting connections, close worker sockets and open event streams,
   * wait up to `shutdownTimeoutMs` for in-flight requests, stop the
   * background services (which release their locks and leases), then close
   * whatever connections remain. Idempotent; concurrent calls share one
   * shutdown.
   */
  stop(): Promise<void> {
    this.stopPromise ??= this.shutdown();
    return this.stopPromise;
  }

  private async shutdown(): Promise<void> {
    this.stopping = true;
    const srv = this.bunServer;
    this.bunServer = undefined;
    // Stop accepting; Bun lets open connections finish.
    void Promise.resolve(srv?.stop(false)).catch(() => {});

    this.liveStreams.closeAll();
    this.agentStreamHub.stop();
    this.unsubscribeWakeups?.();
    this.workerWakeups?.stop();
    this.workerWs.stop();
    this.remoteDeploymentSweeper?.stop();

    await this.inFlight.waitForIdle(this.shutdownTimeoutMs);
    await this.lifecycle.stop();

    try {
      await srv?.stop(true);
    } catch (err) {
      this.logger.error("[zorya] closing connections failed", err);
    }
  }
}
