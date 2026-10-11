// ---------------------------------------------------------------------------
// ZoryaServer — HTTP server that wraps a ZoryaWorkflows service (plus
// optional scheduler / agents).
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
// Auth model (every plane is authenticated; `insecure: true` is the only way
// to run a plane without keys):
//   - /api/*                    operator keys (`apiKeys`), except the public
//                               routes below
//   - GET /api/health, /api/ready  public (liveness / readiness probes)
//   - /api/signal-tokens/:id/describe, /complete
//                               public; the route checks the token bearer
//   - /ws/worker, /rpc/worker, /api/advertisements, /api/worker-protocol/*
//                               worker keys (`remoteWorkers.apiKeys`)
//   - /rpc/storage              worker keys, scoped to `RUNNER_STORAGE_METHODS`;
//                               operator keys get the full storage contract
//   - /webhooks/*               per-source HMAC signature
// Keys travel only in `Authorization: Bearer <key>` (the worker WebSocket
// upgrade included) and are compared in constant time.
// ---------------------------------------------------------------------------

import type { RemoteDeploymentRegistry, SecretsStorage } from "@promin/agent";
import type { WorkflowVersionRegistry, WallClock, WorkflowStorage } from "@promin/workflow";
import type { StepQueue, WorkerRegistry } from "@promin/workflow/distributed";
import { SystemWallClock, InMemoryWorkflowVersionRegistry } from "@promin/workflow";
import { InMemoryStepQueue, InMemoryWorkerRegistry } from "@promin/workflow/distributed";
import {
  RUNNER_STORAGE_METHODS,
  createWorkerApiHandler,
  createWorkflowStorageHandler,
} from "@promin/workflow-remote";
import { Auth, bearerToken, type AuthConfig } from "./auth.ts";
import { ServiceLifecycle, type ManagedService, type ServiceStatus } from "./service-lifecycle.ts";
import { Router, errorResponse, jsonError } from "./router.ts";
import {
  DEFAULT_IDLE_TIMEOUT_SECONDS,
  DEFAULT_MAX_REQUEST_BODY_BYTES,
  DEFAULT_MAX_WEBHOOK_BODY_BYTES,
  bodyLimitFor,
  limitRequestBody,
} from "./request-limits.ts";
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
  heartbeatWorkflowStart,
  listWorkflowStarts,
} from "./routes/workflow-starts.ts";
import { RunEventBus } from "./run-event-bus.ts";
import { RunWatcherPool } from "./run-poll-watcher.ts";
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
import { listSignals } from "./routes/signals.ts";
import { RunsService } from "./services/runs-service.ts";
import {
  completeSignalToken,
  describeSignalToken,
  listSignalTokensForRun,
  mintSignalToken,
} from "./routes/signal-tokens.ts";
import {
  getActiveWorkflowVersion,
  listWorkflowVersions,
  promoteWorkflowVersion,
  rollbackWorkflow,
} from "./routes/workflow-versions.ts";
import { getStreamChunks, sendStreamChunk, streamChunks } from "./routes/streams.ts";
import { getSparklines, getWorkflowGrid, getWorkflowHistory } from "./routes/grid.ts";
import { getWorkflowDef, listWorkflowDefs } from "./routes/workflow-defs.ts";
import {
  cloneAgent,
  compactThread,
  archiveAgentThread,
  createAgent,
  deleteAgent,
  distillThread,
  extractRole,
  getAgent,
  invokeAgent,
  listAgentSources,
  listAgentThreads,
  listAgentVersions,
  listAgents,
  getThreadTrace,
  listThreadMessages,
  renameAgentThread,
  sendThreadMessage,
  streamAgent,
  streamThreadApproval,
  streamThreadMessage,
  updateAgent,
  type AgentGatewayDeps,
} from "./routes/agents.ts";
import {
  getToolCatalogHealth,
  listCatalogFragments,
  listCatalogModels,
  listCatalogSkills,
  listCatalogTools,
  listToolHistory,
} from "./routes/agent-catalog.ts";
import {
  createSkill,
  deleteSkill,
  getSkill,
  listSkillSources,
  listSkillVersions,
  listSkills,
  updateSkill,
} from "./routes/skills.ts";
import {
  createRole,
  deleteRole,
  getRole,
  listRoleVersions,
  listRoles,
  updateRole,
} from "./routes/roles.ts";
import {
  createFragment,
  deleteFragment,
  getFragment,
  listFragmentSources,
  listFragments,
  updateFragment,
} from "./routes/fragments.ts";
import { createSecret, deleteSecret, listSecrets } from "./routes/secrets.ts";
import { createDraft, deleteDraft } from "./routes/agent-drafts.ts";
import {
  ingestWebhook,
  WorkflowStorageWebhookDeliveryStore,
  type WebhookDeliveryStore,
  type WebhookSourceConfig,
  type WebhookGatewayDeps,
} from "./routes/webhooks.ts";
import {
  heartbeatDeployment,
  listDeployments,
  registerDeployment,
  RemoteDeploymentSweeper,
  unregisterDeployment,
  type RemoteDeploymentsGatewayDeps,
  type RemoteEndpointPolicy,
} from "./routes/remote-deployments.ts";
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
import { ZoryaSkills } from "./services/skills/index.ts";
import { ZoryaFragments } from "./services/fragments/index.ts";
import { ZoryaDags } from "./services/dags/index.ts";
import {
  createDag,
  deleteDag,
  getDag,
  listDagVersions,
  listDags,
  runDag,
  type DagGatewayDeps,
} from "./routes/dags.ts";

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
  /**
   * Worker API keys for `/ws/worker`, `/rpc/worker`, `/rpc/storage` (scoped
   * to what a runner needs), `/api/advertisements` and
   * `/api/worker-protocol/*`. Required unless the server is `insecure`.
   */
  apiKeys?: ReadonlyArray<string>;
}

/** Thrown by the ZoryaServer constructor when a plane has no keys and `insecure` is off. */
export class ZoryaInsecureConfigError extends Error {
  readonly _tag = "ZoryaInsecureConfigError";
  constructor(message: string) {
    super(message);
    this.name = "ZoryaInsecureConfigError";
  }
}

/** Default bound on the readiness storage probe (ms). */
export const DEFAULT_READINESS_TIMEOUT_MS = 2_000;
/** Default bound on waiting for in-flight requests during `stop()` (ms). */
export const DEFAULT_SHUTDOWN_TIMEOUT_MS = 10_000;

/** Workflow id the readiness probe looks up; it never exists. */
const READINESS_PROBE_WORKFLOW_ID = "__zorya_readiness_probe__";

export interface ZoryaServerConfig extends AuthConfig {
  /**
   * Allow running without keys. Without it the constructor throws
   * `ZoryaInsecureConfigError` when `apiKeys` is empty, or when
   * `remoteWorkers` is set without `remoteWorkers.apiKeys`. With it, each
   * plane that has no keys is open to anyone who can reach the port, and
   * the server logs a warning. Local development only.
   */
  insecure?: boolean;
  /** Workflow execution + state. Required. */
  workflows: ZoryaWorkflows;
  /** Optional scheduler service (CRUD + tick loop + emit-now). */
  scheduler?: ZoryaScheduler;
  /** Optional agent gateway service. */
  agents?: ZoryaAgents;
  /**
   * Optional skill registry service. Mounts /api/skills/* CRUD and
   * /api/agents/_catalog/skills (the agent editor's skill picker source).
   */
  skills?: ZoryaSkills;
  /**
   * Optional fragment registry service. Mounts /api/fragments/* CRUD and
   * backs /api/agents/_catalog/fragments (the layered-prompt editor source).
   * When set, takes precedence over `config.agents.fragments`.
   */
  fragments?: ZoryaFragments;
  /** Optional DAG gateway service. Mounts /api/dags/* routes. */
  dags?: ZoryaDags;
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
  /**
   * Longest validity a minted signal token may ask for (`expiresInMs`).
   * Default: `DEFAULT_MAX_SIGNAL_TOKEN_TTL_MS` (90 days).
   */
  signalTokenMaxTtlMs?: number;
  /**
   * Workflow version registry. Drives `/api/workflows/:name/versions/*`
   * (promote/rollback, findActive, list-records). Defaults to a fresh
   * in-memory `WorkflowVersionRegistry`; production deployments should
   * pass `PostgresWorkflowVersionRegistry` so lifecycle survives restart.
   */
  versionRegistry?: WorkflowVersionRegistry;
  /**
   * Public-facing base URL for token completion callbacks. When set, the
   * `POST /api/runs/:id/signals/:name/token` mint route returns
   * `url: "<publicBaseUrl>/api/signal-tokens/:tokenId/complete"` so external
   * completers don't have to compose it themselves. Absent → `url: null`.
   */
  publicBaseUrl?: string;
  /**
   * Optional scoped SecretsStorage (BYOK / MCP credentialRef / per-tenant
   * LLM keys). When set, exposes `/api/secrets/*` HTTP CRUD and is
   * available to the agent gateway for setup-time credential resolution
   * (Option A from promin-0p2i). When unset, the secrets surface is not
   * mounted — fine for single-tenant deployments using env vars.
   */
  secrets?: SecretsStorage;
  /**
   * Optional webhook ingress — POST /webhooks/:source/:agentId routes
   * external events into a registered agent after `zorya-v1` signature
   * verification (HMAC over timestamp, delivery id, scope and body) and
   * durable replay dedup. `deliveries` defaults to a store on the
   * workflow storage, shared by every instance on the same backend.
   * Without `agents` configured the routes are not mounted.
   */
  webhooks?: {
    sources: Readonly<Record<string, WebhookSourceConfig>>;
    deliveries?: WebhookDeliveryStore;
  };
  /**
   * Optional remote-deployment self-registration. When set (with
   * `agents`), exposes /api/remote-deployments/* — external Zorya
   * servers register their agents here and keep them alive with
   * heartbeats; expired registrations are swept every `sweepIntervalMs`
   * (default 15s). `endpointPolicy` decides which endpoints this server
   * will call (default: public http(s) hosts only).
   */
  remoteDeployments?: {
    registry: RemoteDeploymentRegistry;
    endpointPolicy?: RemoteEndpointPolicy;
    sweepIntervalMs?: number;
  };
  /**
   * Time source for the server's own time math: signal-token expiry, draft
   * TTL sweep, worker online cutoff, worker-socket heartbeats, SSE / stream
   * polls, query timeouts, schedule next-run projections, and the defaults
   * it builds (version registry, fallback worker registry). Services passed
   * in (scheduler, workflows, agents) keep their own clocks. Default:
   * `SystemWallClock`. Tests pass a `FakeWallClock`.
   */
  clock?: WallClock;
  /** Bound on the `/api/ready` storage probe (ms). Default: 2000. */
  readinessTimeoutMs?: number;
  /**
   * How long `stop()` waits for in-flight requests before closing the
   * remaining connections (ms). Default: 10_000.
   */
  shutdownTimeoutMs?: number;
  /** Backoff for retrying a service whose `start()` failed. Default: 500ms doubling to 30s. */
  startRetry?: { initialDelayMs?: number; maxDelayMs?: number };
}

export interface ListenOptions {
  port?: number;
  hostname?: string;
  /**
   * Largest request body accepted outside `/webhooks/*`, in bytes; larger
   * bodies get a JSON 413. Default: 1 MiB.
   */
  maxRequestBodySize?: number;
  /** Largest `/webhooks/*` request body, in bytes. Default: 25 MiB. */
  maxWebhookBodySize?: number;
  /** Seconds a connection may sit idle before Bun closes it. Default: 60 (max 255). */
  idleTimeout?: number;
  /**
   * Bun development mode (verbose error pages with stacks and source).
   * Default: false. Errors are always answered with JSON by the router.
   */
  development?: boolean;
}

export class ZoryaServer {
  readonly workflows: ZoryaWorkflows;
  readonly scheduler?: ZoryaScheduler;
  readonly agents?: ZoryaAgents;
  readonly skills?: ZoryaSkills;
  readonly fragments?: ZoryaFragments;
  readonly dags?: ZoryaDags;
  readonly versionRegistry: WorkflowVersionRegistry;
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
  /** Optional secrets vault — populated when ZoryaServerConfig provides one. */
  readonly secrets?: SecretsStorage;

  private readonly auth: Auth;
  private readonly workerAuth: Auth;
  private readonly lifecycle: ServiceLifecycle;
  private readonly readinessTimeoutMs: number;
  private readonly shutdownTimeoutMs: number;
  /** Event-stream responses still open; closed by `stop()`. */
  private readonly liveStreams = new Set<{ close(): void }>();
  private inFlight = 0;
  private readonly idleWaiters = new Set<() => void>();
  private stopping = false;
  private stopPromise?: Promise<void>;
  private readonly bus: RunEventBus;
  private readonly runWatchers: RunWatcherPool;
  private readonly router: Router;
  private readonly logger: Logger;
  private readonly clock: WallClock;
  private remoteDeploymentSweeper?: RemoteDeploymentSweeper;
  private bunServer?: { stop(closeActiveConnections?: boolean): Promise<void> | void };

  constructor(config: ZoryaServerConfig) {
    this.workflows = config.workflows;
    if (config.scheduler) this.scheduler = config.scheduler;
    if (config.agents) this.agents = config.agents;
    if (config.skills) this.skills = config.skills;
    if (config.fragments) this.fragments = config.fragments;
    if (config.dags) this.dags = config.dags;
    if (config.secrets) this.secrets = config.secrets;
    this.clock = config.clock ?? SystemWallClock;
    const clock = this.clock;
    this.versionRegistry = config.versionRegistry ?? new InMemoryWorkflowVersionRegistry({ clock });

    this.logger = config.logger ?? console;
    this.auth = new Auth(config);
    this.workerAuth = new Auth({ apiKeys: config.remoteWorkers?.apiKeys });
    this.checkAuthConfigured(config);
    this.readinessTimeoutMs = config.readinessTimeoutMs ?? DEFAULT_READINESS_TIMEOUT_MS;
    this.shutdownTimeoutMs = config.shutdownTimeoutMs ?? DEFAULT_SHUTDOWN_TIMEOUT_MS;
    this.bus = new RunEventBus();
    this.workerWs = new WorkerWebSocketServer({
      authorize: (req) => this.workerAuth.check(req),
      clock,
      onError: (message, err) => this.logger.error(message, err),
    });
    this.agentStreamHub = new AgentStreamHub(this.workerWs);
    this.lifecycle = new ServiceLifecycle({
      services: this.managedServices(),
      clock,
      onError: (message, err) => this.logger.error(message, err),
      ...(config.startRetry?.initialDelayMs !== undefined && {
        initialRetryDelayMs: config.startRetry.initialDelayMs,
      }),
      ...(config.startRetry?.maxDelayMs !== undefined && {
        maxRetryDelayMs: config.startRetry.maxDelayMs,
      }),
    });

    const storage: WorkflowStorage = this.workflows.storage;
    // Walk the workflows chain to find an advertisements registry. The
    // top layer (LocalWorkflows) doesn't expose one, but a Queued or
    // Distributed fallback does — and that's the one workers actually
    // upsert into. Without this, /api/advertisements never mounted and
    // worker registrations silently hit the SPA fallback.
    const advertisements = walkChainFor(this.workflows, (l) => l.advertisements);
    const definitions = walkChainFor(this.workflows, (l) => l.definitions);

    const metrics = config.metrics ?? new StorageMetricsProvider(storage);
    // Workers provider: explicit > derived from DistributedWorkflows.workerRegistry > empty
    const workers =
      config.workers ??
      (this.workflows instanceof DistributedWorkflows && this.workflows.workerRegistry
        ? new RegistryBackedWorkersProvider(this.workflows.workerRegistry, { clock })
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

    const runActions = new RunsService({ storage });

    this.runWatchers = new RunWatcherPool({
      storage,
      bus: this.bus,
      clock,
      onError: ({ workflowId, error }) =>
        this.logger.error(`[zorya] run watch ${workflowId}: storage poll failed`, error),
      ...(config.sseIntervalMs !== undefined && { intervalMs: config.sseIntervalMs }),
    });

    this.router = new Router({ logger: this.logger })
      // Liveness: the process answers. Public and touches nothing.
      .get("/api/health", () => Response.json({ ok: true }))
      // Readiness: storage answers and every background service started.
      .get("/api/ready", () => this.readiness())
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
      .post("/api/runs/:id/rerun", rerunRun({ storage, rerun: (id) => this.workflows.rerun(id) }))
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
      // Signal tokens — public-bearer auth for deliverSignal.
      // Mint and list are auth-gated through the regular /api auth layer;
      // /describe and /complete are reachable without Zorya credentials and
      // validate the token bearer themselves.
      .post(
        "/api/runs/:id/signals/:name/token",
        mintSignalToken({
          storage,
          clock,
          ...(config.signalTokenMaxTtlMs !== undefined && {
            maxTtlMs: config.signalTokenMaxTtlMs,
          }),
          ...(config.publicBaseUrl !== undefined && { publicBaseUrl: config.publicBaseUrl }),
        }),
      )
      .get("/api/runs/:id/signal-tokens", listSignalTokensForRun({ storage }))
      .get("/api/signal-tokens/:tokenId/describe", describeSignalToken({ storage, clock }))
      .post("/api/signal-tokens/:tokenId/complete", completeSignalToken({ storage, clock }))
      // Workflow versions — thin layer over WorkflowVersionRegistry's
      // lifecycle methods. Versions are a property of a workflow, so the
      // routes nest under /api/workflows/:name/...
      .get(
        "/api/workflows/:name/versions",
        listWorkflowVersions({ registry: this.versionRegistry }),
      )
      .get(
        "/api/workflows/:name/versions/active",
        getActiveWorkflowVersion({ registry: this.versionRegistry }),
      )
      .post(
        "/api/workflows/:name/versions/:version/promote",
        promoteWorkflowVersion({ registry: this.versionRegistry }),
      )
      .post("/api/workflows/:name/rollback", rollbackWorkflow({ registry: this.versionRegistry }))
      // Streams — generic typed channels per workflow. Output (workflow →
      // subscribers) reads via the SSE handler; input (subscribers →
      // workflow) appends via the POST handler.
      .get(
        "/api/runs/:id/streams/:streamId",
        streamChunks({
          storage,
          clock,
          ...(config.sseIntervalMs !== undefined && { pollIntervalMs: config.sseIntervalMs }),
        }),
      )
      .get("/api/runs/:id/streams/:streamId/chunks", getStreamChunks({ storage }))
      .post("/api/runs/:id/streams/:streamId", sendStreamChunk({ storage }))
      .get("/api/runs/:id/events", streamRunEvents({ storage, watchers: this.runWatchers, clock }))
      .get("/api/runs/:id/agent-stream", streamAgentEvents(this.agentStreamHub))
      .post("/api/runs/:id/query", queryRun({ workerWs: this.workerWs, clock }))
      .get("/api/workers", listWorkers(workers))
      .get("/api/metrics", getMetrics(metrics));

    if (this.agents) {
      const agentDeps: AgentGatewayDeps = {
        registry: this.agents.registry,
        resolve: this.agents.resolve,
        ...(this.agents.instances !== undefined && { instanceRegistry: this.agents.instances }),
        ...(this.agents.roles !== undefined && { roles: this.agents.roles }),
        ...(this.agents.turnGate !== undefined && { turnGate: this.agents.turnGate }),
        ...(this.agents.workerId !== undefined && { workerId: this.agents.workerId }),
        ...(this.secrets !== undefined && { secrets: this.secrets }),
        ...(this.agents.authorizeScope !== undefined && {
          authorizeScope: this.agents.authorizeScope,
        }),
        fileManagedIds: () => this.agents!.fileManagedIds(),
        clock,
      };
      this.router
        .get("/api/agents", listAgents(agentDeps))
        // Read-only metadata route — designer UI's model dropdown source.
        // Mounted before /api/agents/:id so the literal `_catalog` segment
        // can't be shadowed by an agent that happens to be named `_catalog`
        // (`_` prefix is reserved for catalog routes).
        .get(
          "/api/agents/_catalog/models",
          this.agents.models
            ? listCatalogModels({ models: this.agents.models })
            : async () => new Response(JSON.stringify({ models: [] }), { status: 200 }),
        )
        .get(
          "/api/agents/_catalog/tools",
          this.agents.toolCatalog
            ? listCatalogTools({ tools: this.agents.toolCatalog })
            : async () => new Response(JSON.stringify({ tools: [] }), { status: 200 }),
        )
        // Reconciliation health: which recipe tool refs don't resolve?
        // Pure query at view time — no persistence, no audit trail.
        .get(
          "/api/agents/_catalog/tools/health",
          this.agents.toolCatalog
            ? getToolCatalogHealth({
                tools: this.agents.toolCatalog,
                agents: this.agents.registry,
              })
            : async () =>
                new Response(JSON.stringify({ recipes: [], orphans: [] }), { status: 200 }),
        )
        // Tool catalog history: durable audit trail of exposed tools over
        // time. Mounted only when a tool-history store is wired.
        .get(
          "/api/agents/_catalog/tools/history",
          this.agents.toolHistory
            ? listToolHistory({ history: this.agents.toolHistory })
            : async () => new Response(JSON.stringify({ history: [] }), { status: 200 }),
        )
        // Skill catalog — the agent editor's skill picker source. Mounted
        // here (under the agents block) so the `_catalog` prefix groups with
        // the model/tool catalogs; backed by the skills service registry.
        .get(
          "/api/agents/_catalog/skills",
          this.skills
            ? listCatalogSkills({ skills: this.skills.registry })
            : async () => new Response(JSON.stringify({ skills: [] }), { status: 200 }),
        )
        // Fragment catalog — the agent editor's layered-prompt editor source.
        // Prefers the dedicated ZoryaFragments service (which the manager UI
        // also writes through), else the registry passed on ZoryaAgents.
        .get(
          "/api/agents/_catalog/fragments",
          this.fragments
            ? listCatalogFragments({ fragments: this.fragments.registry })
            : this.agents.fragments
              ? listCatalogFragments({ fragments: this.agents.fragments })
              : async () => new Response(JSON.stringify({ fragments: [] }), { status: 200 }),
        )
        // File-managed agent sources — which recipes are backed by a file on
        // disk (so the editor can disable in-place Save and the list can
        // badge them). Mounted before /api/agents/:id so `_sources` isn't
        // shadowed by an agent named `_sources` (`_`-prefix is reserved).
        .get(
          "/api/agents/_sources",
          listAgentSources({ fileManagedIds: () => this.agents!.fileManagedIds() }),
        )
        // Draft recipes — test a recipe edit before committing it.
        // Mounted before /api/agents/:id so the literal `_draft` segment
        // isn't shadowed by an agent that happens to be named `_draft`.
        .post("/api/agents/_draft", createDraft({ registry: this.agents.registry, clock }))
        .delete("/api/agents/_draft/:id", deleteDraft({ registry: this.agents.registry }))
        // Recipe CRUD — gsze Phase 1. Author/edit/clone agents over HTTP.
        .post("/api/agents", createAgent(agentDeps))
        .get("/api/agents/:id", getAgent(agentDeps))
        .patch("/api/agents/:id", updateAgent(agentDeps))
        .delete("/api/agents/:id", deleteAgent(agentDeps))
        .get("/api/agents/:id/versions", listAgentVersions(agentDeps))
        .post("/api/agents/:id/clone", cloneAgent(agentDeps))
        .post("/api/agents/:id/extract-role", extractRole(agentDeps))
        .post("/api/agents/:id/invoke", invokeAgent(agentDeps))
        .post("/api/agents/:id/stream", streamAgent(agentDeps))
        .get("/api/agents/:id/threads", listAgentThreads(agentDeps))
        .patch("/api/agents/:id/threads/:threadId", renameAgentThread(agentDeps))
        .post("/api/agents/:id/threads/:threadId", sendThreadMessage(agentDeps))
        .post("/api/agents/:id/threads/:threadId/stream", streamThreadMessage(agentDeps))
        .post("/api/agents/:id/threads/:threadId/approve", streamThreadApproval(agentDeps))
        .get("/api/agents/:id/threads/:threadId/messages", listThreadMessages(agentDeps))
        .get("/api/agents/:id/threads/:threadId/trace", getThreadTrace(agentDeps))
        .post("/api/agents/:id/threads/:threadId/distill", distillThread(agentDeps))
        .post("/api/agents/:id/threads/:threadId/compact", compactThread(agentDeps))
        .post("/api/agents/:id/threads/:threadId/archive", archiveAgentThread(agentDeps));

      if (this.agents.memory) {
        // Persona resolution piggybacks on the recipe store + fragment
        // registry the agents gateway already carries, so the Prompt tab
        // can show the role prompt alongside the memory cascade. The
        // standalone `this.fragments` service wins over the agents-config
        // registry, matching the /api/agents/_catalog/fragments precedence.
        const mem = {
          memory: this.agents.memory,
          registry: this.agents.registry,
          ...((this.fragments?.registry ?? this.agents.fragments) && {
            fragments: this.fragments?.registry ?? this.agents.fragments,
          }),
          ...(this.agents.roles !== undefined && { roles: this.agents.roles }),
          ...(this.agents.authorizeScope !== undefined && {
            authorizeScope: this.agents.authorizeScope,
          }),
        };
        this.router
          .get("/api/memory/inspect", inspectMemory(mem))
          .patch("/api/memory/namespace/:namespaceId", patchNamespace(mem))
          .post("/api/memory/namespace/:namespaceId/facts", addNamespaceFact(mem))
          .delete("/api/memory/namespace/:namespaceId/facts/:factId", deleteNamespaceFact(mem));
      }

      if (this.agents.instances && this.agents.memory) {
        const inDeps = {
          registry: this.agents.instances,
          memory: this.agents.memory,
          agents: this.agents.registry,
          ...(this.agents.authorizeScope !== undefined && {
            authorizeScope: this.agents.authorizeScope,
          }),
        };
        this.router
          .get("/api/agents/:id/instances", listAgentInstances(inDeps))
          .post("/api/agents/:id/instances", resolveAgentInstance(inDeps))
          .get("/api/agents/:id/instances/:instanceId", getAgentInstance(inDeps))
          .patch("/api/agents/:id/instances/:instanceId", updateAgentInstance(inDeps))
          .delete("/api/agents/:id/instances/:instanceId", deleteAgentInstance(inDeps))
          .get("/api/instances", listInstancesAcrossAgents(inDeps));
      }

      if (this.agents.roles) {
        const roleDeps = { registry: this.agents.roles, agents: this.agents.registry };
        this.router
          .get("/api/roles", listRoles(roleDeps))
          .post("/api/roles", createRole(roleDeps))
          .get("/api/roles/:id", getRole(roleDeps))
          .patch("/api/roles/:id", updateRole(roleDeps))
          .delete("/api/roles/:id", deleteRole(roleDeps))
          .get("/api/roles/:id/versions", listRoleVersions(roleDeps));
      }
    }

    if (this.skills) {
      const skills = this.skills;
      const skillDeps = {
        registry: skills.registry,
        fileManagedIds: () => skills.fileManagedIds(),
        ...(this.agents !== undefined && { agents: this.agents.registry }),
        ...(this.agents?.roles !== undefined && { roles: this.agents.roles }),
      };
      this.router
        .get("/api/skills", listSkills(skillDeps))
        .post("/api/skills", createSkill(skillDeps))
        // `_sources` before `:id` so the literal segment isn't shadowed
        // (skill ids starting with `_` are rejected at create time).
        .get(
          "/api/skills/_sources",
          listSkillSources({ fileManagedIds: () => skills.fileManagedIds() }),
        )
        .get("/api/skills/:id", getSkill(skillDeps))
        .patch("/api/skills/:id", updateSkill(skillDeps))
        .delete("/api/skills/:id", deleteSkill(skillDeps))
        .get("/api/skills/:id/versions", listSkillVersions(skillDeps));
    }

    if (this.fragments) {
      const fragments = this.fragments;
      const fragmentDeps = {
        registry: fragments.registry,
        setFragment: (key: string, content: string) => fragments.setFragment(key, content),
        deleteFragment: (key: string) => fragments.deleteFragment(key),
      };
      this.router
        .get("/api/fragments", listFragments(fragmentDeps))
        .post("/api/fragments", createFragment(fragmentDeps))
        // `_sources` before `:key` so the literal segment isn't shadowed
        // (operator-supplied fragment keys starting with `_` are rejected).
        .get(
          "/api/fragments/_sources",
          listFragmentSources({ fileManagedIds: () => fragments.fileManagedIds() }),
        )
        .get("/api/fragments/:key", getFragment(fragmentDeps))
        .patch("/api/fragments/:key", updateFragment(fragmentDeps))
        .delete("/api/fragments/:key", deleteFragment(fragmentDeps));
    }

    if (this.secrets) {
      const sec = { secrets: this.secrets };
      this.router
        .post("/api/secrets", createSecret(sec))
        .get("/api/secrets", listSecrets(sec))
        .delete("/api/secrets/:key", deleteSecret(sec));
    }

    if (config.webhooks && this.agents) {
      const webhookDeps: WebhookGatewayDeps = {
        registry: this.agents.registry,
        // Cast through `unknown` — the resolve signature is structurally
        // compatible (returns Agent which has invoke + withScope) but the
        // webhook deps narrow to only the methods it uses.
        resolve: this.agents.resolve as unknown as WebhookGatewayDeps["resolve"],
        sources: config.webhooks.sources,
        deliveries:
          config.webhooks.deliveries ?? new WorkflowStorageWebhookDeliveryStore({ storage }),
        clock,
      };
      this.router.post("/webhooks/:source/:agentId", ingestWebhook(webhookDeps));
    }

    if (config.remoteDeployments && this.agents) {
      const remoteDeps: RemoteDeploymentsGatewayDeps = {
        registry: config.remoteDeployments.registry,
        agents: this.agents.registry,
        ...(config.remoteDeployments.endpointPolicy !== undefined && {
          endpointPolicy: config.remoteDeployments.endpointPolicy,
        }),
      };
      this.remoteDeploymentSweeper = new RemoteDeploymentSweeper({
        ...remoteDeps,
        clock,
        ...(config.remoteDeployments.sweepIntervalMs !== undefined && {
          intervalMs: config.remoteDeployments.sweepIntervalMs,
        }),
        onError: (err) => this.logger.error("[zorya] remote-deployment sweep failed", err),
      });
      this.router
        .post("/api/remote-deployments/register", registerDeployment(remoteDeps))
        .post("/api/remote-deployments/:deploymentId/heartbeat", heartbeatDeployment(remoteDeps))
        .delete("/api/remote-deployments/:deploymentId", unregisterDeployment(remoteDeps))
        .get("/api/remote-deployments", listDeployments(remoteDeps));
    }

    if (this.dags) {
      const dagDeps: DagGatewayDeps = {
        registry: this.dags.registry,
        resolver: this.dags.resolver,
        runner: this.dags.runner,
      };
      this.router
        .get("/api/dags", listDags(dagDeps))
        .post("/api/dags", createDag(dagDeps))
        .get("/api/dags/:id", getDag(dagDeps))
        .delete("/api/dags/:id", deleteDag(dagDeps))
        .get("/api/dags/:id/versions", listDagVersions(dagDeps))
        .post("/api/dags/:id/run", runDag(dagDeps));
    }

    if (this.scheduler) {
      const sch = this.scheduler.storage;
      const sched = this.scheduler;
      this.router
        .get("/api/schedules", listSchedules({ storage: sch, clock }))
        .post("/api/schedules", createSchedule({ storage: sch, clock }))
        .get("/api/schedules/:id", getSchedule({ storage: sch, clock }))
        .patch("/api/schedules/:id", patchSchedule({ storage: sch, clock }))
        .delete("/api/schedules/:id", deleteSchedule(sch))
        .get("/api/schedules/:id/history", getScheduleHistory(sch, storage))
        .get("/api/schedules/:id/upcoming", getScheduleUpcoming({ storage: sch, clock }))
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
      // Worker keys reach only what a runner needs; operator keys get the
      // whole contract (signal-token records, which carry bearers, included).
      const storageHandler = createWorkflowStorageHandler(storage, {
        allow: ({ method, req }) =>
          this.storageCaller(req) === "operator" || RUNNER_STORAGE_METHODS.has(method),
      });
      this.router.post("/rpc/storage", (req) => storageHandler(req));

      // /rpc/worker covers BOTH the step-queue claim path (DistributedWorkflows)
      // AND the worker-registry / heartbeat path used by every connected
      // worker regardless of mode. Always mount it; supply an in-memory
      // step queue when the workflows chain doesn't expose one (workflow-mode
      // workers don't poll it but their heartbeats / list calls still need
      // a working endpoint).
      const stepQueueForRpc = remoteDeps.stepQueue ?? new InMemoryStepQueue();
      const workerRegistryForRpc =
        remoteDeps.workerRegistry ?? new InMemoryWorkerRegistry({ clock: this.clock });
      const workerHandler = createWorkerApiHandler({
        stepQueue: stepQueueForRpc,
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
          .post("/api/worker-protocol/heartbeat-start/:id", heartbeatWorkflowStart(wfs))
          .post("/api/worker-protocol/complete-start/:id", completeWorkflowStart(wfs))
          .get("/api/worker-protocol/starts", listWorkflowStarts(wfs));
      }
    }

    if (config.uiDir) {
      this.router.setStatic(this.buildStaticHandler(config.uiDir));
    }
  }

  /**
   * Answer one request: authenticate it for its plane, route it, and track
   * event-stream responses so `stop()` can close them. `listen()` serves
   * through this; embedders and tests call it directly.
   */
  async handle(req: Request): Promise<Response> {
    const url = new URL(req.url);
    const path = url.pathname;

    if (this.stopping && !isProbePath(path)) return jsonError(503, "shutting_down");

    const denied = this.authenticate({ req, path });
    if (denied) return denied;

    const res = await this.router.handle(req);
    return isEventStream(res) ? this.trackStream(res) : res;
  }

  /**
   * Start the background services (workflows, scheduler, agents, skills,
   * fragments) and the worker socket. A service whose start fails is retried
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
    const port = opts.port ?? 4000;
    const hostname = opts.hostname ?? "0.0.0.0";
    const BunGlobal = (globalThis as { Bun?: { serve: typeof Bun.serve } }).Bun;
    if (!BunGlobal) {
      throw new Error("ZoryaServer.listen requires Bun runtime");
    }
    if (this.stopping) throw new Error("ZoryaServer.listen: the server was stopped");
    const wsHandlers = this.workerWs.websocketHandlers();
    const maxRequestBodySize = opts.maxRequestBodySize ?? DEFAULT_MAX_REQUEST_BODY_BYTES;
    const maxWebhookBodySize = opts.maxWebhookBodySize ?? DEFAULT_MAX_WEBHOOK_BODY_BYTES;
    const logger = this.logger;
    const srv = BunGlobal.serve({
      port,
      hostname,
      development: opts.development ?? false,
      idleTimeout: opts.idleTimeout ?? DEFAULT_IDLE_TIMEOUT_SECONDS,
      // Transport ceiling above the per-path limits so those can answer
      // with a JSON 413 (Bun's own 413 has no body).
      maxRequestBodySize: 2 * Math.max(maxRequestBodySize, maxWebhookBodySize),
      fetch: async (req, server) => {
        this.inFlight += 1;
        try {
          const upgradeResult = this.workerWs.upgradeIfWorkerWs(req, server);
          if (upgradeResult !== undefined) return upgradeResult;
          const url = new URL(req.url);
          if (url.pathname === "/ws/worker") return undefined;
          const limited = await limitRequestBody({
            req,
            maxBytes: bodyLimitFor({
              path: url.pathname,
              maxRequestBodySize,
              maxWebhookBodySize,
            }),
          });
          if (limited instanceof Response) return limited;
          return await this.handle(limited);
        } catch (err) {
          return errorResponse({ err, req, logger });
        } finally {
          this.inFlight -= 1;
          if (this.inFlight === 0) {
            for (const wake of this.idleWaiters) wake();
            this.idleWaiters.clear();
          }
        }
      },
      error: (err) => errorResponse({ err, logger }),
      websocket: wsHandlers,
    });
    this.bunServer = srv;
    const resolvedPort = typeof srv.port === "number" ? srv.port : port;
    const resolvedHost = typeof srv.hostname === "string" ? srv.hostname : hostname;

    this.start();

    return { port: resolvedPort, hostname: resolvedHost, stop: () => this.stop() };
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

    for (const stream of Array.from(this.liveStreams)) stream.close();
    this.liveStreams.clear();
    this.agentStreamHub.stop();
    this.workerWs.stop();
    this.remoteDeploymentSweeper?.stop();

    await this.waitForInFlight();
    await this.lifecycle.stop();

    try {
      await srv?.stop(true);
    } catch (err) {
      this.logger.error("[zorya] closing connections failed", err);
    }
  }

  /** Resolve when no request is in flight, or after `shutdownTimeoutMs`. */
  private async waitForInFlight(): Promise<void> {
    if (this.inFlight === 0) return;
    await new Promise<void>((resolve) => {
      const done = (): void => {
        timer.clear();
        this.idleWaiters.delete(done);
        resolve();
      };
      const timer = this.clock.setTimeout(done, this.shutdownTimeoutMs);
      this.idleWaiters.add(done);
    });
  }

  /** `/api/ready`: 200 when storage answers and every service started, else 503. */
  private async readiness(): Promise<Response> {
    const services = this.lifecycle.statuses();
    if (this.stopping) {
      return Response.json({ ready: false, reason: "stopping", services }, { status: 503 });
    }
    const storage = await this.probeStorage();
    const ready = storage.ok && this.lifecycle.isReady;
    return Response.json({ ready, storage, services }, { status: ready ? 200 : 503 });
  }

  /** One cheap storage read, bounded by `readinessTimeoutMs`. */
  private async probeStorage(): Promise<{ ok: boolean; error?: string }> {
    let timer: { clear(): void } | undefined;
    const timeout = new Promise<never>((_, reject) => {
      timer = this.clock.setTimeout(
        () => reject(new Error(`storage did not answer within ${this.readinessTimeoutMs}ms`)),
        this.readinessTimeoutMs,
      );
    });
    try {
      await Promise.race([
        this.workflows.storage.loadWorkflowStatus(READINESS_PROBE_WORKFLOW_ID),
        timeout,
      ]);
      return { ok: true };
    } catch (err) {
      return { ok: false, error: err instanceof Error ? err.message : String(err) };
    } finally {
      timer?.clear();
    }
  }

  /**
   * The 401 for a request that lacks the credentials its plane requires,
   * or undefined when it may proceed.
   */
  private authenticate(params: { req: Request; path: string }): Response | undefined {
    const { req, path } = params;
    if (isPublicRoute({ method: req.method, path })) return undefined;
    if (path === "/rpc/storage") {
      return this.storageCaller(req) ? undefined : jsonError(401, "unauthorized_worker");
    }
    if (isWorkerPath(path)) {
      return this.workerAuth.check(req) ? undefined : jsonError(401, "unauthorized_worker");
    }
    if (path.startsWith("/api/")) {
      return this.auth.check(req) ? undefined : jsonError(401, "unauthorized");
    }
    return undefined;
  }

  /**
   * Who is calling `/rpc/storage`: an operator (full contract), a worker
   * (runner methods only), or nobody allowed (undefined).
   */
  private storageCaller(req: Request): "operator" | "worker" | undefined {
    const token = bearerToken(req);
    if (this.auth.matches(token)) return "operator";
    if (this.workerAuth.isEnabled) return this.workerAuth.matches(token) ? "worker" : undefined;
    // Insecure: the worker plane is open. Everything is open when the
    // operator plane is too; otherwise anonymous callers get worker scope.
    return this.auth.isEnabled ? "worker" : "operator";
  }

  /** Throw unless every mounted plane has keys or the operator opted into insecure mode. */
  private checkAuthConfigured(config: ZoryaServerConfig): void {
    const open: string[] = [];
    if (!this.auth.isEnabled) open.push("/api (apiKeys)");
    if (config.remoteWorkers && !this.workerAuth.isEnabled) {
      open.push("worker planes /ws/worker, /rpc/* (remoteWorkers.apiKeys)");
    }
    if (open.length === 0) return;
    if (!config.insecure) {
      throw new ZoryaInsecureConfigError(
        `ZoryaServer: no API keys configured for ${open.join(" and ")}. ` +
          "Configure keys, or pass `insecure: true` (ZORYA_INSECURE=1 for the bundled server) " +
          "to run unauthenticated for local development.",
      );
    }
    this.logger.warn(
      "[zorya] WARNING: INSECURE MODE. These planes accept requests without credentials: " +
        `${open.join(", ")}. Anyone who can reach this port can read and change every run. ` +
        "Never expose this server outside local development.",
    );
  }

  /** The background services, in start order. */
  private managedServices(): ManagedService[] {
    const out: ManagedService[] = [{ name: "workflows", ...bindLifecycle(this.workflows) }];
    if (this.scheduler) out.push({ name: "scheduler", ...bindLifecycle(this.scheduler) });
    if (this.agents) out.push({ name: "agents", ...bindLifecycle(this.agents) });
    if (this.skills) out.push({ name: "skills", ...bindLifecycle(this.skills) });
    if (this.fragments) out.push({ name: "fragments", ...bindLifecycle(this.fragments) });
    return out;
  }

  /**
   * Re-wrap an event-stream response so `stop()` can end it: closing
   * cancels the route's stream (running its teardown) and ends the body the
   * client reads.
   */
  private trackStream(res: Response): Response {
    const source = res.body;
    if (!source) return res;
    const reader = source.getReader();
    let entry: { close(): void } | undefined;
    const forget = (): void => {
      if (entry) this.liveStreams.delete(entry);
    };
    const body = new ReadableStream<Uint8Array>({
      start: (controller) => {
        entry = {
          close: () => {
            forget();
            reader.cancel("server_shutdown").catch(() => {});
            try {
              controller.close();
            } catch {
              /* already closed */
            }
          },
        };
        this.liveStreams.add(entry);
      },
      pull: async (controller) => {
        try {
          const { value, done } = await reader.read();
          if (done) {
            forget();
            try {
              controller.close();
            } catch {
              /* closed by stop() */
            }
            return;
          }
          controller.enqueue(value);
        } catch (err) {
          forget();
          try {
            controller.error(err);
          } catch {
            /* closed by stop() */
          }
        }
      },
      cancel: async (reason) => {
        forget();
        await reader.cancel(reason).catch(() => {});
      },
    });
    return new Response(body, {
      status: res.status,
      statusText: res.statusText,
      headers: res.headers,
    });
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
      // API-shaped paths never fall back to the SPA.
      if (/^\/(api|rpc|webhooks)(\/|$)/.test(url.pathname)) return jsonError(404, "not_found");
      const path = url.pathname === "/" ? "/index.html" : url.pathname;
      const file = Bun.file(`${dir}${path}`);
      if (await file.exists()) return new Response(file);
      const indexFile = Bun.file(`${dir}/index.html`);
      if (await indexFile.exists()) return new Response(indexFile);
      return new Response("Not Found", { status: 404 });
    };
  }
}

/**
 * Walk the workflows chain (top → fallback) returning the first non-undefined
 * value the picker yields. Used to surface fields like advertisements /
 * definitions that the outer layer might not own but a fallback layer does.
 */
function walkChainFor<T>(
  start: ZoryaWorkflows,
  pick: (layer: ZoryaWorkflows) => T | undefined,
): T | undefined {
  let layer: ZoryaWorkflows | undefined = start;
  while (layer) {
    const v = pick(layer);
    if (v !== undefined) return v;
    layer = (layer as unknown as { fallback?: ZoryaWorkflows }).fallback;
  }
  return undefined;
}

/** Liveness and readiness probes: public, and answered while shutting down. */
function isProbePath(path: string): boolean {
  return path === "/api/health" || path === "/api/ready";
}

/** Routes that need no Zorya key: the probes and the bearer-checked signal-token routes. */
function isPublicRoute(params: { method: string; path: string }): boolean {
  const { method, path } = params;
  if (method === "GET" && isProbePath(path)) return true;
  if (method === "GET" && /^\/api\/signal-tokens\/[^/]+\/describe$/.test(path)) return true;
  if (method === "POST" && /^\/api\/signal-tokens\/[^/]+\/complete$/.test(path)) return true;
  return false;
}

/** Paths on the worker plane, gated by worker keys (`/rpc/storage` is handled separately). */
function isWorkerPath(path: string): boolean {
  return (
    path.startsWith("/rpc/") ||
    path === "/api/advertisements" ||
    path.startsWith("/api/advertisements/") ||
    path.startsWith("/api/worker-protocol/")
  );
}

function isEventStream(res: Response): boolean {
  return (res.headers.get("content-type") ?? "").startsWith("text/event-stream");
}

function bindLifecycle(service: { start(): Promise<void>; stop(): Promise<void> }): {
  start(): Promise<void>;
  stop(): Promise<void>;
} {
  return { start: () => service.start(), stop: () => service.stop() };
}
