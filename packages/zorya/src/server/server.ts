// ---------------------------------------------------------------------------
// ZoryaServer — HTTP server that wraps a promin WorkflowStorage.
//
// Usage:
// ```ts
// const server = new ZoryaServer({
//   storage,
//   apiKeys: ["secret"],
//   trigger: async (name, input) => myRunner.start(name, input),
//   workers: myWorkerRegistry,
// });
// server.listen({ port: 4000 });
// ```
// ---------------------------------------------------------------------------

import type {
  WorkflowStorage,
  WorkflowRunner,
  SchedulerStorage,
  StepQueue,
  Workflow,
  WorkerRegistry,
  DistributedWorkflowRunner,
} from "@promin/workflow";
import { createDistributedWorkflowRunner, RecoveryStrategy } from "@promin/workflow";
import { createWorkerApiHandler, createWorkflowStorageHandler } from "@promin/workflow-remote";
import { Auth, type AuthConfig } from "./auth.ts";
import { Router, jsonError } from "./router.ts";
import {
  InMemoryWorkflowAdvertisementRegistry,
  type WorkflowAdvertisementRegistry,
} from "./workflow-advertisements.ts";
import { InMemoryWorkflowStartQueue, type WorkflowStartQueue } from "./workflow-starts.ts";
import { TriggerService } from "./services/trigger-service.ts";
import { CoordinatedTriggerService } from "./services/coordinated-trigger-service.ts";
import { SchedulerLoop, type SchedulerLoopConfig } from "./services/scheduler-loop.ts";
import { WorkerWebSocketServer } from "./services/worker-ws-server.ts";
import type { ScheduleTick, DurableScheduleConfig } from "@promin/workflow";
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
  type RunTrigger,
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
import { dispatchAgentSchedule, isAgentSchedule } from "@promin/agent";
import {
  addNamespaceFact,
  deleteNamespaceFact,
  inspectMemory,
  patchNamespace,
  type MemoryInspectorDeps,
} from "./routes/memory.ts";
import {
  deleteAgentInstance,
  getAgentInstance,
  listAgentInstances,
  listInstancesAcrossAgents,
  resolveAgentInstance,
  updateAgentInstance,
  type InstanceDeps,
} from "./routes/instances.ts";

export interface Logger {
  log(message: string, ...args: unknown[]): void;
  warn(message: string, ...args: unknown[]): void;
  error(message: string, ...args: unknown[]): void;
}

export interface ZoryaServerConfig extends AuthConfig {
  storage: WorkflowStorage;
  /** Function that starts a new run by name. Required for POST /api/runs/trigger/:name. */
  trigger?: RunTrigger;
  /** Custom logger. Defaults to `console`. */
  logger?: Logger;
  /** Override metrics with a backend-specific provider (e.g. PgWorkflowMetrics). */
  metrics?: MetricsProvider;
  /** Plug in a worker registry. When omitted, /api/workers returns []. */
  workers?: WorkersProvider;
  /** Plug in a scheduler storage. When omitted, /api/schedules returns a stub. */
  scheduler?: SchedulerStorage;
  /**
   * Registry of known workflow definitions, keyed by workflow name. Used by
   * `GET /api/runs/:id` to enrich the response with the full static step
   * list (including steps that haven't executed yet). Optional — without
   * it the UI still works, it just can't render planned steps.
   */
  workflows?: Readonly<Record<string, Workflow<unknown, unknown>>>;
  /**
   * Returns a sample/default input for a workflow by name. Used by the UI
   * to populate the trigger form with plausible values (so users can tweak
   * fields instead of typing raw JSON from scratch). Return `undefined`
   * for workflows with no default.
   */
  sampleInput?: (workflowName: string) => unknown;
  /**
   * Optional re-run hook. When present, `POST /api/runs/:id/rerun` calls
   * this after `startFreshRun` so the caller can actually drive the
   * workflow again. Without it the storage row is reset but nothing
   * executes (the storage row stays in `running`).
   */
  rerun?: (workflowId: string) => Promise<void>;
  /**
   * Startup recovery. When set, `listen()` runs the strategy once on boot.
   *
   * Stale termination (`cancelStale` / `failStale`) only needs storage —
   * the server runs it directly, no runner required.
   *
   * Resume (`resumeRecent`) needs a runner to look up definitions and
   * re-execute — pass `runner` when your strategy includes it.
   *
   * ```ts
   * // Cancel stale runs only — no runner needed:
   * recovery: {
   *   strategy: RecoveryStrategy.builder()
   *     .failStale({ olderThanMs: 60 * 60 * 1000 })
   *     .build(),
   * }
   *
   * // Cancel stale + resume recent — runner required:
   * recovery: {
   *   runner,
   *   strategy: RecoveryStrategy.builder()
   *     .failStale({ olderThanMs: 60 * 60 * 1000 })
   *     .resumeRecent()
   *     .build(),
   * }
   * ```
   */
  recovery?: {
    strategy: RecoveryStrategy;
    /** Required when strategy includes `resumeRecent()`. */
    runner?: WorkflowRunner;
  };
  /** Directory with compiled dashboard assets (index.html, app.js, app.css). */
  uiDir?: string;
  /** SSE watcher poll interval. Default 1000ms. */
  sseIntervalMs?: number;
  /**
   * Enables the remote worker protocol. When provided, the server mounts:
   *
   *   POST /rpc/storage            — createWorkflowStorageHandler
   *   POST /rpc/worker             — createWorkerApiHandler (steps + workers)
   *   POST /api/advertisements     — workers register their workflow defs
   *   DELETE /api/advertisements/:workerId
   *   GET  /api/advertisements     — debugging
   *
   * And the Workflows page starts pulling from the advertisement registry
   * in addition to the static `workflows` config.
   */
  workerProtocol?: {
    stepQueue: StepQueue;
    workerRegistry?: WorkerRegistry;
    advertisements?: WorkflowAdvertisementRegistry;
    /**
     * Pending workflow-start queue. When configured (or auto-created),
     * the dashboard's `POST /api/runs/trigger/:name` endpoint enqueues
     * a start that connected workers poll and execute. Lets the dashboard
     * trigger button work in split mode without requiring a server-side
     * trigger fn.
     */
    workflowStarts?: WorkflowStartQueue;
    /**
     * API keys required on /rpc/storage, /rpc/worker, and
     * /api/advertisements. When omitted, the worker surface is open —
     * safe behind a private network, risky on a public one. Kept
     * separate from the dashboard's top-level `apiKeys` so you can
     * rotate one without the other and issue worker-only keys.
     */
    apiKeys?: ReadonlyArray<string>;
  };
  /**
   * Enable Temporal-style coordinator-driven step dispatch. When set, the
   * server runs a `WorkflowCoordinator` that owns the workflow state
   * machine and enqueues ready steps to `workerProtocol.stepQueue`.
   * Workers in `mode: 'step'` claim individual steps and execute them
   * locally, writing results back through storage + step-queue endpoints
   * the server already exposes.
   *
   * Implies replacing the default trigger flow: `/api/runs/trigger/:name`
   * routes through the coordinator instead of the workflow-start queue,
   * so workflow-mode workers won't pick triggered runs up. Mix-and-match
   * is a future refinement.
   *
   * Requires `workerProtocol.stepQueue` and at least one connected worker
   * advertising the target workflow.
   */
  coordination?: {
    enabled: boolean;
    /**
     * Coordinator's own polling cadence (leader election + dead-worker
     * sweep). Default: 1000ms.
     */
    pollIntervalMs?: number;
    /**
     * StepQueueExecutor poll cadence — how often the coordinator's
     * runner checks storage for a step's terminal status after enqueuing
     * it to the queue. Default: 500ms.
     */
    stepPollIntervalMs?: number;
    /**
     * Worker dead-timeout — tasks claimed by a worker silent for this
     * long are re-enqueued. Default: 30000ms.
     */
    workerTimeoutMs?: number;
  };
  /**
   * Run the DurableScheduler tick loop inside this server. The "type" of
   * scheduler is implicit in the `scheduler` SchedulerStorage instance you
   * pass at the top level — Postgres / Redis / in-memory all work
   * transparently because the loop only uses the portable
   * `SchedulerStorage` interface.
   *
   * Horizontal scaling: every tick begins with `tryAcquireLeader` against
   * the configured storage. Postgres uses `pg_try_advisory_lock`, Redis
   * uses `SET NX PX`; both ensure at most one Zorya instance fires a tick
   * per `(namespace)`. The deterministic workflowId
   * `${scheduleId}.${tickNumber}` adds a belt-and-suspenders guarantee:
   * even if a brief leader-transition race produces two ticks, the second
   * dispatch is a no-op because `createWorkflow` is idempotent on
   * workflowId.
   *
   * In-memory storage is single-process — running multiple Zorya
   * instances against the same in-memory storage is impossible by
   * construction (no shared state).
   *
   * Without `scheduling.enabled`, ZoryaServer keeps the existing CRUD
   * routes for `/api/schedules` but doesn't tick anything — the operator
   * runs `DurableScheduler` externally if they want firing.
   */
  /**
   * Optional agent gateway. When set, mounts:
   *
   *   GET  /api/agents
   *   GET  /api/agents/:id
   *   POST /api/agents/:id/invoke
   *   POST /api/agents/:id/stream                 (SSE)
   *   POST /api/agents/:id/threads/:threadId
   *   POST /api/agents/:id/threads/:threadId/stream  (SSE)
   *   GET  /api/agents/:id/threads/:threadId/messages
   *
   * `registry` stores the recipes; `resolve` materializes a live `Agent`
   * from a recipe (typically `(r) => resolveLocalAgent(r, deps)` for
   * LocalAgent backends). Tenant binding is handled per request inside
   * the routes via `agent.withScope({ namespaceId, resourceId })`.
   */
  agents?: AgentGatewayDeps;
  /**
   * Optional memory-inspection wiring. When provided, the server mounts
   * `GET /api/memory/inspect` — a read-only snapshot of the three-scope
   * cascade (namespace + resource + thread) for operator debugging.
   * Pass the same `MemoryStore` instance the agent resolver uses.
   */
  memoryInspector?: MemoryInspectorDeps;
  /**
   * Optional agent-instance wiring. When provided, the server mounts the
   * `/api/agents/:id/instances` and `/api/instances` routes. The registry
   * is the index of long-lived (agent, namespace, owner) tuples; the
   * memory store is needed so DELETE can cascade through the resource
   * scope.
   */
  instances?: InstanceDeps;
  scheduling?: {
    enabled: boolean;
    /** Poll cadence in ms. Default: 1000. */
    pollIntervalMs?: number;
    /**
     * Leader-lock TTL in ms. Default: 3 × pollIntervalMs. Lower = faster
     * fail-over after a leader crash; higher = tolerates longer poll
     * cycles without losing leadership.
     */
    leaderLockTtlMs?: number;
    /**
     * Restrict the loop to a single schedule namespace. `undefined`
     * polls only the GLOBAL namespace (schedules with no `namespace`
     * field set). Different namespaces have independent leader locks,
     * so two Zorya instances can each be leader for a different
     * namespace.
     *
     * Mutually exclusive with `namespaces`. To poll across multiple
     * (or all) namespaces from one Zorya instance, set `namespaces`.
     */
    namespace?: string;
    /**
     * Multi-namespace mode for serving many tenants from one Zorya.
     *
     * - `"all"` — every namespace with at least one due schedule on
     *   each tick. Idle namespaces cost zero RPCs (the cross-namespace
     *   `findDueAcross` only returns rows that are actually due).
     * - `string[]` — restrict to a specific tenant set. Pass `""` /
     *   `undefined` element to also include the global namespace.
     *
     * Each namespace acquires its own leader lock, so noisy or slow
     * tenants can't block others. Mutually exclusive with `namespace`.
     */
    namespaces?: "all" | readonly (string | undefined)[];
    /**
     * Stable instance id used by leader election. Default: random UUID
     * generated per server boot.
     */
    instanceId?: string;
    /**
     * Hash partitioning across multiple Zorya instances. Combined with
     * leader election lets you scale beyond one tick-firing instance per
     * namespace by sharding by schedule id. Two instances with
     * `{ index: 0, count: 2 }` and `{ index: 1, count: 2 }` each get one
     * shard.
     */
    partition?: { index: number; count: number };
    /** Max schedules per poll. Default: 100. */
    batchSize?: number;
    /**
     * Max concurrent dispatches per tick. Equivalent to
     * `scheduler.stream().pipe(parMapAsync(N))` from the standalone
     * scheduler. Default: 10.
     */
    dispatchConcurrency?: number;
    /**
     * Custom dispatch callback. Receives the fired tick + the schedule
     * config that produced it. Return a Promise. When omitted the loop
     * routes through the configured `trigger` (which is the
     * coordinator-trigger when `coordination.enabled`, the workflow-start
     * trigger otherwise) using `metadata.workflowName` + `metadata.input`
     * from the schedule config.
     */
    fire?: (tick: ScheduleTick, schedule: DurableScheduleConfig) => Promise<void>;
  };
}

export interface ListenOptions {
  port?: number;
  hostname?: string;
}

export class ZoryaServer {
  readonly config: Required<Pick<ZoryaServerConfig, "storage">> & ZoryaServerConfig;
  /**
   * Coordinator instance when `config.coordination.enabled` is true.
   * Public so embedders / tests can `submit()` directly without going
   * through the HTTP trigger endpoint.
   */
  readonly coordinator?: DistributedWorkflowRunner;
  /**
   * Embedded scheduler tick loop when `config.scheduling.enabled` is
   * true. Public so tests can drive single ticks via `tickOnce()`.
   */
  readonly schedulerLoop?: SchedulerLoop;
  /**
   * Persistent worker → server WebSocket multiplexer. Always present —
   * workers in step / agent mode connect on `/ws/worker` to receive
   * server-pushed commands (query handlers, agent stream start/stop) and
   * push frames back (token deltas, structured events). The downstream
   * tickets (promin-o8dj, promin-i0wi, promin-eg0d) wire onto this.
   */
  readonly workerWs: WorkerWebSocketServer;
  /**
   * Bridges WS frames from workers to SSE clients on `/api/runs/:id/agent-stream`.
   * Always present — the SSE route is mounted unconditionally; it only
   * delivers events when at least one worker hosting that workflow is
   * connected over the WS and a SessionEventBus is registered there.
   */
  readonly agentStreamHub: AgentStreamHub;
  private readonly auth: Auth;
  /** Separate auth for worker-protocol endpoints. Open when no keys set. */
  private readonly workerAuth: Auth;
  private readonly bus: RunEventBus;
  private readonly router: Router;
  private readonly logger: Logger;
  private server?: { stop(): void; port: number; hostname: string };
  /** Background coordinator loop — kicked off in `listen()`, stopped in `stop()`. */
  private coordinatorLoop?: Promise<void>;

  constructor(config: ZoryaServerConfig) {
    this.config = config;
    this.logger = config.logger ?? console;
    this.auth = new Auth(config);
    this.workerAuth = new Auth({ apiKeys: config.workerProtocol?.apiKeys });
    this.bus = new RunEventBus();
    // Worker control socket — upgraded from /ws/worker. Auth gates the
    // upgrade with the same workerAuth keys so the same ENV the worker
    // already uses for /rpc/* applies.
    this.workerWs = new WorkerWebSocketServer({
      authorize: (req) => this.workerAuth.check(req),
    });
    this.agentStreamHub = new AgentStreamHub(this.workerWs);

    const metrics = config.metrics ?? new StorageMetricsProvider(config.storage);
    // Prefer an explicit workers provider; otherwise derive one from the
    // worker protocol's registry so the dashboard auto-populates without
    // extra config. Only falls back to empty when neither is available.
    const workers =
      config.workers ??
      (config.workerProtocol?.workerRegistry
        ? new RegistryBackedWorkersProvider(config.workerProtocol.workerRegistry)
        : emptyWorkersProvider);
    // Auto-create an in-memory advertisement registry when the worker
    // protocol is on but no registry is passed — that's the common case.
    const advertisements: WorkflowAdvertisementRegistry | undefined = config.workerProtocol
      ? (config.workerProtocol.advertisements ?? new InMemoryWorkflowAdvertisementRegistry())
      : undefined;

    // Same idea for the workflow-start queue. When the worker protocol is
    // on we auto-create an in-memory queue so the dashboard's trigger
    // button works out of the box: the auto-trigger pre-creates a pending
    // workflow row and enqueues a start that connected workers poll.
    // Skipped under coordination — the coordinator owns the workflow row
    // and dispatches steps directly, so a workflow-start queue would just
    // sit empty.
    const coordEnabled = config.coordination?.enabled === true;
    const workflowStarts: WorkflowStartQueue | undefined =
      config.workerProtocol && !coordEnabled
        ? (config.workerProtocol.workflowStarts ?? new InMemoryWorkflowStartQueue())
        : undefined;

    // Wire the coordinator first so the trigger fallback below can route
    // through it. Requires the worker protocol's stepQueue — without a
    // queue the coordinator can't dispatch anything.
    if (coordEnabled) {
      if (!config.workerProtocol?.stepQueue) {
        throw new Error("ZoryaServer: coordination.enabled requires workerProtocol.stepQueue");
      }
      if (!advertisements) {
        throw new Error(
          "ZoryaServer: coordination.enabled requires workerProtocol.advertisements " +
            "(auto-created when workerProtocol is set; explicit registry must include it)",
        );
      }
      this.coordinator = createDistributedWorkflowRunner({
        storage: config.storage,
        stepQueue: config.workerProtocol.stepQueue,
        workerRegistry: config.workerProtocol.workerRegistry,
        pollIntervalMs: config.coordination?.pollIntervalMs,
        stepPollIntervalMs: config.coordination?.stepPollIntervalMs,
        workerTimeoutMs: config.coordination?.workerTimeoutMs,
      });
    }

    // Auto-trigger:
    //  - coordination on:    CoordinatedTriggerService → coordinator.submit
    //  - coordination off:   TriggerService → workflow-start queue
    //  - explicit `trigger`: always wins
    const trigger =
      config.trigger ??
      (this.coordinator && advertisements
        ? new CoordinatedTriggerService({
            storage: config.storage,
            coordinator: this.coordinator,
            advertisements,
          }).trigger
        : workflowStarts
          ? new TriggerService({
              storage: config.storage,
              workflowStarts,
              advertisements,
            }).trigger
          : undefined);

    if (config.scheduling?.enabled) {
      if (!config.scheduler) {
        throw new Error(
          "ZoryaServer: scheduling.enabled requires `scheduler: SchedulerStorage` " +
            "(pass an InMemory / Postgres / Redis SchedulerStorage instance)",
        );
      }
      if (!config.scheduling.fire && !trigger) {
        throw new Error(
          "ZoryaServer: scheduling.enabled needs either a `scheduling.fire` callback " +
            "or a configured trigger (via `trigger`, `coordination.enabled`, or `workerProtocol`)",
        );
      }
      // If the host configured `agents`, install an agent-aware fire
      // override by default — agent-targeted ticks (created via the
      // durable scheduler tool) get re-fired through `dispatchAgentSchedule`,
      // and everything else falls through to the loop's default
      // `metadata.workflowName` dispatch. The user's explicit
      // `scheduling.fire` always wins; pass it to opt out.
      const fire =
        config.scheduling.fire ?? (config.agents ? buildAgentAwareFire(config.agents) : undefined);
      this.schedulerLoop = new SchedulerLoop({
        storage: config.scheduler,
        trigger,
        ...(fire !== undefined && { fire }),
        instanceId: config.scheduling.instanceId,
        pollIntervalMs: config.scheduling.pollIntervalMs,
        leaderLockTtlMs: config.scheduling.leaderLockTtlMs,
        namespace: config.scheduling.namespace,
        namespaces: config.scheduling.namespaces,
        partition: config.scheduling.partition,
        batchSize: config.scheduling.batchSize,
        dispatchConcurrency: config.scheduling.dispatchConcurrency,
      });
    }

    const deps = {
      storage: config.storage,
      trigger,
      workflows: config.workflows,
      advertisements,
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
      .get("/api/runs/:id/signals", getRunSignals(config.storage))
      .get("/api/runs/:id/attempts", getRunAttempts(config.storage))
      .get("/api/runs/:id/history", getRunHistory(config.storage))
      .get("/api/runs/:id/children", getRunChildren(config.storage))
      .get("/api/runs/:id/journal/:stepName", getRunStepJournal(config.storage))
      .get("/api/approvals", listApprovals(config.storage))
      .post("/api/runs/:id/mark-success", markRunSuccess(config.storage))
      .post("/api/runs/:id/mark-failed", markRunFailed(config.storage))
      .post("/api/runs/:id/rerun", rerunRun(config.storage, config.rerun))
      .get("/api/workflows/sparklines", getSparklines(config.storage))
      .get(
        "/api/workflows/definitions",
        listWorkflowDefs({
          workflows: config.workflows,
          sampleInput: config.sampleInput,
          advertisements,
        }),
      )
      .get("/api/workflows/:name/grid", getWorkflowGrid(config.storage))
      .get("/api/workflows/:name/history", getWorkflowHistory(config.storage))
      .get(
        "/api/workflows/:name/definition",
        getWorkflowDef({
          workflows: config.workflows,
          sampleInput: config.sampleInput,
          advertisements,
        }),
      )
      .post("/api/runs/trigger/:name", triggerRun(deps))
      .post("/api/runs/:id/cancel", cancelRun(deps))
      .post("/api/runs/:id/signal", sendSignal(deps))
      .get(
        "/api/runs/:id/events",
        streamRunEvents({
          storage: config.storage,
          bus: this.bus,
          pollIntervalMs: config.sseIntervalMs,
        }),
      )
      .get("/api/runs/:id/agent-stream", streamAgentEvents(this.agentStreamHub))
      .post("/api/runs/:id/query", queryRun(this.workerWs))
      .get("/api/workers", listWorkers(workers))
      .get("/api/metrics", getMetrics(metrics));

    if (config.agents) {
      const agentDeps = config.agents;
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
    }

    if (config.memoryInspector) {
      const mem = config.memoryInspector;
      this.router
        .get("/api/memory/inspect", inspectMemory(mem))
        .patch("/api/memory/namespace/:namespaceId", patchNamespace(mem))
        .post("/api/memory/namespace/:namespaceId/facts", addNamespaceFact(mem))
        .delete("/api/memory/namespace/:namespaceId/facts/:factId", deleteNamespaceFact(mem));
    }

    if (config.instances) {
      const inDeps = config.instances;
      this.router
        .get("/api/agents/:id/instances", listAgentInstances(inDeps))
        .post("/api/agents/:id/instances", resolveAgentInstance(inDeps))
        .get("/api/agents/:id/instances/:instanceId", getAgentInstance(inDeps))
        .patch("/api/agents/:id/instances/:instanceId", updateAgentInstance(inDeps))
        .delete("/api/agents/:id/instances/:instanceId", deleteAgentInstance(inDeps))
        .get("/api/instances", listInstancesAcrossAgents(inDeps));
    }

    if (config.scheduler) {
      const sch = config.scheduler;
      this.router
        .get("/api/schedules", listSchedules(sch))
        .post("/api/schedules", createSchedule(sch))
        .get("/api/schedules/:id", getSchedule(sch))
        .patch("/api/schedules/:id", patchSchedule(sch))
        .delete("/api/schedules/:id", deleteSchedule(sch))
        .get("/api/schedules/:id/history", getScheduleHistory(sch, config.storage))
        .get("/api/schedules/:id/upcoming", getScheduleUpcoming(sch))
        .post(
          "/api/schedules/:id/emit",
          emitScheduleNow(sch, this.schedulerLoop?.fireOnce.bind(this.schedulerLoop)),
        );
    } else {
      // Stub response so the UI can tell "scheduler not configured" from
      // "scheduler configured but empty".
      this.router.get(
        "/api/schedules",
        () =>
          new Response(JSON.stringify({ schedules: [], total: 0, configured: false }), {
            headers: { "content-type": "application/json" },
          }),
      );
    }

    if (config.workerProtocol) {
      const { stepQueue, workerRegistry } = config.workerProtocol;
      const storageHandler = createWorkflowStorageHandler(config.storage);
      const workerHandler = createWorkerApiHandler({
        stepQueue,
        storage: config.storage,
        workerRegistry,
      });
      // Mount as catch-all handlers: RPC body is the source of truth, path
      // is just a mount point. Keep paths stable so client SDKs don't need
      // configuration.
      this.router.post("/rpc/storage", (req) => storageHandler(req));
      this.router.post("/rpc/worker", (req) => workerHandler(req));

      if (advertisements) {
        this.router
          .post("/api/advertisements", upsertAdvertisements(advertisements))
          .delete("/api/advertisements/:workerId", removeAdvertisements(advertisements))
          .get("/api/advertisements", listAdvertisements(advertisements));
      }

      if (workflowStarts) {
        this.router
          .post("/api/worker-protocol/claim-starts", claimWorkflowStarts(workflowStarts))
          .post("/api/worker-protocol/complete-start/:id", completeWorkflowStart(workflowStarts))
          .get("/api/worker-protocol/starts", listWorkflowStarts(workflowStarts));
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

    // Worker-protocol surface has its own keyset. /api/advertisements and
    // /api/worker-protocol/* live under /api/ so strip them from the
    // dashboard-auth branch too.
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
    // Use Bun.serve if available; otherwise throw a clear error.
    const BunGlobal = (globalThis as { Bun?: { serve: typeof Bun.serve } }).Bun;
    if (!BunGlobal) {
      throw new Error("ZoryaServer.listen requires Bun runtime");
    }
    const wsHandlers = this.workerWs.websocketHandlers();
    const srv = BunGlobal.serve({
      port,
      hostname,
      fetch: (req, server) => {
        // WebSocket upgrade goes first — Bun's `server.upgrade(req)`
        // returns true on success, in which case fetch must return
        // undefined. The helper returns a Response (with 401 / 400) when
        // auth or protocol fails, or undefined on a successful upgrade.
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
    this.server = { stop: () => srv.stop(), port: resolvedPort, hostname: resolvedHost };
    this.workerWs.start();
    this.agentStreamHub.start();
    this.startCoordinator();
    this.startScheduler();
    this.startRecovery();
    return {
      port: resolvedPort,
      hostname: resolvedHost,
      stop: () => {
        this.stop();
      },
    };
  }

  /**
   * Start the coordinator's leader / dead-worker loop. Called from
   * `listen()` and exposed for tests that drive the server through
   * `handle()` without binding a port.
   */
  startCoordinator(): void {
    if (!this.coordinator || this.coordinatorLoop) return;
    // Failures inside the coordinator's own loop write to its own logs;
    // we only swallow here so a transient error doesn't surface as an
    // unhandled rejection on the server's lifecycle.
    this.coordinatorLoop = this.coordinator.startLoop().catch(() => {});
  }

  /**
   * Start the embedded scheduler tick loop. Called from `listen()` and
   * exposed for tests that drive the server through `handle()` without
   * binding a port.
   */
  startScheduler(): void {
    this.schedulerLoop?.start();
  }

  /**
   * Run the configured recovery strategy once, fire-and-forget. Called from
   * `listen()` and exposed for tests that drive the server through `handle()`
   * without binding a port.
   */
  startRecovery(): void {
    const { recovery } = this.config;
    if (!recovery) return;
    const { strategy, runner } = recovery;
    const opts = strategy._opts;

    void (async () => {
      // Stale termination — storage only, no runner needed.
      let terminated = 0;
      if (opts.staleThresholdMs !== undefined) {
        const errorMsg =
          opts.staleAction.kind === "fail"
            ? opts.staleAction.error
            : "Stale run cancelled on restart";
        const s = this.config.storage as any;
        if (typeof s.cancelStaleWorkflows === "function") {
          terminated = s.cancelStaleWorkflows({
            olderThanMs: opts.staleThresholdMs,
            error: errorMsg,
            statuses: opts.staleStatuses,
          });
        } else {
          const cutoff = Date.now() - opts.staleThresholdMs;
          for (const status of opts.staleStatuses as import("@promin/workflow").WorkflowStatus[]) {
            while (true) {
              const page = await this.config.storage.listWorkflows({
                status,
                limit: 200,
                offset: 0,
                orderBy: "createdAt",
                orderDir: "asc",
              });
              if (page.length === 0) break;
              let anyStale = false;
              for (const wf of page) {
                if (wf.createdAt.getTime() >= cutoff) break;
                anyStale = true;
                if (opts.staleAction.kind === "cancel") {
                  await this.config.storage.cancelWorkflow(wf.workflowId);
                } else {
                  await this.config.storage.failWorkflow(wf.workflowId, opts.staleAction.error);
                }
                terminated++;
              }
              if (!anyStale) break;
            }
          }
        }
        if (terminated > 0) {
          this.logger.log(`[zorya] recovery: auto-failed ${terminated} stale run(s)`);
        }
      }

      // Resume — needs a runner + workflow definitions.
      if (opts.resumeRecent) {
        if (!runner) {
          this.logger.warn(
            "[zorya] recovery: resumeRecent() requires a runner — pass recovery.runner to enable",
          );
          return;
        }
        const workflows = this.config.workflows ?? {};
        let resumed = 0;
        const skipped: Array<{ workflowId: string; name: string }> = [];
        const PAGE = 200;
        for (const status of ["pending", "running"] as const) {
          let offset = 0;
          while (true) {
            const page = await this.config.storage.listWorkflows({ status, limit: PAGE, offset });
            if (page.length === 0) break;
            for (let i = 0; i < page.length; i += opts.resumeConcurrency) {
              const batch = page.slice(i, i + opts.resumeConcurrency);
              for (const wf of batch) {
                const def = workflows[wf.workflowName];
                if (!def) {
                  skipped.push({ workflowId: wf.workflowId, name: wf.workflowName });
                  continue;
                }
                void runner.runSafe({ workflow: def, workflowId: wf.workflowId, input: wf.input });
                resumed++;
              }
              if (i + opts.resumeConcurrency < page.length) {
                await new Promise<void>((r) => setTimeout(r, 0));
              }
            }
            if (page.length < PAGE) break;
            offset += PAGE;
          }
        }
        if (resumed > 0) this.logger.log(`[zorya] recovery: resumed ${resumed} orphaned run(s)`);
        if (skipped.length > 0) {
          this.logger.warn(
            `[zorya] recovery: skipped ${skipped.length} run(s) — no definition in config.workflows`,
          );
        }
      }
    })().catch((err) => {
      this.logger.error("[zorya] recovery error:", err);
    });
  }

  stop(): void {
    this.server?.stop();
    this.server = undefined;
    if (this.coordinator) {
      void this.coordinator.stopLoop();
      this.coordinatorLoop = undefined;
    }
    if (this.schedulerLoop) {
      void this.schedulerLoop.stop();
    }
    this.agentStreamHub.stop();
    this.workerWs.stop();
  }

  private buildStaticHandler(dir: string) {
    return async (req: Request): Promise<Response> => {
      const url = new URL(req.url);
      const path = url.pathname === "/" ? "/index.html" : url.pathname;
      const file = Bun.file(`${dir}${path}`);
      if (await file.exists()) return new Response(file);
      // Fallback to index.html for SPA routing.
      const indexFile = Bun.file(`${dir}/index.html`);
      if (await indexFile.exists()) return new Response(indexFile);
      return new Response("Not Found", { status: 404 });
    };
  }
}

/**
 * Default scheduler-loop dispatch when `agents` is configured but the
 * host didn't supply their own `scheduling.fire`. Agent-targeted ticks
 * (`metadata.agentTrigger === true`) re-invoke the registered agent
 * with `source: { kind: "scheduled", ... }`. Anything else falls
 * through to the loop's default `metadata.workflowName` path.
 */
function buildAgentAwareFire(
  agentDeps: AgentGatewayDeps,
): NonNullable<SchedulerLoopConfig["fire"]> {
  return async (tick, schedule) => {
    if (!isAgentSchedule(schedule)) return { handled: false };
    await dispatchAgentSchedule(tick, schedule, {
      registry: agentDeps.registry,
      resolve: agentDeps.resolve,
    });
    return { handled: true };
  };
}
