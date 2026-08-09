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

import type { RemoteDeploymentRegistry, SecretsStorage } from "@promin/agent";
import type {
  IWorkflowVersionRegistry,
  StepQueue,
  WorkerRegistry,
  WorkflowStorage,
} from "@promin/workflow";
import {
  InMemoryStepQueue,
  InMemoryWorkerRegistry,
  WorkflowVersionRegistry,
} from "@promin/workflow";
import { createWorkerApiHandler, createWorkflowStorageHandler } from "@promin/workflow-remote";
import { Auth, type AuthConfig } from "./auth.ts";
import { Router, jsonError } from "./router.ts";
import { NamespaceService, type NamespaceRegistry } from "./services/namespaces.ts";
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
import { listSignals } from "./routes/signals.ts";
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
  listCatalogRetrievers,
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
  type WebhookSourceConfig,
  type WebhookGatewayDeps,
} from "./routes/webhooks.ts";
import {
  heartbeatDeployment,
  listDeployments,
  registerDeployment,
  unregisterDeployment,
  type RemoteDeploymentsGatewayDeps,
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
  validateWorkflowRetentionConfig,
  WorkflowRetentionCleaner,
} from "./services/workflow-retention.ts";
import { listKnowledgeBases, searchKnowledgeBase } from "./routes/knowledge-bases.ts";
import {
  createDag,
  deleteDag,
  getDag,
  listDagVersions,
  listDags,
  runDag,
  type DagGatewayDeps,
} from "./routes/dags.ts";
import {
  archiveNamespace,
  createNamespace,
  getNamespace,
  listNamespaces,
  updateNamespace,
} from "./routes/namespaces.ts";

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

export interface WorkflowRetentionConfig {
  /** Delete completed and failed runs older than this many days. */
  maxAgeDays: number;
  /** Delay between bounded sweeps. Defaults to one hour. */
  intervalMs?: number;
  /** Maximum terminal runs deleted by one sweep. Defaults to 500. */
  batchSize?: number;
}

export interface ZoryaServerConfig extends AuthConfig {
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
   * Authoritative namespace registry storage. ZoryaServer wraps it in its
   * NamespaceService policy boundary. Defaults to in-memory storage for
   * tests/demo hosts that do not pass a durable registry.
   */
  namespaces?: NamespaceRegistry;
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
   * Workflow version registry. Drives `/api/workflows/:name/versions/*`
   * (promote/rollback, findActive, list-records). Defaults to a fresh
   * in-memory `WorkflowVersionRegistry`; production deployments should
   * pass `PostgresWorkflowVersionRegistry` so lifecycle survives restart.
   */
  versionRegistry?: IWorkflowVersionRegistry;
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
   * external events (Slack / GitHub / Stripe / custom) into a registered
   * agent after HMAC signature verification + replay dedup. Each source
   * declares its secret + signature scheme. Without `agents` configured
   * this is a no-op (no agent to dispatch to). Phase 1 supports
   * 'sha256-hex' (GitHub-style) signatures; Stripe/Slack variants land
   * later.
   */
  webhooks?: { sources: Readonly<Record<string, WebhookSourceConfig>> };
  /**
   * Optional remote-deployment registry. When set, exposes
   * /api/remote-deployments/* — external Zorya servers self-register
   * here at startup, exposing their RemoteAgentBackend recipes via
   * heartbeated TTL. Without it (and without `agents`), the routes
   * are not mounted. See promin-21g5 for the design.
   */
  remoteDeployments?: RemoteDeploymentRegistry;
  /** Optional lifecycle-managed cleanup for old completed/failed workflow runs. */
  retention?: WorkflowRetentionConfig;
}

export interface ListenOptions {
  port?: number;
  hostname?: string;
}

export class ZoryaServer {
  readonly workflows: ZoryaWorkflows;
  readonly scheduler?: ZoryaScheduler;
  readonly agents?: ZoryaAgents;
  readonly skills?: ZoryaSkills;
  readonly fragments?: ZoryaFragments;
  readonly dags?: ZoryaDags;
  readonly namespaces: NamespaceService;
  readonly versionRegistry: IWorkflowVersionRegistry;
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
  private readonly bus: RunEventBus;
  private readonly router: Router;
  private readonly logger: Logger;
  private readonly retention?: WorkflowRetentionCleaner;
  private serverHandle?: { stop(): void; port: number; hostname: string };

  constructor(config: ZoryaServerConfig) {
    this.workflows = config.workflows;
    if (config.scheduler) this.scheduler = config.scheduler;
    if (config.agents) this.agents = config.agents;
    if (config.skills) this.skills = config.skills;
    if (config.fragments) this.fragments = config.fragments;
    if (config.dags) this.dags = config.dags;
    if (config.secrets) this.secrets = config.secrets;
    this.namespaces = new NamespaceService({ registry: config.namespaces });
    this.versionRegistry = config.versionRegistry ?? new WorkflowVersionRegistry();

    this.logger = config.logger ?? console;
    this.auth = new Auth(config);
    this.workerAuth = new Auth({ apiKeys: config.remoteWorkers?.apiKeys });
    this.bus = new RunEventBus();
    this.workerWs = new WorkerWebSocketServer({
      authorize: (req) => this.workerAuth.check(req),
    });
    this.agentStreamHub = new AgentStreamHub(this.workerWs);

    const storage: WorkflowStorage = this.workflows.storage;
    if (config.retention) {
      validateWorkflowRetentionConfig(config.retention);
      this.retention = new WorkflowRetentionCleaner(storage, config.retention, this.logger);
    }
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
      namespaces: this.namespaces,
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
      .get(
        "/api/namespaces",
        listNamespaces({
          namespaces: this.namespaces,
          discover: () => storage.distinctNamespaces(),
        }),
      )
      .post("/api/namespaces", createNamespace({ namespaces: this.namespaces }))
      .get("/api/namespaces/:id", getNamespace({ namespaces: this.namespaces }))
      .patch("/api/namespaces/:id", updateNamespace({ namespaces: this.namespaces }))
      .post("/api/namespaces/:id/archive", archiveNamespace({ namespaces: this.namespaces }))
      .delete("/api/namespaces/:id", archiveNamespace({ namespaces: this.namespaces }))
      .get("/api/workflows", listWorkflowNames(deps))
      .get("/api/runs/:id", getRun(deps))
      .get("/api/runs/:id/signals", getRunSignals(storage))
      .get("/api/runs/:id/attempts", getRunAttempts(storage))
      .get("/api/runs/:id/history", getRunHistory(storage))
      .get("/api/runs/:id/children", getRunChildren(storage))
      .get("/api/runs/:id/journal/:stepName", getRunStepJournal(storage))
      .get("/api/signals", listSignals(storage))
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
      // Signal tokens — public-bearer auth for deliverSignal.
      // Mint and list are auth-gated through the regular /api auth layer;
      // /complete is reachable without Zorya credentials and validates the
      // bearer itself.
      .post(
        "/api/runs/:id/signals/:name/token",
        mintSignalToken({
          storage,
          ...(config.publicBaseUrl !== undefined && { publicBaseUrl: config.publicBaseUrl }),
        }),
      )
      .get("/api/runs/:id/signal-tokens", listSignalTokensForRun({ storage }))
      .get("/api/signal-tokens/:tokenId/describe", describeSignalToken({ storage }))
      .post("/api/signal-tokens/:tokenId/complete", completeSignalToken({ storage }))
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
          ...(config.sseIntervalMs !== undefined && { pollIntervalMs: config.sseIntervalMs }),
        }),
      )
      .get("/api/runs/:id/streams/:streamId/chunks", getStreamChunks({ storage }))
      .post("/api/runs/:id/streams/:streamId", sendStreamChunk({ storage }))
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
        namespaces: this.namespaces,
        ...(this.agents.instances !== undefined && { instanceRegistry: this.agents.instances }),
        ...(this.agents.roles !== undefined && { roles: this.agents.roles }),
        ...(this.agents.turnGate !== undefined && { turnGate: this.agents.turnGate }),
        ...(this.agents.workerId !== undefined && { workerId: this.agents.workerId }),
        ...(this.secrets !== undefined && { secrets: this.secrets }),
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
        .get(
          "/api/agents/_catalog/retrievers",
          this.agents.retrievers
            ? listCatalogRetrievers({ retrievers: this.agents.retrievers })
            : async () => new Response(JSON.stringify({ retrievers: [] }), { status: 200 }),
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
        // also writes through); falls back to a registry passed inline on
        // ZoryaAgents for legacy hosts that haven't moved to the service yet.
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
        .post("/api/agents/_draft", createDraft({ registry: this.agents.registry }))
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
          namespaces: this.namespaces,
          registry: this.agents.registry,
          ...((this.fragments?.registry ?? this.agents.fragments) && {
            fragments: this.fragments?.registry ?? this.agents.fragments,
          }),
        };
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

      if (this.agents.roles) {
        const roleDeps = { registry: this.agents.roles };
        this.router
          .get("/api/roles", listRoles(roleDeps))
          .post("/api/roles", createRole(roleDeps))
          .get("/api/roles/:id", getRole(roleDeps))
          .patch("/api/roles/:id", updateRole(roleDeps))
          .delete("/api/roles/:id", deleteRole(roleDeps))
          .get("/api/roles/:id/versions", listRoleVersions(roleDeps));
      }

      if (this.agents.retrievers) {
        this.router
          .get("/api/knowledge-bases", listKnowledgeBases({ retrievers: this.agents.retrievers }))
          .post(
            "/api/knowledge-bases/:id/search",
            searchKnowledgeBase({ retrievers: this.agents.retrievers }),
          );
      }
    }

    if (this.skills) {
      const skills = this.skills;
      const skillDeps = { registry: skills.registry };
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
      const sec = { secrets: this.secrets, namespaces: this.namespaces };
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
        namespaces: this.namespaces,
      };
      this.router.post("/webhooks/:source/:agentId", ingestWebhook(webhookDeps));
    }

    if (config.remoteDeployments && this.agents) {
      const remoteDeps: RemoteDeploymentsGatewayDeps = {
        registry: config.remoteDeployments,
        agents: this.agents.registry,
      };
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
      const schDeps = { storage: sch, namespaces: this.namespaces };
      const sched = this.scheduler;
      this.router
        .get("/api/schedules", listSchedules(schDeps))
        .post("/api/schedules", createSchedule(schDeps))
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

    // Public-bearer signal token completion — bearer-validated by the
    // route itself, no Zorya API key needed. Skip the global auth gate
    // for the consume endpoint only; mint + list stay auth-gated.
    const isPublicSignalTokenComplete =
      req.method === "POST" && /^\/api\/signal-tokens\/[^/]+\/complete$/.test(path);

    if (isWorkerPath) {
      if (!this.workerAuth.check(req)) return jsonError(401, "unauthorized_worker");
    } else if (path.startsWith("/api/") && !isPublicSignalTokenComplete) {
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
    this.retention?.start();

    // Fire and forget — services own their own logging on errors.
    void Promise.all([
      this.workflows.start(),
      this.scheduler?.start(),
      this.agents?.start(),
      this.skills?.start(),
      this.fragments?.start(),
      this.namespaces.ensureDefaultNamespace(),
    ]).catch((err) => this.logger.error("[zorya] service start error:", err));

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
    this.retention?.stop();
    await Promise.all([
      this.workflows.stop(),
      this.scheduler?.stop(),
      this.agents?.stop(),
      this.skills?.stop(),
      this.fragments?.stop(),
    ]).catch(() => {});
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
