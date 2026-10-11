// ---------------------------------------------------------------------------
// ZoryaServer configuration: the constructor config, `listen()` options,
// the logger contract, and the defaults the server applies.
// ---------------------------------------------------------------------------

import type { OutboundPolicy, RemoteDeploymentRegistry, SecretsStorage } from "@promin/agent";
import type { NamespaceRegistry, WallClock } from "@promin/workflow";
import type { WorkerRegistry } from "@promin/workflow/distributed";
import type { AuthConfig } from "./auth.ts";
import type { MetricsProvider } from "./routes/metrics.ts";
import type { WebhookDeliveryStore, WebhookSourceConfig } from "./routes/webhooks.ts";
import type { WorkersProvider } from "./routes/workers.ts";
import type { ZoryaAgents } from "./services/agents/index.ts";
import type { ZoryaDags } from "./services/dags/index.ts";
import type { ZoryaFragments } from "./services/fragments/index.ts";
import type { ZoryaKnowledgeBases } from "./services/knowledge/index.ts";
import type { ZoryaWorkflowBuilder } from "./services/workflow-builder/index.ts";
import type { ZoryaScheduler } from "./services/scheduler/index.ts";
import type { ZoryaSkills } from "./services/skills/index.ts";
import type { ZoryaWorkflows } from "./services/workflows/index.ts";
import type { RunRetentionConfig } from "./services/workflows/run-retention.ts";

export interface Logger {
  log(message: string, ...args: unknown[]): void;
  warn(message: string, ...args: unknown[]): void;
  error(message: string, ...args: unknown[]): void;
}

/**
 * Mounts the remote-worker surface: `/rpc/storage` (the runner's storage
 * methods, for workers that drive runs), `/rpc/worker` (the worker
 * registry), and — when the workflows chain has a `QueuedWorkflows` —
 * `/api/advertisements` and `/api/worker-protocol/*` over its
 * advertisements and start queue.
 */
export interface RemoteWorkersConfig {
  /**
   * Worker API keys for `/ws/worker`, `/rpc/worker`, `/rpc/storage` (scoped
   * to what a runner needs), `/api/advertisements` and
   * `/api/worker-protocol/*`. Required unless the server is `insecure`.
   */
  apiKeys?: ReadonlyArray<string>;
  /**
   * The worker registry `/rpc/worker` writes and the Workers page reads.
   * Default: the chain's `QueuedWorkflows.workerRegistry`, else an
   * in-memory one.
   */
  workerRegistry?: WorkerRegistry;
  /**
   * "Work available" messages over `/ws/worker`: when the queued layer
   * makes a start claimable, about as many connected workers as there are
   * starts (round-robin among those that advertise the workflow and
   * version) are told to claim now instead of at their next poll. Notices
   * within `coalesceMs` (default 20) of a send are merged into one; a start
   * still unclaimed `followUpMs` (default 250) after its wake-up wakes
   * every matching worker. Only workers connected to this instance are
   * told; in a cluster the others find the start at their next poll.
   * `false` turns it off. Default: on.
   */
  wakeups?: false | { coalesceMs?: number; followUpMs?: number };
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
   * Optional managed knowledge bases. Mounts /api/knowledge-bases/* (the
   * dashboard's Knowledge page) and starts with the server. Pass its
   * `registry` as `ZoryaAgents.retrievers` so recipes can bind the bases.
   */
  knowledgeBases?: ZoryaKnowledgeBases;
  /**
   * Optional visual workflow builder. Mounts /api/workflow-builder/* (the
   * dashboard's Builder page) and starts with the server, which recompiles
   * and serves every published builder workflow. Its `workflows` layer must
   * be part of `workflows`.
   */
  workflowBuilder?: ZoryaWorkflowBuilder;
  /**
   * Mount the remote-worker HTTP surface. Pulls stepQueue / workerRegistry /
   * advertisements / workflowStarts from the workflows service.
   */
  remoteWorkers?: RemoteWorkersConfig;

  /** Override metrics provider. Default: StorageMetricsProvider(workflows.storage). */
  metrics?: MetricsProvider;
  /**
   * Plug a workers provider for the Workers page. Default: the worker
   * registry remote workers register into (`workerRegistry`).
   */
  workers?: WorkersProvider;
  /**
   * Sample input for the dashboard's trigger form, by workflow name. For
   * builder workflows it is inferred from their input schema when this
   * returns `undefined`.
   */
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
   * Public-facing base URL for token completion callbacks. When set, the
   * `POST /api/runs/:id/signals/:name/token` mint route returns
   * `url: "<publicBaseUrl>/api/signal-tokens/:tokenId/complete"` so external
   * completers don't have to compose it themselves. Absent → `url: null`.
   */
  publicBaseUrl?: string;
  /**
   * Optional scoped SecretsStorage (BYOK / MCP credentialRef / per-tenant
   * LLM keys). When set, exposes `/api/secrets/*` HTTP CRUD and is
   * available to the agent gateway for setup-time credential resolution.
   * When unset, the secrets surface is not mounted — fine for
   * single-tenant deployments using env vars.
   */
  secrets?: SecretsStorage;
  /**
   * Optional namespace registry (`SqliteNamespaceRegistry`,
   * `PostgresNamespaceRegistry`, `InMemoryNamespaceRegistry`). When set,
   * `/api/namespaces` manages it, and namespaces stop being free-form:
   * triggering a run, creating a schedule, invoking an agent, writing
   * namespace memory, secrets or agent instances, and accepting a webhook
   * delivery in a namespace that is not registered answer 404
   * `namespace_not_found`, in an archived one 409 `namespace_archived`.
   * Reads are unaffected, and work without a namespace (the global
   * namespace) is always allowed. At start the server registers the
   * namespaces stored runs already use. Without it, any well-formed
   * namespace is accepted and `/api/namespaces` reports `configured: false`.
   */
  namespaces?: NamespaceRegistry;
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
    /**
     * How often the server looks for deliveries a crashed dispatcher left
     * unfinished and runs them again (also once at start). Default 60 s.
     */
    recoveryIntervalMs?: number;
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
    endpointPolicy?: OutboundPolicy;
    sweepIntervalMs?: number;
  };
  /**
   * Delete ended runs (completed, failed, tripwire) older than
   * `maxAgeMs`, with everything stored for them (steps, signals, run
   * history, journal, attempts, signal tokens, streams). A leader-elected
   * loop: with a `cluster` on the workflows chain only one instance
   * sweeps. Every `intervalMs` (default one hour, the first sweep one
   * interval after start) it deletes in batches of `batchSize` (default
   * 500) until no expired run is left. Unset: runs are kept forever.
   */
  retention?: RunRetentionConfig;
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
