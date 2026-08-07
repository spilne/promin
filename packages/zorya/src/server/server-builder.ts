import type { RemoteDeploymentRegistry, SecretsStorage } from "@promin/agent";
import type { IWorkflowVersionRegistry } from "@promin/workflow";
import {
  ZoryaServer,
  type ListenOptions,
  type Logger,
  type RemoteWorkersConfig,
} from "./server.ts";
import type { NamespaceRegistry } from "./services/namespaces.ts";
import type { MetricsProvider } from "./routes/metrics.ts";
import type { WorkersProvider } from "./routes/workers.ts";
import type { WebhookSourceConfig } from "./routes/webhooks.ts";
import type { ZoryaWorkflows } from "./services/workflows/index.ts";
import type { ZoryaScheduler } from "./services/scheduler/index.ts";
import type { ZoryaAgents } from "./services/agents/index.ts";
import type { ZoryaSkills } from "./services/skills/index.ts";
import type { ZoryaFragments } from "./services/fragments/index.ts";
import type { ZoryaDags } from "./services/dags/index.ts";

/**
 * Fluent builder for assembling a `ZoryaServer`.
 *
 * Use this when the object literal constructor starts to read like wiring
 * noise. The builder keeps the same underlying `ZoryaServerConfig` contract:
 * `workflows()` is required, everything else opt-in mounts an API surface or
 * dashboard data source.
 *
 * @example
 * ```ts
 * const server = createZoryaServerBuilder()
 *   .workflows(workflows)
 *   .scheduler(scheduler)
 *   .agents(agents)
 *   .remoteWorkers()
 *   .sampleInput((name) => sampleInputFor(name))
 *   .build();
 *
 * server.listen({ port: 4100 });
 * ```
 */
export class ZoryaServerBuilder {
  private config: Partial<ConstructorParameters<typeof ZoryaServer>[0]> = {};

  /** Required workflow service. Powers runs, definitions, metrics, and trigger routes. */
  workflows(workflows: ZoryaWorkflows): this {
    this.config.workflows = workflows;
    return this;
  }

  /** Mount schedule CRUD and start the embedded scheduler loop on `listen()`. */
  scheduler(scheduler: ZoryaScheduler): this {
    this.config.scheduler = scheduler;
    return this;
  }

  /** Mount the agent gateway, agent catalog, memory, roles, and thread routes. */
  agents(agents: ZoryaAgents): this {
    this.config.agents = agents;
    return this;
  }

  /** Mount skill registry CRUD and skill catalog routes. */
  skills(skills: ZoryaSkills): this {
    this.config.skills = skills;
    return this;
  }

  /** Mount prompt-fragment CRUD and fragment catalog routes. */
  fragments(fragments: ZoryaFragments): this {
    this.config.fragments = fragments;
    return this;
  }

  /** Mount DAG registry and DAG execution routes. */
  dags(dags: ZoryaDags): this {
    this.config.dags = dags;
    return this;
  }

  /** Use a durable namespace registry instead of the server's in-memory default. */
  namespaces(namespaces: NamespaceRegistry): this {
    this.config.namespaces = namespaces;
    return this;
  }

  /** Mount BYOK/secrets CRUD and make the vault available to agent resolution. */
  secrets(secrets: SecretsStorage): this {
    this.config.secrets = secrets;
    return this;
  }

  /** Mount remote worker RPC/advertisement endpoints. Pass keys to auth-gate worker traffic. */
  remoteWorkers(config: RemoteWorkersConfig = {}): this {
    this.config.remoteWorkers = config;
    return this;
  }

  /** Override dashboard metrics. Defaults to metrics derived from workflow storage. */
  metrics(metrics: MetricsProvider): this {
    this.config.metrics = metrics;
    return this;
  }

  /** Override `/api/workers`. Useful when the workflow service cannot expose worker state itself. */
  workers(workers: WorkersProvider): this {
    this.config.workers = workers;
    return this;
  }

  /** Provide default trigger-form input for local and advertised workflows. */
  sampleInput(sampleInput: (workflowName: string) => unknown): this {
    this.config.sampleInput = sampleInput;
    return this;
  }

  /** Route server/service diagnostics through a host logger. Defaults to `console`. */
  logger(logger: Logger): this {
    this.config.logger = logger;
    return this;
  }

  /** Serve the compiled dashboard UI from this directory. */
  uiDir(uiDir: string): this {
    this.config.uiDir = uiDir;
    return this;
  }

  /** Poll interval for run/stream SSE endpoints. */
  sseIntervalMs(sseIntervalMs: number): this {
    this.config.sseIntervalMs = sseIntervalMs;
    return this;
  }

  /** Registry used by workflow version promote/rollback routes and active-version dispatch. */
  versionRegistry(versionRegistry: IWorkflowVersionRegistry): this {
    this.config.versionRegistry = versionRegistry;
    return this;
  }

  /** Public URL used when minting external signal completion links. */
  publicBaseUrl(publicBaseUrl: string): this {
    this.config.publicBaseUrl = publicBaseUrl;
    return this;
  }

  /** Mount webhook ingress routes for the provided source definitions. */
  webhooks(sources: Readonly<Record<string, WebhookSourceConfig>>): this {
    this.config.webhooks = { sources };
    return this;
  }

  /** Mount remote deployment discovery routes backed by this registry. */
  remoteDeployments(remoteDeployments: RemoteDeploymentRegistry): this {
    this.config.remoteDeployments = remoteDeployments;
    return this;
  }

  /** Protect regular `/api/*` routes with bearer/API-key auth. */
  apiKeys(apiKeys: ReadonlyArray<string>): this {
    this.config.apiKeys = apiKeys;
    return this;
  }

  /** Build the server without listening. Throws if `workflows()` was not supplied. */
  build(): ZoryaServer {
    if (!this.config.workflows) {
      throw new Error("ZoryaServerBuilder.build: workflows() is required");
    }
    return new ZoryaServer(this.config as ConstructorParameters<typeof ZoryaServer>[0]);
  }

  /** Convenience for `builder.build().listen(opts)`. */
  listen(opts?: ListenOptions): ReturnType<ZoryaServer["listen"]> {
    return this.build().listen(opts);
  }
}

/** Start a fluent `ZoryaServer` configuration chain. */
export function createZoryaServerBuilder(): ZoryaServerBuilder {
  return new ZoryaServerBuilder();
}
