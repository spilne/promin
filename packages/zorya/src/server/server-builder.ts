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

export class ZoryaServerBuilder {
  private config: Partial<ConstructorParameters<typeof ZoryaServer>[0]> = {};

  workflows(workflows: ZoryaWorkflows): this {
    this.config.workflows = workflows;
    return this;
  }

  scheduler(scheduler: ZoryaScheduler): this {
    this.config.scheduler = scheduler;
    return this;
  }

  agents(agents: ZoryaAgents): this {
    this.config.agents = agents;
    return this;
  }

  skills(skills: ZoryaSkills): this {
    this.config.skills = skills;
    return this;
  }

  fragments(fragments: ZoryaFragments): this {
    this.config.fragments = fragments;
    return this;
  }

  dags(dags: ZoryaDags): this {
    this.config.dags = dags;
    return this;
  }

  namespaces(namespaces: NamespaceRegistry): this {
    this.config.namespaces = namespaces;
    return this;
  }

  secrets(secrets: SecretsStorage): this {
    this.config.secrets = secrets;
    return this;
  }

  remoteWorkers(config: RemoteWorkersConfig = {}): this {
    this.config.remoteWorkers = config;
    return this;
  }

  metrics(metrics: MetricsProvider): this {
    this.config.metrics = metrics;
    return this;
  }

  workers(workers: WorkersProvider): this {
    this.config.workers = workers;
    return this;
  }

  sampleInput(sampleInput: (workflowName: string) => unknown): this {
    this.config.sampleInput = sampleInput;
    return this;
  }

  logger(logger: Logger): this {
    this.config.logger = logger;
    return this;
  }

  uiDir(uiDir: string): this {
    this.config.uiDir = uiDir;
    return this;
  }

  sseIntervalMs(sseIntervalMs: number): this {
    this.config.sseIntervalMs = sseIntervalMs;
    return this;
  }

  versionRegistry(versionRegistry: IWorkflowVersionRegistry): this {
    this.config.versionRegistry = versionRegistry;
    return this;
  }

  publicBaseUrl(publicBaseUrl: string): this {
    this.config.publicBaseUrl = publicBaseUrl;
    return this;
  }

  webhooks(sources: Readonly<Record<string, WebhookSourceConfig>>): this {
    this.config.webhooks = { sources };
    return this;
  }

  remoteDeployments(remoteDeployments: RemoteDeploymentRegistry): this {
    this.config.remoteDeployments = remoteDeployments;
    return this;
  }

  apiKeys(apiKeys: ReadonlyArray<string>): this {
    this.config.apiKeys = apiKeys;
    return this;
  }

  build(): ZoryaServer {
    if (!this.config.workflows) {
      throw new Error("ZoryaServerBuilder.build: workflows() is required");
    }
    return new ZoryaServer(this.config as ConstructorParameters<typeof ZoryaServer>[0]);
  }

  listen(opts?: ListenOptions): ReturnType<ZoryaServer["listen"]> {
    return this.build().listen(opts);
  }
}

export function createZoryaServerBuilder(): ZoryaServerBuilder {
  return new ZoryaServerBuilder();
}
