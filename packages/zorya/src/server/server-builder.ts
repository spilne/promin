// ---------------------------------------------------------------------------
// ZoryaServerBuilder — fluent assembly of a `ZoryaServerConfig`, started
// with `ZoryaServer.builder()`.
// ---------------------------------------------------------------------------

import type { SecretsStorage } from "@promin/agent";
import type { NamespaceRegistry, WallClock } from "@promin/workflow";
import type { MetricsProvider } from "./routes/metrics.ts";
import type { WorkersProvider } from "./routes/workers.ts";
import { ZoryaServer } from "./server.ts";
import type {
  ListenOptions,
  Logger,
  RemoteWorkersConfig,
  ZoryaServerConfig,
} from "./server-config.ts";
import type { ZoryaAgents } from "./services/agents/index.ts";
import type { ZoryaDags } from "./services/dags/index.ts";
import type { ZoryaEvals } from "./services/evals/index.ts";
import type { ZoryaFragments } from "./services/fragments/index.ts";
import type { ZoryaKnowledgeBases } from "./services/knowledge/index.ts";
import type { ZoryaMcpServers } from "./services/mcp/index.ts";
import type { ZoryaScheduler } from "./services/scheduler/index.ts";
import type { ZoryaSkills } from "./services/skills/index.ts";
import type { ZoryaWorkflowBuilder } from "./services/workflow-builder/index.ts";
import type { RunRetentionConfig, ZoryaWorkflows } from "./services/workflows/index.ts";

/**
 * Fluent builder for a `ZoryaServer`; start one with `ZoryaServer.builder()`.
 * Each setter sets the `ZoryaServerConfig` field of the same name (see there
 * for what it mounts and its default). `workflows()` is required; `build()`
 * checks it, and the `ZoryaServer` constructor then checks the keys (see
 * `insecure`).
 *
 * ```ts
 * const server = ZoryaServer.builder()
 *   .workflows(workflows)
 *   .scheduler(scheduler)
 *   .agents(agents)
 *   .apiKeys([process.env.ZORYA_API_KEY!])
 *   .remoteWorkers({ apiKeys: [process.env.ZORYA_WORKER_API_KEY!] })
 *   .sampleInput((name) => sampleInputFor(name))
 *   .build();
 *
 * server.listen({ port: 4100 });
 * ```
 */
export class ZoryaServerBuilder {
  private config: Partial<ZoryaServerConfig> = {};

  /** Required. Workflow execution and state: runs, definitions, triggers, metrics. */
  workflows(workflows: ZoryaWorkflows): this {
    return this.set({ workflows });
  }

  /** Schedule CRUD and the scheduler loop (started by `listen()`). */
  scheduler(scheduler: ZoryaScheduler): this {
    return this.set({ scheduler });
  }

  /** The agent gateway: agents, memory, roles and threads. */
  agents(agents: ZoryaAgents): this {
    return this.set({ agents });
  }

  /** Skill registry CRUD and the agent editor's skill catalog. */
  skills(skills: ZoryaSkills): this {
    return this.set({ skills });
  }

  /** Prompt-fragment registry CRUD and the fragment catalog. */
  fragments(fragments: ZoryaFragments): this {
    return this.set({ fragments });
  }

  /** DAG registry and DAG execution routes. */
  dags(dags: ZoryaDags): this {
    return this.set({ dags });
  }

  /** Eval suites, durable eval runs and promotion gates (`/api/evals/*`). */
  evals(evals: ZoryaEvals): this {
    return this.set({ evals });
  }

  /**
   * The remote-worker surface (`/rpc/*`, `/ws/worker`, advertisements, the
   * worker protocol). Without `apiKeys` it needs `insecure()`.
   */
  remoteWorkers(remoteWorkers: RemoteWorkersConfig = {}): this {
    return this.set({ remoteWorkers });
  }

  /** Operator API keys for the dashboard and `/api`. */
  apiKeys(apiKeys: ReadonlyArray<string>): this {
    return this.set({ apiKeys });
  }

  /** Allow planes without keys. Local development only. */
  insecure(insecure = true): this {
    return this.set({ insecure });
  }

  /** Override the dashboard metrics. Default: derived from workflow storage. */
  metrics(metrics: MetricsProvider): this {
    return this.set({ metrics });
  }

  /** Override the Workers page source. Default: the worker registry. */
  workers(workers: WorkersProvider): this {
    return this.set({ workers });
  }

  /** Sample input for the dashboard's trigger form, by workflow name. */
  sampleInput(sampleInput: (workflowName: string) => unknown): this {
    return this.set({ sampleInput });
  }

  /** Server and service diagnostics. Default: `console`. */
  logger(logger: Logger): this {
    return this.set({ logger });
  }

  /** Serve the compiled dashboard UI from this directory. */
  uiDir(uiDir: string): this {
    return this.set({ uiDir });
  }

  /** SSE poll interval (ms). Default 1000. */
  sseIntervalMs(sseIntervalMs: number): this {
    return this.set({ sseIntervalMs });
  }

  /** Longest validity a minted signal token may ask for (ms). */
  signalTokenMaxTtlMs(signalTokenMaxTtlMs: number): this {
    return this.set({ signalTokenMaxTtlMs });
  }

  /** Public base URL for signal-token completion links. */
  publicBaseUrl(publicBaseUrl: string): this {
    return this.set({ publicBaseUrl });
  }

  /** Managed knowledge bases: `/api/knowledge-bases/*` and the dashboard's Knowledge page. */
  knowledgeBases(knowledgeBases: ZoryaKnowledgeBases): this {
    return this.set({ knowledgeBases });
  }

  /** The visual workflow builder: `/api/workflow-builder/*` and the dashboard's Builder page. */
  workflowBuilder(workflowBuilder: ZoryaWorkflowBuilder): this {
    return this.set({ workflowBuilder });
  }

  /** MCP allowlist and client pool recipes attach servers from. */
  mcp(mcp: ZoryaMcpServers): this {
    return this.set({ mcp });
  }

  /** Secrets vault: `/api/secrets` CRUD and agent credential resolution. */
  secrets(secrets: SecretsStorage): this {
    return this.set({ secrets });
  }

  /** Namespace registry: `/api/namespaces`, and only registered, active namespaces take new work. */
  namespaces(namespaces: NamespaceRegistry): this {
    return this.set({ namespaces });
  }

  /** Webhook ingress into agents (needs `agents()`). */
  webhooks(webhooks: NonNullable<ZoryaServerConfig["webhooks"]>): this {
    return this.set({ webhooks });
  }

  /** Remote-deployment self-registration (needs `agents()`). */
  remoteDeployments(remoteDeployments: NonNullable<ZoryaServerConfig["remoteDeployments"]>): this {
    return this.set({ remoteDeployments });
  }

  /** Retention of ended runs: a leader-elected sweep deletes runs older than `maxAgeMs`. */
  retention(retention: RunRetentionConfig): this {
    return this.set({ retention });
  }

  /** Time source for the server's own time math. Default: `SystemWallClock`. */
  clock(clock: WallClock): this {
    return this.set({ clock });
  }

  /** Bound on the `/api/ready` storage probe (ms). */
  readinessTimeoutMs(readinessTimeoutMs: number): this {
    return this.set({ readinessTimeoutMs });
  }

  /** How long `stop()` waits for in-flight requests (ms). */
  shutdownTimeoutMs(shutdownTimeoutMs: number): this {
    return this.set({ shutdownTimeoutMs });
  }

  /** Backoff for retrying a service whose `start()` failed. */
  startRetry(startRetry: NonNullable<ZoryaServerConfig["startRetry"]>): this {
    return this.set({ startRetry });
  }

  /** The config built so far. Throws if `workflows()` is missing. */
  buildConfig(): ZoryaServerConfig {
    const { workflows } = this.config;
    if (!workflows) throw new Error("ZoryaServer.builder(): workflows() is required");
    return { ...this.config, workflows };
  }

  /** Build the server without listening. Throws if `workflows()` is missing. */
  build(): ZoryaServer {
    return new ZoryaServer(this.buildConfig());
  }

  /** `build().listen(options)`. */
  listen(options?: ListenOptions): ReturnType<ZoryaServer["listen"]> {
    return this.build().listen(options);
  }

  private set(fields: Partial<ZoryaServerConfig>): this {
    this.config = { ...this.config, ...fields };
    return this;
  }
}
