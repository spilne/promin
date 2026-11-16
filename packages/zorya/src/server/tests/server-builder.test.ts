import { afterEach, describe, expect, it } from "bun:test";
import type { SecretsStorage } from "@promin/agent";
import {
  FakeWallClock,
  InMemoryNamespaceRegistry,
  InMemoryWorkflowStorage,
  createWorkflowRunner,
} from "@promin/workflow";
import type { MetricsProvider } from "../routes/metrics.ts";
import type { WorkersProvider } from "../routes/workers.ts";
import { ZoryaServerBuilder } from "../server-builder.ts";
import { ZoryaInsecureConfigError, type Logger, type ZoryaServerConfig } from "../server-config.ts";
import { ZoryaServer } from "../server.ts";
import type { RetainedSubagents, ZoryaAgents } from "../services/agents/index.ts";
import type { ZoryaDags } from "../services/dags/index.ts";
import type { ZoryaEvals } from "../services/evals/index.ts";
import type { ZoryaKnowledgeBases } from "../services/knowledge/index.ts";
import type { ZoryaMcpServers } from "../services/mcp/index.ts";
import type { ZoryaFragments } from "../services/fragments/index.ts";
import type { ZoryaScheduler } from "../services/scheduler/index.ts";
import type { ZoryaSkills } from "../services/skills/index.ts";
import type { ZoryaWorkflowBuilder } from "../services/workflow-builder/index.ts";
import { LocalWorkflows } from "../services/workflows/local-workflows.ts";
import type { ZoryaSecurity } from "../security/index.ts";
import type {
  ZoryaChannel,
  ZoryaLlmPolicy,
  ZoryaRetrieverExtension,
  ZoryaRoutesExtension,
} from "../extensions.ts";

const quietLogger: Logger = { log() {}, warn() {}, error() {} };

function localWorkflows(): LocalWorkflows {
  const storage = new InMemoryWorkflowStorage();
  return new LocalWorkflows({
    storage,
    runner: createWorkflowRunner({ storage }),
    definitions: {},
    sleepScanIntervalMs: 0,
    signalScanIntervalMs: 0,
  });
}

/** A distinct object standing in for a service the builder only stores. */
const stub = <T>(label: string): T => ({ label }) as unknown as T;

let stopListening: (() => Promise<void>) | undefined;
afterEach(async () => {
  await stopListening?.();
  stopListening = undefined;
});

describe("ZoryaServer.builder()", () => {
  it("returns a ZoryaServerBuilder", () => {
    expect(ZoryaServer.builder()).toBeInstanceOf(ZoryaServerBuilder);
  });

  it("requires workflows", () => {
    expect(() => ZoryaServer.builder().insecure().build()).toThrow("workflows() is required");
    expect(() => ZoryaServer.builder().buildConfig()).toThrow("workflows() is required");
  });

  it("sets every config field", () => {
    const workflows = localWorkflows();
    const channel: ZoryaChannel = { id: "http" };
    const retriever: ZoryaRetrieverExtension = {
      id: "external",
      retriever: stub("retriever"),
    };
    const extensionRoutes: ZoryaRoutesExtension = () => {};
    const llmPolicy: ZoryaLlmPolicy = {
      authorize: () => ({ allowed: true, reason: "test" }),
    };
    const expected: Required<ZoryaServerConfig> = {
      apiKeys: ["operator-key"],
      security: stub<ZoryaSecurity>("security"),
      channels: [channel],
      retrievers: [retriever],
      extensionRoutes: [extensionRoutes],
      llmPolicy,
      insecure: false,
      workflows,
      scheduler: stub<ZoryaScheduler>("scheduler"),
      agents: stub<ZoryaAgents>("agents"),
      retainedSubagents: stub<RetainedSubagents>("retainedSubagents"),
      skills: stub<ZoryaSkills>("skills"),
      fragments: stub<ZoryaFragments>("fragments"),
      dags: stub<ZoryaDags>("dags"),
      evals: stub<ZoryaEvals>("evals"),
      remoteWorkers: { apiKeys: ["worker-key"], wakeups: false },
      metrics: stub<MetricsProvider>("metrics"),
      workers: stub<WorkersProvider>("workers"),
      sampleInput: (name) => ({ name }),
      logger: quietLogger,
      uiDir: "/srv/ui",
      sseIntervalMs: 250,
      signalTokenMaxTtlMs: 60_000,
      publicBaseUrl: "https://zorya.example.com",
      secrets: stub<SecretsStorage>("secrets"),
      knowledgeBases: stub<ZoryaKnowledgeBases>("knowledgeBases"),
      mcp: stub<ZoryaMcpServers>("mcp"),
      namespaces: new InMemoryNamespaceRegistry(),
      workflowBuilder: stub<ZoryaWorkflowBuilder>("workflowBuilder"),
      webhooks: { sources: { github: stub("github-source") } },
      remoteDeployments: { registry: stub("registry"), sweepIntervalMs: 1_000 },
      retention: { maxAgeMs: 86_400_000, intervalMs: 60_000, batchSize: 100 },
      clock: FakeWallClock.create(0),
      readinessTimeoutMs: 500,
      shutdownTimeoutMs: 1_000,
      startRetry: { initialDelayMs: 10, maxDelayMs: 100 },
    };

    const config = ZoryaServer.builder()
      .apiKeys(expected.apiKeys)
      .security(expected.security)
      .channel(channel)
      .retriever(retriever)
      .routes(extensionRoutes)
      .llmPolicy(llmPolicy)
      .insecure(expected.insecure)
      .workflows(expected.workflows)
      .scheduler(expected.scheduler)
      .agents(expected.agents)
      .retainedSubagents(expected.retainedSubagents)
      .skills(expected.skills)
      .fragments(expected.fragments)
      .dags(expected.dags)
      .evals(expected.evals)
      .remoteWorkers(expected.remoteWorkers)
      .metrics(expected.metrics)
      .workers(expected.workers)
      .sampleInput(expected.sampleInput)
      .logger(expected.logger)
      .uiDir(expected.uiDir)
      .sseIntervalMs(expected.sseIntervalMs)
      .signalTokenMaxTtlMs(expected.signalTokenMaxTtlMs)
      .publicBaseUrl(expected.publicBaseUrl)
      .secrets(expected.secrets)
      .knowledgeBases(expected.knowledgeBases)
      .mcp(expected.mcp)
      .namespaces(expected.namespaces)
      .workflowBuilder(expected.workflowBuilder)
      .webhooks(expected.webhooks)
      .remoteDeployments(expected.remoteDeployments)
      .retention(expected.retention)
      .clock(expected.clock)
      .readinessTimeoutMs(expected.readinessTimeoutMs)
      .shutdownTimeoutMs(expected.shutdownTimeoutMs)
      .startRetry(expected.startRetry)
      .buildConfig();

    expect(config).toEqual(expected);
    // Every field of the config has a setter.
    expect(Object.keys(config).sort()).toEqual(Object.keys(expected).sort());
  });

  it("defaults the flag setters", () => {
    const config = ZoryaServer.builder()
      .workflows(localWorkflows())
      .insecure()
      .remoteWorkers()
      .buildConfig();
    expect(config.insecure).toBe(true);
    expect(config.remoteWorkers).toEqual({});
  });

  it("composes synchronous extension bundles and rolls back a failed bundle", () => {
    const builder = ZoryaServer.builder().workflows(localWorkflows()).insecure();
    builder.use((target) => target.channel({ id: "http" }).routes(() => {}));
    expect(builder.buildConfig().channels?.map((channel) => channel.id)).toEqual(["http"]);
    expect(() =>
      builder.use((target) => {
        target.channel({ id: "temporary" });
        throw new Error("bundle failed");
      }),
    ).toThrow("bundle failed");
    expect(builder.buildConfig().channels?.map((channel) => channel.id)).toEqual(["http"]);
  });

  it("rejects duplicate extensions and user-mapping channels without security", () => {
    const builder = ZoryaServer.builder().workflows(localWorkflows()).insecure();
    builder.channel({ id: "http" });
    expect(() => builder.channel({ id: "http" })).toThrow("duplicate channel");
    builder.retriever({ id: "docs", retriever: stub("one") });
    expect(() => builder.retriever({ id: "docs", retriever: stub("two") })).toThrow(
      "duplicate retriever",
    );
    expect(() =>
      ZoryaServer.builder()
        .workflows(localWorkflows())
        .insecure()
        .channel({ id: "slack", mapsExternalUsers: true })
        .buildConfig(),
    ).toThrow("requires security()");
    expect(() =>
      ZoryaServer.builder()
        .workflows(localWorkflows())
        .security(stub<ZoryaSecurity>("security"))
        .channel({ id: "slack", mapsExternalUsers: true })
        .buildConfig(),
    ).toThrow("requires a channel runtime");
  });

  it("leaves the key checks to the server", () => {
    expect(() => ZoryaServer.builder().workflows(localWorkflows()).build()).toThrow(
      ZoryaInsecureConfigError,
    );
  });

  it("mounts extension routes behind the server's normal authentication and access guard", async () => {
    const server = ZoryaServer.builder()
      .workflows(localWorkflows())
      .apiKeys(["operator-key"])
      .logger(quietLogger)
      .routes((routes) =>
        routes.route({
          method: "GET",
          path: "/api/extensions/proof",
          access: {
            kind: "protected",
            permission: { resource: "agent", action: "read" },
            resolveTarget: () => ({
              resource: { kind: "agent", namespaceId: "org", id: "proof" },
            }),
          },
          handler: () => Response.json({ ok: true }),
        }),
      )
      .build();
    expect(
      (await server.handle(new Request("http://zorya.test/api/extensions/proof"))).status,
    ).toBe(401);
    const response = await server.handle(
      new Request("http://zorya.test/api/extensions/proof", {
        headers: { authorization: "Bearer operator-key" },
      }),
    );
    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({ ok: true });
  });

  it("refuses insecure or conflicting extension routes while mounting", () => {
    expect(() =>
      ZoryaServer.builder()
        .workflows(localWorkflows())
        .insecure()
        .logger(quietLogger)
        .routes((routes) =>
          routes.route({
            method: "GET",
            path: "/api/extensions/public",
            access: { kind: "public", purpose: "lifecycle" },
            handler: () => new Response(),
          } as never),
        )
        .build(),
    ).toThrow("protected action and resource");
    expect(() =>
      ZoryaServer.builder()
        .workflows(localWorkflows())
        .insecure()
        .logger(quietLogger)
        .routes((routes) =>
          routes.route({
            method: "GET",
            path: "/api/health",
            access: {
              kind: "protected",
              permission: { resource: "agent", action: "read" },
              resolveTarget: () => ({
                resource: { kind: "agent", namespaceId: "org", id: "proof" },
              }),
            },
            handler: () => new Response(),
          }),
        )
        .build(),
    ).toThrow("conflicts with an existing route");
  });

  it("starts and stops channel extensions with the server lifecycle", async () => {
    const events: string[] = [];
    const runtime = { handle: async () => ({ kind: "duplicate" as const, correlationId: "test" }) };
    const server = ZoryaServer.builder()
      .workflows(localWorkflows())
      .insecure()
      .logger(quietLogger)
      .channel({
        id: "http",
        runtime,
        start: async (context) => {
          expect(context.runtime).toBe(runtime);
          events.push("start");
        },
        stop: async () => void events.push("stop"),
      })
      .build();
    const handle = server.listen({ port: 0, hostname: "127.0.0.1" });
    stopListening = handle.stop;
    await server.ready();
    expect(events).toEqual(["start"]);
    await handle.stop();
    stopListening = undefined;
    expect(events).toEqual(["start", "stop"]);
  });

  it("builds and listens", async () => {
    const builder = ZoryaServer.builder()
      .workflows(localWorkflows())
      .insecure()
      .logger(quietLogger);
    expect(builder.build()).toBeInstanceOf(ZoryaServer);

    const handle = builder.listen({ port: 0, hostname: "127.0.0.1" });
    stopListening = handle.stop;
    const res = await fetch(`http://127.0.0.1:${handle.port}/api/health`);
    expect(res.status).toBe(200);
  });
});
