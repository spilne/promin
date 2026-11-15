import { afterEach, describe, expect, it } from "bun:test";
import { FakeWallClock, InMemoryWorkflowStorage, createWorkflowRunner } from "@promin/workflow";
import { Auth } from "../../auth.ts";
import { json, type Router } from "../../router.ts";
import { ZoryaInsecureConfigError, type Logger } from "../../server-config.ts";
import { ZoryaServer } from "../../server.ts";
import { LocalWorkflows } from "../../services/workflows/local-workflows.ts";
import {
  OPERATOR_PRINCIPAL,
  createApiKeySecurity,
  principalRef,
  requestPrincipal,
  type Principal,
  type ZoryaSecurity,
} from "../index.ts";

const API_KEY = "operator-key";
const WORKER_KEY = "worker-key";
const quietLogger: Logger = { log() {}, warn() {}, error() {} };

function workflows(): LocalWorkflows {
  const storage = new InMemoryWorkflowStorage();
  return new LocalWorkflows({
    storage,
    runner: createWorkflowRunner({ storage }),
    definitions: {},
    sleepScanIntervalMs: 0,
    signalScanIntervalMs: 0,
  });
}

function bearer(key: string): { authorization: string } {
  return { authorization: `Bearer ${key}` };
}

function userPrincipal(): Principal {
  return {
    kind: "user",
    id: "user-1",
    email: "user@example.com",
    accessRoles: { acme: ["member"] },
    canActFor: false,
  };
}

function fakeSecurity(
  params: {
    readonly authenticate?: (req: Request) => Promise<Principal | undefined>;
    readonly mount?: (router: Router) => void;
    readonly start?: () => Promise<void>;
    readonly stop?: () => Promise<void>;
    readonly authorize?: ZoryaSecurity["authorize"];
    readonly recordAudit?: ZoryaSecurity["recordAudit"];
  } = {},
): ZoryaSecurity {
  return {
    authenticate: async ({ request }) =>
      params.authenticate?.(request) ?? Promise.resolve(userPrincipal()),
    resolvePrincipal: async () => undefined,
    resolveIdentity: async ({ actor }) => ({ actor, effective: principalRef(actor) }),
    identityFilter: async ({ namespaceId }) => ({ kind: "namespace", namespaceId }),
    authorize: params.authorize ?? (async () => ({ allowed: true, reason: "access_role_allow" })),
    recordAudit: params.recordAudit ?? (async () => {}),
    ...(params.mount !== undefined && { mount: params.mount }),
    ...(params.start !== undefined && { start: params.start }),
    ...(params.stop !== undefined && { stop: params.stop }),
  };
}

const servers: ZoryaServer[] = [];
afterEach(async () => {
  await Promise.all(servers.splice(0).map((server) => server.stop()));
});

describe("ZoryaServer security", () => {
  it("API keys: a request without the key answers 401, with it 200", async () => {
    const server = new ZoryaServer({
      workflows: workflows(),
      apiKeys: [API_KEY],
      logger: quietLogger,
    });

    expect((await server.handle(new Request("http://zorya/api/namespaces"))).status).toBe(401);
    expect(
      (
        await server.handle(
          new Request("http://zorya/api/namespaces", { headers: bearer(API_KEY) }),
        )
      ).status,
    ).toBe(200);
  });

  it("API keys: the principal of an authenticated request is the operator", async () => {
    const security = createApiKeySecurity({
      operatorAuth: new Auth({ apiKeys: [API_KEY] }),
      workerAuth: new Auth({ apiKeys: [WORKER_KEY] }),
    });
    const authenticated = new Request("http://zorya/api/namespaces", {
      headers: bearer(API_KEY),
    });

    expect(await security.authenticate({ request: authenticated, plane: "operator" })).toEqual(
      OPERATOR_PRINCIPAL,
    );
    expect(
      await security.authenticate({
        request: new Request("http://zorya/api/namespaces"),
        plane: "operator",
      }),
    ).toBeUndefined();
  });

  it("module: authenticate decides; undefined answers 401", async () => {
    let principal: Principal | undefined;
    const security = fakeSecurity({
      authenticate: async () => principal,
      mount: (router) => {
        router.get("/api/whoami", (req) => json(200, requestPrincipal(req)));
      },
    });
    const server = new ZoryaServer({ workflows: workflows(), security, logger: quietLogger });

    expect((await server.handle(new Request("http://zorya/api/whoami"))).status).toBe(401);
    principal = userPrincipal();
    const response = await server.handle(new Request("http://zorya/api/whoami"));
    expect(response.status).toBe(200);
    expect(await response.json()).toEqual(principal);
  });

  it("module: worker plane and probes do not call authenticate", async () => {
    let calls = 0;
    const server = new ZoryaServer({
      workflows: workflows(),
      security: fakeSecurity({
        authenticate: async () => {
          calls += 1;
          return userPrincipal();
        },
      }),
      remoteWorkers: { apiKeys: [WORKER_KEY] },
      logger: quietLogger,
    });

    expect((await server.handle(new Request("http://zorya/api/health"))).status).toBe(200);
    expect((await server.handle(new Request("http://zorya/api/advertisements"))).status).toBe(401);
    expect(calls).toBe(0);
  });

  it("module with apiKeys throws ZoryaInsecureConfigError", () => {
    expect(
      () =>
        new ZoryaServer({
          workflows: workflows(),
          security: fakeSecurity(),
          apiKeys: [API_KEY],
          logger: quietLogger,
        }),
    ).toThrow(ZoryaInsecureConfigError);
  });

  it("module: mount adds routes", async () => {
    const server = new ZoryaServer({
      workflows: workflows(),
      security: fakeSecurity({
        mount: (router) => router.get("/auth/ping", () => json(200, "pong")),
      }),
      logger: quietLogger,
    });

    const response = await server.handle(new Request("http://zorya/auth/ping"));
    expect(response.status).toBe(200);
    expect(await response.json()).toBe("pong");
  });

  it("module: declared protected routes use central authorization and audit", async () => {
    const events: string[] = [];
    const security = fakeSecurity({
      authorize: async ({ resource }) => {
        events.push("authorize:" + resource.id);
        return { allowed: true, reason: "access_role_allow" };
      },
      recordAudit: async ({ decision }) => {
        events.push("audit:" + decision?.reason);
      },
      mount: (router) => {
        router.route({
          method: "GET",
          path: "/api/declared-agent/:id",
          access: {
            kind: "protected",
            permission: { resource: "agent", action: "read" },
            resolveTarget: ({ params }) => ({
              resource: { kind: "agent", namespaceId: "acme", id: params.id },
            }),
          },
          handler: () => {
            events.push("handler");
            return new Response(null, { status: 204 });
          },
        });
      },
    });
    const server = new ZoryaServer({ workflows: workflows(), security, logger: quietLogger });

    expect(
      (await server.handle(new Request("http://zorya/api/declared-agent/agent-1"))).status,
    ).toBe(204);
    expect(events).toEqual(["authorize:agent-1", "audit:access_role_allow", "handler"]);
  });

  it("module: a rejecting start keeps startup unready", async () => {
    const clock = FakeWallClock.create(0);
    const server = new ZoryaServer({
      workflows: workflows(),
      security: fakeSecurity({
        start: async () => Promise.reject(new Error("security unavailable")),
      }),
      clock,
      logger: quietLogger,
    });
    servers.push(server);

    server.start();
    for (let i = 0; i < 10; i++) await Promise.resolve();
    expect(server.serviceStatuses().find((status) => status.name === "security")).toMatchObject({
      state: "retrying",
      attempts: 1,
      lastError: "security unavailable",
    });
    const response = await server.handle(new Request("http://zorya/api/ready"));
    expect(response.status).toBe(503);
  });
});
