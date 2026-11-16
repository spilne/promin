import { afterAll, afterEach, beforeAll, describe, expect, it } from "bun:test";
import {
  InMemoryAgentRegistry,
  InMemoryMemoryStore,
  InMemoryPendingApprovalStore,
  InMemoryRetriever,
  InMemoryRetrieverRegistry,
  resolveLocalAgent,
  type EmbeddingProvider,
  type RegisteredAgent,
} from "@promin/agent";
import {
  LocalWorkflows,
  ORG_WIDE_NAMESPACE,
  ZoryaAgents,
  ZoryaKnowledgeBases,
  ZoryaServer,
} from "../../index.ts";
import { createSqliteZoryaStack } from "../../server/sqlite-stack.ts";
import { InMemorySecurityStore } from "../in-memory-security-store.ts";
import { oidcLoginProvider } from "../oidc-login-provider.ts";
import type { AccessBinding, SecurityUser } from "../security-store.ts";
import { createZoryaSecurity } from "../zorya-security.ts";
import { startFakeOidcIssuer, type FakeOidcIssuer } from "./fake-oidc-issuer.ts";
import { searchEverythingLlm } from "./search-everything-llm.ts";

const PUBLIC_BASE_URL = "http://zorya.test";
const TEAM = "team-a";
const SECRET = "0123456789abcdef0123456789abcdef";
const TASK = "what are the rules?";
const OPERATOR_KEY = "op-key";
const SERVICE_ACCOUNT_TOKEN = "sa-token";

const searchAllEmbeddings: EmbeddingProvider = {
  embed: async () => [1],
};

let issuer: FakeOidcIssuer;
const environments = new Set<ScenarioEnvironment>();

beforeAll(async () => {
  issuer = await startFakeOidcIssuer();
});

afterEach(async () => {
  await Promise.all([...environments].map((environment) => environment.close()));
  environments.clear();
});

afterAll(async () => {
  await issuer.stop();
});

interface ScenarioEnvironment {
  readonly server: ZoryaServer;
  readonly securityStore?: InMemorySecurityStore;
  close(): Promise<void>;
}

interface ListedUser extends SecurityUser {
  readonly bindings: AccessBinding[];
}

interface InvokeAnswer {
  readonly text: string;
}

async function buildServer(mode: "module" | "api-keys"): Promise<ScenarioEnvironment> {
  const stack = createSqliteZoryaStack({ path: ":memory:" });
  const retrievers = new InMemoryRetrieverRegistry();
  const knowledgeBases = new ZoryaKnowledgeBases({
    registry: retrievers,
    embeddings: searchAllEmbeddings,
    ingestion: { pollIntervalMs: 0 },
    refreshIntervalMs: 0,
  });

  try {
    await stack.namespaceRegistry.create({ id: TEAM });
    await knowledgeBases.create({ namespaceId: TEAM, id: "policies" });
    await knowledgeBases.ingest({
      namespaceId: TEAM,
      baseId: "policies",
      source: {
        id: "travel-policy",
        title: "Travel policy",
        text: "Flights are booked in economy.",
      },
    });
    await waitForReady(knowledgeBases);

    retrievers.register({
      id: "handbook",
      retriever: new InMemoryRetriever({
        embeddings: searchAllEmbeddings,
        documents: [
          {
            id: "handbook",
            title: "Handbook",
            text: "Offices close at 18:00.",
          },
        ],
      }),
    });

    const workflows = new LocalWorkflows({
      storage: stack.storage,
      runner: stack.runner,
      definitions: {},
      sleepScanIntervalMs: 0,
      signalScanIntervalMs: 0,
      orphanRecovery: false,
    });
    const registry = new InMemoryAgentRegistry();
    await registry.register({
      id: "helper",
      backend: {
        type: "local",
        model: { provider: "test", id: "search-everything" },
        role: { inline: { systemPrompt: "Answer from the available knowledge.", tools: [] } },
        knowledge: [{ id: "policies" }, { id: "handbook" }],
      },
    });
    const memory = new InMemoryMemoryStore();
    const approvals = new InMemoryPendingApprovalStore();
    const llm = searchEverythingLlm();
    const agents = new ZoryaAgents({
      registry,
      retrievers,
      memory,
      resolve: async (recipe: RegisteredAgent, scope) => {
        const namespaceId = scope?.namespaceId;
        return resolveLocalAgent(recipe, {
          runner: stack.runner,
          memory,
          approvals,
          llm: () => llm,
          tools: {},
          retrievers,
          ...(namespaceId !== undefined && { namespaceId }),
          ...(scope?.resourceId !== undefined && { resourceId: scope.resourceId }),
        });
      },
    });

    const builder = ZoryaServer.builder()
      .workflows(workflows)
      .agents(agents)
      .knowledgeBases(knowledgeBases)
      .namespaces(stack.namespaceRegistry);
    let securityStore: InMemorySecurityStore | undefined;
    if (mode === "module") {
      securityStore = new InMemorySecurityStore();
      builder.security(
        createZoryaSecurity({
          store: securityStore,
          loginProviders: [
            oidcLoginProvider({
              issuer: issuer.issuer,
              clientId: issuer.clientId,
              clientSecret: issuer.clientSecret,
            }),
          ],
          publicBaseUrl: PUBLIC_BASE_URL,
          allowedDomains: ["example.com"],
          auditSink: { recordAudit: async () => {} },
          adminEmails: ["admin@example.com"],
          serviceAccounts: [{ id: "channel", token: SERVICE_ACCOUNT_TOKEN, canActFor: true }],
          secret: SECRET,
        }),
      );
    } else {
      builder.apiKeys([OPERATOR_KEY]);
    }
    const server = builder.build();

    const environment: ScenarioEnvironment = {
      server,
      ...(securityStore !== undefined && { securityStore }),
      close: async () => {
        await knowledgeBases.stop();
        stack.db.close();
      },
    };
    environments.add(environment);
    return environment;
  } catch (error) {
    await knowledgeBases.stop();
    stack.db.close();
    throw error;
  }
}

async function waitForReady(knowledgeBases: ZoryaKnowledgeBases): Promise<void> {
  for (let attempt = 0; attempt < 100; attempt += 1) {
    const sources = await knowledgeBases.listSources({ namespaceId: TEAM, baseId: "policies" });
    if (sources[0]?.status === "ready") return;
    if (sources[0]?.status === "failed") {
      throw new Error(`policy ingestion failed: ${sources[0].error ?? "unknown error"}`);
    }
    await Bun.sleep(0);
  }
  throw new Error("policy ingestion did not become ready after 100 event-loop yields");
}

async function login(
  environment: ScenarioEnvironment,
  email: string,
): Promise<{ readonly cookie: string }> {
  issuer.nextLogin({ email });
  const start = await environment.server.handle(new Request(`${PUBLIC_BASE_URL}/auth/login/oidc`));
  expect(start.status).toBe(302);
  const stateCookie = cookiePair(start, "zorya_login_state");
  const authorizationUrl = start.headers.get("location");
  if (!stateCookie || !authorizationUrl) throw new Error("OIDC login did not start");

  const authorization = await fetch(authorizationUrl, { redirect: "manual" });
  const callbackUrl = authorization.headers.get("location");
  if (!callbackUrl) throw new Error("OIDC issuer did not return a callback");
  const callback = await environment.server.handle(
    new Request(callbackUrl, { headers: { cookie: stateCookie } }),
  );
  expect(callback.status).toBe(302);
  const cookie = cookiePair(callback, "zorya_session");
  if (!cookie) throw new Error("OIDC callback did not create a session");
  return { cookie };
}

function cookiePair(response: Response, name: string): string | undefined {
  const header = response.headers.get("set-cookie");
  const match = header?.match(new RegExp(`(?:^|[, ]+)${name}=([^;,]*)`));
  return match ? `${name}=${match[1]}` : undefined;
}

function request(params: {
  readonly method?: string;
  readonly path: string;
  readonly body?: unknown;
  readonly cookie?: string;
  readonly bearer?: string;
}): Request {
  return new Request(`${PUBLIC_BASE_URL}${params.path}`, {
    method: params.method ?? "GET",
    headers: {
      ...(params.body !== undefined && { "content-type": "application/json" }),
      ...(params.cookie !== undefined && { cookie: params.cookie }),
      ...(params.bearer !== undefined && { authorization: `Bearer ${params.bearer}` }),
    },
    ...(params.body !== undefined && { body: JSON.stringify(params.body) }),
  });
}

async function listedUsers(
  environment: ScenarioEnvironment,
  adminCookie: string,
): Promise<ListedUser[]> {
  const response = await environment.server.handle(
    request({ path: "/api/access/users", cookie: adminCookie }),
  );
  expect(response.status).toBe(200);
  return ((await response.json()) as { users: ListedUser[] }).users;
}

async function accessMutation(params: {
  readonly environment: ScenarioEnvironment;
  readonly adminCookie: string;
  readonly method?: "PUT" | "DELETE";
  readonly path: string;
  readonly body?: unknown;
}): Promise<void> {
  const response = await params.environment.server.handle(
    request({
      method: params.method ?? "PUT",
      path: params.path,
      cookie: params.adminCookie,
      ...(params.body !== undefined && { body: params.body }),
    }),
  );
  expect(response.status).toBe(204);
}

async function invoke(params: {
  readonly environment: ScenarioEnvironment;
  readonly cookie?: string;
  readonly bearer?: string;
  readonly resourceId?: string;
}): Promise<Response> {
  return params.environment.server.handle(
    request({
      method: "POST",
      path: "/api/agents/helper/invoke",
      ...(params.cookie !== undefined && { cookie: params.cookie }),
      ...(params.bearer !== undefined && { bearer: params.bearer }),
      body: {
        task: TASK,
        namespaceId: TEAM,
        ...(params.resourceId !== undefined && { resourceId: params.resourceId }),
      },
    }),
  );
}

async function invokeText(params: Parameters<typeof invoke>[0]): Promise<string> {
  const response = await invoke(params);
  expect(response.status).toBe(200);
  return ((await response.json()) as InvokeAnswer).text;
}

async function configureGrants(params: {
  readonly environment: ScenarioEnvironment;
  readonly adminCookie: string;
  readonly aliceId?: string;
}): Promise<void> {
  await accessMutation({
    ...params,
    path: "/api/access/knowledge-grants",
    body: { namespaceId: TEAM, baseId: "policies", accessRole: "member" },
  });
  if (params.aliceId !== undefined) {
    await accessMutation({
      ...params,
      path: `/api/access/users/${params.aliceId}/bindings`,
      body: { namespaceId: TEAM, accessRole: "analyst" },
    });
  }
  await accessMutation({
    ...params,
    path: "/api/access/knowledge-grants",
    body: { namespaceId: ORG_WIDE_NAMESPACE, baseId: "handbook", accessRole: "analyst" },
  });
}

describe("enterprise security and knowledge scenario", () => {
  it("two users with different access roles get different cited answers", async () => {
    const environment = await buildServer("module");
    const admin = await login(environment, "admin@example.com");
    const alice = await login(environment, "alice@example.com");
    const bob = await login(environment, "bob@example.com");
    const users = await listedUsers(environment, admin.cookie);
    const aliceUser = users.find((user) => user.email === "alice@example.com");
    const bobUser = users.find((user) => user.email === "bob@example.com");
    if (!aliceUser) throw new Error("Alice was not provisioned");
    if (!bobUser) throw new Error("Bob was not provisioned");
    expect(aliceUser?.bindings).toEqual([
      {
        userId: aliceUser.id,
        namespaceId: ORG_WIDE_NAMESPACE,
        accessRole: "member",
      },
    ]);
    expect(bobUser?.bindings).toEqual([
      {
        userId: bobUser.id,
        namespaceId: ORG_WIDE_NAMESPACE,
        accessRole: "member",
      },
    ]);
    await configureGrants({
      environment,
      adminCookie: admin.cookie,
      aliceId: aliceUser.id,
    });

    const aliceAnswer = await invokeText({ environment, cookie: alice.cookie });
    expect(aliceAnswer).toContain("Flights are booked in economy. [source: Travel policy]");
    expect(aliceAnswer).toContain("Offices close at 18:00. [source: Handbook]");

    const bobAnswer = await invokeText({ environment, cookie: bob.cookie });
    expect(bobAnswer).toContain("Flights are booked in economy. [source: Travel policy]");
    expect(bobAnswer).not.toContain("Handbook");

    await accessMutation({
      environment,
      adminCookie: admin.cookie,
      method: "DELETE",
      path: "/api/access/knowledge-grants",
      body: { namespaceId: ORG_WIDE_NAMESPACE, baseId: "handbook", accessRole: "analyst" },
    });
    const afterRevocation = await invokeText({ environment, cookie: alice.cookie });
    expect(afterRevocation).toContain("Flights are booked in economy. [source: Travel policy]");
    expect(afterRevocation).not.toContain("Handbook");
  });

  it("a user with no grants still gets an answer", async () => {
    const environment = await buildServer("module");
    const bob = await login(environment, "bob@example.com");

    expect(await invokeText({ environment, cookie: bob.cookie })).toBe("No knowledge available.");
  });

  it("a user naming another user in the body is refused", async () => {
    const environment = await buildServer("module");
    const alice = await login(environment, "alice@example.com");
    await login(environment, "bob@example.com");
    const bobUser = (await environment.securityStore?.listUsers())?.find(
      (user) => user.email === "bob@example.com",
    );
    if (!bobUser) throw new Error("Bob was not provisioned");

    const response = await invoke({
      environment,
      cookie: alice.cookie,
      resourceId: bobUser.id,
    });
    expect(response.status).toBe(403);
    expect(await response.json()).toMatchObject({ error: "identity_forbidden" });
  });

  it("a service account acting for a user gets that user's answer", async () => {
    const environment = await buildServer("module");
    const admin = await login(environment, "admin@example.com");
    const bob = await login(environment, "bob@example.com");
    const bobUser = (await environment.securityStore?.listUsers())?.find(
      (user) => user.email === "bob@example.com",
    );
    if (!bobUser) throw new Error("Bob was not provisioned");
    await configureGrants({ environment, adminCookie: admin.cookie });

    const answer = await invokeText({
      environment,
      bearer: SERVICE_ACCOUNT_TOKEN,
      resourceId: bobUser.id,
    });
    expect(answer).toBe(await invokeText({ environment, cookie: bob.cookie }));
    expect(answer).toContain("Flights are booked in economy. [source: Travel policy]");
    expect(answer).not.toContain("Handbook");
  });

  it("a request with no session answers 401", async () => {
    const environment = await buildServer("module");

    const response = await invoke({ environment });
    expect(response.status).toBe(401);
    expect(await response.json()).toMatchObject({ error: "unauthorized" });
  });

  it("the same server on API keys answers from both bases", async () => {
    const environment = await buildServer("api-keys");

    const answer = await invokeText({
      environment,
      bearer: OPERATOR_KEY,
      resourceId: "anyone",
    });
    expect(answer).toContain("Flights are booked in economy. [source: Travel policy]");
    expect(answer).toContain("Offices close at 18:00. [source: Handbook]");

    const unauthenticated = await invoke({ environment, resourceId: "anyone" });
    expect(unauthenticated.status).toBe(401);
    expect(await unauthenticated.json()).toMatchObject({ error: "unauthorized" });
  });
});
