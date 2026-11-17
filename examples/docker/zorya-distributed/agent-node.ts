import { resolveLocalAgent, type RegisteredAgent } from "@promin/agent";
import { echoLLM } from "@promin/agent/testing";
import { LocalWorkflows, ZoryaAgents, ZoryaServer } from "@promin/zorya";
import { createSqliteZoryaStack } from "@promin/zorya/sqlite";

interface Registration {
  readonly deploymentId: string;
  readonly heartbeatPath: string;
  readonly unregisterPath: string;
  readonly ttlMs: number;
}

const agentId = required("AGENT_ID");
const agentApiKey = required("AGENT_API_KEY");
const coordinatorUrl = process.env.ZORYA_URL ?? "http://central:4100";
const coordinatorApiKey = required("ZORYA_API_KEY");
const publicUrl = required("AGENT_PUBLIC_URL").replace(/\/+$/, "");
const port = Number(process.env.PORT ?? 4200);
const stack = createSqliteZoryaStack({ path: process.env.ZORYA_DB ?? "/data/agent.db" });

if (!(await stack.agentRegistry.get({ id: agentId }))) {
  await stack.agentRegistry.register({
    id: agentId,
    version: "v1",
    backend: {
      type: "local",
      model: { provider: "mock", id: "echo-v1" },
      role: {
        inline: {
          systemPrompt: `You are ${agentId}, a deterministic Docker example agent.`,
          tools: [],
        },
      },
    },
    metadata: {
      description: `Distributed agent hosted by ${publicUrl}`,
      capabilities: ["chat"],
      tags: ["docker", "distributed"],
    },
  });
}

const llm = echoLLM({ template: `${agentId} on ${publicUrl} heard: {task}` });
const workflows = new LocalWorkflows({
  storage: stack.storage,
  runner: stack.runner,
  definitions: {},
  sleepScanIntervalMs: 0,
});
const agents = new ZoryaAgents({
  registry: stack.agentRegistry,
  memory: stack.memoryStore,
  approvals: stack.pendingApprovalStore,
  instances: stack.instanceRegistry,
  roles: stack.roleRegistry,
  resolve: (recipe: RegisteredAgent) =>
    resolveLocalAgent(recipe, {
      runner: stack.runner,
      memory: stack.memoryStore,
      approvals: stack.pendingApprovalStore,
      llm: () => llm,
      tools: {},
    }),
});
const server = new ZoryaServer({ workflows, agents, apiKeys: [agentApiKey] });
const listening = server.listen({ port });
await server.ready();

let registration: Registration | undefined;
let heartbeatTimer: ReturnType<typeof setInterval> | undefined;
let stopping = false;

await registerUntilReady();
console.log(`[${agentId}] ready on ${publicUrl} (local port ${listening.port})`);

async function registerUntilReady(): Promise<void> {
  while (!stopping) {
    try {
      registration = await register();
      scheduleHeartbeat(registration);
      console.log(`[${agentId}] registered as deployment ${registration.deploymentId}`);
      return;
    } catch (error) {
      console.warn(`[${agentId}] registration failed; retrying`, messageOf(error));
      await Bun.sleep(1_000);
    }
  }
}

async function register(): Promise<Registration> {
  const response = await coordinatorFetch("/api/remote-deployments/register", {
    method: "POST",
    body: JSON.stringify({
      endpoint: publicUrl,
      agents: [agentId],
      auth: { kind: "bearer", token: agentApiKey },
      ttlMs: 15_000,
    }),
  });
  if (!response.ok) throw new Error(`register: ${response.status} ${await response.text()}`);
  return (await response.json()) as Registration;
}

function scheduleHeartbeat(current: Registration): void {
  if (heartbeatTimer) clearInterval(heartbeatTimer);
  heartbeatTimer = setInterval(() => void heartbeat(current), Math.floor(current.ttlMs / 3));
}

async function heartbeat(current: Registration): Promise<void> {
  if (stopping || registration?.deploymentId !== current.deploymentId) return;
  try {
    const response = await coordinatorFetch(current.heartbeatPath, { method: "POST" });
    if (response.status === 410) {
      registration = undefined;
      if (heartbeatTimer) clearInterval(heartbeatTimer);
      await registerUntilReady();
      return;
    }
    if (!response.ok) throw new Error(`${response.status} ${await response.text()}`);
  } catch (error) {
    console.warn(`[${agentId}] heartbeat failed; will retry`, messageOf(error));
  }
}

async function coordinatorFetch(path: string, init: RequestInit): Promise<Response> {
  return fetch(`${coordinatorUrl}${path}`, {
    ...init,
    headers: {
      authorization: `Bearer ${coordinatorApiKey}`,
      "content-type": "application/json",
      ...init.headers,
    },
  });
}

async function stop(): Promise<void> {
  if (stopping) return;
  stopping = true;
  if (heartbeatTimer) clearInterval(heartbeatTimer);
  if (registration) {
    await coordinatorFetch(registration.unregisterPath, { method: "DELETE" }).catch(() => {});
  }
  await server.stop();
  stack.db.close();
}

process.on("SIGINT", () => void stop());
process.on("SIGTERM", () => void stop());

function required(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`${name} is required`);
  return value;
}

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
