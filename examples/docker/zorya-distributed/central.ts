import { createOutboundFetch, resolveRemoteAgent, type RegisteredAgent } from "@promin/agent";
import { QueuedWorkflows, ZoryaAgents, ZoryaServer } from "@promin/zorya";
import { createSqliteZoryaStack } from "@promin/zorya/sqlite";

const port = Number(process.env.PORT ?? 4100);
const apiKey = required("ZORYA_API_KEY");
const workerApiKey = required("ZORYA_WORKER_API_KEY");
const allowedAgentHosts = (process.env.REMOTE_AGENT_HOSTS ?? "agent-one,agent-two")
  .split(",")
  .map((host) => host.trim())
  .filter(Boolean);

const stack = createSqliteZoryaStack({
  path: process.env.ZORYA_DB ?? "/data/zorya.db",
  secretsPassphrase: required("PROMIN_SECRETS_PASSPHRASE"),
});

const workflows = new QueuedWorkflows({
  storage: stack.storage,
  workflowStarts: stack.workflowStarts,
  advertisements: stack.advertisements,
  workerRegistry: stack.workerRegistry,
  acceptAny: false,
});

// Private-network access is deliberately limited to the two Compose service
// names. Production deployments should use their own service discovery names
// or public TLS endpoints, and keep the default private-network denial when
// agents do not live in the same trusted network.
const remoteAgentPolicy = {
  allowPrivateNetworks: true,
  allowedHosts: allowedAgentHosts,
};
const remoteAgentFetch = createOutboundFetch({ policy: remoteAgentPolicy, maxRedirects: 0 });

const agents = new ZoryaAgents({
  registry: stack.agentRegistry,
  memory: stack.memoryStore,
  approvals: stack.pendingApprovalStore,
  instances: stack.instanceRegistry,
  roles: stack.roleRegistry,
  resolve: (recipe: RegisteredAgent) => {
    if (recipe.backend.type !== "remote") {
      throw new Error(`The coordinator only accepts remote agents; got ${recipe.id}.`);
    }
    return resolveRemoteAgent({ agent: recipe, fetch: remoteAgentFetch });
  },
});

const server = new ZoryaServer({
  workflows,
  agents,
  apiKeys: [apiKey],
  remoteWorkers: { apiKeys: [workerApiKey] },
  remoteDeployments: {
    registry: stack.remoteDeploymentRegistry,
    endpointPolicy: remoteAgentPolicy,
    sweepIntervalMs: 5_000,
  },
});

const listening = server.listen({ port });
await server.ready();
console.log(`[central] ready on http://${listening.hostname}:${listening.port}`);
console.log(`[central] allowed remote-agent hosts: ${allowedAgentHosts.join(", ")}`);

let stopping = false;
async function stop(): Promise<void> {
  if (stopping) return;
  stopping = true;
  console.log("[central] stopping");
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
