// ---------------------------------------------------------------------------
// Shared harness for agent-gateway route tests: a real ZoryaServer with
// LocalAgents over an in-memory memory store and a scripted LLM.
// ---------------------------------------------------------------------------

import {
  AgentTurnGate,
  InMemoryAgentInstanceRegistry,
  InMemoryAgentRegistry,
  InMemoryDagRegistry,
  InMemoryLeaseStore,
  InMemoryMemoryStore,
  InMemoryPendingApprovalStore,
  InMemoryRoleRegistry,
  InMemorySecretsStorage,
  resolveLocalAgent,
  type ResolveLocalAgentDeps,
  type LLMProvider,
  type RegisteredAgent,
} from "@promin/agent";
import {
  FakeWallClock,
  InMemoryWorkflowStorage,
  createWorkflowRunner,
  type NamespaceRegistry,
} from "@promin/workflow";
import { LocalWorkflows, ZoryaAgents, ZoryaDags } from "../../../index.ts";
import { ZoryaServer } from "../../server.ts";
import type { ScopeAuthorizer } from "../agent-gateway-kit.ts";
import type { TurnStreamLimits } from "../agents/agent-sse.ts";

export const localBackend = {
  type: "local" as const,
  model: { provider: "anthropic", id: "claude-sonnet-4-6" },
  role: { inline: { systemPrompt: "Helpful", tools: [] as string[] } },
};

export interface HarnessOpts {
  /** Reply text; a function makes the LLM throw / vary per call. */
  reply?: () => string;
  /** Replaces the reply-based LLM entirely. */
  llm?: LLMProvider;
  /** Host tools recipes may pick by name. */
  tools?: ResolveLocalAgentDeps["tools"];
  /** `true`: a gate over the test lease store; `false`: no gate at all; unset: the default gate. */
  turnGate?: boolean;
  secrets?: boolean;
  authorizeScope?: ScopeAuthorizer;
  /** Wrap the resolved agent (e.g. to make `thread()` throw). */
  wrapAgent?: (agent: Awaited<ReturnType<typeof resolveLocalAgent>>) => unknown;
  /** Mount the DAG gateway over the same agents. */
  dags?: boolean;
  /** Namespace registry the server checks agent writes against. */
  namespaces?: NamespaceRegistry;
  /** Bounds of the agent routes' event streams. */
  streaming?: TurnStreamLimits;
  /** The agent registry (default: a fresh in-memory one). */
  registry?: InMemoryAgentRegistry;
}

export async function bootAgents(opts: HarnessOpts = {}) {
  const clock = FakeWallClock.create("2026-01-01T00:00:00Z");
  const storage = new InMemoryWorkflowStorage();
  const runner = createWorkflowRunner({ storage });
  const memory = new InMemoryMemoryStore();
  const approvals = new InMemoryPendingApprovalStore({ clock });
  const registry = opts.registry ?? new InMemoryAgentRegistry({ clock });
  const instances = new InMemoryAgentInstanceRegistry();
  const roles = new InMemoryRoleRegistry();
  const secrets = opts.secrets ? new InMemorySecretsStorage() : undefined;
  const leaseStore = new InMemoryLeaseStore({ now: () => clock.currentTimeMs() });
  const turnGate = opts.turnGate
    ? new AgentTurnGate({ leaseStore, defaultTtlMs: 60_000 })
    : undefined;
  await registry.register({
    id: "support",
    backend: localBackend,
    metadata: { description: "d", capabilities: ["chat"], tags: ["beta"] },
  });
  await registry.register({ id: "other", backend: localBackend });
  const llm: LLMProvider = opts.llm ?? {
    chat: async () => ({ content: opts.reply ? opts.reply() : "ok", finishReason: "stop" }),
  };
  const workflows = new LocalWorkflows({
    storage,
    runner,
    definitions: {},
    sleepScanIntervalMs: 0,
  });
  const agents = new ZoryaAgents({
    registry,
    resolve: async (recipe: RegisteredAgent) => {
      const agent = await resolveLocalAgent(recipe, {
        runner,
        memory,
        approvals,
        llm: () => llm,
        tools: opts.tools ?? {},
      });
      return (opts.wrapAgent ? opts.wrapAgent(agent) : agent) as never;
    },
    memory,
    instances,
    roles,
    ...(turnGate && { turnGate, workerId: "w-1" }),
    ...(opts.turnGate === false && { turnGate: false as const }),
    ...(opts.authorizeScope && { authorizeScope: opts.authorizeScope }),
    ...(opts.streaming && { streaming: opts.streaming }),
  });
  const dags = opts.dags
    ? new ZoryaDags({ registry: new InMemoryDagRegistry({ clock }), workflows, agents })
    : undefined;
  const server = new ZoryaServer({
    insecure: true,
    workflows,
    agents,
    clock,
    ...(secrets && { secrets }),
    ...(dags && { dags }),
    ...(opts.namespaces && { namespaces: opts.namespaces }),
  });
  return {
    server,
    registry,
    memory,
    approvals,
    instances,
    roles,
    secrets,
    dags,
    workflows,
    leaseStore,
    clock,
    storage,
  };
}

export function req(
  method: string,
  path: string,
  body?: unknown,
  headers: Record<string, string> = {},
) {
  return new Request(`http://test${path}`, {
    method,
    headers: { "content-type": "application/json", ...headers },
    ...(body !== undefined && { body: JSON.stringify(body) }),
  });
}

export async function jsonOf<T>(res: Response): Promise<T> {
  return (await res.json()) as T;
}
