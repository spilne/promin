// ---------------------------------------------------------------------------
// Shared harness for agent-gateway route tests: a real ZoryaServer with
// LocalAgents over an in-memory memory store and a scripted LLM.
// ---------------------------------------------------------------------------

import {
  AgentTurnGate,
  InMemoryAgentInstanceRegistry,
  InMemoryAgentRegistry,
  InMemoryLeaseStore,
  InMemoryMemoryStore,
  InMemoryRoleRegistry,
  InMemorySecretsStorage,
  resolveLocalAgent,
  type LLMProvider,
  type RegisteredAgent,
} from "@promin/agent";
import { FakeWallClock, InMemoryWorkflowStorage, createWorkflowRunner } from "@promin/workflow";
import { LocalWorkflows, ZoryaAgents } from "../../../index.ts";
import { ZoryaServer } from "../../server.ts";
import type { ScopeAuthorizer } from "../agent-gateway-kit.ts";

export const localBackend = {
  type: "local" as const,
  model: { provider: "anthropic", id: "claude-sonnet-4-6" },
  role: { inline: { systemPrompt: "Helpful", tools: [] as string[] } },
};

export interface HarnessOpts {
  /** Reply text; a function makes the LLM throw / vary per call. */
  reply?: () => string;
  turnGate?: boolean;
  secrets?: boolean;
  authorizeScope?: ScopeAuthorizer;
  /** Wrap the resolved agent (e.g. to make `thread()` throw). */
  wrapAgent?: (agent: ReturnType<typeof resolveLocalAgent>) => unknown;
}

export async function bootAgents(opts: HarnessOpts = {}) {
  const clock = FakeWallClock.create("2026-01-01T00:00:00Z");
  const storage = new InMemoryWorkflowStorage();
  const runner = createWorkflowRunner({ storage });
  const memory = new InMemoryMemoryStore();
  const registry = new InMemoryAgentRegistry({ clock });
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
  const llm: LLMProvider = {
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
    resolve: (recipe: RegisteredAgent) => {
      const agent = resolveLocalAgent(recipe, { runner, memory, llm: () => llm, tools: {} });
      return (opts.wrapAgent ? opts.wrapAgent(agent) : agent) as never;
    },
    memory,
    instances,
    roles,
    ...(turnGate && { turnGate, workerId: "w-1" }),
    ...(opts.authorizeScope && { authorizeScope: opts.authorizeScope }),
  });
  const server = new ZoryaServer({
    insecure: true,
    workflows,
    agents,
    clock,
    ...(secrets && { secrets }),
  });
  return { server, registry, memory, instances, roles, secrets, leaseStore, clock, storage };
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
