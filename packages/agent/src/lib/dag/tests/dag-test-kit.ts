import {
  InMemoryWorkflowStorage,
  createWorkflowRunner,
  type Workflow,
  type WorkflowRunner,
} from "@promin/workflow";
import type { Agent, AgentInput, AgentRunOutput } from "../../agent/types.ts";
import type { TurnBudget } from "../../budget/budget.ts";
import type { LLMProvider } from "../../llm-provider.ts";
import type { Message } from "../../message.ts";
import { resolveLocalAgent } from "../../registry/resolve-local-agent.ts";
import type { AgentTool } from "../../tool.ts";
import { InMemoryAgentRegistry } from "../../versioned-registry/in-memory-registries.ts";
import { InMemoryDagRegistry } from "../../versioned-registry/in-memory-registries.ts";
import {
  createDagWorkflow,
  type CreateDagWorkflowConfig,
  type DurableDagInput,
  type DurableDagOutput,
} from "../durable-executor.ts";
import { createRegistryAgentResolver } from "../registry-agent-resolver.ts";
import type { AgentResolver, AgenticDagRecipe, DagRunScope } from "../types.ts";

export const SCOPE: DagRunScope = { namespaceId: "acme", resourceId: "r-1" };

async function* emptyStream<T>(): AsyncGenerator<T> {}

export function textOnly(text: string): AgentRunOutput<unknown> {
  return {
    textStream: emptyStream<string>(),
    fullStream: emptyStream(),
    text: Promise.resolve(text),
    output: Promise.resolve(undefined),
    toolCalls: Promise.resolve([]),
    toolResults: Promise.resolve([]),
    steps: Promise.resolve([]),
    usage: Promise.resolve({ inputTokens: 0, outputTokens: 0 }),
    finishReason: Promise.resolve("stop"),
    messages: Promise.resolve([]),
    cancel: async () => {},
  };
}

export function stubAgent(transform: (task: string) => string = (task) => task): Agent {
  const agent: Agent = {
    invoke: async (input: AgentInput) => textOnly(transform(input.task)),
    stream: (input: AgentInput) => textOnly(transform(input.task)),
    thread: async () => {
      throw new Error("stub: thread");
    },
    listThreads: async () => [],
    compactThread: async () => {
      throw new Error("stub: compactThread");
    },
    distillThread: async () => {
      throw new Error("stub: distillThread");
    },
    withScope() {
      return this;
    },
  };
  return agent;
}

export function invokeAgent(
  invoke: (input: AgentInput) => Promise<AgentRunOutput<unknown>>,
): Agent {
  const agent: Agent = {
    invoke,
    stream: () => {
      throw new Error("stub: stream");
    },
    thread: async () => {
      throw new Error("stub: thread");
    },
    listThreads: async () => [],
    compactThread: async () => {
      throw new Error("stub: compactThread");
    },
    distillThread: async () => {
      throw new Error("stub: distillThread");
    },
    withScope: () => agent,
  };
  return agent;
}

export function makeResolver(byId: Record<string, Agent>): AgentResolver {
  return async ({ agentId }) => {
    const agent = byId[agentId];
    if (!agent) throw new Error(`unknown agent: ${agentId}`);
    return agent;
  };
}

export function taskOf(messages: ReadonlyArray<Message>): string {
  const last = messages.at(-1);
  return typeof last?.content === "string" ? last.content : "";
}

export async function runDag(params: {
  dag: AgenticDagRecipe;
  resolver: AgentResolver;
  initialInput?: Record<string, unknown>;
  workflowId?: string;
  budget?: TurnBudget;
  extraDags?: ReadonlyArray<AgenticDagRecipe>;
  storage?: InMemoryWorkflowStorage;
  config?: Partial<CreateDagWorkflowConfig>;
}): Promise<{
  result: DurableDagOutput;
  storage: InMemoryWorkflowStorage;
  registry: InMemoryDagRegistry;
  runner: WorkflowRunner;
  workflow: Workflow<DurableDagInput, DurableDagOutput>;
}> {
  const storage = params.storage ?? new InMemoryWorkflowStorage();
  const registry = new InMemoryDagRegistry();
  for (const dag of params.extraDags ?? []) await registry.register(dag);
  await registry.register(params.dag);
  const runner = createWorkflowRunner({ storage });
  const workflow = createDagWorkflow({
    ...params.config,
    registry,
    resolver: params.resolver,
  });
  const result = await runner.run({
    workflow,
    workflowId: params.workflowId ?? `dag-${params.dag.id}`,
    input: {
      dagId: params.dag.id,
      version: params.dag.version,
      initialInput: params.initialInput ?? {},
      scope: SCOPE,
      ...(params.budget === undefined ? {} : { budget: params.budget }),
    },
  });
  return { result, storage, registry, runner, workflow };
}

export async function localAgents(params: {
  llms: Record<string, LLMProvider>;
  storage?: InMemoryWorkflowStorage;
  tools?: Record<string, Record<string, AgentTool>>;
}): Promise<{
  storage: InMemoryWorkflowStorage;
  boot(): { runner: WorkflowRunner; resolver: AgentResolver };
}> {
  const storage = params.storage ?? new InMemoryWorkflowStorage();
  const registry = new InMemoryAgentRegistry();
  for (const id of Object.keys(params.llms)) {
    await registry.register({
      id,
      backend: {
        type: "local",
        model: { provider: "test", id },
        role: { inline: { systemPrompt: null, tools: [] } },
      },
    });
  }
  return {
    storage,
    boot() {
      const runner = createWorkflowRunner({ storage });
      const resolver = createRegistryAgentResolver({
        registry,
        resolve: (recipe) =>
          resolveLocalAgent(recipe, {
            runner,
            llm: () => params.llms[recipe.id]!,
            tools: params.tools?.[recipe.id] ?? {},
          }),
      });
      return { runner, resolver };
    },
  };
}
