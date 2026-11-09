import {
  InMemoryAgentRegistry,
  InMemoryMemoryStore,
  resolveLocalAgent,
  type LLMProvider,
  type MemoryStore,
} from "@promin/agent";
import {
  InMemoryWorkflowStorage,
  createWorkflowRunner,
  type WorkflowStorage,
} from "@promin/workflow";
import { LocalWorkflows } from "../../workflows/local-workflows.ts";
import { RetainedSubagents, type RetainedParent } from "../retained-subagents.ts";
import { ZoryaAgents } from "../zorya-agents.ts";
import { retainedSubagentTools, RETAINED_SUBAGENT_TOOL_NAMES } from "../retained-subagent-tools.ts";

export const parent: RetainedParent = {
  namespaceId: "acme",
  resourceId: "alice",
  agentId: "parent",
  threadId: "conversation",
};

export interface HarnessOptions {
  readonly retained?: Partial<ConstructorParameters<typeof RetainedSubagents>[0]>;
  readonly wrapStorage?: (storage: InMemoryWorkflowStorage) => InMemoryWorkflowStorage;
  readonly chat?: (content: string) => string;
  /** Async model reply; `signal` aborts when the turn is interrupted. */
  readonly chatAsync?: (content: string, signal: AbortSignal | undefined) => Promise<string>;
  readonly turnGate?: false;
  /** Full control of the worker model's reply; wins over `chat` / `chatAsync`. */
  readonly rawChat?: LLMProvider["chat"];
  /** Real stores (e.g. Postgres) instead of the in-memory ones. */
  readonly stores?: { readonly storage: WorkflowStorage; readonly memory: MemoryStore };
  /** Model of the "orchestrator" recipe, which gets the retained subagent tools. */
  readonly orchestratorLlm?: LLMProvider;
  /** Input tokens each model reply reports. */
  readonly replyTokens?: number;
}

/** Overrides one storage method, delegating everything else to the real store. */
export function overrideStorage<K extends keyof InMemoryWorkflowStorage>(
  storage: InMemoryWorkflowStorage,
  method: K,
  replacement: (original: InMemoryWorkflowStorage[K]) => InMemoryWorkflowStorage[K],
): InMemoryWorkflowStorage {
  return new Proxy(storage, {
    get(target, prop) {
      const value = Reflect.get(target, prop, target);
      if (prop === method) return replacement((value as Function).bind(target) as never);
      return typeof value === "function" ? value.bind(target) : value;
    },
  });
}

export async function harness(options: HarnessOptions = {}) {
  // A real store stands in under the in-memory type; only overrideStorage needs the class.
  const baseStorage = (options.stores?.storage ??
    new InMemoryWorkflowStorage()) as InMemoryWorkflowStorage;
  const storage = options.wrapStorage ? options.wrapStorage(baseStorage) : baseStorage;
  const memory = options.stores?.memory ?? new InMemoryMemoryStore();
  const registry = new InMemoryAgentRegistry();
  const backend = {
    type: "local" as const,
    model: { provider: "test", id: "test" },
    role: { inline: { systemPrompt: "help", tools: [] as string[] } },
  };
  await registry.register({ id: "parent", backend });
  await registry.register({ id: "worker", backend });
  await registry.register({
    id: "orchestrator",
    backend: {
      ...backend,
      role: { inline: { systemPrompt: "delegate", tools: [...RETAINED_SUBAGENT_TOOL_NAMES] } },
    },
  });
  let retainedRef: RetainedSubagents | undefined;
  let runner = createWorkflowRunner({ storage });
  const make = () => {
    runner = createWorkflowRunner({ storage });
    const workflows = new LocalWorkflows({
      storage,
      runner,
      definitions: {},
      signalScanIntervalMs: 10,
      sleepScanIntervalMs: 10,
      orphanRecovery: false,
    });
    const agents = new ZoryaAgents({
      registry,
      memory,
      ...(options.turnGate === false && { turnGate: false as const }),
      resolve: (recipe) =>
        resolveLocalAgent(recipe, {
          runner,
          memory,
          tools:
            recipe.id === "orchestrator" && retainedRef ? retainedSubagentTools(retainedRef) : {},
          llm: () =>
            recipe.id === "orchestrator" && options.orchestratorLlm
              ? options.orchestratorLlm
              : {
                  chat: async (request) => {
                    if (options.rawChat) return options.rawChat(request);
                    const last = String(
                      request.messages.findLast((m) => m.role === "user")?.content,
                    );
                    if (options.chatAsync) {
                      return {
                        content: await options.chatAsync(last, request.signal),
                        finishReason: "stop" as const,
                      };
                    }
                    return {
                      content: options.chat ? options.chat(last) : `reply:${last}`,
                      finishReason: "stop" as const,
                      ...(options.replyTokens !== undefined && {
                        usage: { inputTokens: options.replyTokens, outputTokens: 0 },
                      }),
                    };
                  },
                },
        }),
    });
    const retained = new RetainedSubagents({
      agents,
      workflows,
      memory,
      canDelegate: (from, to) => from === "parent" && to === "worker",
      ...options.retained,
    });
    retainedRef = retained;
    return { workflows, agents, retained };
  };
  const current = make();
  const recipe = await registry.get({ id: "parent" });
  const agent = (await current.agents.resolve(recipe!, parent)).withScope(parent);
  await agent.thread(parent.threadId);
  return { storage, memory, make, ...current };
}

export async function waitFor(predicate: () => Promise<boolean>) {
  for (let i = 0; i < 200; i++) {
    if (await predicate()) return;
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  throw new Error("condition not reached");
}
