// The demo agents wired for evaluation without a network: the recipes from
// ../agents, scripted LLMs from ../llms, the org handbook from ../kb in an
// in-memory retriever, the weather tool, and in-memory workflow and memory
// stores. `evalTargets` builds the eval targets the suites run against.

import {
  InMemoryAgentRegistry,
  InMemoryMemoryStore,
  InMemoryRetriever,
  resolveLocalAgent,
  type Agent,
  type LLMProvider,
  type RegisteredAgent,
} from "@promin/agent";
import { agentTarget, fnTarget, type EvalTarget } from "@promin/evals";
import { InMemoryWorkflowStorage, createWorkflowRunner } from "@promin/workflow";
import { KNOWLEDGE_BOT } from "../agents/knowledge-bot.ts";
import { WEATHER_BOT } from "../agents/weather-bot.ts";
import { ORG_KNOWLEDGE_BASE } from "../kb/org-knowledge-base.ts";
import { factExtractorLLM, knowledgeBotLLM, weatherBotLLM } from "../llms/scripted-llms.ts";
import weather from "../tools/weather.ts";

/** Namespace every eval conversation runs in. */
const NAMESPACE = "evals-ci";

const LLMS: Readonly<Record<string, () => LLMProvider>> = {
  "knowledge-bot": knowledgeBotLLM,
  "weather-bot": weatherBotLLM,
};

export interface EvalHarness {
  /** The agent under test, by recipe id, scoped to one eval resource. */
  agent(params: { readonly agentId: string; readonly resourceId: string }): Promise<Agent>;
  readonly memory: InMemoryMemoryStore;
  readonly recipes: ReadonlyMap<string, RegisteredAgent>;
}

export async function createEvalHarness(): Promise<EvalHarness> {
  const storage = new InMemoryWorkflowStorage();
  const runner = createWorkflowRunner({ storage });
  const memory = new InMemoryMemoryStore();
  const handbook = new InMemoryRetriever({
    documents: [...ORG_KNOWLEDGE_BASE.entries()].map(([id, doc]) => ({
      id,
      title: doc.title,
      tags: doc.tags,
      text: doc.body,
    })),
  });
  const registry = new InMemoryAgentRegistry();
  const recipes = new Map<string, RegisteredAgent>();
  for (const recipe of [KNOWLEDGE_BOT, WEATHER_BOT]) {
    const { id, ...body } = recipe;
    recipes.set(id, await registry.register({ id, ...body }));
  }
  return {
    memory,
    recipes,
    async agent({ agentId, resourceId }) {
      const recipe = recipes.get(agentId);
      const llm = LLMS[agentId];
      if (!recipe || !llm) throw new Error(`no eval wiring for agent "${agentId}"`);
      const agent = await resolveLocalAgent(recipe, {
        runner,
        memory,
        llm: () => llm(),
        consolidatorLlm: factExtractorLLM(),
        tools: { weather },
        retrievers: { "org-handbook": handbook },
      });
      return agent.withScope({ namespaceId: NAMESPACE, resourceId });
    },
  };
}

/** One agent turn per case: the agent suites' target. */
export function agentEvalTarget(params: {
  readonly harness: EvalHarness;
  readonly agentId: string;
}): EvalTarget {
  const recipe = params.harness.recipes.get(params.agentId);
  return agentTarget({
    id: params.agentId,
    ...(recipe !== undefined && { version: recipe.version }),
    agent: () => params.harness.agent({ agentId: params.agentId, resourceId: "eval-turns" }),
  });
}

/**
 * The memory suite's target: each case's `turns` are sent to knowledge-bot
 * on a fresh thread of a fresh user, the thread is distilled, and the
 * output is the facts stored for that user.
 */
export function memoryFactsTarget(params: { readonly harness: EvalHarness }): EvalTarget {
  const { harness } = params;
  let conversation = 0;
  return fnTarget({
    id: "knowledge-bot-memory",
    fn: async (input) => {
      const turns = (input as { turns?: unknown }).turns;
      if (!Array.isArray(turns) || !turns.every((t) => typeof t === "string")) {
        throw new Error('a memory case input needs "turns": string[]');
      }
      conversation += 1;
      const resourceId = `user-${conversation}`;
      const agent = await harness.agent({ agentId: "knowledge-bot", resourceId });
      const threadId = `conversation-${conversation}`;
      const thread = await agent.thread(threadId);
      for (const task of turns) await (await thread.send({ task })).text;
      // Re-distill the whole thread: knowledge-bot's own autoDistill may
      // have distilled its first turns already, and must not duplicate them.
      await agent.distillThread(threadId, { force: true });
      const facts = await harness.memory.listResourceFacts({ namespaceId: NAMESPACE, resourceId });
      return facts.map((fact) => fact.text);
    },
  });
}
