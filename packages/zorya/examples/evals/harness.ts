// The demo agents wired for evaluation without a network: the recipes from
// ../agents, scripted LLMs from ../llms, the org handbook from ../kb in an
// in-memory retriever, the weather tool, and in-memory workflow and memory
// stores. `evalTargets` builds the eval targets the suites run against.

import {
  createLayeredMemoryTool,
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
 * output is the facts stored for that user. A case's `earlierTurns` run
 * first, in an earlier thread of the same user that is distilled too, so
 * the facts it stored can be contradicted.
 */
export function memoryFactsTarget(params: { readonly harness: EvalHarness }): EvalTarget {
  const { harness } = params;
  let conversation = 0;
  return fnTarget({
    id: "knowledge-bot-memory",
    fn: async (input) => {
      const { turns, earlierTurns = [] } = input as { turns?: unknown; earlierTurns?: unknown };
      const isTurns = (v: unknown): v is string[] =>
        Array.isArray(v) && v.every((t) => typeof t === "string");
      if (!isTurns(turns) || !isTurns(earlierTurns)) {
        throw new Error(
          'a memory case input needs "turns": string[] (and optional "earlierTurns")',
        );
      }
      conversation += 1;
      const resourceId = `user-${conversation}`;
      const agent = await harness.agent({ agentId: "knowledge-bot", resourceId });
      if (earlierTurns.length > 0) {
        // An earlier conversation of the same user, distilled before this one starts.
        const earlierId = `conversation-${conversation}-earlier`;
        const earlier = await agent.thread(earlierId);
        for (const task of earlierTurns) await (await earlier.send({ task })).text;
        await agent.distillThread(earlierId, { force: true });
      }
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

/** One remembered item a recall case seeds. */
interface RecallSeed {
  readonly text: string;
  readonly source?: "operator" | "user" | "model";
  /** Facts: 0..1 confidence; defaults by source. */
  readonly confidence?: number;
  /** Set for an episode: its salience. Facts leave it out. */
  readonly salience?: number;
  /** Episodes: how long ago it happened. */
  readonly ageDays?: number;
}

const DAY_MS = 24 * 60 * 60 * 1000;

/**
 * The recall suite's target: each case seeds a fresh user's facts and
 * episodes, runs the `memory` tool's `recall` for `query` (at most `limit`
 * hits), and outputs the texts it returned. `FENCE-BROKEN` is appended when
 * the result is not exactly one reference-data block, i.e. stored text
 * escaped the fence.
 */
export function memoryRecallTarget(): EvalTarget {
  let conversation = 0;
  return fnTarget({
    id: "memory-recall",
    fn: async (input) => {
      const { items, query, limit } = input as {
        items?: ReadonlyArray<RecallSeed>;
        query?: string;
        limit?: number;
      };
      if (!Array.isArray(items) || typeof query !== "string") {
        throw new Error('a recall case input needs "items": RecallSeed[] and "query": string');
      }
      conversation += 1;
      const store = new InMemoryMemoryStore();
      const resource = { namespaceId: NAMESPACE, resourceId: `user-${conversation}` };
      for (const item of items) {
        if (item.salience === undefined) {
          await store.appendResourceFact(resource, {
            text: item.text,
            source: item.source ?? "model",
            ...(item.confidence !== undefined && { confidence: item.confidence }),
          });
        } else {
          await store.appendResourceEpisode(resource, {
            summary: item.text,
            salience: item.salience,
            occurredAt: Date.now() - (item.ageDays ?? 0) * DAY_MS,
          });
        }
      }
      const memory = createLayeredMemoryTool({
        store,
        ...resource,
        threadId: `thread-${conversation}`,
      });
      const out = (await memory.execute(
        memory.parameters.parse({ command: "recall", query, limit: limit ?? 5 }),
      )) as string;
      const hits = out
        .split("\n")
        .map((line) => /^\d+\. \[[^\]]*\] \([\d.]+\) (.*)$/.exec(line)?.[1])
        .filter((text): text is string => text !== undefined);
      const closings = out.split("</reference-data>").length - 1;
      return hits.length > 0 && closings !== 1 ? [...hits, "FENCE-BROKEN"] : hits;
    },
  });
}
