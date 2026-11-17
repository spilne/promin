import { describe, expect, it } from "bun:test";
import { FakeWallClock, InMemoryWorkflowStorage } from "@promin/workflow";

import { RESPOND_TOOL_NAME } from "../../agent-action.ts";
import { deferred, reactiveLLM, scriptedLLM, until } from "../../tests/engine-harness.ts";
import type { AgenticDagRecipe, ForEach } from "../types.ts";
import {
  invokeAgent,
  localAgents,
  makeResolver,
  runDag,
  taskOf,
  textOnly,
} from "./dag-test-kit.ts";

const questionSchema = {
  type: "object",
  properties: { questions: { type: "array", items: { type: "string" } } },
  required: ["questions"],
};

function fanDag(forEach: ForEach, withSynthesizer = false): AgenticDagRecipe {
  return {
    id: withSynthesizer ? "fan-with-synthesizer" : "fan",
    version: "v1",
    nodes: [
      {
        id: "plan",
        kind: "agent",
        agentId: "planner",
        inputs: { topic: { kind: "initial", path: "topic" } },
        output: { schema: questionSchema },
      },
      {
        id: "research",
        kind: "agent",
        agentId: "researcher",
        inputs: { topic: { kind: "initial", path: "topic" } },
        forEach,
      },
      ...(withSynthesizer
        ? [
            {
              id: "synthesizer",
              kind: "agent" as const,
              agentId: "synthesizer",
              inputs: {
                findings: {
                  kind: "node" as const,
                  nodeId: "research",
                  path: "",
                },
              },
            },
          ]
        : []),
    ],
    edges: [
      { from: "plan", to: "research" },
      ...(withSynthesizer ? [{ from: "research", to: "synthesizer" }] : []),
    ],
    entry: ["plan"],
    terminals: [withSynthesizer ? "synthesizer" : "research"],
  };
}

const questionsSource = {
  items: { kind: "node" as const, nodeId: "plan", path: "questions" },
  as: "question",
  maxItems: 10,
};

function planner(questions: ReadonlyArray<string>) {
  return invokeAgent(async () => textOnly(JSON.stringify({ questions })));
}

function parsedTask(task: string): Record<string, unknown> {
  return JSON.parse(task) as Record<string, unknown>;
}

describe("createDagWorkflow — forEach", () => {
  it("runs once per item and keeps output in item order", async () => {
    const allStarted = deferred();
    let started = 0;
    const researcher = invokeAgent(async (input) => {
      const task = parsedTask(input.task);
      started++;
      if (started === 3) allStarted.resolve();
      if (task.index === 0) await allStarted.promise;
      return textOnly(`R:${String(task.question)}`);
    });
    const { result, storage } = await runDag({
      dag: fanDag(questionsSource),
      resolver: makeResolver({
        planner: planner(["q0", "q1", "q2"]),
        researcher,
      }),
      initialInput: { topic: "tea" },
      workflowId: "wf",
    });

    expect(result.nodeOutputs.research).toEqual(["R:q0", "R:q1", "R:q2"]);
    for (let i = 0; i < 3; i++) {
      expect(await storage.loadWorkflow(`wf.node-research.item-${i}`)).not.toBeNull();
    }
  });

  it("passes each item and its index beside the node inputs", async () => {
    const tasks: Array<Record<string, unknown>> = [];
    const researcher = invokeAgent(async (input) => {
      tasks.push(parsedTask(input.task));
      return textOnly("ok");
    });
    await runDag({
      dag: fanDag(questionsSource),
      resolver: makeResolver({ planner: planner(["q0", "q1", "q2"]), researcher }),
      initialInput: { topic: "tea" },
    });

    expect(tasks.sort((a, b) => Number(a.index) - Number(b.index))).toEqual([
      { topic: "tea", question: "q0", index: 0 },
      { topic: "tea", question: "q1", index: 1 },
      { topic: "tea", question: "q2", index: 2 },
    ]);
  });

  it("uses null for a failed item and reports it", async () => {
    const researcher = invokeAgent(async (input) => {
      const task = parsedTask(input.task);
      if (task.index === 1) throw new Error("item one failed");
      return textOnly(`R:${String(task.question)}`);
    });
    const { result } = await runDag({
      dag: fanDag(questionsSource),
      resolver: makeResolver({ planner: planner(["q0", "q1", "q2"]), researcher }),
      initialInput: { topic: "tea" },
    });

    expect(result.nodeOutputs.research).toEqual(["R:q0", null, "R:q2"]);
    expect(result.errors["research.item-1"]).toContain("item one failed");
    expect(result.ok).toBe(true);
    expect(result.nodeStats.research).toMatchObject({ items: 3, failedItems: 1 });
  });

  it("fails the node when every item fails", async () => {
    const researcher = invokeAgent(async () => {
      throw new Error("all failed");
    });
    const { result } = await runDag({
      dag: fanDag(questionsSource),
      resolver: makeResolver({ planner: planner(["q0", "q1"]), researcher }),
      initialInput: { topic: "tea" },
    });

    expect(result.errors.research).toBeDefined();
    expect(result.ok).toBe(false);
  });

  it("onItemError fail reports the lowest-index failure", async () => {
    const releaseOne = deferred();
    const researcher = invokeAgent(async (input) => {
      const task = parsedTask(input.task);
      if (task.index === 2) {
        releaseOne.resolve();
        throw new Error("item two failed first");
      }
      if (task.index === 1) {
        await releaseOne.promise;
        throw new Error("item one failed");
      }
      return textOnly("item zero");
    });
    const { result } = await runDag({
      dag: fanDag({ ...questionsSource, onItemError: "fail" }),
      resolver: makeResolver({ planner: planner(["q0", "q1", "q2"]), researcher }),
      initialInput: { topic: "tea" },
    });

    expect(result.errors.research).toContain("item one failed");
  });

  it("refuses too many items without starting a child run", async () => {
    const { result, storage } = await runDag({
      dag: fanDag({ ...questionsSource, maxItems: 4 }),
      resolver: makeResolver({
        planner: planner(["q0", "q1", "q2", "q3", "q4"]),
        researcher: invokeAgent(async () => textOnly("should not run")),
      }),
      initialInput: { topic: "tea" },
      workflowId: "too-many",
    });

    expect(result.errors.research).toMatch(/^too_many_items: 5 > 4/);
    expect(await storage.loadWorkflow("too-many.node-research.item-0")).toBeNull();
  });

  it("refuses a non-array and accepts an empty array", async () => {
    const single = (id: string): AgenticDagRecipe => ({
      id,
      version: "v1",
      nodes: [
        {
          id: "research",
          kind: "agent",
          agentId: "researcher",
          inputs: {},
          forEach: {
            items: { kind: "initial", path: "items" },
            as: "item",
            maxItems: 4,
          },
        },
      ],
      edges: [],
      entry: ["research"],
      terminals: ["research"],
    });
    const resolver = makeResolver({
      researcher: invokeAgent(async () => textOnly("should not run")),
    });
    const bad = await runDag({ dag: single("bad-items"), resolver, initialInput: { items: "x" } });
    const empty = await runDag({
      dag: single("empty-items"),
      resolver,
      initialInput: { items: [] },
    });

    expect(bad.result.errors.research).toMatch(/^not_an_array:/);
    expect(empty.result.nodeOutputs.research).toEqual([]);
    expect(empty.result.ok).toBe(true);
  });

  it("runs no more than concurrency items at once", async () => {
    const gates = Array.from({ length: 6 }, () => deferred());
    let active = 0;
    let highest = 0;
    let calls = 0;
    const researcher = invokeAgent(async (input) => {
      const task = parsedTask(input.task);
      const index = Number(task.index);
      calls++;
      active++;
      highest = Math.max(highest, active);
      await gates[index]!.promise;
      active--;
      return textOnly(`R:${index}`);
    });
    const running = runDag({
      dag: fanDag({ ...questionsSource, concurrency: 2 }),
      resolver: makeResolver({
        planner: planner(["q0", "q1", "q2", "q3", "q4", "q5"]),
        researcher,
      }),
      initialInput: { topic: "tea" },
    });
    await until(() => calls === 2);

    expect(highest).toBe(2);
    gates.forEach((gate) => gate.resolve());
    await running;
    expect(highest).toBe(2);
  });

  it("splits its budget between items and refuses an empty item share", async () => {
    const boot = async () => {
      const plannerLlm = scriptedLLM([
        {
          content: null,
          finishReason: "tool_calls",
          toolCalls: [
            {
              id: "respond",
              name: RESPOND_TOOL_NAME,
              input: { questions: Array.from({ length: 10 }, (_, i) => `q${i}`) },
            },
          ],
        },
      ]);
      const researcherLlm = reactiveLLM((messages) => ({
        content: `R:${parsedTask(taskOf(messages)).question}`,
        finishReason: "stop",
      }));
      const synthesizerLlm = reactiveLLM(() => ({ content: "done", finishReason: "stop" }));
      return localAgents({
        llms: {
          planner: plannerLlm,
          researcher: researcherLlm,
          synthesizer: synthesizerLlm,
        },
      });
    };
    const successfulAgents = await boot();
    const successful = await runDag({
      dag: fanDag(questionsSource, true),
      resolver: successfulAgents.boot().resolver,
      storage: successfulAgents.storage,
      initialInput: { topic: "tea" },
      budget: { maxLlmCalls: 20 },
    });
    const starvedAgents = await boot();
    const starved = await runDag({
      dag: fanDag(questionsSource, true),
      resolver: starvedAgents.boot().resolver,
      storage: starvedAgents.storage,
      initialInput: { topic: "tea" },
      workflowId: "starved",
      budget: { maxLlmCalls: 3 },
    });

    expect(successful.result.ok).toBe(true);
    expect(successful.result.spend.llmCalls).toBeLessThanOrEqual(20);
    expect(successful.result.nodeStats.research.items).toBe(10);
    expect(starved.result.errors.research).toMatch(/^budget_exceeded:/);
    expect(await starved.storage.loadWorkflow("starved.node-research.item-0")).toBeNull();
  });

  it("resumes mid fan-out without re-running finished items", async () => {
    const clock = FakeWallClock.create(1_000);
    const storage = new InMemoryWorkflowStorage({ clock });
    const stalled = deferred();
    const calls = [0, 0, 0];
    const researcherLlm = reactiveLLM(async (messages) => {
      const index = Number(parsedTask(taskOf(messages)).index);
      calls[index]!++;
      if (index === 2 && calls[index] === 1) await stalled.promise;
      return { content: `R:${index}`, finishReason: "stop" };
    });
    const agents = await localAgents({ llms: { researcher: researcherLlm }, storage });
    const dag: AgenticDagRecipe = {
      id: "recover-fan-out",
      version: "v1",
      nodes: [
        {
          id: "research",
          kind: "agent",
          agentId: "researcher",
          inputs: {},
          forEach: {
            items: { kind: "initial", path: "items" },
            as: "item",
            maxItems: 3,
            concurrency: 3,
          },
        },
      ],
      edges: [],
      entry: ["research"],
      terminals: ["research"],
    };
    const run = () =>
      runDag({
        dag,
        resolver: agents.boot().resolver,
        storage,
        config: { clock },
        initialInput: { items: ["q0", "q1", "q2"] },
        workflowId: "recover-fan-out",
      });
    const first = run().catch(() => undefined);
    await until(() => calls[0] === 1 && calls[1] === 1 && calls[2] === 1);

    clock.set(clock.currentTimeMs() + 121_000);
    const recovered = await run();
    stalled.resolve();
    await first;

    expect(recovered.result.nodeOutputs.research).toEqual(["R:0", "R:1", "R:2"]);
    expect(calls).toEqual([1, 1, 2]);
  });
});
