import { describe, expect, it } from "bun:test";
import { FakeWallClock, InMemoryWorkflowStorage, createWorkflowRunner } from "@promin/workflow";
import { EMPTY_USAGE } from "../../budget/budget.ts";
import { createDagWorkflow } from "../durable-executor.ts";
import type { AgenticDagRecipe, DagRegistry } from "../types.ts";
import { SCOPE, invokeAgent, makeResolver, runDag, stubAgent, textOnly } from "./dag-test-kit.ts";

const single: AgenticDagRecipe = {
  id: "single-stats",
  version: "v1",
  nodes: [
    {
      id: "n1",
      kind: "agent",
      agentId: "writer",
      inputs: { task: { kind: "initial", path: "task" } },
    },
  ],
  edges: [],
  entry: ["n1"],
  terminals: ["n1"],
};

const skipDag: AgenticDagRecipe = {
  id: "skip-stats",
  version: "v1",
  nodes: [
    {
      id: "a",
      kind: "agent",
      agentId: "fails",
      inputs: { task: { kind: "initial", path: "task" } },
      onError: "skip",
    },
    {
      id: "b",
      kind: "agent",
      agentId: "writer",
      inputs: { task: { kind: "node", nodeId: "a", path: "" } },
    },
  ],
  edges: [{ from: "a", to: "b" }],
  entry: ["a"],
  terminals: ["b"],
};

describe("createDagWorkflow — node stats and settlement", () => {
  it("reports each node's duration and spend, the same on replay", async () => {
    const clock = FakeWallClock.create(1_000);
    const slow = invokeAgent(async (input) => {
      clock.advance(250);
      return textOnly(input.task);
    });
    const first = await runDag({
      dag: single,
      resolver: makeResolver({ writer: slow }),
      initialInput: { task: "hi" },
      workflowId: "stats-1",
      config: { clock },
    });

    expect(first.result.nodeStats.n1!.durationMs).toBe(250);
    expect(first.result.nodeStats.n1!.spend).toEqual(EMPTY_USAGE);

    const again = await first.runner.run({
      workflow: first.workflow,
      workflowId: "stats-1",
      input: undefined,
      useStoredInput: true,
    });
    expect(again.nodeStats).toEqual(first.result.nodeStats);
  });

  it("journals a settlement record for a skipped node", async () => {
    const { storage } = await runDag({
      dag: skipDag,
      resolver: makeResolver({
        fails: invokeAgent(async () => {
          throw new Error("nope");
        }),
        writer: stubAgent(),
      }),
      initialInput: { task: "x" },
      workflowId: "stats-2",
    });
    const entries = await storage.loadJournal({
      workflowId: "stats-2",
      stepName: "execute-dag",
    });
    const settle = entries.find((entry) => entry.activityName === "node-b-settle");
    expect(settle?.exit).toMatchObject({
      tag: "Success",
      value: { state: "skipped" },
    });
  });

  it("fails cleanly on a stored recipe that no longer matches the schema", async () => {
    const legacyKindlessRecipe = {
      id: "old",
      version: "v1",
      createdAt: 0,
      active: true,
      enabled: true,
      trust: "trusted",
      nodes: [{ id: "n1", agentId: "writer", inputs: {} }],
      edges: [],
      entry: ["n1"],
      terminals: ["n1"],
    };
    const stale = {
      get: async () => ({ ...legacyKindlessRecipe }),
    } as unknown as DagRegistry;

    await expect(
      createWorkflowRunner({ storage: new InMemoryWorkflowStorage() }).run({
        workflow: createDagWorkflow({ registry: stale, resolver: makeResolver({}) }),
        workflowId: "stale-1",
        input: {
          dagId: "old",
          version: "v1",
          initialInput: {},
          scope: SCOPE,
        },
      }),
    ).rejects.toThrow("dag_invalid");
  });
});
