import { describe, expect, it } from "bun:test";

import { reactiveLLM } from "../../tests/engine-harness.ts";
import type { AgenticDagRecipe } from "../types.ts";
import {
  invokeAgent,
  localAgents,
  makeResolver,
  runDag,
  stubAgent,
  textOnly,
} from "./dag-test-kit.ts";

function router(params: { joinAny: boolean }): AgenticDagRecipe {
  return {
    id: params.joinAny ? "router-any" : "router-all",
    version: "v1",
    nodes: [
      {
        id: "classify",
        kind: "agent",
        agentId: "classify",
        inputs: { task: { kind: "initial", path: "task" } },
      },
      {
        id: "a",
        kind: "agent",
        agentId: "a",
        inputs: { task: { kind: "node", nodeId: "classify", path: "" } },
      },
      {
        id: "b",
        kind: "agent",
        agentId: "b",
        inputs: { task: { kind: "node", nodeId: "classify", path: "" } },
      },
      {
        id: "merge",
        kind: "agent",
        agentId: "merge",
        inputs: {
          fromA: { kind: "node", nodeId: "a", path: "" },
          fromB: { kind: "node", nodeId: "b", path: "" },
        },
        ...(params.joinAny ? { join: "any" as const } : {}),
      },
      {
        id: "after",
        kind: "agent",
        agentId: "after",
        inputs: {
          merged: { kind: "node", nodeId: "merge", path: "" },
          fromB: { kind: "node", nodeId: "b", path: "" },
        },
      },
    ],
    edges: [
      {
        from: "classify",
        to: "a",
        condition: { kind: "equals", path: "", value: "a" },
      },
      {
        from: "classify",
        to: "b",
        condition: { kind: "equals", path: "", value: "b" },
      },
      { from: "a", to: "merge" },
      { from: "b", to: "merge" },
      { from: "merge", to: "after" },
    ],
    entry: ["classify"],
    terminals: ["after"],
  };
}

describe("createDagWorkflow — routing and transforms", () => {
  it("a merge with join any runs when one branch ran; inputs from the other are null", async () => {
    const seen: Record<string, string> = {};
    const record = (id: string) =>
      invokeAgent(async (input) => {
        seen[id] = input.task;
        return textOnly(id);
      });
    const { result } = await runDag({
      dag: router({ joinAny: true }),
      initialInput: { task: "x" },
      resolver: makeResolver({
        classify: stubAgent(() => "a"),
        a: record("a"),
        b: record("b"),
        merge: record("merge"),
        after: record("after"),
      }),
    });

    expect(result.ok).toBe(true);
    expect(result.skipped).toEqual(["b"]);
    expect(JSON.parse(seen.merge!)).toEqual({ fromA: "a", fromB: null });
    expect(JSON.parse(seen.after!)).toEqual({ merged: "merge", fromB: null });
  });

  it("the default join still skips a node when any branch was skipped", async () => {
    const { result } = await runDag({
      dag: router({ joinAny: false }),
      initialInput: { task: "x" },
      resolver: makeResolver({
        classify: stubAgent(() => "a"),
        a: stubAgent(() => "a"),
        b: stubAgent(() => "b"),
        merge: stubAgent(() => "merge"),
        after: stubAgent(() => "after"),
      }),
    });

    expect(result.ok).toBe(true);
    expect(result.skipped).toEqual(["b", "merge", "after"]);
  });

  it("a transform node runs without a child run and its result is journaled", async () => {
    const dag: AgenticDagRecipe = {
      id: "transform-journal",
      version: "v1",
      nodes: [
        {
          id: "n1",
          kind: "agent",
          agentId: "one",
          inputs: { task: { kind: "initial", path: "task" } },
        },
        {
          id: "t",
          kind: "transform",
          inputs: {
            x: { kind: "node", nodeId: "n1", path: "" },
            y: { kind: "literal", value: null },
          },
          ops: [{ op: "collect", fields: ["x", "y"] }, { op: "compact" }, { op: "count" }],
        },
      ],
      edges: [{ from: "n1", to: "t" }],
      entry: ["n1"],
      terminals: ["t"],
    };
    const { result, storage } = await runDag({
      dag,
      resolver: makeResolver({ one: stubAgent(() => "one") }),
      initialInput: { task: "x" },
      workflowId: "wf",
    });

    expect(result.nodeOutputs.t).toBe(1);
    expect(await storage.loadWorkflow("wf.node-t")).toBeNull();
    const entries = await storage.loadJournal({ workflowId: "wf", stepName: "execute-dag" });
    expect(entries.some((entry) => entry.activityName === "node-t-transform")).toBe(true);
  });

  it("a failing transform follows onError", async () => {
    const dag: AgenticDagRecipe = {
      id: "transform-error",
      version: "v1",
      nodes: [
        {
          id: "t",
          kind: "transform",
          inputs: { x: { kind: "initial", path: "task" } },
          from: "x",
          ops: [{ op: "flatten" }],
          onError: "skip",
        },
      ],
      edges: [],
      entry: ["t"],
      terminals: ["t"],
    };
    const { result } = await runDag({
      dag,
      resolver: makeResolver({}),
      initialInput: { task: "not-an-array" },
    });

    expect(result.ok).toBe(true);
    expect(result.errors.t).toMatch(/^transform_failed/);
  });

  it("a transform takes no share of the run budget", async () => {
    const dag: AgenticDagRecipe = {
      id: "transform-budget",
      version: "v1",
      nodes: [
        {
          id: "a",
          kind: "agent",
          agentId: "a",
          inputs: { task: { kind: "initial", path: "task" } },
        },
        {
          id: "t",
          kind: "transform",
          inputs: { x: { kind: "node", nodeId: "a", path: "" } },
          from: "x",
          ops: [{ op: "pick", path: "" }],
        },
        {
          id: "b",
          kind: "agent",
          agentId: "b",
          inputs: { task: { kind: "node", nodeId: "t", path: "" } },
        },
      ],
      edges: [
        { from: "a", to: "t" },
        { from: "t", to: "b" },
      ],
      entry: ["a"],
      terminals: ["b"],
    };
    const llm = reactiveLLM(() => ({ content: "ok", finishReason: "stop" }));
    const agents = await localAgents({ llms: { a: llm, b: llm } });
    const { resolver } = agents.boot();
    const { result } = await runDag({
      dag,
      resolver,
      storage: agents.storage,
      initialInput: { task: "x" },
      budget: { maxLlmCalls: 2 },
    });

    expect(result.ok).toBe(true);
    expect(llm.calls).toHaveLength(2);
  });
});
