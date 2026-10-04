import { describe, it, expect } from "bun:test";
import { TaggedError, succeed, fail, tryPromise, all, runSafe } from "@spilne/perfect-core";
import { FakeWallClock } from "../../shared/wall-clock.ts";
import type { StepEff } from "../step-definition.ts";
import { workflow, flow } from "../workflow-builder.ts";
import { InMemoryWorkflowStorage } from "../in-memory-storage.ts";
import { createWorkflowRunner } from "../workflow-runner.ts";
import {
  WorkflowLockError,
  WorkflowSuspendedError,
  WorkflowTimeoutError,
  StepTimeoutError,
  WorkflowDeadlineError,
} from "../durable-pipeline-error.ts";
import { topologicalSort, computeReadySet } from "../workflow-dag.ts";

// ---------------------------------------------------------------------------
// Test error types
// ---------------------------------------------------------------------------

class FetchError extends TaggedError("FetchError")<{
  readonly message: string;
}>() {}

class HttpStatusError extends TaggedError("HttpStatusError")<{
  readonly status: number;
  readonly message: string;
}>() {}

// ---------------------------------------------------------------------------
// DAG utilities
// ---------------------------------------------------------------------------

describe("workflow-dag", () => {
  describe("topologicalSort", () => {
    it("sorts a linear chain", () => {
      const sorted = topologicalSort({
        nodes: [
          { name: "a", dependsOn: [] },
          { name: "b", dependsOn: ["a"] },
          { name: "c", dependsOn: ["b"] },
        ],
        workflowId: "test",
      });
      expect(sorted).toEqual(["a", "b", "c"]);
    });

    it("sorts a diamond DAG", () => {
      const sorted = topologicalSort({
        nodes: [
          { name: "a", dependsOn: [] },
          { name: "b", dependsOn: ["a"] },
          { name: "c", dependsOn: ["a"] },
          { name: "d", dependsOn: ["b", "c"] },
        ],
        workflowId: "test",
      });
      expect(sorted[0]).toBe("a");
      expect(sorted[sorted.length - 1]).toBe("d");
      expect(sorted.indexOf("b")).toBeGreaterThan(sorted.indexOf("a"));
      expect(sorted.indexOf("c")).toBeGreaterThan(sorted.indexOf("a"));
    });

    it("detects cycles", () => {
      expect(() =>
        topologicalSort({
          nodes: [
            { name: "a", dependsOn: ["b"] },
            { name: "b", dependsOn: ["a"] },
          ],
          workflowId: "test",
        }),
      ).toThrow(/Cycle detected/);
    });

    it("detects self-cycles", () => {
      expect(() =>
        topologicalSort({
          nodes: [{ name: "a", dependsOn: ["a"] }],
          workflowId: "test",
        }),
      ).toThrow(/Cycle detected/);
    });

    it("throws on unknown dependency", () => {
      expect(() =>
        topologicalSort({
          nodes: [{ name: "a", dependsOn: ["nonexistent"] }],
          workflowId: "test",
        }),
      ).toThrow(/unknown step "nonexistent"/);
    });

    it("handles single node", () => {
      const sorted = topologicalSort({
        nodes: [{ name: "only", dependsOn: [] }],
        workflowId: "test",
      });
      expect(sorted).toEqual(["only"]);
    });

    it("handles multiple roots", () => {
      const sorted = topologicalSort({
        nodes: [
          { name: "a", dependsOn: [] },
          { name: "b", dependsOn: [] },
          { name: "c", dependsOn: ["a", "b"] },
        ],
        workflowId: "test",
      });
      expect(sorted.indexOf("c")).toBe(2);
    });
  });

  describe("computeReadySet", () => {
    const nodes = [
      { name: "a", dependsOn: [] as string[] },
      { name: "b", dependsOn: ["a"] },
      { name: "c", dependsOn: ["a"] },
      { name: "d", dependsOn: ["b", "c"] },
    ];

    it("returns root nodes when nothing is completed", () => {
      const ready = computeReadySet({ nodes, completed: new Set(), running: new Set() });
      expect(ready).toEqual(["a"]);
    });

    it("returns b and c when a is completed", () => {
      const ready = computeReadySet({ nodes, completed: new Set(["a"]), running: new Set() });
      expect(ready.sort()).toEqual(["b", "c"]);
    });

    it("returns d when b and c are completed", () => {
      const ready = computeReadySet({
        nodes,
        completed: new Set(["a", "b", "c"]),
        running: new Set(),
      });
      expect(ready).toEqual(["d"]);
    });

    it("excludes running steps", () => {
      const ready = computeReadySet({
        nodes,
        completed: new Set(["a"]),
        running: new Set(["b"]),
      });
      expect(ready).toEqual(["c"]);
    });

    it("returns empty when all completed", () => {
      const ready = computeReadySet({
        nodes,
        completed: new Set(["a", "b", "c", "d"]),
        running: new Set(),
      });
      expect(ready).toEqual([]);
    });
  });
});

// ---------------------------------------------------------------------------
// WorkflowBuilder — linear chains
// ---------------------------------------------------------------------------

describe("WorkflowBuilder", () => {
  describe("linear chain", () => {
    it("executes a single step", async () => {
      const storage = new InMemoryWorkflowStorage();
      const runner = createWorkflowRunner({ storage });
      const wf = workflow<{ value: number }>({ name: "single-step" })
        .step("double", ({ input }) => succeed(input.value * 2))
        .build();
      const result = await runner.run({ workflow: wf, workflowId: "wf-1", input: { value: 21 } });
      expect(result).toBe(42);
    });

    it("chains multiple steps", async () => {
      const storage = new InMemoryWorkflowStorage();
      const runner = createWorkflowRunner({ storage });
      const wf = workflow<{ name: string }>({ name: "multi-step" })
        .step("greet", ({ input }) => succeed(`Hello, ${input.name}`))
        .step("upper", ({ prev }) => succeed(prev.toUpperCase()))
        .step("exclaim", ({ prev }) => succeed(`${prev}!`))
        .build();
      const result = await runner.run({
        workflow: wf,
        workflowId: "wf-2",
        input: { name: "World" },
      });
      expect(result).toBe("HELLO, WORLD!");
    });

    it("passes input to all steps", async () => {
      const storage = new InMemoryWorkflowStorage();
      const runner = createWorkflowRunner({ storage });
      const wf = workflow<{ x: number; y: number }>({ name: "input-access" })
        .step("sum", ({ input }) => succeed(input.x + input.y))
        .step("multiply", ({ input, prev }) => succeed(prev * input.x))
        .build();
      const result = await runner.run({ workflow: wf, workflowId: "wf-3", input: { x: 3, y: 4 } });
      expect(result).toBe(21);
    });

    it("stores workflow state as completed", async () => {
      const storage = new InMemoryWorkflowStorage();
      const runner = createWorkflowRunner({ storage });
      const wf = workflow<{ value: number }>({ name: "state-check" })
        .step("compute", ({ input }) => succeed(input.value + 1))
        .build();
      await runner.run({ workflow: wf, workflowId: "wf-state", input: { value: 10 } });

      const state = storage.getWorkflow("wf-state");
      expect(state?.status).toBe("completed");
      expect(state?.result).toBe(11);
      expect(state?.steps["compute"]?.status).toBe("completed");
    });

    it("stores step results for each step", async () => {
      const storage = new InMemoryWorkflowStorage();
      const runner = createWorkflowRunner({ storage });
      const wf = workflow<{ n: number }>({ name: "step-results" })
        .step("add-one", ({ input }) => succeed(input.n + 1))
        .step("double", ({ prev }) => succeed(prev * 2))
        .build();
      await runner.run({ workflow: wf, workflowId: "wf-steps", input: { n: 5 } });

      const state = storage.getWorkflow("wf-steps");
      expect(state?.steps["add-one"]?.result).toBe(6);
      expect(state?.steps["double"]?.result).toBe(12);
    });
  });

  // ---------------------------------------------------------------------------
  // DAG mode
  // ---------------------------------------------------------------------------

  describe("DAG mode", () => {
    it("runs independent steps in parallel", async () => {
      const storage = new InMemoryWorkflowStorage();
      const runner = createWorkflowRunner({ storage });
      const executionOrder: string[] = [];

      const wf = workflow<{ text: string }>({ name: "dag-parallel" })
        .step("parse", ({ input }) => {
          executionOrder.push("parse");
          return succeed(input.text);
        })
        .step("summarize", { dependsOn: ["parse"] }, ({ deps }) => {
          executionOrder.push("summarize");
          return succeed(`Summary: ${deps.parse.slice(0, 10)}`);
        })
        .step("keywords", { dependsOn: ["parse"] }, ({ deps }) => {
          executionOrder.push("keywords");
          return succeed(deps.parse.split(" ").slice(0, 3));
        })
        .step("publish", { dependsOn: ["summarize", "keywords"] }, ({ deps }) => {
          executionOrder.push("publish");
          return succeed({ summary: deps.summarize, keywords: deps.keywords });
        })
        .build();
      const result = await runner.run({
        workflow: wf,
        workflowId: "wf-dag",
        input: { text: "hello world from the DAG" },
      });

      expect(result).toEqual({
        summary: "Summary: hello worl",
        keywords: ["hello", "world", "from"],
      });
      expect(executionOrder[0]).toBe("parse");
      expect(executionOrder[executionOrder.length - 1]).toBe("publish");
    });

    it("correctly types deps in DAG steps", async () => {
      const storage = new InMemoryWorkflowStorage();
      const runner = createWorkflowRunner({ storage });
      const wf = workflow<{ id: number }>({ name: "dag-types" })
        .step("fetch", ({ input }) => succeed({ name: `User ${input.id}`, age: 30 }))
        .step("format-name", { dependsOn: ["fetch"] }, ({ deps }) =>
          succeed(deps.fetch.name.toUpperCase()),
        )
        .step("format-age", { dependsOn: ["fetch"] }, ({ deps }) =>
          succeed(`Age: ${deps.fetch.age}`),
        )
        .step("combine", { dependsOn: ["format-name", "format-age"] }, ({ deps }) =>
          succeed(`${deps["format-name"]} - ${deps["format-age"]}`),
        )
        .build();
      const result = await runner.run({
        workflow: wf,
        workflowId: "wf-dag-types",
        input: { id: 42 },
      });
      expect(result).toBe("USER 42 - Age: 30");
    });

    it("DAG with multiple roots", async () => {
      const storage = new InMemoryWorkflowStorage();
      const runner = createWorkflowRunner({ storage });
      const noDeps: "left"[] = [];
      const wf = workflow<{ a: number; b: number }>({ name: "multi-root" })
        .step("left", ({ input }) => succeed(input.a * 10))
        .step("right", { dependsOn: noDeps }, ({ input }) => succeed(input.b * 10))
        .step("merge", { dependsOn: ["left", "right"] }, ({ deps }) =>
          succeed(deps.left + deps.right),
        )
        .build();
      const result = await runner.run({
        workflow: wf,
        workflowId: "wf-multi-root",
        input: { a: 3, b: 4 },
      });
      expect(result).toBe(70);
    });
  });

  // ---------------------------------------------------------------------------
  // stepAsync
  // ---------------------------------------------------------------------------

  describe("stepAsync", () => {
    it("executes a single async step", async () => {
      const storage = new InMemoryWorkflowStorage();
      const runner = createWorkflowRunner({ storage });
      const wf = workflow<{ value: number }>({ name: "async-single" })
        .stepAsync("double", async ({ input }) => input.value * 2)
        .build();
      const result = await runner.run({
        workflow: wf,
        workflowId: "wf-async-1",
        input: { value: 21 },
      });
      expect(result).toBe(42);
    });

    it("chains stepAsync with step", async () => {
      const storage = new InMemoryWorkflowStorage();
      const runner = createWorkflowRunner({ storage });
      const wf = workflow<{ name: string }>({ name: "async-chain" })
        .stepAsync("fetch", async ({ input }) => ({ name: input.name, id: 1 }))
        .step("format", ({ prev }) => succeed(`${prev.name} (${prev.id})`))
        .build();
      const result = await runner.run({
        workflow: wf,
        workflowId: "wf-async-2",
        input: { name: "Alice" },
      });
      expect(result).toBe("Alice (1)");
    });

    it("DAG mode with stepAsync", async () => {
      const storage = new InMemoryWorkflowStorage();
      const runner = createWorkflowRunner({ storage });
      const wf = workflow<{ text: string }>({ name: "async-dag" })
        .stepAsync("parse", async ({ input }) => input.text.split(" "))
        .stepAsync("count", { dependsOn: ["parse"] }, async ({ deps }) => deps.parse.length)
        .stepAsync("join", { dependsOn: ["parse"] }, async ({ deps }) => deps.parse.join("-"))
        .stepAsync(
          "result",
          { dependsOn: ["count", "join"] },
          async ({ deps }) => `${deps.join} (${deps.count} words)`,
        )
        .build();
      const result = await runner.run({
        workflow: wf,
        workflowId: "wf-async-dag",
        input: { text: "hello beautiful world" },
      });
      expect(result).toBe("hello-beautiful-world (3 words)");
    });
  });

  // ---------------------------------------------------------------------------
  // mapOver — fan-out
  // ---------------------------------------------------------------------------

  describe("mapOver", () => {
    it("fans out over an array", async () => {
      const storage = new InMemoryWorkflowStorage();
      const runner = createWorkflowRunner({ storage });
      const wf = workflow<{ urls: string[] }>({ name: "fan-out" })
        .step("get-urls", ({ input }) => succeed(input.urls))
        .mapOver("fetch-all", { array: "get-urls" }, (url) => succeed(`Response from ${url}`))
        .build();
      const result = await runner.run({
        workflow: wf,
        workflowId: "wf-fan-out",
        input: { urls: ["https://a.com", "https://b.com", "https://c.com"] },
      });

      expect(result).toEqual([
        "Response from https://a.com",
        "Response from https://b.com",
        "Response from https://c.com",
      ]);
    });

    it("preserves element order", async () => {
      const storage = new InMemoryWorkflowStorage();
      const runner = createWorkflowRunner({ storage });
      const wf = workflow<{ items: number[] }>({ name: "order" })
        .step("source", ({ input }) => succeed(input.items))
        .mapOver("double", { array: "source" }, (n) => succeed(n * 2))
        .build();
      const result = await runner.run({
        workflow: wf,
        workflowId: "wf-order",
        input: { items: [1, 2, 3, 4, 5] },
      });
      expect(result).toEqual([2, 4, 6, 8, 10]);
    });

    it("respects concurrency limit", async () => {
      const storage = new InMemoryWorkflowStorage();
      const runner = createWorkflowRunner({ storage });
      let maxConcurrent = 0;
      let currentConcurrent = 0;

      const wf = workflow<{ items: number[] }>({ name: "concurrency" })
        .step("source", ({ input }) => succeed(input.items))
        .mapOver("process", { array: "source", concurrency: 2 }, (n) =>
          tryPromise(
            async () => {
              currentConcurrent++;
              maxConcurrent = Math.max(maxConcurrent, currentConcurrent);
              await new Promise((r) => setTimeout(r, 10));
              currentConcurrent--;
              return n * 10;
            },
            (e) => e,
          ).orDie(),
        )
        .build();
      const result = await runner.run({
        workflow: wf,
        workflowId: "wf-conc",
        input: { items: [1, 2, 3, 4] },
      });

      expect(result).toEqual([10, 20, 30, 40]);
      expect(maxConcurrent).toBeLessThanOrEqual(2);
    });

    it("saves per-task results", async () => {
      const storage = new InMemoryWorkflowStorage();
      const runner = createWorkflowRunner({ storage });
      const wf = workflow<{ items: string[] }>({ name: "task-persist" })
        .step("source", ({ input }) => succeed(input.items))
        .mapOver("process", { array: "source" }, (item) => succeed(item.toUpperCase()))
        .build();
      await runner.run({ workflow: wf, workflowId: "wf-tasks", input: { items: ["a", "b"] } });

      const state = storage.getWorkflow("wf-tasks");
      const step = state?.steps["process"];
      expect(step?.status).toBe("completed");
      expect(step?.result).toEqual(["A", "B"]);
    });

    it("mapOver with empty array", async () => {
      const storage = new InMemoryWorkflowStorage();
      const runner = createWorkflowRunner({ storage });
      const wf = workflow<{ items: number[] }>({ name: "empty-map" })
        .step("source", ({ input }) => succeed(input.items))
        .mapOver("process", { array: "source" }, (n) => succeed(n * 2))
        .build();
      const result = await runner.run({
        workflow: wf,
        workflowId: "wf-empty-map",
        input: { items: [] },
      });
      expect(result).toEqual([]);
    });

    it("mapOver provides MapStepContext", async () => {
      const storage = new InMemoryWorkflowStorage();
      const runner = createWorkflowRunner({ storage });
      const contexts: { taskIndex: number; workflowId: string }[] = [];

      const wf = workflow<{ items: string[] }>({ name: "ctx-test" })
        .step("source", ({ input }) => succeed(input.items))
        .mapOver("process", { array: "source" }, (_item, ctx) => {
          contexts.push({ taskIndex: ctx.taskIndex, workflowId: ctx.workflowId });
          return succeed("ok");
        })
        .build();
      await runner.run({ workflow: wf, workflowId: "wf-ctx", input: { items: ["a", "b", "c"] } });

      expect(contexts).toEqual([
        { taskIndex: 0, workflowId: "wf-ctx" },
        { taskIndex: 1, workflowId: "wf-ctx" },
        { taskIndex: 2, workflowId: "wf-ctx" },
      ]);
    });
  });

  // ---------------------------------------------------------------------------
  // mapOverAsync
  // ---------------------------------------------------------------------------

  describe("mapOverAsync", () => {
    it("fans out with async functions", async () => {
      const storage = new InMemoryWorkflowStorage();
      const runner = createWorkflowRunner({ storage });
      const wf = workflow<{ nums: number[] }>({ name: "async-map" })
        .step("source", ({ input }) => succeed(input.nums))
        .mapOverAsync("double", { array: "source" }, async (n) => n * 2)
        .build();
      const result = await runner.run({
        workflow: wf,
        workflowId: "wf-async-map",
        input: { nums: [1, 2, 3] },
      });
      expect(result).toEqual([2, 4, 6]);
    });
  });

  // ---------------------------------------------------------------------------
  // branch
  // ---------------------------------------------------------------------------

  describe("branch", () => {
    it("takes the ifTrue branch when condition is true", async () => {
      const storage = new InMemoryWorkflowStorage();
      const runner = createWorkflowRunner({ storage });
      const wf = workflow<{ n: number }>({ name: "branch-true" })
        .step("compute", ({ input }) => succeed(input.n))
        .branch("decide", {
          condition: (n) => n > 10,
          ifTrue: ({ prev }) => succeed(`big: ${prev}`),
          ifFalse: ({ prev }) => succeed(`small: ${prev}`),
        })
        .build();
      const result = await runner.run({
        workflow: wf,
        workflowId: "wf-branch-true",
        input: { n: 42 },
      });
      expect(result).toBe("big: 42");
    });

    it("takes the ifFalse branch when condition is false", async () => {
      const storage = new InMemoryWorkflowStorage();
      const runner = createWorkflowRunner({ storage });
      const wf = workflow<{ n: number }>({ name: "branch-false" })
        .step("compute", ({ input }) => succeed(input.n))
        .branch("decide", {
          condition: (n) => n > 10,
          ifTrue: ({ prev }) => succeed(`big: ${prev}`),
          ifFalse: ({ prev }) => succeed(`small: ${prev}`),
        })
        .build();
      const result = await runner.run({
        workflow: wf,
        workflowId: "wf-branch-false",
        input: { n: 3 },
      });
      expect(result).toBe("small: 3");
    });

    it("branch result is checkpointed", async () => {
      const storage = new InMemoryWorkflowStorage();
      const runner = createWorkflowRunner({ storage });
      const wf = workflow<{ n: number }>({ name: "branch-cp" })
        .step("compute", ({ input }) => succeed(input.n))
        .branch("decide", {
          condition: (n) => n > 0,
          ifTrue: () => succeed("positive"),
          ifFalse: () => succeed("non-positive"),
        })
        .build();
      await runner.run({ workflow: wf, workflowId: "wf-branch-cp", input: { n: 5 } });

      const state = storage.getWorkflow("wf-branch-cp");
      expect(state?.steps["decide"]?.status).toBe("completed");
      expect(state?.steps["decide"]?.result).toBe("positive");
    });

    it("branch can chain with more steps", async () => {
      const storage = new InMemoryWorkflowStorage();
      const runner = createWorkflowRunner({ storage });
      const wf = workflow<{ n: number }>({ name: "branch-chain" })
        .step("compute", ({ input }) => succeed(input.n))
        .branch("classify", {
          condition: (n) => n % 2 === 0,
          ifTrue: ({ prev }) => succeed(`even:${prev}`),
          ifFalse: ({ prev }) => succeed(`odd:${prev}`),
        })
        .step("format", ({ prev }) => succeed(prev.toUpperCase()))
        .build();
      const result = await runner.run({
        workflow: wf,
        workflowId: "wf-branch-chain",
        input: { n: 4 },
      });
      expect(result).toBe("EVEN:4");
    });
  });

  // ---------------------------------------------------------------------------
  // match — multi-way branching
  // ---------------------------------------------------------------------------

  describe("match (selector mode)", () => {
    type Order = { type: "express" | "standard" | "freight"; total: number };

    it("routes to the case matching the selector key", async () => {
      const storage = new InMemoryWorkflowStorage();
      const runner = createWorkflowRunner({ storage });
      const wf = workflow<Order>({ name: "match-sel" })
        .step("load", ({ input }) => succeed(input))
        .match("route", {
          on: (o) => o.type,
          cases: {
            express: ({ prev }) => succeed(`EXP:${prev.total}`),
            standard: ({ prev }) => succeed(`STD:${prev.total}`),
            freight: ({ prev }) => succeed(`FRT:${prev.total}`),
          },
        })
        .build();
      const result = await runner.run({
        workflow: wf,
        workflowId: "wf-sel-1",
        input: { type: "freight", total: 500 },
      });
      expect(result).toBe("FRT:500");
    });

    it("falls back to default when key has no case", async () => {
      const storage = new InMemoryWorkflowStorage();
      const runner = createWorkflowRunner({ storage });
      const wf = workflow<Order>({ name: "match-default" })
        .step("load", ({ input }) => succeed(input))
        .match("route", {
          on: (o) => o.type,
          cases: {
            express: ({ prev }) => succeed(`EXP:${prev.total}`),
          },
          default: ({ prev }) => succeed(`DEFAULT:${prev.total}`),
        })
        .build();
      const result = await runner.run({
        workflow: wf,
        workflowId: "wf-sel-2",
        input: { type: "standard", total: 50 },
      });
      expect(result).toBe("DEFAULT:50");
    });

    it("throws MatchError when no case matches and no default", async () => {
      const storage = new InMemoryWorkflowStorage();
      const runner = createWorkflowRunner({ storage });
      const wf = workflow<Order>({ name: "match-no-case" })
        .step("load", ({ input }) => succeed(input))
        .match("route", {
          on: (o) => o.type,
          cases: {
            express: ({ prev }) => succeed(`EXP:${prev.total}`),
          },
        })
        .build();
      await expect(
        runner.run({ workflow: wf, workflowId: "wf-sel-3", input: { type: "freight", total: 50 } }),
      ).rejects.toThrow(/no case for selector key "freight"/);
    });

    it("checkpoints the result like any other step", async () => {
      const storage = new InMemoryWorkflowStorage();
      const runner = createWorkflowRunner({ storage });
      const wf = workflow<Order>({ name: "match-cp" })
        .step("load", ({ input }) => succeed(input))
        .match("route", {
          on: (o) => o.type,
          cases: {
            express: () => succeed("FAST"),
            standard: () => succeed("OK"),
            freight: () => succeed("SLOW"),
          },
        })
        .build();
      await runner.run({
        workflow: wf,
        workflowId: "wf-cp-1",
        input: { type: "express", total: 1 },
      });

      const state = storage.getWorkflow("wf-cp-1");
      expect(state?.steps["route"]?.status).toBe("completed");
      expect(state?.steps["route"]?.result).toBe("FAST");
    });

    it("records the chosen case on StepState.metadata (selector mode)", async () => {
      const storage = new InMemoryWorkflowStorage();
      const runner = createWorkflowRunner({ storage });
      const wf = workflow<Order>({ name: "match-meta-sel" })
        .step("load", ({ input }) => succeed(input))
        .match("route", {
          on: (o) => o.type,
          cases: {
            express: () => succeed("FAST"),
            standard: () => succeed("OK"),
            freight: () => succeed("SLOW"),
          },
        })
        .build();
      await runner.run({
        workflow: wf,
        workflowId: "wf-meta-sel",
        input: { type: "freight", total: 1 },
      });

      const step = storage.getWorkflow("wf-meta-sel")!.steps["route"]!;
      expect(step.metadata).toEqual({ matchCase: "freight", matchMode: "selector" });
    });

    it("metadata records 'default' when the default case fires (selector mode)", async () => {
      const storage = new InMemoryWorkflowStorage();
      const runner = createWorkflowRunner({ storage });
      const wf = workflow<Order>({ name: "match-meta-default-sel" })
        .step("load", ({ input }) => succeed(input))
        .match("route", {
          on: (o) => o.type,
          cases: {
            express: () => succeed("FAST"),
          },
          default: () => succeed("FALLBACK"),
        })
        .build();
      await runner.run({
        workflow: wf,
        workflowId: "wf-meta-default-sel",
        input: { type: "standard", total: 1 },
      });

      const step = storage.getWorkflow("wf-meta-default-sel")!.steps["route"]!;
      expect(step.metadata).toEqual({ matchCase: "default", matchMode: "selector" });
    });

    it("metadata survives a failing branch (stored on the failure row)", async () => {
      // Use a typed failure so it flows through the engine's step failure
      // path. The point is: even when the branch fails, ops should still
      // see `matchCase` on the failed step row.
      class BranchError extends TaggedError("BranchError")<{
        readonly stepName: string;
        readonly message: string;
      }>() {}

      const storage = new InMemoryWorkflowStorage();
      const runner = createWorkflowRunner({ storage });
      const wf = workflow<Order>({ name: "match-meta-fail" })
        .step("load", ({ input }) => succeed(input))
        .match("route", {
          on: (o) => o.type,
          cases: {
            express: () =>
              fail(new BranchError({ stepName: "route", message: "branch blew up" })) as never,
            standard: () => succeed("OK"),
            freight: () => succeed("SLOW"),
          },
        })
        .build();
      await expect(
        runner.run({
          workflow: wf,
          workflowId: "wf-meta-fail",
          input: { type: "express", total: 1 },
        }),
      ).rejects.toThrow();

      const step = storage.getWorkflow("wf-meta-fail")!.steps["route"]!;
      expect(step.status).toBe("failed");
      // The chosen case is still visible for debugging even though the branch threw.
      expect(step.metadata).toEqual({ matchCase: "express", matchMode: "selector" });
    });

    it("can chain with downstream steps", async () => {
      const storage = new InMemoryWorkflowStorage();
      const runner = createWorkflowRunner({ storage });
      const wf = workflow<Order>({ name: "match-chain" })
        .step("load", ({ input }) => succeed(input))
        .match("route", {
          on: (o) => o.type,
          cases: {
            express: ({ prev }) => succeed(`exp-${prev.total}`),
            standard: ({ prev }) => succeed(`std-${prev.total}`),
            freight: ({ prev }) => succeed(`frt-${prev.total}`),
          },
        })
        .step("upper", ({ prev }) => succeed(prev.toUpperCase()))
        .build();
      const result = await runner.run({
        workflow: wf,
        workflowId: "wf-chain-1",
        input: { type: "express", total: 99 },
      });
      expect(result).toBe("EXP-99");
    });
  });

  describe("match (predicate mode)", () => {
    type Order = { type: string; total: number };

    it("first matching predicate wins, even when later ones would also match", async () => {
      const storage = new InMemoryWorkflowStorage();
      const runner = createWorkflowRunner({ storage });
      const wf = workflow<Order>({ name: "match-pred-order" })
        .step("load", ({ input }) => succeed(input))
        .match("route", {
          cases: [
            // VIP rule fires first — total > 10K wins even though express would too.
            {
              when: (o) => o.total > 10_000,
              then: ({ prev }) => succeed(`VIP:${prev.total}`),
            },
            {
              when: (o) => o.type === "express",
              then: ({ prev }) => succeed(`EXP:${prev.total}`),
            },
          ],
          default: ({ prev }) => succeed(`STD:${prev.total}`),
        })
        .build();
      const result = await runner.run({
        workflow: wf,
        workflowId: "wf-pred-1",
        input: { type: "express", total: 25_000 },
      });
      expect(result).toBe("VIP:25000");
    });

    it("falls back to default when no predicate matches", async () => {
      const storage = new InMemoryWorkflowStorage();
      const runner = createWorkflowRunner({ storage });
      const wf = workflow<Order>({ name: "match-pred-default" })
        .step("load", ({ input }) => succeed(input))
        .match("route", {
          cases: [
            {
              when: (o) => o.type === "express",
              then: ({ prev }) => succeed(`EXP:${prev.total}`),
            },
          ],
          default: ({ prev }) => succeed(`STD:${prev.total}`),
        })
        .build();
      const result = await runner.run({
        workflow: wf,
        workflowId: "wf-pred-2",
        input: { type: "standard", total: 50 },
      });
      expect(result).toBe("STD:50");
    });

    it("throws MatchError when nothing matches and no default", async () => {
      const storage = new InMemoryWorkflowStorage();
      const runner = createWorkflowRunner({ storage });
      const wf = workflow<Order>({ name: "match-pred-no-match" })
        .step("load", ({ input }) => succeed(input))
        .match("route", {
          cases: [{ when: (o) => o.type === "express", then: () => succeed("E") }],
        })
        .build();
      await expect(
        runner.run({ workflow: wf, workflowId: "wf-pred-3", input: { type: "ground", total: 1 } }),
      ).rejects.toThrow(/no predicate matched/);
    });

    it("records the matched case label on StepState.metadata (predicate mode)", async () => {
      const storage = new InMemoryWorkflowStorage();
      const runner = createWorkflowRunner({ storage });
      const wf = workflow<Order>({ name: "match-meta-pred" })
        .step("load", ({ input }) => succeed(input))
        .match("route", {
          cases: [
            {
              label: "vip",
              when: (o) => o.total > 10_000,
              then: () => succeed("VIP"),
            },
            {
              when: (o) => o.type === "express",
              then: () => succeed("EXP"),
            },
          ],
          default: () => succeed("STD"),
        })
        .build();
      await runner.run({
        workflow: wf,
        workflowId: "wf-meta-pred",
        input: { type: "standard", total: 25_000 },
      });

      const step = storage.getWorkflow("wf-meta-pred")!.steps["route"]!;
      expect(step.metadata).toEqual({ matchCase: "vip", matchMode: "predicate" });
    });

    it("metadata uses 'case[N]' fallback when label is omitted (predicate mode)", async () => {
      const storage = new InMemoryWorkflowStorage();
      const runner = createWorkflowRunner({ storage });
      const wf = workflow<Order>({ name: "match-meta-pred-idx" })
        .step("load", ({ input }) => succeed(input))
        .match("route", {
          cases: [
            { when: (o) => o.type === "express", then: () => succeed("E") },
            { when: (o) => o.type === "standard", then: () => succeed("S") },
          ],
        })
        .build();
      await runner.run({
        workflow: wf,
        workflowId: "wf-meta-pred-idx",
        input: { type: "standard", total: 1 },
      });

      const step = storage.getWorkflow("wf-meta-pred-idx")!.steps["route"]!;
      expect(step.metadata).toEqual({ matchCase: "case[1]", matchMode: "predicate" });
    });
  });

  // ---------------------------------------------------------------------------
  // sleep — durable timer
  // ---------------------------------------------------------------------------

  describe("sleep", () => {
    it("suspends workflow on first execution", async () => {
      const storage = new InMemoryWorkflowStorage();
      const runner = createWorkflowRunner({ storage });
      const wf = workflow<{}>({ name: "sleep-test" })
        .step("compute", () => succeed(42))
        .sleep("wait", 60_000)
        .build();
      const { error } = await runner.runSafe({
        workflow: wf,
        workflowId: "wf-sleep",
        input: {},
      });

      expect(error).not.toBeNull();
      expect((error as WorkflowSuspendedError)._tag).toBe("WorkflowSuspendedError");
      expect((error as WorkflowSuspendedError).reason).toBe("sleep");

      const state = storage.getWorkflow("wf-sleep");
      expect(state?.status).toBe("suspended");
      expect(state?.steps["wait"]?.status).toBe("sleeping");
      expect(state?.steps["wait"]?.wakeAt).toBeInstanceOf(Date);
    });

    it("resumes after wake time passes", async () => {
      const storage = new InMemoryWorkflowStorage();
      const runner = createWorkflowRunner({ storage });
      const wf = workflow<{}>({ name: "sleep-resume" })
        .step("before", () => succeed("ready"))
        .sleep("wait", 1) // 1ms sleep
        .step("after", ({ prev }) => succeed(`done: ${prev}`))
        .build();

      // First run: suspends
      const { error } = await runner.runSafe({
        workflow: wf,
        workflowId: "wf-sleep-resume",
        input: {},
      });
      expect((error as WorkflowSuspendedError)._tag).toBe("WorkflowSuspendedError");

      // Wait for the sleep to expire
      await new Promise((r) => setTimeout(r, 10));

      // Resume: should complete
      const result = await runner.run({ workflow: wf, workflowId: "wf-sleep-resume", input: {} });
      expect(result).toBe("done: ready");
    });
  });

  // ---------------------------------------------------------------------------
  // waitForSignal
  // ---------------------------------------------------------------------------

  describe("waitForSignal", () => {
    it("suspends while waiting for signal", async () => {
      const storage = new InMemoryWorkflowStorage();
      const runner = createWorkflowRunner({ storage });
      const wf = workflow<{}>({ name: "signal-test" })
        .step("compute", () => succeed(42))
        .waitForSignal("approval", { signalName: "manager-approved" })
        .build();
      const { error } = await runner.runSafe({
        workflow: wf,
        workflowId: "wf-signal",
        input: {},
      });

      expect(error).not.toBeNull();
      expect((error as WorkflowSuspendedError)._tag).toBe("WorkflowSuspendedError");
      expect((error as WorkflowSuspendedError).reason).toBe("signal");

      const state = storage.getWorkflow("wf-signal");
      expect(state?.status).toBe("suspended");
      expect(state?.steps["approval"]?.status).toBe("waiting_for_signal");
      expect(state?.steps["approval"]?.signalName).toBe("manager-approved");
    });

    it("resumes when signal is delivered", async () => {
      const storage = new InMemoryWorkflowStorage();
      const runner = createWorkflowRunner({ storage });
      const wf = workflow<{}>({ name: "signal-resume" })
        .step("before", () => succeed("ready"))
        .waitForSignal<{ approved: boolean }>("approval", {
          signalName: "manager-approved",
        })
        .step("after", ({ prev }) => succeed(`approved: ${prev.approved}`))
        .build();

      // First run: suspends
      await runner.runSafe({ workflow: wf, workflowId: "wf-signal-resume", input: {} });

      // Deliver signal
      await storage.deliverSignal({
        workflowId: "wf-signal-resume",
        signalName: "manager-approved",
        payload: { approved: true },
      });

      // Resume: should complete with signal payload
      const result = await runner.run({ workflow: wf, workflowId: "wf-signal-resume", input: {} });
      expect(result).toBe("approved: true");
    });

    it("times out if signal not delivered", async () => {
      const storage = new InMemoryWorkflowStorage();
      const runner = createWorkflowRunner({ storage });
      const wf = workflow<{}>({ name: "signal-timeout" })
        .step("before", () => succeed("ready"))
        .waitForSignal("approval", { signalName: "never-comes", timeoutMs: 1 })
        .build();

      // First run: suspends
      await runner.runSafe({ workflow: wf, workflowId: "wf-signal-timeout", input: {} });

      // Wait for timeout
      await new Promise((r) => setTimeout(r, 10));

      // Resume: should timeout
      const { error } = await runner.runSafe({
        workflow: wf,
        workflowId: "wf-signal-timeout",
        input: {},
      });
      expect(error).not.toBeNull();
      expect((error as WorkflowTimeoutError)._tag).toBe("WorkflowTimeoutError");
    });
  });

  // ---------------------------------------------------------------------------
  // .map() — pure transform
  // ---------------------------------------------------------------------------

  describe("map", () => {
    it("transforms the last step result", async () => {
      const storage = new InMemoryWorkflowStorage();
      const runner = createWorkflowRunner({ storage });
      const wf = workflow<{ n: number }>({ name: "map-test" })
        .step("compute", ({ input }) => succeed({ value: input.n * 2, extra: "data" }))
        .map((obj) => obj.value)
        .build();
      const result = await runner.run({ workflow: wf, workflowId: "wf-map", input: { n: 5 } });
      expect(result).toBe(10);
    });

    it("chains multiple maps", async () => {
      const storage = new InMemoryWorkflowStorage();
      const runner = createWorkflowRunner({ storage });
      const wf = workflow<{ s: string }>({ name: "multi-map" })
        .step("get", ({ input }) => succeed(input.s))
        .map((s) => s.toUpperCase())
        .map((s) => s + "!")
        .build();
      const result = await runner.run({
        workflow: wf,
        workflowId: "wf-multi-map",
        input: { s: "hello" },
      });
      expect(result).toBe("HELLO!");
    });
  });

  // ---------------------------------------------------------------------------
  // Resume after crash
  // ---------------------------------------------------------------------------

  describe("resume after crash", () => {
    it("resumes from last completed step", async () => {
      const storage = new InMemoryWorkflowStorage();
      const runner = createWorkflowRunner({ storage });
      let step2Calls = 0;

      const wf = workflow<{ value: number }>({ name: "resumable" })
        .step("step-1", ({ input }) => succeed(input.value + 1))
        .step("step-2", ({ prev }) => {
          step2Calls++;
          return succeed(prev * 10);
        })
        .build();

      await storage.createWorkflow({
        workflowId: "wf-resume",
        workflowName: "resumable",
        input: { value: 5 },
      });
      await storage.saveStepResult({
        workflowId: "wf-resume",
        stepName: "step-1",
        result: 6,
        durationMs: 10,
        startedAt: new Date(),
      });

      const result = await runner.run({
        workflow: wf,
        workflowId: "wf-resume",
        input: { value: 5 },
      });
      expect(result).toBe(60);
      expect(step2Calls).toBe(1);
    });

    it("skips all steps if fully completed in storage", async () => {
      const storage = new InMemoryWorkflowStorage();
      const runner = createWorkflowRunner({ storage });
      let callCount = 0;
      const now = new Date();

      await storage.createWorkflow({
        workflowId: "wf-done",
        workflowName: "done",
        input: { n: 1 },
      });
      await storage.saveStepResult({
        workflowId: "wf-done",
        stepName: "a",
        result: 10,
        durationMs: 5,
        startedAt: now,
      });
      await storage.saveStepResult({
        workflowId: "wf-done",
        stepName: "b",
        result: 20,
        durationMs: 5,
        startedAt: now,
      });

      const wf = workflow<{ n: number }>({ name: "done" })
        .step("a", ({ input }) => {
          callCount++;
          return succeed(input.n * 10);
        })
        .step("b", ({ prev }) => {
          callCount++;
          return succeed(prev * 2);
        })
        .build();
      const result = await runner.run({ workflow: wf, workflowId: "wf-done", input: { n: 1 } });

      expect(result).toBe(20);
      expect(callCount).toBe(0);
    });
  });

  // ---------------------------------------------------------------------------
  // Error handling
  // ---------------------------------------------------------------------------

  describe("error handling", () => {
    it("step failure marks workflow as failed", async () => {
      const storage = new InMemoryWorkflowStorage();
      const runner = createWorkflowRunner({ storage });
      const wf = workflow<{ url: string }>({ name: "failing" })
        .step("fetch", () => fail(new FetchError({ message: "404" })))
        .build();
      const { error } = await runner.runSafe({
        workflow: wf,
        workflowId: "wf-fail",
        input: { url: "https://example.com" },
      });

      expect(error).not.toBeNull();
      expect(storage.getWorkflow("wf-fail")?.status).toBe("failed");
    });

    it("runSafe returns data on success", async () => {
      const storage = new InMemoryWorkflowStorage();
      const runner = createWorkflowRunner({ storage });
      const wf = workflow<{ n: number }>({ name: "safe-ok" })
        .step("compute", ({ input }) => succeed(input.n * 2))
        .build();
      const { data, error } = await runner.runSafe({
        workflow: wf,
        workflowId: "wf-safe-ok",
        input: { n: 5 },
      });
      expect(data).toBe(10);
      expect(error).toBeNull();
    });

    it("runSafe returns error on failure", async () => {
      const storage = new InMemoryWorkflowStorage();
      const runner = createWorkflowRunner({ storage });
      const wf = workflow<{}>({ name: "safe-fail" })
        .step("boom", () => fail(new FetchError({ message: "oops" })))
        .build();
      const { data, error } = await runner.runSafe({
        workflow: wf,
        workflowId: "wf-safe-fail",
        input: {},
      });
      expect(data).toBeNull();
      expect(error).not.toBeNull();
    });
  });

  // ---------------------------------------------------------------------------
  // Locking
  // ---------------------------------------------------------------------------

  describe("locking", () => {
    it("rejects concurrent execution", async () => {
      const storage = new InMemoryWorkflowStorage();
      const runner = createWorkflowRunner({ storage });
      await storage.tryLock({ workflowId: "wf-locked", lockDurationMs: 60_000 });

      const wf = workflow<{}>({ name: "locked" })
        .step("noop", () => succeed("done"))
        .build();
      const { error } = await runner.runSafe({
        workflow: wf,
        workflowId: "wf-locked",
        input: {},
      });
      expect((error as WorkflowLockError)._tag).toBe("WorkflowLockError");
    });

    it("releases lock after success", async () => {
      const storage = new InMemoryWorkflowStorage();
      const runner = createWorkflowRunner({ storage });
      const wf = workflow<{}>({ name: "release" })
        .step("noop", () => succeed("ok"))
        .build();
      await runner.run({ workflow: wf, workflowId: "wf-release", input: {} });
      expect(
        (await storage.tryLock({ workflowId: "wf-release", lockDurationMs: 60_000 })).acquired,
      ).toBe(true);
    });

    it("releases lock on failure", async () => {
      const storage = new InMemoryWorkflowStorage();
      const runner = createWorkflowRunner({ storage });
      const wf = workflow<{}>({ name: "fail-release" })
        .step("boom", () => fail(new FetchError({ message: "fail" })))
        .build();
      await runner.runSafe({ workflow: wf, workflowId: "wf-fail-release", input: {} });
      expect(
        (await storage.tryLock({ workflowId: "wf-fail-release", lockDurationMs: 60_000 })).acquired,
      ).toBe(true);
    });
  });

  // ---------------------------------------------------------------------------
  // Duplicate step names
  // ---------------------------------------------------------------------------

  describe("duplicate step names", () => {
    it("throws on duplicate step name", () => {
      const storage = new InMemoryWorkflowStorage();
      expect(() =>
        workflow<{}>({ name: "dup" })
          .step("same", () => succeed(1))
          .step("same", () => succeed(2)),
      ).toThrow(/Duplicate step name/);
    });
  });

  // ---------------------------------------------------------------------------
  // Timestamps
  // ---------------------------------------------------------------------------

  describe("timestamps", () => {
    it("completed workflow has completedAt", async () => {
      const storage = new InMemoryWorkflowStorage();
      const runner = createWorkflowRunner({ storage });
      const wf = workflow<{}>({ name: "ts" })
        .step("noop", () => succeed("ok"))
        .build();
      await runner.run({ workflow: wf, workflowId: "wf-ts", input: {} });
      expect(storage.getWorkflow("wf-ts")?.completedAt).toBeInstanceOf(Date);
    });

    it("steps have startedAt, completedAt, durationMs", async () => {
      const storage = new InMemoryWorkflowStorage();
      const runner = createWorkflowRunner({ storage });
      const wf = workflow<{}>({ name: "ts-step" })
        .step("compute", () => succeed(42))
        .build();
      await runner.run({ workflow: wf, workflowId: "wf-ts-step", input: {} });

      const step = storage.getWorkflow("wf-ts-step")?.steps["compute"];
      expect(step?.startedAt).toBeInstanceOf(Date);
      expect(step?.completedAt).toBeInstanceOf(Date);
      expect(step?.durationMs).toBeGreaterThanOrEqual(0);
    });
  });

  // ---------------------------------------------------------------------------
  // Eff combinators inside steps
  // ---------------------------------------------------------------------------

  describe("Eff combinators inside steps", () => {
    it("steps can use map and flatMap", async () => {
      const storage = new InMemoryWorkflowStorage();
      const runner = createWorkflowRunner({ storage });
      const wf = workflow<{ n: number }>({ name: "pipeline-features" })
        .step("compute", ({ input }) =>
          succeed(input.n)
            .map((n) => n * 2)
            .flatMap((n) => succeed(n + 1)),
        )
        .build();
      const result = await runner.run({ workflow: wf, workflowId: "wf-features", input: { n: 5 } });
      expect(result).toBe(11);
    });

    it("steps can use all", async () => {
      const storage = new InMemoryWorkflowStorage();
      const runner = createWorkflowRunner({ storage });
      const wf = workflow<{}>({ name: "all-in-step" })
        .step("parallel", () =>
          all([succeed(1), succeed(2), succeed(3)]).map(([a, b, c]) => a + b + c),
        )
        .build();
      const result = await runner.run({ workflow: wf, workflowId: "wf-all", input: {} });
      expect(result).toBe(6);
    });
  });
});

// ---------------------------------------------------------------------------
// Eff error recovery (cats-aligned) — the combinators step bodies use
// ---------------------------------------------------------------------------

const promise = <A>(f: () => Promise<A>) => tryPromise(f, (e) => e).orDie();

describe("Eff.catch (handleError)", () => {
  it("handles any error with a plain function", async () => {
    const result = await fail(new FetchError({ message: "404" }))
      .catch((err) => succeed(`fallback: ${err._tag}`))
      .run();
    expect(result).toBe("fallback: FetchError");
  });

  it("passes through on success", async () => {
    const result = await succeed(42)
      .catch(() => succeed(0))
      .run();
    expect(result).toBe(42);
  });

  it("removes the error type", async () => {
    const { data, error } = await runSafe(
      fail(new FetchError({ message: "fail" })).catch(() => succeed("recovered")),
    );
    expect(data).toBe("recovered");
    expect(error).toBeNull();
  });
});

describe("Eff.catch with an Eff handler", () => {
  it("handles error with an Eff", async () => {
    const result = await fail(new FetchError({ message: "fail" }))
      .catch(() => succeed("from-eff"))
      .run();
    expect(result).toBe("from-eff");
  });

  it("can chain to a different Eff with different error", async () => {
    const { data } = await runSafe(
      fail(new FetchError({ message: "fail" })).catch(() => succeed(42)),
    );
    expect(data).toBe(42);
  });
});

describe("Eff.catch with async recovery", () => {
  it("handles error with async function", async () => {
    const result = await fail(new FetchError({ message: "404" }))
      .catch((err) => promise(async () => `fallback: ${err._tag}`))
      .run();
    expect(result).toBe("fallback: FetchError");
  });

  it("can do async work", async () => {
    const result = await fail(new FetchError({ message: "fail" }))
      .catch(() =>
        promise(async () => {
          await new Promise((r) => setTimeout(r, 5));
          return "async-recovered";
        }),
      )
      .run();
    expect(result).toBe("async-recovered");
  });
});

describe("Eff.recover", () => {
  it("recovers when predicate matches", async () => {
    const { data } = await runSafe(
      fail(new HttpStatusError({ status: 404, message: "Not Found" })).recover(
        (err) => err._tag === "HttpStatusError" && err.status === 404,
        () => null,
      ),
    );
    expect(data).toBeNull();
  });

  it("passes error through when predicate doesn't match", async () => {
    const { error } = await runSafe(
      fail(new HttpStatusError({ status: 500, message: "Server Error" })).recover(
        (err) => err._tag === "HttpStatusError" && err.status === 404,
        () => null,
      ),
    );
    expect(error).not.toBeNull();
    expect((error as HttpStatusError).status).toBe(500);
  });

  it("passes through on success", async () => {
    const result = await succeed("ok")
      .recover(
        () => true,
        () => "fallback",
      )
      .run();
    expect(result).toBe("ok");
  });

  it("receives the error in recovery fn", async () => {
    const { data } = await runSafe(
      fail(new HttpStatusError({ status: 404, message: "Not Found" })).recover(
        (err) => err._tag === "HttpStatusError",
        (err) => `recovered from ${err.status}`,
      ),
    );
    expect(data).toBe("recovered from 404");
  });
});

describe("Eff.recoverWith", () => {
  it("recovers with an Eff when predicate matches", async () => {
    const { data } = await runSafe(
      fail(new HttpStatusError({ status: 404, message: "Not Found" })).recoverWith(
        (err) => err._tag === "HttpStatusError" && err.status === 404,
        () => succeed("from-cache"),
      ),
    );
    expect(data).toBe("from-cache");
  });

  it("passes error through when predicate doesn't match", async () => {
    const { error } = await runSafe(
      fail(new HttpStatusError({ status: 500, message: "Server Error" })).recoverWith(
        (err) => err._tag === "HttpStatusError" && err.status === 404,
        () => succeed("from-cache"),
      ),
    );
    expect(error).not.toBeNull();
    expect((error as HttpStatusError).status).toBe(500);
  });
});

describe("Eff.recoverWith with async recovery", () => {
  it("recovers with async fn when predicate matches", async () => {
    const { data } = await runSafe(
      fail(new HttpStatusError({ status: 404, message: "Not Found" })).recoverWith(
        (err) => err._tag === "HttpStatusError" && err.status === 404,
        () => promise(async () => null),
      ),
    );
    expect(data).toBeNull();
  });

  it("passes error through when predicate doesn't match", async () => {
    const { error } = await runSafe(
      fail(new HttpStatusError({ status: 500, message: "Server Error" })).recoverWith(
        (err) => err._tag === "HttpStatusError" && err.status === 404,
        () => promise(async () => null),
      ),
    );
    expect(error).not.toBeNull();
    expect((error as HttpStatusError).status).toBe(500);
  });
});

describe("Eff.redeem", () => {
  it("maps error path", async () => {
    const result = await fail(new FetchError({ message: "fail" }))
      .redeem(
        (err) => `error: ${err._tag}`,
        (val) => `ok: ${val}`,
      )
      .run();
    expect(result).toBe("error: FetchError");
  });

  it("maps success path", async () => {
    const result = await succeed(42)
      .redeem(
        () => "error",
        (val) => `ok: ${val}`,
      )
      .run();
    expect(result).toBe("ok: 42");
  });

  it("always removes the error type", async () => {
    const { data, error } = await runSafe(
      fail(new FetchError({ message: "x" })).redeem(
        () => "recovered",
        (v) => v,
      ),
    );
    expect(data).toBe("recovered");
    expect(error).toBeNull();
  });
});

describe("Eff.redeemWith", () => {
  it("runs error Eff on failure", async () => {
    const result = await fail(new FetchError({ message: "fail" }))
      .redeemWith(
        (err) => succeed(`error: ${err._tag}`),
        (val) => succeed(`ok: ${val}`),
      )
      .run();
    expect(result).toBe("error: FetchError");
  });

  it("runs success Eff on success", async () => {
    const result = await succeed(42)
      .redeemWith(
        () => succeed(-1),
        (val) => succeed(val * 2),
      )
      .run();
    expect(result).toBe(84);
  });

  it("error Eff can itself fail", async () => {
    const { error } = await runSafe(
      fail(new FetchError({ message: "original" })).redeemWith(
        () => fail(new HttpStatusError({ status: 503, message: "unavailable" })),
        (_val: never) => succeed("ok" as const),
      ),
    );
    expect((error as HttpStatusError).status).toBe(503);
  });
});

describe("Eff.redeemWith with async handlers", () => {
  it("maps error path with async fn", async () => {
    const result = await fail(new FetchError({ message: "fail" }))
      .redeemWith(
        (err) => promise(async () => `error: ${err._tag}`),
        (val) => promise(async () => `ok: ${val}`),
      )
      .run();
    expect(result).toBe("error: FetchError");
  });

  it("maps success path with async fn", async () => {
    const result = await succeed(42)
      .redeemWith(
        () => promise(async () => -1),
        (val) => promise(async () => val * 2),
      )
      .run();
    expect(result).toBe(84);
  });

  it("can do async work in both paths", async () => {
    const result = await fail(new FetchError({ message: "x" }))
      .redeemWith(
        () =>
          promise(async () => {
            await new Promise((r) => setTimeout(r, 5));
            return "async-recovered";
          }),
        (v) => promise(async () => v),
      )
      .run();
    expect(result).toBe("async-recovered");
  });
});

// ---------------------------------------------------------------------------
// InMemoryWorkflowStorage
// ---------------------------------------------------------------------------

describe("InMemoryWorkflowStorage", () => {
  it("creates and loads workflow", async () => {
    const storage = new InMemoryWorkflowStorage();
    await storage.createWorkflow({ workflowId: "t1", workflowName: "test", input: { foo: "bar" } });
    const state = await storage.loadWorkflow("t1");
    expect(state?.workflowId).toBe("t1");
    expect(state?.status).toBe("pending");
    expect(state?.input).toEqual({ foo: "bar" });
  });

  it("returns null for non-existent workflow", async () => {
    const storage = new InMemoryWorkflowStorage();
    expect(await storage.loadWorkflow("nonexistent")).toBeNull();
  });

  it("saves and loads step results with timestamps", async () => {
    const storage = new InMemoryWorkflowStorage();
    await storage.createWorkflow({ workflowId: "t2", workflowName: "test", input: {} });
    const startedAt = new Date();
    await storage.saveStepResult({
      workflowId: "t2",
      stepName: "a",
      result: { x: 42 },
      durationMs: 100,
      startedAt,
    });
    const state = await storage.loadWorkflow("t2");
    expect(state?.steps["a"]?.status).toBe("completed");
    expect(state?.steps["a"]?.result).toEqual({ x: 42 });
    expect(state?.steps["a"]?.startedAt).toEqual(startedAt);
  });

  it("saves step failures", async () => {
    const storage = new InMemoryWorkflowStorage();
    await storage.createWorkflow({ workflowId: "t3", workflowName: "test", input: {} });
    await storage.saveStepFailure({
      workflowId: "t3",
      stepName: "bad",
      error: "broke",
      durationMs: 50,
      startedAt: new Date(),
    });
    expect((await storage.loadWorkflow("t3"))?.steps["bad"]?.status).toBe("failed");
  });

  it("saves and loads task results", async () => {
    const storage = new InMemoryWorkflowStorage();
    await storage.createWorkflow({ workflowId: "t-task", workflowName: "test", input: {} });
    await storage.saveTaskResult({
      workflowId: "t-task",
      stepName: "map-step",
      taskIndex: 0,
      result: "a",
    });
    await storage.saveTaskResult({
      workflowId: "t-task",
      stepName: "map-step",
      taskIndex: 1,
      result: "b",
    });

    const state = await storage.loadWorkflow("t-task");
    const tasks = state?.steps["map-step"]?.tasks;
    expect(tasks).toHaveLength(2);
    expect(tasks?.[0]?.result).toBe("a");
    expect(tasks?.[1]?.result).toBe("b");
  });

  it("saves task failures", async () => {
    const storage = new InMemoryWorkflowStorage();
    await storage.createWorkflow({ workflowId: "t-task-fail", workflowName: "test", input: {} });
    await storage.saveTaskFailure({
      workflowId: "t-task-fail",
      stepName: "s",
      taskIndex: 2,
      error: "boom",
    });

    const tasks = (await storage.loadWorkflow("t-task-fail"))?.steps["s"]?.tasks;
    expect(tasks?.[0]?.taskIndex).toBe(2);
    expect(tasks?.[0]?.status).toBe("failed");
    expect(tasks?.[0]?.error).toBe("boom");
  });

  it("completes workflow with completedAt", async () => {
    const storage = new InMemoryWorkflowStorage();
    await storage.createWorkflow({ workflowId: "t4", workflowName: "test", input: {} });
    await storage.completeWorkflow({ workflowId: "t4", result: "result" });
    const state = await storage.loadWorkflow("t4");
    expect(state?.status).toBe("completed");
    expect(state?.completedAt).toBeInstanceOf(Date);
  });

  it("suspends workflow", async () => {
    const storage = new InMemoryWorkflowStorage();
    await storage.createWorkflow({ workflowId: "t-suspend", workflowName: "test", input: {} });
    await storage.suspendWorkflow({
      workflowId: "t-suspend",
      stepName: "wait-step",
      stepUpdate: {
        status: "sleeping",
        stepType: "sleep",
        wakeAt: new Date(Date.now() + 60_000),
      },
    });

    const state = await storage.loadWorkflow("t-suspend");
    expect(state?.status).toBe("suspended");
    expect(state?.steps["wait-step"]?.status).toBe("sleeping");
  });

  it("delivers and loads signals", async () => {
    const storage = new InMemoryWorkflowStorage();
    await storage.deliverSignal({
      workflowId: "wf-1",
      signalName: "approval",
      payload: { ok: true },
    });
    await storage.deliverSignal({ workflowId: "wf-1", signalName: "other", payload: { data: 42 } });

    const signals = await storage.loadSignals("wf-1");
    expect(signals).toHaveLength(2);
    expect(signals[0]!.signalName).toBe("approval");
    expect(signals[0]!.payload).toEqual({ ok: true });
  });

  it("lock prevents double acquisition", async () => {
    const storage = new InMemoryWorkflowStorage();
    expect((await storage.tryLock({ workflowId: "l1", lockDurationMs: 60_000 })).acquired).toBe(
      true,
    );
    expect((await storage.tryLock({ workflowId: "l1", lockDurationMs: 60_000 })).acquired).toBe(
      false,
    );
  });

  it("lock can be released and re-acquired", async () => {
    const storage = new InMemoryWorkflowStorage();
    await storage.tryLock({ workflowId: "l2", lockDurationMs: 60_000 });
    await storage.releaseLock({ workflowId: "l2" });
    expect((await storage.tryLock({ workflowId: "l2", lockDurationMs: 60_000 })).acquired).toBe(
      true,
    );
  });

  it("expired lock can be re-acquired", async () => {
    // FakeWallClock advances time deterministically — no real wait, no flake.
    // Before clock injection this test slept 10ms to clear a 1ms lock.
    const clock = FakeWallClock.create("2026-01-01T00:00:00Z");
    const storage = new InMemoryWorkflowStorage({ clock });
    await storage.tryLock({ workflowId: "l3", lockDurationMs: 1 });
    clock.advance(10);
    expect((await storage.tryLock({ workflowId: "l3", lockDurationMs: 60_000 })).acquired).toBe(
      true,
    );
  });

  it("heartbeat extends the lock", async () => {
    // A heartbeat on an already-expired lock is rejected, so drive time with
    // a FakeWallClock: extend before the original expiry, then move past it.
    const clock = FakeWallClock.create("2026-01-01T00:00:00Z");
    const storage = new InMemoryWorkflowStorage({ clock });
    const { token } = await storage.tryLock({ workflowId: "l4", lockDurationMs: 1_000 });
    clock.advance(500);
    await storage.heartbeat({
      workflowId: "l4",
      lockDurationMs: 60_000,
      guard: token ? { fenceToken: token } : undefined,
    });
    clock.advance(1_000);
    expect((await storage.tryLock({ workflowId: "l4", lockDurationMs: 60_000 })).acquired).toBe(
      false,
    );
  });

  it("clear removes all data", async () => {
    const storage = new InMemoryWorkflowStorage();
    await storage.createWorkflow({ workflowId: "clear", workflowName: "t", input: {} });
    storage.clear();
    expect(await storage.loadWorkflow("clear")).toBeNull();
  });
});

// ---------------------------------------------------------------------------
// flow() — non-durable convenience
// ---------------------------------------------------------------------------

describe("flow", () => {
  it("executes a linear chain without storage/workflowId", async () => {
    const result = await flow<{ name: string }>("greet")
      .step("greet", ({ input }) => succeed(`Hello, ${input.name}`))
      .step("upper", ({ prev }) => succeed(prev.toUpperCase()))
      .execute({ name: "World" });

    expect(result).toBe("HELLO, WORLD");
  });

  it("executes a DAG", async () => {
    const result = await flow<{ text: string }>("analyze")
      .step("parse", ({ input }) => succeed(input.text.split(" ")))
      .step("count", { dependsOn: ["parse"] }, ({ deps }) => succeed(deps.parse.length))
      .step("join", { dependsOn: ["parse"] }, ({ deps }) => succeed(deps.parse.join("-")))
      .step("combine", { dependsOn: ["count", "join"] }, ({ deps }) =>
        succeed(`${deps.join} (${deps.count})`),
      )
      .execute({ text: "hello beautiful world" });

    expect(result).toBe("hello-beautiful-world (3)");
  });

  it("works with stepAsync", async () => {
    const result = await flow<{ n: number }>("compute")
      .stepAsync("double", async ({ input }) => input.n * 2)
      .stepAsync("add-ten", async ({ prev }) => prev + 10)
      .execute({ n: 5 });

    expect(result).toBe(20);
  });

  it("works with mapOver", async () => {
    const result = await flow<{ items: number[] }>("batch")
      .step("source", ({ input }) => succeed(input.items))
      .mapOver("double", { array: "source" }, (n) => succeed(n * 2))
      .execute({ items: [1, 2, 3] });

    expect(result).toEqual([2, 4, 6]);
  });

  it("works with branch", async () => {
    const result = await flow<{ n: number }>("classify")
      .step("get", ({ input }) => succeed(input.n))
      .branch("decide", {
        condition: (n) => n > 10,
        ifTrue: ({ prev }) => succeed(`big: ${prev}`),
        ifFalse: ({ prev }) => succeed(`small: ${prev}`),
      })
      .execute({ n: 42 });

    expect(result).toBe("big: 42");
  });

  it("works with map", async () => {
    const result = await flow<{ s: string }>("transform")
      .step("get", ({ input }) => succeed(input.s))
      .map((s) => s.length)
      .execute({ s: "hello" });

    expect(result).toBe(5);
  });

  it("execute on a flow surfaces step failures by throwing", async () => {
    await expect(
      flow<{}>("fail")
        .step("boom", () => fail(new FetchError({ message: "oops" })))
        .execute({}),
    ).rejects.toThrow();
  });

  it("accepts hooks", async () => {
    const steps: string[] = [];

    await flow<{ n: number }>("hooked", {
      onStepComplete: ({ stepName }) => {
        steps.push(stepName);
      },
    })
      .step("a", ({ input }) => succeed(input.n + 1))
      .step("b", ({ prev }) => succeed(prev * 2))
      .execute({ n: 5 });

    expect(steps).toEqual(["a", "b"]);
  });
});

// ---------------------------------------------------------------------------
// Subworkflows — invoke(), .subworkflow(), parent-child tracking
// ---------------------------------------------------------------------------

describe("Subworkflows", () => {
  describe(".subworkflow()", () => {
    it("invokes child with builder sugar", async () => {
      const storage = new InMemoryWorkflowStorage();
      const runner = createWorkflowRunner({ storage });
      const enrichUser = workflow<{ userId: string }>({ name: "enrich" })
        .step("fetch", ({ input }) => succeed({ userId: input.userId, score: 95 }))
        .build();

      const parent = workflow<{ userId: string }>({ name: "parent" })
        .step("create", ({ input }) => succeed({ id: input.userId, name: "Alice" }))
        .subworkflow("enrich", enrichUser, {
          input: (prev) => ({ userId: prev.id }),
          workflowId: (prev) => `enrich-${prev.id}`,
        })
        .build();
      const result = await runner.run({
        workflow: parent,
        workflowId: "parent-2",
        input: { userId: "u_1" },
      });

      expect(result).toEqual({ userId: "u_1", score: 95 });
    });

    it("chains after subworkflow", async () => {
      const storage = new InMemoryWorkflowStorage();
      const runner = createWorkflowRunner({ storage });
      const child = workflow<{ n: number }>({ name: "child" })
        .step("double", ({ input }) => succeed(input.n * 2))
        .build();

      const parent = workflow<{ n: number }>({ name: "parent" })
        .step("start", ({ input }) => succeed(input.n))
        .subworkflow("child", child, {
          input: (prev) => ({ n: prev }),
          workflowId: (prev) => `child-${prev}`,
        })
        .step("finish", ({ prev }) => succeed(prev + 100))
        .build();
      const result = await runner.run({
        workflow: parent,
        workflowId: "parent-3",
        input: { n: 5 },
      });

      expect(result).toBe(110); // 5 * 2 + 100
    });

    it("seeds parentWorkflowId on child rows", async () => {
      const storage = new InMemoryWorkflowStorage();
      const runner = createWorkflowRunner({ storage });
      const child = workflow<{ n: number }>({ name: "child" })
        .step("compute", ({ input }) => succeed(input.n))
        .build();

      const parent = workflow<{}>({ name: "parent" })
        .subworkflow("delegate", child, {
          input: () => ({ n: 42 }),
          workflowId: () => "child-1",
        })
        .build();

      await runner.run({ workflow: parent, workflowId: "parent-1", input: {} });

      const childState = await storage.loadWorkflow("child-1");
      expect(childState?.parentWorkflowId).toBe("parent-1");
    });
  });

  describe("parent-child tracking", () => {
    it("lists children by parentId", async () => {
      const storage = new InMemoryWorkflowStorage();
      const runner = createWorkflowRunner({ storage });
      const child = workflow<{ n: number }>({ name: "child" })
        .step("compute", ({ input }) => succeed(input.n))
        .build();

      const parent = workflow<{}>({ name: "parent" })
        .step("a", () => succeed(undefined))
        .subworkflow("first", child, {
          input: () => ({ n: 1 }),
          workflowId: () => "child-a",
        })
        .subworkflow("second", child, {
          input: () => ({ n: 2 }),
          workflowId: () => "child-b",
        })
        .build();

      await runner.run({ workflow: parent, workflowId: "parent-x", input: {} });

      const children = await storage.listWorkflows({ parentId: "parent-x" });
      expect(children).toHaveLength(2);
    });

    it("cascade cancel cancels children", async () => {
      const storage = new InMemoryWorkflowStorage();

      await storage.createWorkflow({
        workflowId: "parent-cancel",
        workflowName: "parent",
        input: {},
      });
      await storage.createWorkflow({
        workflowId: "child-cancel-1",
        workflowName: "child",
        input: {},
        parentWorkflowId: "parent-cancel",
      });
      await storage.createWorkflow({
        workflowId: "child-cancel-2",
        workflowName: "child",
        input: {},
        parentWorkflowId: "parent-cancel",
      });

      await storage.cancelWorkflow({ workflowId: "parent-cancel", cascade: true });

      expect((await storage.loadWorkflow("parent-cancel"))!.status).toBe("failed");
      expect((await storage.loadWorkflow("child-cancel-1"))!.status).toBe("failed");
      expect((await storage.loadWorkflow("child-cancel-2"))!.status).toBe("failed");
    });

    it("non-cascade cancel leaves children alone", async () => {
      const storage = new InMemoryWorkflowStorage();

      await storage.createWorkflow({
        workflowId: "parent-nocancel",
        workflowName: "parent",
        input: {},
      });
      await storage.createWorkflow({
        workflowId: "child-nocancel",
        workflowName: "child",
        input: {},
        parentWorkflowId: "parent-nocancel",
      });

      await storage.cancelWorkflow({ workflowId: "parent-nocancel" });

      expect((await storage.loadWorkflow("parent-nocancel"))!.status).toBe("failed");
      expect((await storage.loadWorkflow("child-nocancel"))!.status).toBe("pending");
    });
  });
});

// ---------------------------------------------------------------------------
// Step failure strategies — retry, skip, fallback
// ---------------------------------------------------------------------------

describe("Step failure strategies", () => {
  describe("step retry", () => {
    it("retries a failing step", async () => {
      let attempts = 0;
      const result = await flow<{}>("retry-test")
        .step(
          "flaky",
          () => {
            attempts++;
            if (attempts < 3) return fail(new FetchError({ message: "transient" }));
            return succeed("ok");
          },
          { retry: { maxRetries: 5 } },
        )
        .execute({});

      expect(result).toBe("ok");
      expect(attempts).toBe(3);
    });
  });

  describe("onFailure: skip", () => {
    it("skips failed step and continues", async () => {
      const result = await flow<{}>("skip-test")
        .step("optional", () => fail(new FetchError({ message: "fail" })), {
          onFailure: "skip",
        })
        .step("next", () => succeed("continued"))
        .execute({});

      expect(result).toBe("continued");
    });
  });

  describe("onFailure: fallback", () => {
    it("uses fallback value on failure", async () => {
      const result = await flow<{}>("fallback-test")
        .step("risky", () => fail(new FetchError({ message: "fail" })), {
          onFailure: { fallback: () => "default-value" },
        })
        .step("use-it", ({ prev }) => succeed(`got: ${prev}`))
        .execute({});

      expect(result).toBe("got: default-value");
    });

    it("fallback receives the error", async () => {
      const result = await flow<{}>("fallback-err")
        .step("risky", () => fail(new FetchError({ message: "specific-error" })), {
          onFailure: {
            fallback: (err) => `recovered from ${(err as FetchError).message}`,
          },
        })
        .execute({});

      expect(result).toBe("recovered from specific-error");
    });
  });

  describe("retry + fallback combined", () => {
    it("retries then falls back", async () => {
      let attempts = 0;
      const result = await flow<{}>("retry-fallback")
        .step(
          "always-fail",
          () => {
            attempts++;
            return fail(new FetchError({ message: "always" }));
          },
          {
            retry: { maxRetries: 2 },
            onFailure: { fallback: () => "gave-up" },
          },
        )
        .execute({});

      expect(result).toBe("gave-up");
      expect(attempts).toBeGreaterThan(1);
    });
  });

  describe("step retry with when predicate", () => {
    it("retries only matching errors", async () => {
      let attempts = 0;
      const result = await flow<{}>("retry-when")
        .step(
          "flaky",
          () => {
            attempts++;
            if (attempts < 3) return fail(new FetchError({ message: "transient" }));
            return succeed("ok");
          },
          {
            retry: {
              maxRetries: 5,
              when: (err) => err._tag === "FetchError",
            },
          },
        )
        .execute({});

      expect(result).toBe("ok");
      expect(attempts).toBe(3);
    });

    it("does not retry non-matching errors", async () => {
      let attempts = 0;
      await expect(
        flow<{}>("retry-when-no-match")
          .step(
            "fail",
            (): StepEff<string, FetchError | HttpStatusError> => {
              attempts++;
              return fail(new HttpStatusError({ status: 400, message: "not retryable" }));
            },
            {
              retry: {
                maxRetries: 5,
                when: (err) => err._tag === "FetchError", // only retry FetchError
              },
            },
          )
          .execute({}),
      ).rejects.toThrow();

      expect(attempts).toBe(1); // no retry — HttpStatusError doesn't match
    });
  });

  describe("default: fail", () => {
    it("fails the workflow by default", async () => {
      await expect(
        flow<{}>("fail-default")
          .step("boom", () => fail(new FetchError({ message: "fail" })))
          .execute({}),
      ).rejects.toThrow();
    });
  });
});

// ---------------------------------------------------------------------------
// Per-step activity timeout
// ---------------------------------------------------------------------------

describe("Per-step activity timeout", () => {
  it("step times out after timeoutMs", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const wf = workflow<{}>({ name: "step-timeout" })
      .stepAsync(
        "slow",
        async () => {
          await new Promise((r) => setTimeout(r, 500));
          return "done";
        },
        { timeoutMs: 30 },
      )
      .build();

    const { error } = await runner.runSafe({
      workflow: wf,
      workflowId: "t-step-timeout",
      input: {},
    });
    expect(error).not.toBeNull();
    expect((error as StepTimeoutError)._tag).toBe("StepTimeoutError");
    expect((error as StepTimeoutError).stepName).toBe("slow");
    expect((error as StepTimeoutError).timeoutMs).toBe(30);
  });

  it("step without timeout runs normally", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const wf = workflow<{}>({ name: "no-timeout" })
      .stepAsync("fast", async () => "done")
      .build();

    const result = await runner.run({ workflow: wf, workflowId: "t-no-timeout", input: {} });
    expect(result).toBe("done");
  });

  it("step completes within timeout succeeds", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const wf = workflow<{}>({ name: "within-timeout" })
      .stepAsync(
        "quick",
        async () => {
          await new Promise((r) => setTimeout(r, 5));
          return "ok";
        },
        { timeoutMs: 5000 },
      )
      .build();

    const result = await runner.run({ workflow: wf, workflowId: "t-within-timeout", input: {} });
    expect(result).toBe("ok");
  });

  it("Eff-returning step times out", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const wf = workflow<{}>({ name: "pipeline-step-timeout" })
      .step(
        "slow-pipeline",
        () =>
          tryPromise(
            async () => {
              await new Promise((r) => setTimeout(r, 500));
              return "done";
            },
            (e) => e,
          ).orDie(),
        { timeoutMs: 30 },
      )
      .build();

    const { error } = await runner.runSafe({
      workflow: wf,
      workflowId: "t-pipeline-timeout",
      input: {},
    });
    expect(error).not.toBeNull();
    expect((error as StepTimeoutError)._tag).toBe("StepTimeoutError");
    expect((error as StepTimeoutError).stepName).toBe("slow-pipeline");
  });
});

// ---------------------------------------------------------------------------
// Workflow global deadline
// ---------------------------------------------------------------------------

describe("Workflow global deadline", () => {
  it("workflow times out after global deadline", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    // 3 steps each taking 30ms = ~90ms total, deadline at 50ms
    // After step1 completes (~30ms < 50ms), step2 starts.
    // After step2 completes (~60ms > 50ms), deadline check triggers before step3.
    const wf = workflow<{}>({ name: "deadline-test", timeoutMs: 50 })
      .stepAsync("step1", async () => {
        await new Promise((r) => setTimeout(r, 30));
        return "a";
      })
      .stepAsync("step2", async () => {
        await new Promise((r) => setTimeout(r, 30));
        return "b";
      })
      .stepAsync("step3", async () => {
        await new Promise((r) => setTimeout(r, 30));
        return "c";
      })
      .build();

    const { error } = await runner.runSafe({ workflow: wf, workflowId: "t-deadline", input: {} });
    expect(error).not.toBeNull();
    expect((error as WorkflowDeadlineError)._tag).toBe("WorkflowDeadlineError");
    expect((error as WorkflowDeadlineError).timeoutMs).toBe(50);
  });

  it("workflow within deadline completes normally", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const wf = workflow<{}>({ name: "within-deadline", timeoutMs: 5000 })
      .stepAsync("step1", async () => "a")
      .stepAsync("step2", async () => "b")
      .build();

    const result = await runner.run({ workflow: wf, workflowId: "t-within-deadline", input: {} });
    expect(result).toBe("b");
  });

  it("workflow without timeoutMs has no deadline", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const wf = workflow<{}>({ name: "no-deadline" })
      .stepAsync("step1", async () => {
        await new Promise((r) => setTimeout(r, 10));
        return "a";
      })
      .stepAsync("step2", async () => {
        await new Promise((r) => setTimeout(r, 10));
        return "b";
      })
      .build();

    const result = await runner.run({ workflow: wf, workflowId: "t-no-deadline", input: {} });
    expect(result).toBe("b");
  });
});

// ---------------------------------------------------------------------------
// .guard() — precondition assertions
// ---------------------------------------------------------------------------

describe("guard", () => {
  it("passes through when predicate returns true", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const wf = workflow<{ paid: boolean }>({ name: "guard-pass" })
      .guard("ensure-paid", (input) => input.paid)
      .stepAsync("ship", async () => "shipped")
      .build();
    const result = await runner.run({ workflow: wf, workflowId: "g-1", input: { paid: true } });

    expect(result).toBe("shipped");
  });

  it("fails with GuardError when predicate returns false", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const wf = workflow<{ paid: boolean }>({ name: "guard-fail" })
      .guard("ensure-paid", (input) => input.paid, {
        failureMessage: "Cannot ship unpaid order",
      })
      .stepAsync("ship", async () => "shipped")
      .build();

    await expect(
      runner.run({ workflow: wf, workflowId: "g-2", input: { paid: false } }),
    ).rejects.toThrow("Cannot ship unpaid order");
  });

  it("uses default failure message when none provided", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const wf = workflow<{ ok: boolean }>({ name: "guard-default-msg" })
      .guard("check", (input) => input.ok)
      .stepAsync("next", async () => "done")
      .build();

    await expect(
      runner.run({ workflow: wf, workflowId: "g-3", input: { ok: false } }),
    ).rejects.toThrow('Guard "check" failed');
  });

  it("passes prev value through to next step", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const wf = workflow<{ items: string[] }>({ name: "guard-passthrough" })
      .guard("has-items", (input) => input.items.length > 0)
      .step("count", ({ prev }) => succeed(prev.items.length))
      .build();
    const result = await runner.run({
      workflow: wf,
      workflowId: "g-4",
      input: { items: ["a", "b"] },
    });

    expect(result).toBe(2);
  });
});

// ---------------------------------------------------------------------------
// skipWhen — conditional step skip
// ---------------------------------------------------------------------------

describe("skipWhen", () => {
  it("skips step when predicate returns true", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    let stepRan = false;
    const wf = workflow<{ n: number }>({ name: "skip-true" })
      .stepAsync(
        "maybe",
        async ({ input }) => {
          stepRan = true;
          return input.n * 2;
        },
        { skipWhen: (prev: unknown) => (prev as { n: number }).n === 0 },
      )
      .build();
    const result = await runner.run({ workflow: wf, workflowId: "s-1", input: { n: 0 } });

    expect(stepRan).toBe(false);
    expect(result).toEqual({ n: 0 });
  });

  it("runs step when predicate returns false", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    let stepRan = false;
    const wf = workflow<{ n: number }>({ name: "skip-false" })
      .stepAsync(
        "maybe",
        async ({ input }) => {
          stepRan = true;
          return input.n * 2;
        },
        { skipWhen: (prev: unknown) => (prev as { n: number }).n === 0 },
      )
      .build();
    const result = await runner.run({ workflow: wf, workflowId: "s-2", input: { n: 5 } });

    expect(stepRan).toBe(true);
    expect(result).toBe(10);
  });

  it("uses skipValue when provided", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const wf = workflow<{ n: number }>({ name: "skip-value" })
      .stepAsync("double", async ({ input }) => input.n * 2, {
        skipWhen: (prev: unknown) => (prev as { n: number }).n === 0,
        skipValue: () => -1,
      })
      .build();
    const result = await runner.run({ workflow: wf, workflowId: "s-3", input: { n: 0 } });

    expect(result).toBe(-1);
  });

  it("does not trigger onStepComplete hook when skipped", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const completedSteps: string[] = [];
    const wf = workflow<{ skip: boolean }>({
      name: "skip-hook",
      hooks: {
        onStepComplete: async ({ stepName }) => {
          completedSteps.push(stepName);
        },
      },
    })
      .stepAsync("first", async () => "a")
      .stepAsync("skippable", async () => "b", {
        skipWhen: (prev: unknown) => (prev as string) === "a",
      })
      .stepAsync("last", async () => "c")
      .build();
    const result = await runner.run({ workflow: wf, workflowId: "s-4", input: { skip: true } });

    expect(result).toBe("c");
    expect(completedSteps).toContain("first");
    expect(completedSteps).not.toContain("skippable");
    expect(completedSteps).toContain("last");
  });
});
