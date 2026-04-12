import { describe, it, expect } from "bun:test";
import { Data } from "effect";
import { Pipeline } from "@promin/core";
import { workflow, flow } from "./durable-pipeline.ts";
import { InMemoryWorkflowStorage } from "./in-memory-storage.ts";
import {
  WorkflowLockError,
  WorkflowSuspendedError,
  WorkflowTimeoutError,
  StepTimeoutError,
  WorkflowDeadlineError,
} from "./durable-pipeline-error.ts";
import { topologicalSort, computeReadySet } from "./workflow-dag.ts";

// ---------------------------------------------------------------------------
// Test error types
// ---------------------------------------------------------------------------

class FetchError extends Data.TaggedError("FetchError")<{
  readonly message: string;
}> {}

class HttpStatusError extends Data.TaggedError("HttpStatusError")<{
  readonly status: number;
  readonly message: string;
}> {}

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
      const result = await workflow<{ value: number }>({ name: "single-step", storage })
        .step("double", ({ input }) => Pipeline.succeed(input.value * 2))
        .run({ workflowId: "wf-1", input: { value: 21 } });
      expect(result).toBe(42);
    });

    it("chains multiple steps", async () => {
      const storage = new InMemoryWorkflowStorage();
      const result = await workflow<{ name: string }>({ name: "multi-step", storage })
        .step("greet", ({ input }) => Pipeline.succeed(`Hello, ${input.name}`))
        .step("upper", ({ prev }) => Pipeline.succeed(prev.toUpperCase()))
        .step("exclaim", ({ prev }) => Pipeline.succeed(`${prev}!`))
        .run({ workflowId: "wf-2", input: { name: "World" } });
      expect(result).toBe("HELLO, WORLD!");
    });

    it("passes input to all steps", async () => {
      const storage = new InMemoryWorkflowStorage();
      const result = await workflow<{ x: number; y: number }>({ name: "input-access", storage })
        .step("sum", ({ input }) => Pipeline.succeed(input.x + input.y))
        .step("multiply", ({ input, prev }) => Pipeline.succeed(prev * input.x))
        .run({ workflowId: "wf-3", input: { x: 3, y: 4 } });
      expect(result).toBe(21);
    });

    it("stores workflow state as completed", async () => {
      const storage = new InMemoryWorkflowStorage();
      await workflow<{ value: number }>({ name: "state-check", storage })
        .step("compute", ({ input }) => Pipeline.succeed(input.value + 1))
        .run({ workflowId: "wf-state", input: { value: 10 } });

      const state = storage.getWorkflow("wf-state");
      expect(state?.status).toBe("completed");
      expect(state?.result).toBe(11);
      expect(state?.steps["compute"]?.status).toBe("completed");
    });

    it("stores step results for each step", async () => {
      const storage = new InMemoryWorkflowStorage();
      await workflow<{ n: number }>({ name: "step-results", storage })
        .step("add-one", ({ input }) => Pipeline.succeed(input.n + 1))
        .step("double", ({ prev }) => Pipeline.succeed(prev * 2))
        .run({ workflowId: "wf-steps", input: { n: 5 } });

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
      const executionOrder: string[] = [];

      const result = await workflow<{ text: string }>({ name: "dag-parallel", storage })
        .step("parse", ({ input }) => {
          executionOrder.push("parse");
          return Pipeline.succeed(input.text);
        })
        .step("summarize", { dependsOn: ["parse"] }, ({ deps }) => {
          executionOrder.push("summarize");
          return Pipeline.succeed(`Summary: ${deps.parse.slice(0, 10)}`);
        })
        .step("keywords", { dependsOn: ["parse"] }, ({ deps }) => {
          executionOrder.push("keywords");
          return Pipeline.succeed(deps.parse.split(" ").slice(0, 3));
        })
        .step("publish", { dependsOn: ["summarize", "keywords"] }, ({ deps }) => {
          executionOrder.push("publish");
          return Pipeline.succeed({ summary: deps.summarize, keywords: deps.keywords });
        })
        .run({ workflowId: "wf-dag", input: { text: "hello world from the DAG" } });

      expect(result).toEqual({
        summary: "Summary: hello worl",
        keywords: ["hello", "world", "from"],
      });
      expect(executionOrder[0]).toBe("parse");
      expect(executionOrder[executionOrder.length - 1]).toBe("publish");
    });

    it("correctly types deps in DAG steps", async () => {
      const storage = new InMemoryWorkflowStorage();
      const result = await workflow<{ id: number }>({ name: "dag-types", storage })
        .step("fetch", ({ input }) => Pipeline.succeed({ name: `User ${input.id}`, age: 30 }))
        .step("format-name", { dependsOn: ["fetch"] }, ({ deps }) =>
          Pipeline.succeed(deps.fetch.name.toUpperCase()),
        )
        .step("format-age", { dependsOn: ["fetch"] }, ({ deps }) =>
          Pipeline.succeed(`Age: ${deps.fetch.age}`),
        )
        .step("combine", { dependsOn: ["format-name", "format-age"] }, ({ deps }) =>
          Pipeline.succeed(`${deps["format-name"]} - ${deps["format-age"]}`),
        )
        .run({ workflowId: "wf-dag-types", input: { id: 42 } });
      expect(result).toBe("USER 42 - Age: 30");
    });

    it("DAG with multiple roots", async () => {
      const storage = new InMemoryWorkflowStorage();
      const noDeps: "left"[] = [];
      const result = await workflow<{ a: number; b: number }>({ name: "multi-root", storage })
        .step("left", ({ input }) => Pipeline.succeed(input.a * 10))
        .step("right", { dependsOn: noDeps }, ({ input }) => Pipeline.succeed(input.b * 10))
        .step("merge", { dependsOn: ["left", "right"] }, ({ deps }) =>
          Pipeline.succeed(deps.left + deps.right),
        )
        .run({ workflowId: "wf-multi-root", input: { a: 3, b: 4 } });
      expect(result).toBe(70);
    });
  });

  // ---------------------------------------------------------------------------
  // stepAsync
  // ---------------------------------------------------------------------------

  describe("stepAsync", () => {
    it("executes a single async step", async () => {
      const storage = new InMemoryWorkflowStorage();
      const result = await workflow<{ value: number }>({ name: "async-single", storage })
        .stepAsync("double", async ({ input }) => input.value * 2)
        .run({ workflowId: "wf-async-1", input: { value: 21 } });
      expect(result).toBe(42);
    });

    it("chains stepAsync with step", async () => {
      const storage = new InMemoryWorkflowStorage();
      const result = await workflow<{ name: string }>({ name: "async-chain", storage })
        .stepAsync("fetch", async ({ input }) => ({ name: input.name, id: 1 }))
        .step("format", ({ prev }) => Pipeline.succeed(`${prev.name} (${prev.id})`))
        .run({ workflowId: "wf-async-2", input: { name: "Alice" } });
      expect(result).toBe("Alice (1)");
    });

    it("DAG mode with stepAsync", async () => {
      const storage = new InMemoryWorkflowStorage();
      const result = await workflow<{ text: string }>({ name: "async-dag", storage })
        .stepAsync("parse", async ({ input }) => input.text.split(" "))
        .stepAsync("count", { dependsOn: ["parse"] }, async ({ deps }) => deps.parse.length)
        .stepAsync("join", { dependsOn: ["parse"] }, async ({ deps }) => deps.parse.join("-"))
        .stepAsync(
          "result",
          { dependsOn: ["count", "join"] },
          async ({ deps }) => `${deps.join} (${deps.count} words)`,
        )
        .run({ workflowId: "wf-async-dag", input: { text: "hello beautiful world" } });
      expect(result).toBe("hello-beautiful-world (3 words)");
    });
  });

  // ---------------------------------------------------------------------------
  // mapOver — fan-out
  // ---------------------------------------------------------------------------

  describe("mapOver", () => {
    it("fans out over an array", async () => {
      const storage = new InMemoryWorkflowStorage();
      const result = await workflow<{ urls: string[] }>({ name: "fan-out", storage })
        .step("get-urls", ({ input }) => Pipeline.succeed(input.urls))
        .mapOver("fetch-all", { array: "get-urls" }, (url) =>
          Pipeline.succeed(`Response from ${url}`),
        )
        .run({
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
      const result = await workflow<{ items: number[] }>({ name: "order", storage })
        .step("source", ({ input }) => Pipeline.succeed(input.items))
        .mapOver("double", { array: "source" }, (n) => Pipeline.succeed(n * 2))
        .run({ workflowId: "wf-order", input: { items: [1, 2, 3, 4, 5] } });
      expect(result).toEqual([2, 4, 6, 8, 10]);
    });

    it("respects concurrency limit", async () => {
      const storage = new InMemoryWorkflowStorage();
      let maxConcurrent = 0;
      let currentConcurrent = 0;

      const result = await workflow<{ items: number[] }>({ name: "concurrency", storage })
        .step("source", ({ input }) => Pipeline.succeed(input.items))
        .mapOver("process", { array: "source", concurrency: 2 }, (n) =>
          Pipeline.fromPromise(async () => {
            currentConcurrent++;
            maxConcurrent = Math.max(maxConcurrent, currentConcurrent);
            await new Promise((r) => setTimeout(r, 10));
            currentConcurrent--;
            return n * 10;
          }),
        )
        .run({ workflowId: "wf-conc", input: { items: [1, 2, 3, 4] } });

      expect(result).toEqual([10, 20, 30, 40]);
      expect(maxConcurrent).toBeLessThanOrEqual(2);
    });

    it("saves per-task results", async () => {
      const storage = new InMemoryWorkflowStorage();
      await workflow<{ items: string[] }>({ name: "task-persist", storage })
        .step("source", ({ input }) => Pipeline.succeed(input.items))
        .mapOver("process", { array: "source" }, (item) => Pipeline.succeed(item.toUpperCase()))
        .run({ workflowId: "wf-tasks", input: { items: ["a", "b"] } });

      const state = storage.getWorkflow("wf-tasks");
      const step = state?.steps["process"];
      expect(step?.status).toBe("completed");
      expect(step?.result).toEqual(["A", "B"]);
    });

    it("mapOver with empty array", async () => {
      const storage = new InMemoryWorkflowStorage();
      const result = await workflow<{ items: number[] }>({ name: "empty-map", storage })
        .step("source", ({ input }) => Pipeline.succeed(input.items))
        .mapOver("process", { array: "source" }, (n) => Pipeline.succeed(n * 2))
        .run({ workflowId: "wf-empty-map", input: { items: [] } });
      expect(result).toEqual([]);
    });

    it("mapOver provides MapStepContext", async () => {
      const storage = new InMemoryWorkflowStorage();
      const contexts: { taskIndex: number; workflowId: string }[] = [];

      await workflow<{ items: string[] }>({ name: "ctx-test", storage })
        .step("source", ({ input }) => Pipeline.succeed(input.items))
        .mapOver("process", { array: "source" }, (_item, ctx) => {
          contexts.push({ taskIndex: ctx.taskIndex, workflowId: ctx.workflowId });
          return Pipeline.succeed("ok");
        })
        .run({ workflowId: "wf-ctx", input: { items: ["a", "b", "c"] } });

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
      const result = await workflow<{ nums: number[] }>({ name: "async-map", storage })
        .step("source", ({ input }) => Pipeline.succeed(input.nums))
        .mapOverAsync("double", { array: "source" }, async (n) => n * 2)
        .run({ workflowId: "wf-async-map", input: { nums: [1, 2, 3] } });
      expect(result).toEqual([2, 4, 6]);
    });
  });

  // ---------------------------------------------------------------------------
  // branch
  // ---------------------------------------------------------------------------

  describe("branch", () => {
    it("takes the ifTrue branch when condition is true", async () => {
      const storage = new InMemoryWorkflowStorage();
      const result = await workflow<{ n: number }>({ name: "branch-true", storage })
        .step("compute", ({ input }) => Pipeline.succeed(input.n))
        .branch("decide", {
          condition: (n) => n > 10,
          ifTrue: ({ prev }) => Pipeline.succeed(`big: ${prev}`),
          ifFalse: ({ prev }) => Pipeline.succeed(`small: ${prev}`),
        })
        .run({ workflowId: "wf-branch-true", input: { n: 42 } });
      expect(result).toBe("big: 42");
    });

    it("takes the ifFalse branch when condition is false", async () => {
      const storage = new InMemoryWorkflowStorage();
      const result = await workflow<{ n: number }>({ name: "branch-false", storage })
        .step("compute", ({ input }) => Pipeline.succeed(input.n))
        .branch("decide", {
          condition: (n) => n > 10,
          ifTrue: ({ prev }) => Pipeline.succeed(`big: ${prev}`),
          ifFalse: ({ prev }) => Pipeline.succeed(`small: ${prev}`),
        })
        .run({ workflowId: "wf-branch-false", input: { n: 3 } });
      expect(result).toBe("small: 3");
    });

    it("branch result is checkpointed", async () => {
      const storage = new InMemoryWorkflowStorage();
      await workflow<{ n: number }>({ name: "branch-cp", storage })
        .step("compute", ({ input }) => Pipeline.succeed(input.n))
        .branch("decide", {
          condition: (n) => n > 0,
          ifTrue: () => Pipeline.succeed("positive"),
          ifFalse: () => Pipeline.succeed("non-positive"),
        })
        .run({ workflowId: "wf-branch-cp", input: { n: 5 } });

      const state = storage.getWorkflow("wf-branch-cp");
      expect(state?.steps["decide"]?.status).toBe("completed");
      expect(state?.steps["decide"]?.result).toBe("positive");
    });

    it("branch can chain with more steps", async () => {
      const storage = new InMemoryWorkflowStorage();
      const result = await workflow<{ n: number }>({ name: "branch-chain", storage })
        .step("compute", ({ input }) => Pipeline.succeed(input.n))
        .branch("classify", {
          condition: (n) => n % 2 === 0,
          ifTrue: ({ prev }) => Pipeline.succeed(`even:${prev}`),
          ifFalse: ({ prev }) => Pipeline.succeed(`odd:${prev}`),
        })
        .step("format", ({ prev }) => Pipeline.succeed(prev.toUpperCase()))
        .run({ workflowId: "wf-branch-chain", input: { n: 4 } });
      expect(result).toBe("EVEN:4");
    });
  });

  // ---------------------------------------------------------------------------
  // sleep — durable timer
  // ---------------------------------------------------------------------------

  describe("sleep", () => {
    it("suspends workflow on first execution", async () => {
      const storage = new InMemoryWorkflowStorage();
      const { error } = await workflow<{}>({ name: "sleep-test", storage })
        .step("compute", () => Pipeline.succeed(42))
        .sleep("wait", 60_000)
        .runSafe({ workflowId: "wf-sleep", input: {} });

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
      const wf = () =>
        workflow<{}>({ name: "sleep-resume", storage })
          .step("before", () => Pipeline.succeed("ready"))
          .sleep("wait", 1) // 1ms sleep
          .step("after", ({ prev }) => Pipeline.succeed(`done: ${prev}`));

      // First run: suspends
      const { error } = await wf().runSafe({ workflowId: "wf-sleep-resume", input: {} });
      expect((error as WorkflowSuspendedError)._tag).toBe("WorkflowSuspendedError");

      // Wait for the sleep to expire
      await new Promise((r) => setTimeout(r, 10));

      // Resume: should complete
      const result = await wf().run({ workflowId: "wf-sleep-resume", input: {} });
      expect(result).toBe("done: undefined");
    });
  });

  // ---------------------------------------------------------------------------
  // waitForSignal
  // ---------------------------------------------------------------------------

  describe("waitForSignal", () => {
    it("suspends while waiting for signal", async () => {
      const storage = new InMemoryWorkflowStorage();
      const { error } = await workflow<{}>({ name: "signal-test", storage })
        .step("compute", () => Pipeline.succeed(42))
        .waitForSignal("approval", { signalName: "manager-approved" })
        .runSafe({ workflowId: "wf-signal", input: {} });

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
      const wf = () =>
        workflow<{}>({ name: "signal-resume", storage })
          .step("before", () => Pipeline.succeed("ready"))
          .waitForSignal<{ approved: boolean }>("approval", {
            signalName: "manager-approved",
          })
          .step("after", ({ prev }) => Pipeline.succeed(`approved: ${prev.approved}`));

      // First run: suspends
      await wf().runSafe({ workflowId: "wf-signal-resume", input: {} });

      // Deliver signal
      await storage.deliverSignal("wf-signal-resume", "manager-approved", { approved: true });

      // Resume: should complete with signal payload
      const result = await wf().run({ workflowId: "wf-signal-resume", input: {} });
      expect(result).toBe("approved: true");
    });

    it("times out if signal not delivered", async () => {
      const storage = new InMemoryWorkflowStorage();
      const wf = () =>
        workflow<{}>({ name: "signal-timeout", storage })
          .step("before", () => Pipeline.succeed("ready"))
          .waitForSignal("approval", { signalName: "never-comes", timeoutMs: 1 });

      // First run: suspends
      await wf().runSafe({ workflowId: "wf-signal-timeout", input: {} });

      // Wait for timeout
      await new Promise((r) => setTimeout(r, 10));

      // Resume: should timeout
      const { error } = await wf().runSafe({ workflowId: "wf-signal-timeout", input: {} });
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
      const result = await workflow<{ n: number }>({ name: "map-test", storage })
        .step("compute", ({ input }) => Pipeline.succeed({ value: input.n * 2, extra: "data" }))
        .map((obj) => obj.value)
        .run({ workflowId: "wf-map", input: { n: 5 } });
      expect(result).toBe(10);
    });

    it("chains multiple maps", async () => {
      const storage = new InMemoryWorkflowStorage();
      const result = await workflow<{ s: string }>({ name: "multi-map", storage })
        .step("get", ({ input }) => Pipeline.succeed(input.s))
        .map((s) => s.toUpperCase())
        .map((s) => s + "!")
        .run({ workflowId: "wf-multi-map", input: { s: "hello" } });
      expect(result).toBe("HELLO!");
    });
  });

  // ---------------------------------------------------------------------------
  // Resume after crash
  // ---------------------------------------------------------------------------

  describe("resume after crash", () => {
    it("resumes from last completed step", async () => {
      const storage = new InMemoryWorkflowStorage();
      let step2Calls = 0;

      const makeWorkflow = () =>
        workflow<{ value: number }>({ name: "resumable", storage })
          .step("step-1", ({ input }) => Pipeline.succeed(input.value + 1))
          .step("step-2", ({ prev }) => {
            step2Calls++;
            return Pipeline.succeed(prev * 10);
          });

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

      const result = await makeWorkflow().run({
        workflowId: "wf-resume",
        input: { value: 5 },
      });
      expect(result).toBe(60);
      expect(step2Calls).toBe(1);
    });

    it("skips all steps if fully completed in storage", async () => {
      const storage = new InMemoryWorkflowStorage();
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

      const result = await workflow<{ n: number }>({ name: "done", storage })
        .step("a", ({ input }) => {
          callCount++;
          return Pipeline.succeed(input.n * 10);
        })
        .step("b", ({ prev }) => {
          callCount++;
          return Pipeline.succeed(prev * 2);
        })
        .run({ workflowId: "wf-done", input: { n: 1 } });

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
      const { error } = await workflow<{ url: string }>({ name: "failing", storage })
        .step("fetch", () => Pipeline.fail(new FetchError({ message: "404" })))
        .runSafe({ workflowId: "wf-fail", input: { url: "https://example.com" } });

      expect(error).not.toBeNull();
      expect(storage.getWorkflow("wf-fail")?.status).toBe("failed");
    });

    it("runSafe returns data on success", async () => {
      const storage = new InMemoryWorkflowStorage();
      const { data, error } = await workflow<{ n: number }>({ name: "safe-ok", storage })
        .step("compute", ({ input }) => Pipeline.succeed(input.n * 2))
        .runSafe({ workflowId: "wf-safe-ok", input: { n: 5 } });
      expect(data).toBe(10);
      expect(error).toBeNull();
    });

    it("runSafe returns error on failure", async () => {
      const storage = new InMemoryWorkflowStorage();
      const { data, error } = await workflow<{}>({ name: "safe-fail", storage })
        .step("boom", () => Pipeline.fail(new FetchError({ message: "oops" })))
        .runSafe({ workflowId: "wf-safe-fail", input: {} });
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
      await storage.tryLock("wf-locked", 60_000);

      const { error } = await workflow<{}>({ name: "locked", storage })
        .step("noop", () => Pipeline.succeed("done"))
        .runSafe({ workflowId: "wf-locked", input: {} });
      expect((error as WorkflowLockError)._tag).toBe("WorkflowLockError");
    });

    it("releases lock after success", async () => {
      const storage = new InMemoryWorkflowStorage();
      await workflow<{}>({ name: "release", storage })
        .step("noop", () => Pipeline.succeed("ok"))
        .run({ workflowId: "wf-release", input: {} });
      expect(await storage.tryLock("wf-release", 60_000)).toBe(true);
    });

    it("releases lock on failure", async () => {
      const storage = new InMemoryWorkflowStorage();
      await workflow<{}>({ name: "fail-release", storage })
        .step("boom", () => Pipeline.fail(new FetchError({ message: "fail" })))
        .runSafe({ workflowId: "wf-fail-release", input: {} });
      expect(await storage.tryLock("wf-fail-release", 60_000)).toBe(true);
    });
  });

  // ---------------------------------------------------------------------------
  // Duplicate step names
  // ---------------------------------------------------------------------------

  describe("duplicate step names", () => {
    it("throws on duplicate step name", () => {
      const storage = new InMemoryWorkflowStorage();
      expect(() =>
        workflow<{}>({ name: "dup", storage })
          .step("same", () => Pipeline.succeed(1))
          .step("same", () => Pipeline.succeed(2)),
      ).toThrow(/Duplicate step name/);
    });
  });

  // ---------------------------------------------------------------------------
  // Timestamps
  // ---------------------------------------------------------------------------

  describe("timestamps", () => {
    it("completed workflow has completedAt", async () => {
      const storage = new InMemoryWorkflowStorage();
      await workflow<{}>({ name: "ts", storage })
        .step("noop", () => Pipeline.succeed("ok"))
        .run({ workflowId: "wf-ts", input: {} });
      expect(storage.getWorkflow("wf-ts")?.completedAt).toBeInstanceOf(Date);
    });

    it("steps have startedAt, completedAt, durationMs", async () => {
      const storage = new InMemoryWorkflowStorage();
      await workflow<{}>({ name: "ts-step", storage })
        .step("compute", () => Pipeline.succeed(42))
        .run({ workflowId: "wf-ts-step", input: {} });

      const step = storage.getWorkflow("wf-ts-step")?.steps["compute"];
      expect(step?.startedAt).toBeInstanceOf(Date);
      expect(step?.completedAt).toBeInstanceOf(Date);
      expect(step?.durationMs).toBeGreaterThanOrEqual(0);
    });
  });

  // ---------------------------------------------------------------------------
  // Pipeline features inside steps
  // ---------------------------------------------------------------------------

  describe("Pipeline features inside steps", () => {
    it("steps can use Pipeline.map and flatMap", async () => {
      const storage = new InMemoryWorkflowStorage();
      const result = await workflow<{ n: number }>({ name: "pipeline-features", storage })
        .step("compute", ({ input }) =>
          Pipeline.succeed(input.n)
            .map((n) => n * 2)
            .flatMap((n) => Pipeline.succeed(n + 1)),
        )
        .run({ workflowId: "wf-features", input: { n: 5 } });
      expect(result).toBe(11);
    });

    it("steps can use Pipeline.all", async () => {
      const storage = new InMemoryWorkflowStorage();
      const result = await workflow<{}>({ name: "all-in-step", storage })
        .step("parallel", () =>
          Pipeline.all(Pipeline.succeed(1), Pipeline.succeed(2), Pipeline.succeed(3)).map(
            ([a, b, c]) => a + b + c,
          ),
        )
        .run({ workflowId: "wf-all", input: {} });
      expect(result).toBe(6);
    });
  });
});

// ---------------------------------------------------------------------------
// Pipeline error recovery (cats-aligned)
// ---------------------------------------------------------------------------

describe("Pipeline.handleError", () => {
  it("handles any error with a plain function", async () => {
    const result = await Pipeline.fail(new FetchError({ message: "404" }))
      .handleError((err) => `fallback: ${err._tag}`)
      .runPromise();
    expect(result).toBe("fallback: FetchError");
  });

  it("passes through on success", async () => {
    const result = await Pipeline.succeed(42)
      .handleError(() => 0)
      .runPromise();
    expect(result).toBe(42);
  });

  it("removes the error type", async () => {
    const { data, error } = await Pipeline.fail(new FetchError({ message: "fail" }))
      .handleError(() => "recovered")
      .runSafe();
    expect(data).toBe("recovered");
    expect(error).toBeNull();
  });
});

describe("Pipeline.handleErrorWith", () => {
  it("handles error with a Pipeline", async () => {
    const result = await Pipeline.fail(new FetchError({ message: "fail" }))
      .handleErrorWith(() => Pipeline.succeed("from-pipeline"))
      .runPromise();
    expect(result).toBe("from-pipeline");
  });

  it("can chain to a different pipeline with different error", async () => {
    const { data } = await Pipeline.fail(new FetchError({ message: "fail" }))
      .handleErrorWith(() => Pipeline.succeed(42))
      .runSafe();
    expect(data).toBe(42);
  });
});

describe("Pipeline.handleErrorAsync", () => {
  it("handles error with async function", async () => {
    const result = await Pipeline.fail(new FetchError({ message: "404" }))
      .handleErrorAsync(async (err) => `fallback: ${err._tag}`)
      .runPromise();
    expect(result).toBe("fallback: FetchError");
  });

  it("can do async work", async () => {
    const result = await Pipeline.fail(new FetchError({ message: "fail" }))
      .handleErrorAsync(async () => {
        await new Promise((r) => setTimeout(r, 5));
        return "async-recovered";
      })
      .runPromise();
    expect(result).toBe("async-recovered");
  });
});

describe("Pipeline.recover", () => {
  it("recovers when predicate matches", async () => {
    const result = await Pipeline.fail(new HttpStatusError({ status: 404, message: "Not Found" }))
      .recover(
        (err) => err._tag === "HttpStatusError" && err.status === 404,
        () => null,
      )
      .runPromise();
    expect(result).toBeNull();
  });

  it("passes error through when predicate doesn't match", async () => {
    const { error } = await Pipeline.fail(
      new HttpStatusError({ status: 500, message: "Server Error" }),
    )
      .recover(
        (err) => err._tag === "HttpStatusError" && err.status === 404,
        () => null,
      )
      .runSafe();
    expect(error).not.toBeNull();
    expect((error as HttpStatusError).status).toBe(500);
  });

  it("passes through on success", async () => {
    const result = await Pipeline.succeed("ok")
      .recover(
        () => true,
        () => "fallback",
      )
      .runPromise();
    expect(result).toBe("ok");
  });

  it("receives the error in recovery fn", async () => {
    const result = await Pipeline.fail(new HttpStatusError({ status: 404, message: "Not Found" }))
      .recover(
        (err) => err._tag === "HttpStatusError",
        (err) => `recovered from ${(err as HttpStatusError).status}`,
      )
      .runPromise();
    expect(result).toBe("recovered from 404");
  });
});

describe("Pipeline.recoverWith", () => {
  it("recovers with a Pipeline when predicate matches", async () => {
    const result = await Pipeline.fail(new HttpStatusError({ status: 404, message: "Not Found" }))
      .recoverWith(
        (err) => err._tag === "HttpStatusError" && err.status === 404,
        () => Pipeline.succeed("from-cache"),
      )
      .runPromise();
    expect(result).toBe("from-cache");
  });

  it("passes error through when predicate doesn't match", async () => {
    const { error } = await Pipeline.fail(
      new HttpStatusError({ status: 500, message: "Server Error" }),
    )
      .recoverWith(
        (err) => err._tag === "HttpStatusError" && err.status === 404,
        () => Pipeline.succeed("from-cache"),
      )
      .runSafe();
    expect(error).not.toBeNull();
    expect((error as HttpStatusError).status).toBe(500);
  });
});

describe("Pipeline.recoverAsync", () => {
  it("recovers with async fn when predicate matches", async () => {
    const result = await Pipeline.fail(new HttpStatusError({ status: 404, message: "Not Found" }))
      .recoverAsync(
        (err) => err._tag === "HttpStatusError" && err.status === 404,
        async () => null,
      )
      .runPromise();
    expect(result).toBeNull();
  });

  it("passes error through when predicate doesn't match", async () => {
    const { error } = await Pipeline.fail(
      new HttpStatusError({ status: 500, message: "Server Error" }),
    )
      .recoverAsync(
        (err) => err._tag === "HttpStatusError" && err.status === 404,
        async () => null,
      )
      .runSafe();
    expect(error).not.toBeNull();
    expect((error as HttpStatusError).status).toBe(500);
  });
});

describe("Pipeline.redeem", () => {
  it("maps error path", async () => {
    const result = await Pipeline.fail(new FetchError({ message: "fail" }))
      .redeem(
        (err) => `error: ${err._tag}`,
        (val) => `ok: ${val}`,
      )
      .runPromise();
    expect(result).toBe("error: FetchError");
  });

  it("maps success path", async () => {
    const result = await Pipeline.succeed(42)
      .redeem(
        () => "error",
        (val) => `ok: ${val}`,
      )
      .runPromise();
    expect(result).toBe("ok: 42");
  });

  it("always removes the error type", async () => {
    const { data, error } = await Pipeline.fail(new FetchError({ message: "x" }))
      .redeem(
        () => "recovered",
        (v) => v,
      )
      .runSafe();
    expect(data).toBe("recovered");
    expect(error).toBeNull();
  });
});

describe("Pipeline.redeemWith", () => {
  it("runs error pipeline on failure", async () => {
    const result = await Pipeline.fail(new FetchError({ message: "fail" }))
      .redeemWith(
        (err) => Pipeline.succeed(`error: ${err._tag}`),
        (val) => Pipeline.succeed(`ok: ${val}`),
      )
      .runPromise();
    expect(result).toBe("error: FetchError");
  });

  it("runs success pipeline on success", async () => {
    const result = await Pipeline.succeed(42)
      .redeemWith(
        () => Pipeline.succeed(-1),
        (val) => Pipeline.succeed(val * 2),
      )
      .runPromise();
    expect(result).toBe(84);
  });

  it("error pipeline can itself fail", async () => {
    const { error } = await Pipeline.fail(new FetchError({ message: "original" }))
      .redeemWith(
        () => Pipeline.fail(new HttpStatusError({ status: 503, message: "unavailable" })),
        (_val: never) => Pipeline.succeed("ok" as const),
      )
      .runSafe();
    expect((error as HttpStatusError).status).toBe(503);
  });
});

describe("Pipeline.redeemAsync", () => {
  it("maps error path with async fn", async () => {
    const result = await Pipeline.fail(new FetchError({ message: "fail" }))
      .redeemAsync(
        async (err) => `error: ${err._tag}`,
        async (val) => `ok: ${val}`,
      )
      .runPromise();
    expect(result).toBe("error: FetchError");
  });

  it("maps success path with async fn", async () => {
    const result = await Pipeline.succeed(42)
      .redeemAsync(
        async () => -1,
        async (val) => val * 2,
      )
      .runPromise();
    expect(result).toBe(84);
  });

  it("can do async work in both paths", async () => {
    const result = await Pipeline.fail(new FetchError({ message: "x" }))
      .redeemAsync(
        async () => {
          await new Promise((r) => setTimeout(r, 5));
          return "async-recovered";
        },
        async (v) => v,
      )
      .runPromise();
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
    await storage.completeWorkflow("t4", "result");
    const state = await storage.loadWorkflow("t4");
    expect(state?.status).toBe("completed");
    expect(state?.completedAt).toBeInstanceOf(Date);
  });

  it("suspends workflow", async () => {
    const storage = new InMemoryWorkflowStorage();
    await storage.createWorkflow({ workflowId: "t-suspend", workflowName: "test", input: {} });
    await storage.suspendWorkflow("t-suspend", "wait-step", {
      status: "sleeping",
      stepType: "sleep",
      wakeAt: new Date(Date.now() + 60_000),
    });

    const state = await storage.loadWorkflow("t-suspend");
    expect(state?.status).toBe("suspended");
    expect(state?.steps["wait-step"]?.status).toBe("sleeping");
  });

  it("delivers and loads signals", async () => {
    const storage = new InMemoryWorkflowStorage();
    await storage.deliverSignal("wf-1", "approval", { ok: true });
    await storage.deliverSignal("wf-1", "other", { data: 42 });

    const signals = await storage.loadSignals("wf-1");
    expect(signals).toHaveLength(2);
    expect(signals[0]!.signalName).toBe("approval");
    expect(signals[0]!.payload).toEqual({ ok: true });
  });

  it("lock prevents double acquisition", async () => {
    const storage = new InMemoryWorkflowStorage();
    expect(await storage.tryLock("l1", 60_000)).toBe(true);
    expect(await storage.tryLock("l1", 60_000)).toBe(false);
  });

  it("lock can be released and re-acquired", async () => {
    const storage = new InMemoryWorkflowStorage();
    await storage.tryLock("l2", 60_000);
    await storage.releaseLock("l2");
    expect(await storage.tryLock("l2", 60_000)).toBe(true);
  });

  it("expired lock can be re-acquired", async () => {
    const storage = new InMemoryWorkflowStorage();
    await storage.tryLock("l3", 1);
    await new Promise((r) => setTimeout(r, 10));
    expect(await storage.tryLock("l3", 60_000)).toBe(true);
  });

  it("heartbeat extends the lock", async () => {
    const storage = new InMemoryWorkflowStorage();
    await storage.tryLock("l4", 1);
    await storage.heartbeat("l4", 60_000);
    expect(await storage.tryLock("l4", 60_000)).toBe(false);
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
      .step("greet", ({ input }) => Pipeline.succeed(`Hello, ${input.name}`))
      .step("upper", ({ prev }) => Pipeline.succeed(prev.toUpperCase()))
      .execute({ name: "World" });

    expect(result).toBe("HELLO, WORLD");
  });

  it("executes a DAG", async () => {
    const result = await flow<{ text: string }>("analyze")
      .step("parse", ({ input }) => Pipeline.succeed(input.text.split(" ")))
      .step("count", { dependsOn: ["parse"] }, ({ deps }) => Pipeline.succeed(deps.parse.length))
      .step("join", { dependsOn: ["parse"] }, ({ deps }) => Pipeline.succeed(deps.parse.join("-")))
      .step("combine", { dependsOn: ["count", "join"] }, ({ deps }) =>
        Pipeline.succeed(`${deps.join} (${deps.count})`),
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
      .step("source", ({ input }) => Pipeline.succeed(input.items))
      .mapOver("double", { array: "source" }, (n) => Pipeline.succeed(n * 2))
      .execute({ items: [1, 2, 3] });

    expect(result).toEqual([2, 4, 6]);
  });

  it("works with branch", async () => {
    const result = await flow<{ n: number }>("classify")
      .step("get", ({ input }) => Pipeline.succeed(input.n))
      .branch("decide", {
        condition: (n) => n > 10,
        ifTrue: ({ prev }) => Pipeline.succeed(`big: ${prev}`),
        ifFalse: ({ prev }) => Pipeline.succeed(`small: ${prev}`),
      })
      .execute({ n: 42 });

    expect(result).toBe("big: 42");
  });

  it("works with map", async () => {
    const result = await flow<{ s: string }>("transform")
      .step("get", ({ input }) => Pipeline.succeed(input.s))
      .map((s) => s.length)
      .execute({ s: "hello" });

    expect(result).toBe(5);
  });

  it("executeSafe returns data on success", async () => {
    const { data, error } = await flow<{ n: number }>("safe")
      .step("compute", ({ input }) => Pipeline.succeed(input.n * 2))
      .executeSafe({ n: 5 });

    expect(data).toBe(10);
    expect(error).toBeNull();
  });

  it("executeSafe returns error on failure", async () => {
    const { data, error } = await flow<{}>("fail")
      .step("boom", () => Pipeline.fail(new FetchError({ message: "oops" })))
      .executeSafe({});

    expect(data).toBeNull();
    expect(error).not.toBeNull();
  });

  it("accepts hooks", async () => {
    const steps: string[] = [];

    await flow<{ n: number }>("hooked", {
      onStepComplete: ({ stepName }) => {
        steps.push(stepName);
      },
    })
      .step("a", ({ input }) => Pipeline.succeed(input.n + 1))
      .step("b", ({ prev }) => Pipeline.succeed(prev * 2))
      .execute({ n: 5 });

    expect(steps).toEqual(["a", "b"]);
  });
});

// ---------------------------------------------------------------------------
// Subworkflows — invoke(), .subworkflow(), parent-child tracking
// ---------------------------------------------------------------------------

describe("Subworkflows", () => {
  describe("invoke", () => {
    it("invokes a child workflow as a Pipeline", async () => {
      const storage = new InMemoryWorkflowStorage();
      const child = workflow<{ n: number }>({ name: "child", storage })
        .step("double", ({ input }) => Pipeline.succeed(input.n * 2))
        .build();

      const result = await workflow<{ n: number }>({ name: "parent", storage })
        .step("compute", ({ input }) => Pipeline.succeed(input.n + 1))
        .step("delegate", ({ prev }) =>
          child.invoke({ workflowId: `child-${prev}`, input: { n: prev } }),
        )
        .run({ workflowId: "parent-1", input: { n: 5 } });

      expect(result).toBe(12); // (5 + 1) * 2
    });

    it("sets parentWorkflowId on child", async () => {
      const storage = new InMemoryWorkflowStorage();
      const child = workflow<{ n: number }>({ name: "child", storage })
        .step("compute", ({ input }) => Pipeline.succeed(input.n))
        .build();

      await workflow<{}>({ name: "parent", storage })
        .step("delegate", () =>
          child.invoke({ workflowId: "child-1", input: { n: 42 }, parentWorkflowId: "parent-1" }),
        )
        .run({ workflowId: "parent-1", input: {} });

      const childState = await storage.loadWorkflow("child-1");
      expect(childState?.parentWorkflowId).toBe("parent-1");
    });
  });

  describe(".subworkflow()", () => {
    it("invokes child with builder sugar", async () => {
      const storage = new InMemoryWorkflowStorage();
      const enrichUser = workflow<{ userId: string }>({ name: "enrich", storage })
        .step("fetch", ({ input }) => Pipeline.succeed({ userId: input.userId, score: 95 }))
        .build();

      const result = await workflow<{ userId: string }>({ name: "parent", storage })
        .step("create", ({ input }) => Pipeline.succeed({ id: input.userId, name: "Alice" }))
        .subworkflow("enrich", enrichUser, {
          input: (prev) => ({ userId: prev.id }),
          workflowId: (prev) => `enrich-${prev.id}`,
        })
        .run({ workflowId: "parent-2", input: { userId: "u_1" } });

      expect(result).toEqual({ userId: "u_1", score: 95 });
    });

    it("chains after subworkflow", async () => {
      const storage = new InMemoryWorkflowStorage();
      const child = workflow<{ n: number }>({ name: "child", storage })
        .step("double", ({ input }) => Pipeline.succeed(input.n * 2))
        .build();

      const result = await workflow<{ n: number }>({ name: "parent", storage })
        .step("start", ({ input }) => Pipeline.succeed(input.n))
        .subworkflow("child", child, {
          input: (prev) => ({ n: prev }),
          workflowId: (prev) => `child-${prev}`,
        })
        .step("finish", ({ prev }) => Pipeline.succeed(prev + 100))
        .run({ workflowId: "parent-3", input: { n: 5 } });

      expect(result).toBe(110); // 5 * 2 + 100
    });
  });

  describe("parent-child tracking", () => {
    it("lists children by parentId", async () => {
      const storage = new InMemoryWorkflowStorage();
      const child = workflow<{ n: number }>({ name: "child", storage })
        .step("compute", ({ input }) => Pipeline.succeed(input.n))
        .build();

      // Create parent + children manually
      await storage.createWorkflow({
        workflowId: "parent-x",
        workflowName: "parent",
        input: {},
      });
      await child
        .invoke({ workflowId: "child-a", input: { n: 1 }, parentWorkflowId: "parent-x" })
        .runPromise();
      await child
        .invoke({ workflowId: "child-b", input: { n: 2 }, parentWorkflowId: "parent-x" })
        .runPromise();

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

      await storage.cancelWorkflow("parent-cancel", { cascade: true });

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

      await storage.cancelWorkflow("parent-nocancel");

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
            if (attempts < 3) return Pipeline.fail(new FetchError({ message: "transient" }));
            return Pipeline.succeed("ok");
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
        .step("optional", () => Pipeline.fail(new FetchError({ message: "fail" })), {
          onFailure: "skip",
        })
        .step("next", () => Pipeline.succeed("continued"))
        .execute({});

      expect(result).toBe("continued");
    });
  });

  describe("onFailure: fallback", () => {
    it("uses fallback value on failure", async () => {
      const result = await flow<{}>("fallback-test")
        .step("risky", () => Pipeline.fail(new FetchError({ message: "fail" })), {
          onFailure: { fallback: () => "default-value" },
        })
        .step("use-it", ({ prev }) => Pipeline.succeed(`got: ${prev}`))
        .execute({});

      expect(result).toBe("got: default-value");
    });

    it("fallback receives the error", async () => {
      const result = await flow<{}>("fallback-err")
        .step("risky", () => Pipeline.fail(new FetchError({ message: "specific-error" })), {
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
            return Pipeline.fail(new FetchError({ message: "always" }));
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
            if (attempts < 3) return Pipeline.fail(new FetchError({ message: "transient" }));
            return Pipeline.succeed("ok");
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
      const { error } = await flow<{}>("retry-when-no-match")
        .step(
          "fail",
          (): Pipeline<string, FetchError | HttpStatusError> => {
            attempts++;
            return Pipeline.fail(new HttpStatusError({ status: 400, message: "not retryable" }));
          },
          {
            retry: {
              maxRetries: 5,
              when: (err) => err._tag === "FetchError", // only retry FetchError
            },
          },
        )
        .executeSafe({});

      expect(error).not.toBeNull();
      expect(attempts).toBe(1); // no retry — HttpStatusError doesn't match
    });
  });

  describe("default: fail", () => {
    it("fails the workflow by default", async () => {
      const { error } = await flow<{}>("fail-default")
        .step("boom", () => Pipeline.fail(new FetchError({ message: "fail" })))
        .executeSafe({});

      expect(error).not.toBeNull();
    });
  });
});

// ---------------------------------------------------------------------------
// Per-step activity timeout
// ---------------------------------------------------------------------------

describe("Per-step activity timeout", () => {
  it("step times out after timeoutMs", async () => {
    const storage = new InMemoryWorkflowStorage();
    const wf = workflow<{}>({ name: "step-timeout", storage }).stepAsync(
      "slow",
      async () => {
        await new Promise((r) => setTimeout(r, 500));
        return "done";
      },
      { timeoutMs: 30 },
    );

    const { error } = await wf.runSafe({ workflowId: "t-step-timeout", input: {} });
    expect(error).not.toBeNull();
    expect((error as StepTimeoutError)._tag).toBe("StepTimeoutError");
    expect((error as StepTimeoutError).stepName).toBe("slow");
    expect((error as StepTimeoutError).timeoutMs).toBe(30);
  });

  it("step without timeout runs normally", async () => {
    const storage = new InMemoryWorkflowStorage();
    const wf = workflow<{}>({ name: "no-timeout", storage }).stepAsync("fast", async () => "done");

    const result = await wf.run({ workflowId: "t-no-timeout", input: {} });
    expect(result).toBe("done");
  });

  it("step completes within timeout succeeds", async () => {
    const storage = new InMemoryWorkflowStorage();
    const wf = workflow<{}>({ name: "within-timeout", storage }).stepAsync(
      "quick",
      async () => {
        await new Promise((r) => setTimeout(r, 5));
        return "ok";
      },
      { timeoutMs: 5000 },
    );

    const result = await wf.run({ workflowId: "t-within-timeout", input: {} });
    expect(result).toBe("ok");
  });

  it("Pipeline-returning step times out", async () => {
    const storage = new InMemoryWorkflowStorage();
    const wf = workflow<{}>({ name: "pipeline-step-timeout", storage }).step(
      "slow-pipeline",
      () =>
        Pipeline.fromPromise(async () => {
          await new Promise((r) => setTimeout(r, 500));
          return "done";
        }),
      { timeoutMs: 30 },
    );

    const { error } = await wf.runSafe({ workflowId: "t-pipeline-timeout", input: {} });
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
    // 3 steps each taking 30ms = ~90ms total, deadline at 50ms
    // After step1 completes (~30ms < 50ms), step2 starts.
    // After step2 completes (~60ms > 50ms), deadline check triggers before step3.
    const wf = workflow<{}>({ name: "deadline-test", storage, timeoutMs: 50 })
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
      });

    const { error } = await wf.runSafe({ workflowId: "t-deadline", input: {} });
    expect(error).not.toBeNull();
    expect((error as WorkflowDeadlineError)._tag).toBe("WorkflowDeadlineError");
    expect((error as WorkflowDeadlineError).timeoutMs).toBe(50);
  });

  it("workflow within deadline completes normally", async () => {
    const storage = new InMemoryWorkflowStorage();
    const wf = workflow<{}>({ name: "within-deadline", storage, timeoutMs: 5000 })
      .stepAsync("step1", async () => "a")
      .stepAsync("step2", async () => "b");

    const result = await wf.run({ workflowId: "t-within-deadline", input: {} });
    expect(result).toBe("b");
  });

  it("workflow without timeoutMs has no deadline", async () => {
    const storage = new InMemoryWorkflowStorage();
    const wf = workflow<{}>({ name: "no-deadline", storage })
      .stepAsync("step1", async () => {
        await new Promise((r) => setTimeout(r, 10));
        return "a";
      })
      .stepAsync("step2", async () => {
        await new Promise((r) => setTimeout(r, 10));
        return "b";
      });

    const result = await wf.run({ workflowId: "t-no-deadline", input: {} });
    expect(result).toBe("b");
  });
});
