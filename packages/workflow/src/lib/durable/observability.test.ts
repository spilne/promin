import { describe, it, expect } from "bun:test";
import { Data } from "effect";
import { Pipeline } from "@promin/core";
import { workflow, dagToMermaid, dagToDot } from "./durable-pipeline.ts";
import { InMemoryWorkflowStorage } from "./in-memory-storage.ts";
import { createWorkflowRunner } from "./workflow-runner.ts";

// ---------------------------------------------------------------------------
// Test error types
// ---------------------------------------------------------------------------

class TestError extends Data.TaggedError("TestError")<{
  readonly message: string;
}> {}

// ---------------------------------------------------------------------------
// WorkflowStorage — listWorkflows
// ---------------------------------------------------------------------------

describe("WorkflowStorage.listWorkflows", () => {
  it("returns all workflows", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const wfA = workflow<{}>({ name: "a" })
      .step("s", () => Pipeline.succeed(1))
      .build();
    await runner.run({ workflow: wfA, workflowId: "wf-1", input: {} });
    const wfB = workflow<{}>({ name: "b" })
      .step("s", () => Pipeline.succeed(2))
      .build();
    await runner.run({ workflow: wfB, workflowId: "wf-2", input: {} });

    const all = await storage.listWorkflows();
    expect(all).toHaveLength(2);
  });

  it("filters by status", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const okWf = workflow<{}>({ name: "ok" })
      .step("s", () => Pipeline.succeed(1))
      .build();
    await runner.run({ workflow: okWf, workflowId: "wf-ok", input: {} });
    const failWf = workflow<{}>({ name: "fail" })
      .step("s", () => Pipeline.fail(new TestError({ message: "x" })))
      .build();
    await runner.runSafe({ workflow: failWf, workflowId: "wf-fail", input: {} });

    const completed = await storage.listWorkflows({ status: "completed" });
    expect(completed).toHaveLength(1);
    expect(completed[0]!.workflowId).toBe("wf-ok");

    const failed = await storage.listWorkflows({ status: "failed" });
    expect(failed).toHaveLength(1);
    expect(failed[0]!.workflowId).toBe("wf-fail");
  });

  it("filters by name", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const alphaWf = workflow<{}>({ name: "alpha" })
      .step("s", () => Pipeline.succeed(1))
      .build();
    await runner.run({ workflow: alphaWf, workflowId: "wf-a", input: {} });
    const betaWf = workflow<{}>({ name: "beta" })
      .step("s", () => Pipeline.succeed(2))
      .build();
    await runner.run({ workflow: betaWf, workflowId: "wf-b", input: {} });

    const alphas = await storage.listWorkflows({ name: "alpha" });
    expect(alphas).toHaveLength(1);
    expect(alphas[0]!.workflowName).toBe("alpha");
  });

  it("supports limit and offset", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    for (let i = 0; i < 5; i++) {
      const wf = workflow<{}>({ name: "paginated" })
        .step("s", () => Pipeline.succeed(i))
        .build();
      await runner.run({ workflow: wf, workflowId: `wf-${i}`, input: {} });
    }

    const page1 = await storage.listWorkflows({ limit: 2 });
    expect(page1).toHaveLength(2);

    const page2 = await storage.listWorkflows({ limit: 2, offset: 2 });
    expect(page2).toHaveLength(2);

    const page3 = await storage.listWorkflows({ limit: 2, offset: 4 });
    expect(page3).toHaveLength(1);
  });
});

// ---------------------------------------------------------------------------
// WorkflowStorage — cancelWorkflow
// ---------------------------------------------------------------------------

describe("WorkflowStorage.cancelWorkflow", () => {
  it("cancels a running workflow", async () => {
    const storage = new InMemoryWorkflowStorage();
    await storage.createWorkflow({ workflowId: "cancel-1", workflowName: "test", input: {} });

    await storage.cancelWorkflow("cancel-1");

    const state = await storage.loadWorkflow("cancel-1");
    expect(state?.status).toBe("failed");
    expect(state?.error).toBe("Cancelled");
    expect(state?.completedAt).toBeInstanceOf(Date);
  });

  it("cancels a suspended workflow", async () => {
    const storage = new InMemoryWorkflowStorage();
    await storage.createWorkflow({ workflowId: "cancel-2", workflowName: "test", input: {} });
    await storage.suspendWorkflow("cancel-2", "wait", { status: "sleeping", stepType: "sleep" });

    await storage.cancelWorkflow("cancel-2");

    const state = await storage.loadWorkflow("cancel-2");
    expect(state?.status).toBe("failed");
    expect(state?.error).toBe("Cancelled");
  });

  it("no-ops on already completed workflow", async () => {
    const storage = new InMemoryWorkflowStorage();
    await storage.createWorkflow({ workflowId: "cancel-3", workflowName: "test", input: {} });
    await storage.completeWorkflow("cancel-3", "done");

    await storage.cancelWorkflow("cancel-3");

    const state = await storage.loadWorkflow("cancel-3");
    expect(state?.status).toBe("completed");
  });

  it("no-ops on non-existent workflow", async () => {
    const storage = new InMemoryWorkflowStorage();
    // Should not throw
    await storage.cancelWorkflow("nonexistent");
  });
});

// ---------------------------------------------------------------------------
// WorkflowHooks
// ---------------------------------------------------------------------------

describe("WorkflowHooks", () => {
  it("calls onStepComplete for each step", async () => {
    const storage = new InMemoryWorkflowStorage();
    const events: { stepName: string; result: unknown }[] = [];

    const wf = workflow<{ n: number }>({
      name: "hooks-test",
      hooks: {
        onStepComplete: ({ stepName, result }) => {
          events.push({ stepName, result });
        },
      },
    })
      .step("add", ({ input }) => Pipeline.succeed(input.n + 1))
      .step("double", ({ prev }) => Pipeline.succeed(prev * 2))
      .build();
    const runner = createWorkflowRunner({ storage });
    await runner.run({ workflow: wf, workflowId: "wf-hooks-1", input: { n: 5 } });

    expect(events).toEqual([
      { stepName: "add", result: 6 },
      { stepName: "double", result: 12 },
    ]);
  });

  it("calls onWorkflowComplete on success", async () => {
    const storage = new InMemoryWorkflowStorage();
    let completed: { workflowId: string; result: unknown } | null = null;

    const wf = workflow<{}>({
      name: "hooks-wf-complete",
      hooks: {
        onWorkflowComplete: ({ workflowId, result }) => {
          completed = { workflowId, result };
        },
      },
    })
      .step("compute", () => Pipeline.succeed(42))
      .build();
    const runner = createWorkflowRunner({ storage });
    await runner.run({ workflow: wf, workflowId: "wf-hooks-2", input: {} });

    expect(completed).not.toBeNull();
    expect(completed!.workflowId).toBe("wf-hooks-2");
    expect(completed!.result).toBe(42);
  });

  it("calls onStepFailure on step error", async () => {
    const storage = new InMemoryWorkflowStorage();
    let failedStep: { stepName: string; error: string } | null = null;

    const wf = workflow<{}>({
      name: "hooks-step-fail",
      hooks: {
        onStepFailure: ({ stepName, error }) => {
          failedStep = { stepName, error };
        },
      },
    })
      .step("boom", () => Pipeline.fail(new TestError({ message: "kaboom" })))
      .build();
    const runner = createWorkflowRunner({ storage });
    await runner.runSafe({ workflow: wf, workflowId: "wf-hooks-3", input: {} });

    expect(failedStep).not.toBeNull();
    expect(failedStep!.stepName).toBe("boom");
  });

  it("calls onWorkflowFailure on workflow error", async () => {
    const storage = new InMemoryWorkflowStorage();
    let failedWf: { workflowId: string; error: string } | null = null;

    const wf = workflow<{}>({
      name: "hooks-wf-fail",
      hooks: {
        onWorkflowFailure: ({ workflowId, error }) => {
          failedWf = { workflowId, error };
        },
      },
    })
      .step("boom", () => Pipeline.fail(new TestError({ message: "fail" })))
      .build();
    const runner = createWorkflowRunner({ storage });
    await runner.runSafe({ workflow: wf, workflowId: "wf-hooks-4", input: {} });

    expect(failedWf).not.toBeNull();
    expect(failedWf!.workflowId).toBe("wf-hooks-4");
  });

  it("onWorkflowComplete includes durationMs", async () => {
    const storage = new InMemoryWorkflowStorage();
    let durationMs = -1;

    const wf = workflow<{}>({
      name: "hooks-duration",
      hooks: {
        onWorkflowComplete: (params) => {
          durationMs = params.durationMs;
        },
      },
    })
      .stepAsync("wait", async () => {
        await new Promise((r) => setTimeout(r, 10));
        return "done";
      })
      .build();
    const runner = createWorkflowRunner({ storage });
    await runner.run({ workflow: wf, workflowId: "wf-hooks-5", input: {} });

    expect(durationMs).toBeGreaterThanOrEqual(5);
  });

  it("hooks are async-safe", async () => {
    const storage = new InMemoryWorkflowStorage();
    const events: string[] = [];

    const wf = workflow<{}>({
      name: "hooks-async",
      hooks: {
        onStepComplete: async ({ stepName }) => {
          await new Promise((r) => setTimeout(r, 5));
          events.push(stepName);
        },
        onWorkflowComplete: async () => {
          await new Promise((r) => setTimeout(r, 5));
          events.push("workflow-done");
        },
      },
    })
      .step("a", () => Pipeline.succeed(1))
      .step("b", () => Pipeline.succeed(2))
      .build();
    const runner = createWorkflowRunner({ storage });
    await runner.run({ workflow: wf, workflowId: "wf-hooks-6", input: {} });

    expect(events).toEqual(["a", "b", "workflow-done"]);
  });
});

// ---------------------------------------------------------------------------
// toJSON + dagToMermaid / dagToDot — DAG visualization
// ---------------------------------------------------------------------------

describe("DAG visualization", () => {
  describe("toJSON", () => {
    it("exports the DAG structure", () => {
      const storage = new InMemoryWorkflowStorage();
      const dag = workflow<{}>({ name: "my-wf" })
        .step("a", () => Pipeline.succeed(1))
        .step("b", { dependsOn: ["a"] }, () => Pipeline.succeed(2))
        .toJSON();

      expect(dag.name).toBe("my-wf");
      expect(dag.steps).toHaveLength(2);
      expect(dag.steps[0]!.name).toBe("a");
      expect(dag.steps[1]!.dependsOn).toEqual(["a"]);
    });
  });

  describe("dagToMermaid", () => {
    it("generates Mermaid for linear chain", () => {
      const storage = new InMemoryWorkflowStorage();
      const dag = workflow<{}>({ name: "linear" })
        .step("fetch", () => Pipeline.succeed(1))
        .step("process", () => Pipeline.succeed(2))
        .step("save", () => Pipeline.succeed(3))
        .toJSON();

      const mermaid = dagToMermaid(dag);
      expect(mermaid).toContain("graph LR");
      expect(mermaid).toContain('fetch["fetch"]');
      expect(mermaid).toContain("fetch --> process");
      expect(mermaid).toContain("process --> save");
    });

    it("generates Mermaid for DAG", () => {
      const storage = new InMemoryWorkflowStorage();
      const dag = workflow<{}>({ name: "dag" })
        .step("scrape", () => Pipeline.succeed("html"))
        .step("summarize", { dependsOn: ["scrape"] }, () => Pipeline.succeed("summary"))
        .step("keywords", { dependsOn: ["scrape"] }, () => Pipeline.succeed(["kw"]))
        .step("publish", { dependsOn: ["summarize", "keywords"] }, () => Pipeline.succeed("done"))
        .toJSON();

      const mermaid = dagToMermaid(dag);
      expect(mermaid).toContain("scrape --> summarize");
      expect(mermaid).toContain("scrape --> keywords");
      expect(mermaid).toContain("summarize --> publish");
      expect(mermaid).toContain("keywords --> publish");
    });

    it("handles step names with special characters", () => {
      const storage = new InMemoryWorkflowStorage();
      const dag = workflow<{}>({ name: "special" })
        .step("fetch-data", () => Pipeline.succeed(1))
        .step("process_result", () => Pipeline.succeed(2))
        .toJSON();

      const mermaid = dagToMermaid(dag);
      expect(mermaid).toContain('fetch_data["fetch-data"]');
      expect(mermaid).toContain("fetch_data --> process_result");
    });
  });

  describe("dagToDot", () => {
    it("generates DOT for DAG", () => {
      const storage = new InMemoryWorkflowStorage();
      const dag = workflow<{}>({ name: "my-workflow" })
        .step("a", () => Pipeline.succeed(1))
        .step("b", { dependsOn: ["a"] }, () => Pipeline.succeed(2))
        .step("c", { dependsOn: ["a"] }, () => Pipeline.succeed(3))
        .step("d", { dependsOn: ["b", "c"] }, () => Pipeline.succeed(4))
        .toJSON();

      const dot = dagToDot(dag);
      expect(dot).toContain('digraph "my-workflow"');
      expect(dot).toContain('"a" -> "b"');
      expect(dot).toContain('"a" -> "c"');
      expect(dot).toContain('"b" -> "d"');
      expect(dot).toContain('"c" -> "d"');
      expect(dot).toContain("}");
    });
  });

  describe("match step visualization", () => {
    type Order = { type: string; total: number };

    it("toJSON exposes case labels for selector-mode match", () => {
      const storage = new InMemoryWorkflowStorage();
      const dag = workflow<Order>({ name: "shipping" })
        .step("load", ({ input }) => Pipeline.succeed(input))
        .match("route", {
          on: (o) => o.type,
          cases: {
            express: () => Pipeline.succeed("E"),
            standard: () => Pipeline.succeed("S"),
            freight: () => Pipeline.succeed("F"),
          },
          default: () => Pipeline.succeed("D"),
        })
        .toJSON();

      const route = dag.steps.find((s) => s.name === "route")!;
      expect(route.kind).toBe("match");
      expect(route.cases).toEqual(["express", "standard", "freight"]);
      expect(route.hasDefault).toBe(true);
    });

    it("toJSON exposes labels for predicate-mode match (uses provided labels)", () => {
      const storage = new InMemoryWorkflowStorage();
      const dag = workflow<Order>({ name: "shipping-pred" })
        .step("load", ({ input }) => Pipeline.succeed(input))
        .match("route", {
          cases: [
            { label: "vip", when: (o) => o.total > 10_000, then: () => Pipeline.succeed("V") },
            {
              label: "express",
              when: (o) => o.type === "express",
              then: () => Pipeline.succeed("E"),
            },
          ],
        })
        .toJSON();

      const route = dag.steps.find((s) => s.name === "route")!;
      expect(route.cases).toEqual(["vip", "express"]);
      expect(route.hasDefault).toBeFalsy();
    });

    it("toJSON falls back to case[N] for unlabeled predicate cases", () => {
      const storage = new InMemoryWorkflowStorage();
      const dag = workflow<Order>({ name: "shipping-unlabeled" })
        .step("load", ({ input }) => Pipeline.succeed(input))
        .match("route", {
          cases: [
            { when: (o) => o.total > 10_000, then: () => Pipeline.succeed("V") },
            { when: (o) => o.type === "express", then: () => Pipeline.succeed("E") },
          ],
        })
        .toJSON();

      const route = dag.steps.find((s) => s.name === "route")!;
      expect(route.cases).toEqual(["case[0]", "case[1]"]);
    });

    it("dagToMermaid renders match as decision node with labeled outgoing edges", () => {
      const storage = new InMemoryWorkflowStorage();
      const dag = workflow<Order>({ name: "viz-mermaid" })
        .step("load", ({ input }) => Pipeline.succeed(input))
        .match("route", {
          on: (o) => o.type,
          cases: {
            express: () => Pipeline.succeed("E"),
            standard: () => Pipeline.succeed("S"),
          },
          default: () => Pipeline.succeed("D"),
        })
        .toJSON();

      const mermaid = dagToMermaid(dag);
      // Decision node uses diamond shape `{...}`.
      expect(mermaid).toContain('route{"route"}');
      // Each case becomes a labeled edge to a phantom case node.
      expect(mermaid).toContain('route -->|"express"| route_express');
      expect(mermaid).toContain('route -->|"standard"| route_standard');
      expect(mermaid).toContain('route -->|"default"| route_default');
      // Phantom case nodes use rounded shape `(...)`.
      expect(mermaid).toContain('route_express(["express"])');
    });

    it("dagToDot renders match as diamond with labeled edges", () => {
      const storage = new InMemoryWorkflowStorage();
      const dag = workflow<Order>({ name: "viz-dot" })
        .step("load", ({ input }) => Pipeline.succeed(input))
        .match("route", {
          on: (o) => o.type,
          cases: {
            express: () => Pipeline.succeed("E"),
            standard: () => Pipeline.succeed("S"),
          },
        })
        .toJSON();

      const dot = dagToDot(dag);
      expect(dot).toContain('"route" [shape=diamond];');
      expect(dot).toContain('"route" -> "route.express" [label="express"];');
      expect(dot).toContain('"route" -> "route.standard" [label="standard"];');
      expect(dot).not.toContain('label="default"');
    });

    it("non-match steps render as plain rectangles, not diamonds", () => {
      const storage = new InMemoryWorkflowStorage();
      const dag = workflow<{}>({ name: "no-match" })
        .step("a", () => Pipeline.succeed(1))
        .step("b", () => Pipeline.succeed(2))
        .toJSON();

      const mermaid = dagToMermaid(dag);
      expect(mermaid).toContain('a["a"]');
      expect(mermaid).not.toContain("a{");
    });
  });
});
