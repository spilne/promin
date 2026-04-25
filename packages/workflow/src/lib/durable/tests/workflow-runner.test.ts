// ---------------------------------------------------------------------------
// Coverage for the WorkflowRunner facade — `run`, `runSafe`, `start`,
// `getStatus`, plus registry-backed name resolution and version-drain-resume.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { Data } from "effect";
import { Pipeline } from "@promin/core";
import { workflow } from "../durable-pipeline.ts";
import { InMemoryWorkflowStorage } from "../in-memory-storage.ts";
import { InProcessStepExecutor, createWorkflowRunner } from "../workflow-runner.ts";
import { WorkflowVersionRegistry } from "../workflow-version-registry.ts";

class TestError extends Data.TaggedError("TestError")<{ readonly message: string }> {}

describe("WorkflowRunner", () => {
  it("run({ workflow }) drives a pure Workflow through the runner's storage", async () => {
    const storage = new InMemoryWorkflowStorage();
    const wf = workflow<{ n: number }>({ name: "runner-run-direct" })
      .step("triple", ({ input }) => Pipeline.succeed(input.n * 3))
      .build();

    const runner = createWorkflowRunner({ storage });
    const result = await runner.run({
      workflow: wf,
      workflowId: "run-direct-1",
      input: { n: 4 },
    });
    expect(result).toBe(12);
    const state = await storage.loadWorkflow("run-direct-1");
    expect(state?.status).toBe("completed");
  });

  it("runSafe({ workflow }) returns { data, error } for failures", async () => {
    const storage = new InMemoryWorkflowStorage();
    const wf = workflow<void>({ name: "runner-run-fail" })
      .step("boom", () => Pipeline.fail(new TestError({ message: "nope" })))
      .build();

    const runner = createWorkflowRunner({ storage });
    const { data, error } = await runner.runSafe({
      workflow: wf,
      workflowId: "run-fail-2",
      input: undefined,
    });
    expect(data).toBeNull();
    expect(error).not.toBeNull();
  });

  it("run({ name }) resolves the latest version via the registry", async () => {
    const storage = new InMemoryWorkflowStorage();
    const v1 = workflow<{ n: number }>({ name: "by-name", version: "1" })
      .step("bump", ({ input }) => Pipeline.succeed(input.n + 1))
      .build();
    const v2 = workflow<{ n: number }>({ name: "by-name", version: "2" })
      .step("bump", ({ input }) => Pipeline.succeed(input.n + 100))
      .build();

    const registry = new WorkflowVersionRegistry();
    registry.register(v1);
    registry.register(v2);

    const runner = createWorkflowRunner({ storage, registry });
    const result = await runner.run({
      name: "by-name",
      workflowId: "by-name-1",
      input: { n: 5 },
    });
    expect(result).toBe(105); // v2 is latest
  });

  it("run({ name }) drains an in-flight workflow on the version it was created under", async () => {
    const storage = new InMemoryWorkflowStorage();
    const v1 = workflow<{ n: number }>({ name: "drain", version: "1" })
      .step("bump", ({ input }) => Pipeline.succeed(input.n + 1))
      .build();
    const v2 = workflow<{ n: number }>({ name: "drain", version: "2" })
      .step("bump", ({ input }) => Pipeline.succeed(input.n + 100))
      .build();

    // Seed storage with a row under v1 so the name-based resume must drain.
    await storage.createWorkflow({
      workflowId: "drain-1",
      workflowName: "drain",
      input: { n: 5 },
      version: "1",
    });

    const registry = new WorkflowVersionRegistry();
    registry.register(v1);
    registry.register(v2);

    const runner = createWorkflowRunner({ storage, registry });
    const result = await runner.run({
      name: "drain",
      workflowId: "drain-1",
      input: { n: 5 },
    });
    expect(result).toBe(6); // v1 still drives the resume
  });

  it("run({ name }) without registry throws a pointed error", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    await expect(
      runner.run({ name: "missing", workflowId: "x", input: undefined }),
    ).rejects.toThrow(/registry/);
  });

  it("InProcessStepExecutor runs a step body and returns the encoded result", async () => {
    const storage = new InMemoryWorkflowStorage();
    const wf = workflow<{ n: number }>({ name: "executor-test" })
      .step("double", ({ input }) => Pipeline.succeed((input as { n: number }).n * 2))
      .build();

    const executor = new InProcessStepExecutor(wf, { storage });
    const res = await executor.executeStep({
      workflowId: "exec-1",
      stepName: "double",
      input: { n: 7 },
      prevResults: {},
      attempt: 1,
    });
    expect(res.ok).toBe(true);
    if (res.ok) expect(res.result).toBe(14);
  });

  // ---------------------------------------------------------------------------
  // Eager save — saveStepResult fires when each step's body resolves, not
  // after the slowest sibling in the wave finishes. Both runner paths
  // (legacy Pipeline.all + executor) carry the behavior.
  // ---------------------------------------------------------------------------

  describe("eager save", () => {
    /**
     * Storage decorator that records the wall-clock time of every
     * saveStepResult call. Lets the timing tests assert that fast steps
     * persisted long before slow siblings in their wave.
     */
    class RecordingStorage extends InMemoryWorkflowStorage {
      readonly saves: Array<{ stepName: string; at: number }> = [];
      override async saveStepResult(
        ...args: Parameters<InMemoryWorkflowStorage["saveStepResult"]>
      ): Promise<void> {
        this.saves.push({ stepName: args[0].stepName, at: Date.now() });
        return super.saveStepResult(...args);
      }
    }

    it("legacy path: a fast parallel step persists before its slow sibling finishes", async () => {
      const storage = new RecordingStorage();
      const wf = workflow<void>({ name: "eager-legacy" })
        // Two parallel roots (no dependsOn) → same wave.
        .stepAsync("fast", async () => {
          // ~immediate
          return "fast-done";
        })
        .stepAsync(
          "slow",
          async () => {
            await new Promise((r) => setTimeout(r, 200));
            return "slow-done";
          },
          { dependsOn: [] },
        )
        .build();

      const runner = createWorkflowRunner({ storage });
      await runner.run({ workflow: wf, workflowId: "eager-legacy-1", input: undefined });

      const fastSave = storage.saves.find((s) => s.stepName === "fast");
      const slowSave = storage.saves.find((s) => s.stepName === "slow");
      expect(fastSave).toBeDefined();
      expect(slowSave).toBeDefined();
      // The whole point of eager save: fast lands well before slow.
      // Pre-eager, both timestamps would cluster within a few ms of each
      // other (post-wave serial loop). 100ms gives ample margin against
      // the 200ms slow body without flaking on slow CI.
      expect(slowSave!.at - fastSave!.at).toBeGreaterThan(100);
    });

    it("executor path: a fast parallel step persists before its slow sibling finishes", async () => {
      const storage = new RecordingStorage();
      const wf = workflow<void>({ name: "eager-executor" })
        .stepAsync("fast", async () => "fast-done")
        .stepAsync(
          "slow",
          async () => {
            await new Promise((r) => setTimeout(r, 200));
            return "slow-done";
          },
          { dependsOn: [] },
        )
        .build();

      const executor = new InProcessStepExecutor(wf, { storage });
      const runner = createWorkflowRunner({ storage, stepExecutor: executor });
      await runner.run({ workflow: wf, workflowId: "eager-executor-1", input: undefined });

      const fastSave = storage.saves.find((s) => s.stepName === "fast");
      const slowSave = storage.saves.find((s) => s.stepName === "slow");
      expect(fastSave).toBeDefined();
      expect(slowSave).toBeDefined();
      expect(slowSave!.at - fastSave!.at).toBeGreaterThan(100);
    });

    it("partial-wave failure: a sibling that already completed has its row in storage", async () => {
      // One parallel step succeeds, the other throws. Eager save is the
      // contract: the successful sibling's row should be in storage even
      // though the wave failed overall.
      const storage = new RecordingStorage();
      const wf = workflow<void>({ name: "eager-partial" })
        .stepAsync("ok", async () => "yay")
        .stepAsync(
          "boom",
          async () => {
            // Tiny stagger so "ok" completes (and its eager save fires)
            // before this throws.
            await new Promise((r) => setTimeout(r, 20));
            throw new Error("kaboom");
          },
          { dependsOn: [] },
        )
        .build();

      const runner = createWorkflowRunner({ storage });
      const { error } = await runner.runSafe({
        workflow: wf,
        workflowId: "eager-partial-1",
        input: undefined,
      });
      expect(error).not.toBeNull();

      const state = await storage.loadWorkflow("eager-partial-1");
      // Workflow is failed overall, but `ok`'s row persists at completed.
      expect(state?.status).toBe("failed");
      expect(state?.steps["ok"]?.status).toBe("completed");
      expect(state?.steps["ok"]?.result).toBe("yay");
    });

    it("post-wave loop doesn't double-save (storageAlreadyCheckpointed honored)", async () => {
      const storage = new RecordingStorage();
      const wf = workflow<{ n: number }>({ name: "eager-no-double" })
        .step("once", ({ input }) => Pipeline.succeed(input.n + 1))
        .build();

      const runner = createWorkflowRunner({ storage });
      await runner.run({
        workflow: wf,
        workflowId: "eager-no-double-1",
        input: { n: 5 },
      });

      // Exactly one save call for the step — eager save fires it once,
      // post-wave loop sees `storageAlreadyCheckpointed: true` and skips.
      const onceSaves = storage.saves.filter((s) => s.stepName === "once");
      expect(onceSaves).toHaveLength(1);
    });
  });
});
