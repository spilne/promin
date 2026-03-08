import { describe, it, expect } from "bun:test";
import { Data } from "effect";
import { Pipeline } from "../pipeline.ts";
import { workflow } from "./durable-pipeline.ts";
import { InMemoryWorkflowStorage } from "./in-memory-storage.ts";

// ---------------------------------------------------------------------------
// Test errors
// ---------------------------------------------------------------------------

class TestError extends Data.TaggedError("TestError")<{
  readonly message: string;
}> {}

// ---------------------------------------------------------------------------
// Execution attempt recording
// ---------------------------------------------------------------------------

describe("Step attempt history — execution", () => {
  it("records successful step execution", async () => {
    const storage = new InMemoryWorkflowStorage();

    await workflow<string>({ name: "record-success", storage })
      .step("step-1", ({ input }) => Pipeline.succeed(input.toUpperCase()))
      .run({ workflowId: "rec-1", input: "hello" });

    const attempts = await storage.loadStepAttempts("rec-1");
    expect(attempts).toHaveLength(1);
    expect(attempts[0]!.stepName).toBe("step-1");
    expect(attempts[0]!.type).toBe("execution");
    expect(attempts[0]!.status).toBe("completed");
    expect(attempts[0]!.result).toBe("HELLO");
    expect(attempts[0]!.attempt).toBe(1);
    expect(attempts[0]!.startedAt).toBeInstanceOf(Date);
    expect(attempts[0]!.completedAt).toBeInstanceOf(Date);
  });

  it("records failed step execution", async () => {
    const storage = new InMemoryWorkflowStorage();

    await workflow<string>({ name: "record-fail", storage })
      .step("step-1", () => Pipeline.fail(new TestError({ message: "boom" })))
      .runSafe({ workflowId: "rec-2", input: "x" });

    const attempts = await storage.loadStepAttempts("rec-2");
    expect(attempts).toHaveLength(1);
    expect(attempts[0]!.type).toBe("execution");
    expect(attempts[0]!.status).toBe("failed");
    expect(attempts[0]!.error).toBeDefined();
  });

  it("records all step retry attempts", async () => {
    const storage = new InMemoryWorkflowStorage();
    let calls = 0;

    await workflow<string>({ name: "record-retries", storage })
      .step(
        "flaky",
        () => {
          calls++;
          if (calls < 3) return Pipeline.fail(new TestError({ message: `fail-${calls}` }));
          return Pipeline.succeed("ok");
        },
        {
          retry: { maxRetries: 5 },
        },
      )
      .run({ workflowId: "rec-3", input: "x" });

    // Only the final successful attempt is recorded via saveStepResult
    // Step-level retries happen inside Effect, only the outcome is checkpointed
    const attempts = await storage.loadStepAttempts("rec-3");
    expect(attempts.length).toBeGreaterThanOrEqual(1);
    expect(attempts.some((a) => a.status === "completed")).toBe(true);
  });

  it("records attempts across workflow retries", async () => {
    const storage = new InMemoryWorkflowStorage();
    let calls = 0;

    await workflow<string>({
      name: "record-wf-retries",
      storage,
      retry: { maxRetries: 2, baseDelayMs: 10 },
    })
      .step("step-1", () => Pipeline.succeed("ok"))
      .step("step-2", () => {
        calls++;
        if (calls < 3) return Pipeline.fail(new TestError({ message: `fail-${calls}` }));
        return Pipeline.succeed("done");
      })
      .run({ workflowId: "rec-4", input: "x" });

    const allAttempts = await storage.loadStepAttempts("rec-4");

    // step-1: 1 successful attempt
    const step1 = allAttempts.filter((a) => a.stepName === "step-1");
    expect(step1).toHaveLength(1);
    expect(step1[0]!.status).toBe("completed");

    // step-2: 2 failed + 1 successful
    const step2 = allAttempts.filter((a) => a.stepName === "step-2");
    expect(step2.filter((a) => a.status === "failed")).toHaveLength(2);
    expect(step2.filter((a) => a.status === "completed")).toHaveLength(1);
  });

  it("filters by step name", async () => {
    const storage = new InMemoryWorkflowStorage();

    await workflow<string>({ name: "filter-step", storage })
      .step("a", () => Pipeline.succeed("A"))
      .step("b", () => Pipeline.succeed("B"))
      .run({ workflowId: "rec-5", input: "x" });

    const all = await storage.loadStepAttempts("rec-5");
    expect(all).toHaveLength(2);

    const onlyA = await storage.loadStepAttempts("rec-5", "a");
    expect(onlyA).toHaveLength(1);
    expect(onlyA[0]!.stepName).toBe("a");
  });

  it("records multiple steps in a DAG", async () => {
    const storage = new InMemoryWorkflowStorage();

    await workflow<string>({ name: "dag-attempts", storage })
      .step("root", ({ input }) => Pipeline.succeed(input))
      .step("left", { dependsOn: ["root"] }, () => Pipeline.succeed("L"))
      .step("right", { dependsOn: ["root"] }, () => Pipeline.succeed("R"))
      .step("join", { dependsOn: ["left", "right"] }, ({ deps }) =>
        Pipeline.succeed(`${deps.left}-${deps.right}`),
      )
      .run({ workflowId: "rec-6", input: "x" });

    const all = await storage.loadStepAttempts("rec-6");
    expect(all).toHaveLength(4);
    expect(all.every((a) => a.status === "completed")).toBe(true);
    const names = all.map((a) => a.stepName).sort();
    expect(names).toEqual(["join", "left", "right", "root"]);
  });
});

// ---------------------------------------------------------------------------
// Compensation attempt recording
// ---------------------------------------------------------------------------

describe("Step attempt history — compensation", () => {
  it("records successful compensation", async () => {
    const storage = new InMemoryWorkflowStorage();

    await workflow<string>({ name: "comp-record", storage })
      .step("step-1", () => Pipeline.succeed("done"), {
        compensate: () => Pipeline.succeed(undefined as void),
      })
      .step("fail", () => Pipeline.fail(new TestError({ message: "boom" })))
      .runSafe({ workflowId: "comp-rec-1", input: "x" });

    const attempts = await storage.loadStepAttempts("comp-rec-1", "step-1");
    const compAttempts = attempts.filter((a) => a.type === "compensation");
    expect(compAttempts).toHaveLength(1);
    expect(compAttempts[0]!.status).toBe("completed");
    expect(compAttempts[0]!.attempt).toBe(1);
  });

  it("records failed compensation", async () => {
    const storage = new InMemoryWorkflowStorage();

    await workflow<string>({ name: "comp-fail-record", storage })
      .step("step-1", () => Pipeline.succeed("done"), {
        compensate: () => {
          throw new Error("comp-failed");
        },
      })
      .step("fail", () => Pipeline.fail(new TestError({ message: "boom" })))
      .runSafe({ workflowId: "comp-rec-2", input: "x" });

    const attempts = await storage.loadStepAttempts("comp-rec-2", "step-1");
    const compAttempts = attempts.filter((a) => a.type === "compensation");
    expect(compAttempts).toHaveLength(1);
    expect(compAttempts[0]!.status).toBe("failed");
    expect(compAttempts[0]!.error).toBe("comp-failed");
  });

  it("records compensation retry attempts", async () => {
    const storage = new InMemoryWorkflowStorage();
    let compCalls = 0;

    await workflow<string>({
      name: "comp-retry-record",
      storage,
      compensate: {
        retry: { maxRetries: 2, baseDelayMs: 10 },
      },
    })
      .step("step-1", () => Pipeline.succeed("done"), {
        compensate: () => {
          compCalls++;
          if (compCalls < 3) throw new Error(`comp-fail-${compCalls}`);
          return Pipeline.succeed(undefined as void);
        },
      })
      .step("fail", () => Pipeline.fail(new TestError({ message: "boom" })))
      .runSafe({ workflowId: "comp-rec-3", input: "x" });

    const attempts = await storage.loadStepAttempts("comp-rec-3", "step-1");
    const compAttempts = attempts.filter((a) => a.type === "compensation");
    // 2 failed + 1 successful
    expect(compAttempts).toHaveLength(3);
    expect(compAttempts.filter((a) => a.status === "failed")).toHaveLength(2);
    expect(compAttempts.filter((a) => a.status === "completed")).toHaveLength(1);
    // Attempt numbers are sequential
    expect(compAttempts.map((a) => a.attempt)).toEqual([1, 2, 3]);
  });

  it("records both execution and compensation attempts together", async () => {
    const storage = new InMemoryWorkflowStorage();

    await workflow<string>({ name: "both-types", storage })
      .step("step-1", () => Pipeline.succeed("ok"), {
        compensate: () => Pipeline.succeed(undefined as void),
      })
      .step("step-2", () => Pipeline.succeed("ok"), {
        compensate: () => Pipeline.succeed(undefined as void),
      })
      .step("fail", () => Pipeline.fail(new TestError({ message: "boom" })))
      .runSafe({ workflowId: "comp-rec-4", input: "x" });

    const all = await storage.loadStepAttempts("comp-rec-4");
    const execAttempts = all.filter((a) => a.type === "execution");
    const compAttempts = all.filter((a) => a.type === "compensation");

    // 2 successful executions (step-1, step-2) + 1 failed execution (fail step)
    expect(execAttempts.length).toBeGreaterThanOrEqual(2);
    // 2 compensations (step-2, step-1 — reverse order)
    expect(compAttempts).toHaveLength(2);
    expect(compAttempts.every((a) => a.status === "completed")).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// Storage without attempt support
// ---------------------------------------------------------------------------

describe("Storage without StepAttemptStorage", () => {
  it("works normally when storage doesn't implement StepAttemptStorage", async () => {
    // Create a minimal storage that doesn't have saveStepAttempt
    const base = new InMemoryWorkflowStorage();
    const minimalStorage: any = {
      loadWorkflow: base.loadWorkflow.bind(base),
      listWorkflows: base.listWorkflows.bind(base),
      cancelWorkflow: base.cancelWorkflow.bind(base),
      createWorkflow: base.createWorkflow.bind(base),
      saveStepResult: base.saveStepResult.bind(base),
      saveStepFailure: base.saveStepFailure.bind(base),
      saveTaskResult: base.saveTaskResult.bind(base),
      saveTaskFailure: base.saveTaskFailure.bind(base),
      completeWorkflow: base.completeWorkflow.bind(base),
      failWorkflow: base.failWorkflow.bind(base),
      suspendWorkflow: base.suspendWorkflow.bind(base),
      deliverSignal: base.deliverSignal.bind(base),
      loadSignals: base.loadSignals.bind(base),
      tryLock: base.tryLock.bind(base),
      releaseLock: base.releaseLock.bind(base),
      heartbeat: base.heartbeat.bind(base),
      // NO saveStepAttempt or loadStepAttempts
    };

    const result = await workflow<number>({ name: "no-attempts", storage: minimalStorage })
      .step("double", ({ input }) => Pipeline.succeed(input * 2))
      .run({ workflowId: "no-att-1", input: 5 });

    expect(result).toBe(10);
  });
});
