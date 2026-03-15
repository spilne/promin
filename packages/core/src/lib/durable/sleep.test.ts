import { describe, it, expect } from "bun:test";
import { Data } from "effect";
import { Pipeline } from "../pipeline.ts";
import { workflow, WorkflowSuspendedError, InMemoryWorkflowStorage } from "./index.ts";

class TestError extends Data.TaggedError("TestError")<{
  readonly message: string;
}> {}

// ---------------------------------------------------------------------------
// Basic sleep
// ---------------------------------------------------------------------------

describe("Durable sleep — pause a workflow and resume it later", () => {
  it("email campaign waits 60s before sending — workflow suspends at the delay", async () => {
    const storage = new InMemoryWorkflowStorage();
    const log: string[] = [];

    const { error } = await workflow<string>({ name: "basic-sleep", storage })
      .step("before", ({ input }) => {
        log.push("before");
        return Pipeline.succeed(input);
      })
      .sleep("nap", 60_000)
      .step("after", ({ prev }) => {
        log.push("after");
        return Pipeline.succeed(prev);
      })
      .runSafe({ workflowId: "s-1", input: "hello" });

    expect((error as WorkflowSuspendedError)._tag).toBe("WorkflowSuspendedError");
    expect((error as WorkflowSuspendedError).reason).toBe("sleep");
    expect(log).toEqual(["before"]); // "after" not reached

    const state = await storage.loadWorkflow("s-1");
    expect(state?.status).toBe("suspended");
    expect(state?.steps["nap"]?.status).toBe("sleeping");
    expect(state?.steps["nap"]?.wakeAt).toBeInstanceOf(Date);
    expect(state?.steps["before"]?.status).toBe("completed");
    expect(state?.steps["before"]?.result).toBe("hello");
  });

  it("delay expires and workflow picks up where it left off — prior results preserved", async () => {
    const storage = new InMemoryWorkflowStorage();
    const log: string[] = [];

    const buildWf = () =>
      workflow<number>({ name: "resume-sleep", storage })
        .step("double", ({ input }) => {
          log.push("double");
          return Pipeline.succeed(input * 2);
        })
        .sleep("nap", 1)
        .step("add-100", ({ prev }) => {
          log.push("add-100");
          return Pipeline.succeed(prev + 100);
        });

    // First run: suspends
    const { error } = await buildWf().runSafe({ workflowId: "s-2", input: 5 });
    expect((error as WorkflowSuspendedError)._tag).toBe("WorkflowSuspendedError");
    expect(log).toEqual(["double"]);

    // Wait for sleep to expire
    await new Promise((r) => setTimeout(r, 10));

    // Resume: completes
    await buildWf().run({ workflowId: "s-2", input: 5 });

    // "double" should NOT re-execute — it was checkpointed
    // But prev through sleep may be undefined (sleep doesn't pass through value)
    expect(log).toEqual(["double", "add-100"]);

    const state = await storage.loadWorkflow("s-2");
    expect(state?.status).toBe("completed");
  });

  it("multi-stage drip campaign — pause between each email send", async () => {
    const storage = new InMemoryWorkflowStorage();
    const log: string[] = [];

    const buildWf = () =>
      workflow<string>({ name: "multi-sleep", storage })
        .step("step-1", ({ input }) => {
          log.push("step-1");
          return Pipeline.succeed(input);
        })
        .sleep("sleep-1", 1)
        .step("step-2", () => {
          log.push("step-2");
          return Pipeline.succeed("after-sleep-1");
        })
        .sleep("sleep-2", 1)
        .step("step-3", () => {
          log.push("step-3");
          return Pipeline.succeed("after-sleep-2");
        });

    // Run 1: suspends at sleep-1
    const { error: e1 } = await buildWf().runSafe({ workflowId: "s-3", input: "start" });
    expect((e1 as WorkflowSuspendedError).stepName).toBe("sleep-1");
    expect(log).toEqual(["step-1"]);

    await new Promise((r) => setTimeout(r, 10));

    // Run 2: resumes, executes step-2, suspends at sleep-2
    const { error: e2 } = await buildWf().runSafe({ workflowId: "s-3", input: "start" });
    expect((e2 as WorkflowSuspendedError).stepName).toBe("sleep-2");
    expect(log).toEqual(["step-1", "step-2"]);

    await new Promise((r) => setTimeout(r, 10));

    // Run 3: resumes, executes step-3, completes
    const result = await buildWf().run({ workflowId: "s-3", input: "start" });
    expect(result).toBe("after-sleep-2");
    expect(log).toEqual(["step-1", "step-2", "step-3"]);
  });

  it("already-expired sleep completes immediately — no double suspension", async () => {
    const storage = new InMemoryWorkflowStorage();

    const buildWf = () =>
      workflow<string>({ name: "no-re-suspend", storage })
        .step("before", () => Pipeline.succeed("ok"))
        .sleep("nap", 1) // 1ms
        .step("after", () => Pipeline.succeed("done"));

    // Suspend
    await buildWf().runSafe({ workflowId: "s-4", input: "x" });
    await new Promise((r) => setTimeout(r, 10));

    // Resume — should complete without re-suspending
    const result = await buildWf().run({ workflowId: "s-4", input: "x" });
    expect(result).toBe("done");

    // Run again — already completed, should just return
    const state = await storage.loadWorkflow("s-4");
    expect(state?.status).toBe("completed");
  });

  it("expensive API call before sleep is not repeated on resume — checkpointed", async () => {
    const storage = new InMemoryWorkflowStorage();
    let step1Calls = 0;

    const buildWf = () =>
      workflow<number>({ name: "no-reexec", storage })
        .step("expensive", ({ input }) => {
          step1Calls++;
          return Pipeline.succeed(input * 100);
        })
        .sleep("nap", 1)
        .step("cheap", () => Pipeline.succeed("done"));

    await buildWf().runSafe({ workflowId: "s-5", input: 5 });
    expect(step1Calls).toBe(1);

    await new Promise((r) => setTimeout(r, 10));

    await buildWf().run({ workflowId: "s-5", input: 5 });
    expect(step1Calls).toBe(1); // NOT re-executed
  });
});

// ---------------------------------------------------------------------------
// Sleep + compensation
// ---------------------------------------------------------------------------

describe("Sleep + compensation — rollback pre-sleep work if post-sleep step fails", () => {
  it("resource provisioned before delay, usage fails after — resource is cleaned up", async () => {
    const storage = new InMemoryWorkflowStorage();
    const log: string[] = [];

    const buildWf = () =>
      workflow<string>({ name: "sleep-comp", storage })
        .step(
          "create",
          () => {
            log.push("create");
            return Pipeline.succeed("resource-1");
          },
          {
            compensate: ({ result }) => {
              log.push(`compensate-create(${result})`);
              return Pipeline.succeed(undefined as void);
            },
          },
        )
        .sleep("nap", 1)
        .step("use", () => {
          log.push("use-fails");
          return Pipeline.fail(new TestError({ message: "post-sleep failure" }));
        });

    // Suspend at sleep
    await buildWf().runSafe({ workflowId: "sc-1", input: "x" });
    expect(log).toEqual(["create"]);

    await new Promise((r) => setTimeout(r, 10));

    // Resume — "use" fails → compensate "create"
    const { error } = await buildWf().runSafe({ workflowId: "sc-1", input: "x" });
    expect(error).not.toBeNull();
    expect(log).toContain("use-fails");
    expect(log).toContain("compensate-create(resource-1)");
  });
});

// ---------------------------------------------------------------------------
// Sleep + workflow retry
// ---------------------------------------------------------------------------

describe("Sleep + workflow retry — resume from where the workflow left off", () => {
  it("flaky step after sleep retries without re-sleeping — delay already elapsed", async () => {
    const storage = new InMemoryWorkflowStorage();
    const log: string[] = [];
    let step2Calls = 0;

    const buildWf = () =>
      workflow<string>({
        name: "sleep-retry",
        storage,
        retry: { maxRetries: 1, baseDelayMs: 10 },
      })
        .step("before", () => {
          log.push("before");
          return Pipeline.succeed("ok");
        })
        .sleep("nap", 1)
        .step("flaky", () => {
          step2Calls++;
          log.push(`flaky-${step2Calls}`);
          if (step2Calls < 2) {
            return Pipeline.fail(new TestError({ message: "transient" }));
          }
          return Pipeline.succeed("recovered");
        });

    // Suspend
    await buildWf().runSafe({ workflowId: "sr-1", input: "x" });
    await new Promise((r) => setTimeout(r, 10));

    // Resume — flaky fails once, workflow retries, flaky succeeds
    const result = await buildWf().run({ workflowId: "sr-1", input: "x" });
    expect(result).toBe("recovered");
    expect(step2Calls).toBe(2);

    // "before" only ran once (checkpointed)
    expect(log.filter((l) => l === "before")).toHaveLength(1);
  });
});

// ---------------------------------------------------------------------------
// Long sleep values
// ---------------------------------------------------------------------------

describe("Long sleep durations — schedule workflows days or months in the future", () => {
  it("30-day trial expiry reminder — wake-at timestamp is accurate", async () => {
    const storage = new InMemoryWorkflowStorage();
    const before = Date.now();

    await workflow<string>({ name: "long-sleep", storage })
      .step("start", () => Pipeline.succeed("ok"))
      .sleep("30-days", 30 * 24 * 60 * 60 * 1000)
      .runSafe({ workflowId: "ls-1", input: "x" });

    const state = await storage.loadWorkflow("ls-1");
    const wakeAt = state?.steps["30-days"]?.wakeAt;
    expect(wakeAt).toBeInstanceOf(Date);

    // wakeAt should be ~30 days from now
    const expectedMs = 30 * 24 * 60 * 60 * 1000;
    const actualMs = wakeAt!.getTime() - before;
    expect(actualMs).toBeGreaterThan(expectedMs - 1000);
    expect(actualMs).toBeLessThan(expectedMs + 1000);
  });

  it("annual contract renewal in 1 year — wake-at timestamp is accurate", async () => {
    const storage = new InMemoryWorkflowStorage();
    const before = Date.now();

    await workflow<string>({ name: "year-sleep", storage })
      .step("start", () => Pipeline.succeed("ok"))
      .sleep("1-year", 365 * 24 * 60 * 60 * 1000)
      .runSafe({ workflowId: "ls-2", input: "x" });

    const state = await storage.loadWorkflow("ls-2");
    const wakeAt = state?.steps["1-year"]?.wakeAt;
    expect(wakeAt).toBeInstanceOf(Date);

    const expectedMs = 365 * 24 * 60 * 60 * 1000;
    const actualMs = wakeAt!.getTime() - before;
    expect(actualMs).toBeGreaterThan(expectedMs - 1000);
    expect(actualMs).toBeLessThan(expectedMs + 1000);
  });
});
