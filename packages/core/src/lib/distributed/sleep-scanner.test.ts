import { describe, it, expect } from "bun:test";
import { Pipeline } from "../pipeline.ts";
import { workflow, InMemoryWorkflowStorage } from "../durable/index.ts";
import { createSleepScanner } from "./sleep-scanner.ts";

// ---------------------------------------------------------------------------
// SleepScanner — resumes expired sleeps
// ---------------------------------------------------------------------------

describe("SleepScanner", () => {
  it("resumes a workflow after sleep expires", async () => {
    const storage = new InMemoryWorkflowStorage();
    const log: string[] = [];

    const wf = workflow<{ msg: string }>({ name: "sleepy", storage })
      .step("before", ({ input }) => {
        log.push("before");
        return Pipeline.succeed(input.msg);
      })
      .sleep("nap", 1) // 1ms sleep — expires immediately
      .step("after", ({ prev }) => {
        log.push("after");
        return Pipeline.succeed(`woke: ${prev}`);
      })
      .build();

    // Run — will suspend at the sleep step
    const { error } = await wf.runSafe({ workflowId: "sleep-1", input: { msg: "hello" } });
    expect((error as any)?._tag).toBe("WorkflowSuspendedError");
    expect(log).toEqual(["before"]);

    // Wait for sleep to expire
    await new Promise((r) => setTimeout(r, 50));

    // Scanner picks it up and resumes
    const resumed: string[] = [];
    const scanner = createSleepScanner({
      storage,
      scanIntervalMs: 50,
      resolveWorkflow: (name) => (name === "sleepy" ? wf : undefined),
      onResume: (id) => resumed.push(id),
    });

    void scanner.start();
    await new Promise((r) => setTimeout(r, 200));
    await scanner.stop();

    expect(resumed).toContain("sleep-1");
    expect(log).toEqual(["before", "after"]);

    const state = await storage.loadWorkflow("sleep-1");
    expect(state?.status).toBe("completed");
  });

  it("ignores workflows whose sleep has not expired", async () => {
    const storage = new InMemoryWorkflowStorage();

    const wf = workflow<string>({ name: "long-sleep", storage })
      .step("before", () => Pipeline.succeed("ok"))
      .sleep("nap", 999_999_999) // ~31 years
      .step("after", () => Pipeline.succeed("done"))
      .build();

    await wf.runSafe({ workflowId: "sleep-2", input: "x" });

    const resumed: string[] = [];
    const scanner = createSleepScanner({
      storage,
      scanIntervalMs: 50,
      resolveWorkflow: (name) => (name === "long-sleep" ? wf : undefined),
      onResume: (id) => resumed.push(id),
    });

    void scanner.start();
    await new Promise((r) => setTimeout(r, 200));
    await scanner.stop();

    // Should NOT resume — sleep hasn't expired
    expect(resumed).toHaveLength(0);

    const state = await storage.loadWorkflow("sleep-2");
    expect(state?.status).toBe("suspended");
  });

  it("skips workflows with unknown definition", async () => {
    const storage = new InMemoryWorkflowStorage();

    const wf = workflow<string>({ name: "unknown-wf", storage })
      .step("before", () => Pipeline.succeed("ok"))
      .sleep("nap", 1)
      .step("after", () => Pipeline.succeed("done"))
      .build();

    await wf.runSafe({ workflowId: "sleep-3", input: "x" });
    await new Promise((r) => setTimeout(r, 50));

    const errors: string[] = [];
    const scanner = createSleepScanner({
      storage,
      scanIntervalMs: 50,
      resolveWorkflow: () => undefined, // can't resolve
      onError: (id) => errors.push(id),
    });

    void scanner.start();
    await new Promise((r) => setTimeout(r, 200));
    await scanner.stop();

    // No error, no resume — silently skipped
    expect(errors).toHaveLength(0);
  });

  it("handles multiple suspended workflows", async () => {
    const storage = new InMemoryWorkflowStorage();

    const wf = workflow<string>({ name: "multi", storage })
      .step("before", ({ input }) => Pipeline.succeed(input))
      .sleep("nap", 1)
      .step("after", ({ prev }) => Pipeline.succeed(`done: ${prev}`))
      .build();

    await wf.runSafe({ workflowId: "sleep-a", input: "a" });
    await wf.runSafe({ workflowId: "sleep-b", input: "b" });
    await wf.runSafe({ workflowId: "sleep-c", input: "c" });

    await new Promise((r) => setTimeout(r, 50));

    const resumed: string[] = [];
    const scanner = createSleepScanner({
      storage,
      scanIntervalMs: 50,
      resolveWorkflow: (name) => (name === "multi" ? wf : undefined),
      onResume: (id) => resumed.push(id),
    });

    void scanner.start();
    await new Promise((r) => setTimeout(r, 300));
    await scanner.stop();

    expect(resumed.sort()).toEqual(["sleep-a", "sleep-b", "sleep-c"]);
  });

  it("handles resumption errors", async () => {
    const storage = new InMemoryWorkflowStorage();

    // Manually create a suspended workflow that will fail on resume
    await storage.createWorkflow({
      workflowId: "sleep-err",
      workflowName: "broken",
      input: "x",
    });
    await storage.suspendWorkflow("sleep-err", "nap", {
      status: "sleeping",
      stepType: "sleep",
      wakeAt: new Date(Date.now() - 1000), // already expired
    });

    const errors: { id: string; err: unknown }[] = [];
    const scanner = createSleepScanner({
      storage,
      scanIntervalMs: 50,
      resolveWorkflow: (name) => {
        if (name !== "broken") return undefined;
        // Return a definition that will fail
        return {
          name: "broken",
          storage,
          dag: { name: "broken", steps: [] },
          run: async () => {
            throw new Error("resume failed");
          },
          runSafe: async () => ({ data: null, error: new Error("resume failed") }),
          invoke: () => Pipeline.fail(new Error("nope") as never),
        };
      },
      onError: (id, err) => errors.push({ id, err }),
    });

    void scanner.start();
    await new Promise((r) => setTimeout(r, 200));
    await scanner.stop();

    expect(errors.length).toBeGreaterThanOrEqual(1);
    expect(errors[0]!.id).toBe("sleep-err");
  });
});
