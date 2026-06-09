import { describe, it, expect } from "bun:test";
import { Data } from "effect";
import { Pipeline, type TaggedError } from "@promin/core";
import { workflow, InMemoryWorkflowStorage } from "../../durable/index.ts";
import { createWorkflowRunner } from "../../durable/workflow-runner.ts";
import { createSleepScanner } from "../sleep-scanner.ts";

// ---------------------------------------------------------------------------
// SleepScanner — resumes expired sleeps
// ---------------------------------------------------------------------------

describe("Sleep scanner — background process that wakes up sleeping workflows", () => {
  it("1ms sleep expires — scanner detects it and resumes the workflow", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const log: string[] = [];

    const wfDef = workflow<{ msg: string }>({ name: "sleepy" })
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
    const { error } = await runner.runSafe({
      workflow: wfDef,
      workflowId: "sleep-1",
      input: { msg: "hello" },
    });
    expect((error as any)?._tag).toBe("WorkflowSuspendedError");
    expect(log).toEqual(["before"]);

    // Wait for sleep to expire
    await new Promise((r) => setTimeout(r, 50));

    // Scanner picks it up and resumes
    const resumed: string[] = [];
    const scanner = createSleepScanner({
      storage,
      runner,
      scanIntervalMs: 50,
      resolveWorkflow: (name) => (name === "sleepy" ? wfDef : undefined),
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

  it("workflow sleeping for 31 years is not woken up prematurely", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });

    const wfDef = workflow<string>({ name: "long-sleep" })
      .step("before", () => Pipeline.succeed("ok"))
      .sleep("nap", 999_999_999) // ~31 years
      .step("after", () => Pipeline.succeed("done"))
      .build();

    await runner.runSafe({ workflow: wfDef, workflowId: "sleep-2", input: "x" });

    const resumed: string[] = [];
    const scanner = createSleepScanner({
      storage,
      runner,
      scanIntervalMs: 50,
      resolveWorkflow: (name) => (name === "long-sleep" ? wfDef : undefined),
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

  it("unrecognized workflow name — scanner skips it silently without crashing", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });

    const wfDef = workflow<string>({ name: "unknown-wf" })
      .step("before", () => Pipeline.succeed("ok"))
      .sleep("nap", 1)
      .step("after", () => Pipeline.succeed("done"))
      .build();

    await runner.runSafe({ workflow: wfDef, workflowId: "sleep-3", input: "x" });
    await new Promise((r) => setTimeout(r, 50));

    const errors: string[] = [];
    const scanner = createSleepScanner({
      storage,
      runner,
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

  it("passes the stored workflow version to resolveWorkflow", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const seenVersions: Array<string | undefined> = [];

    const wfDef = workflow<string>({ name: "versioned-sleep", version: "1" })
      .step("before", () => Pipeline.succeed("ok"))
      .sleep("nap", 1)
      .step("after", () => Pipeline.succeed("done"))
      .build();

    await runner.runSafe({ workflow: wfDef, workflowId: "sleep-versioned", input: "x" });
    await new Promise((r) => setTimeout(r, 50));

    const scanner = createSleepScanner({
      storage,
      runner,
      scanIntervalMs: 50,
      resolveWorkflow: (name, version) => {
        seenVersions.push(version);
        return name === "versioned-sleep" && version === "1" ? wfDef : undefined;
      },
    });

    void scanner.start();
    await new Promise((r) => setTimeout(r, 200));
    await scanner.stop();

    expect(seenVersions).toContain("1");
    const state = await storage.loadWorkflow("sleep-versioned");
    expect(state?.status).toBe("completed");
  });

  it("three workflows sleeping — scanner wakes all of them in one scan cycle", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });

    const wfDef = workflow<string>({ name: "multi" })
      .step("before", ({ input }) => Pipeline.succeed(input))
      .sleep("nap", 1)
      .step("after", ({ prev }) => Pipeline.succeed(`done: ${prev}`))
      .build();

    await runner.runSafe({ workflow: wfDef, workflowId: "sleep-a", input: "a" });
    await runner.runSafe({ workflow: wfDef, workflowId: "sleep-b", input: "b" });
    await runner.runSafe({ workflow: wfDef, workflowId: "sleep-c", input: "c" });

    await new Promise((r) => setTimeout(r, 50));

    const resumed: string[] = [];
    const scanner = createSleepScanner({
      storage,
      runner,
      scanIntervalMs: 50,
      resolveWorkflow: (name) => (name === "multi" ? wfDef : undefined),
      onResume: (id) => resumed.push(id),
    });

    void scanner.start();
    await new Promise((r) => setTimeout(r, 300));
    await scanner.stop();

    expect(resumed.sort()).toEqual(["sleep-a", "sleep-b", "sleep-c"]);
  });

  it("resume fails — error callback fires but scanner keeps running", async () => {
    class ResumeFailure extends Data.TaggedError("ResumeFailure")<{ readonly message: string }> {}

    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });

    // A workflow whose post-sleep step always fails — triggers the scanner's
    // onError path during resumption.
    const broken = workflow<string>({ name: "broken" })
      .step("before", () => Pipeline.succeed("ok"))
      .sleep("nap", 1)
      .step("boom", () =>
        Pipeline.fail(new ResumeFailure({ message: "resume failed" }) as TaggedError),
      )
      .build();

    // Kick it off so storage has a row sleeping on "nap"; wait for wake.
    await runner.runSafe({ workflow: broken, workflowId: "sleep-err", input: "x" });
    await new Promise((r) => setTimeout(r, 50));

    const errors: { id: string; err: unknown }[] = [];
    const scanner = createSleepScanner({
      storage,
      runner,
      scanIntervalMs: 50,
      resolveWorkflow: (name) => (name === "broken" ? broken : undefined),
      onError: (id, err) => errors.push({ id, err }),
    });

    void scanner.start();
    await new Promise((r) => setTimeout(r, 200));
    await scanner.stop();

    expect(errors.length).toBeGreaterThanOrEqual(1);
    expect(errors[0]!.id).toBe("sleep-err");
  });

  it("storage RPC failures don't kill the scan loop — keeps polling, calls onError, recovers", async () => {
    // Reproduces the symptom seen against a remote storage when the
    // server briefly drops out: `listWorkflows` throws ConnectionRefused.
    // Before: the unhandled exception terminated the scan loop. After:
    // the loop catches, logs, hands off to onError, and keeps going so
    // it picks up where it left off once the server is back.
    let nextThrow: Error | null = new Error("Unable to connect");
    (nextThrow as { code?: string }).code = "ConnectionRefused";
    let listCalls = 0;
    const fakeStorage = {
      async listWorkflows() {
        listCalls += 1;
        if (nextThrow) throw nextThrow;
        return [];
      },
    } as unknown as Parameters<typeof createSleepScanner>[0]["storage"];

    const errors: { id: string; err: unknown }[] = [];
    const scanner = createSleepScanner({
      storage: fakeStorage,
      runner: { run: async () => undefined } as unknown as Parameters<
        typeof createSleepScanner
      >[0]["runner"],
      scanIntervalMs: 10,
      resolveWorkflow: () => undefined,
      onError: (id, err) => errors.push({ id, err }),
    });

    void scanner.start();
    // First few ticks should fail with ConnectionRefused and call onError.
    await new Promise((r) => setTimeout(r, 60));
    expect(listCalls).toBeGreaterThanOrEqual(2);
    expect(errors.length).toBeGreaterThanOrEqual(1);
    expect(errors[0]!.id).toBe("(scan-loop)");
    expect((errors[0]!.err as { code?: string }).code).toBe("ConnectionRefused");

    // Server "comes back" — clear the throw and verify the loop is
    // still alive and keeps polling cleanly.
    nextThrow = null;
    const callsBefore = listCalls;
    await new Promise((r) => setTimeout(r, 60));
    expect(listCalls).toBeGreaterThan(callsBefore);

    await scanner.stop();
  });
});
