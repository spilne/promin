import { describe, it, expect } from "bun:test";
import { Data } from "effect";
import { Pipeline } from "../pipeline.ts";
import { StreamPipeline } from "../stream-pipeline.ts";
import { workflow } from "./durable-pipeline.ts";
import { InMemoryWorkflowStorage } from "./in-memory-storage.ts";
import { trigger, WorkflowResult } from "./workflow-trigger.ts";

// ---------------------------------------------------------------------------
// Test error types
// ---------------------------------------------------------------------------

class ProcessError extends Data.TaggedError("ProcessError")<{
  readonly message: string;
}> {}

// ---------------------------------------------------------------------------
// WorkflowResult ADT
// ---------------------------------------------------------------------------

describe("WorkflowResult", () => {
  it("completed constructor", () => {
    const r = WorkflowResult.completed({ workflowId: "wf-1", result: 42, durationMs: 100 });
    expect(r._tag).toBe("completed");
    expect(r.result).toBe(42);
  });

  it("failed constructor", () => {
    const r = WorkflowResult.failed({ workflowId: "wf-1", error: "boom", durationMs: 50 });
    expect(r._tag).toBe("failed");
    expect(r.error).toBe("boom");
  });

  it("skipped constructor", () => {
    const r = WorkflowResult.skipped({ workflowId: "wf-1", reason: "duplicate" });
    expect(r._tag).toBe("skipped");
  });

  it("type guards", () => {
    const completed = WorkflowResult.completed({
      workflowId: "w",
      result: 1,
      durationMs: 0,
    });
    const failed = WorkflowResult.failed({ workflowId: "w", error: "e", durationMs: 0 });
    const skipped = WorkflowResult.skipped({ workflowId: "w", reason: "duplicate" });

    expect(WorkflowResult.isCompleted(completed)).toBe(true);
    expect(WorkflowResult.isCompleted(failed)).toBe(false);
    expect(WorkflowResult.isFailed(failed)).toBe(true);
    expect(WorkflowResult.isFailed(completed)).toBe(false);
    expect(WorkflowResult.isSkipped(skipped)).toBe(true);
    expect(WorkflowResult.isSkipped(completed)).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// WorkflowBuilder.build()
// ---------------------------------------------------------------------------

describe("WorkflowBuilder.build", () => {
  it("returns a WorkflowDefinition with run and runSafe", async () => {
    const storage = new InMemoryWorkflowStorage();
    const def = workflow<{ n: number }>({ name: "buildable", storage })
      .step("double", ({ input }) => Pipeline.succeed(input.n * 2))
      .build();

    expect(def.name).toBe("buildable");
    expect(def.storage).toBe(storage);

    const result = await def.run({ workflowId: "b-1", input: { n: 5 } });
    expect(result).toBe(10);
  });

  it("runSafe works on built definition", async () => {
    const storage = new InMemoryWorkflowStorage();
    const def = workflow<{}>({ name: "safe-build", storage })
      .step("fail", () => Pipeline.fail(new ProcessError({ message: "oops" })))
      .build();

    const { data, error } = await def.runSafe({ workflowId: "b-2", input: {} });
    expect(data).toBeNull();
    expect(error).not.toBeNull();
  });

  it("built definition is reusable across multiple runs", async () => {
    const storage = new InMemoryWorkflowStorage();
    const def = workflow<{ n: number }>({ name: "reusable", storage })
      .step("inc", ({ input }) => Pipeline.succeed(input.n + 1))
      .build();

    const r1 = await def.run({ workflowId: "r-1", input: { n: 10 } });
    const r2 = await def.run({ workflowId: "r-2", input: { n: 20 } });
    expect(r1).toBe(11);
    expect(r2).toBe(21);
  });
});

// ---------------------------------------------------------------------------
// trigger() — stream transformer
// ---------------------------------------------------------------------------

describe("trigger", () => {
  it("triggers a workflow for each stream item", async () => {
    const storage = new InMemoryWorkflowStorage();
    const def = workflow<{ value: number }>({ name: "triggered", storage })
      .step("double", ({ input }) => Pipeline.succeed(input.value * 2))
      .build();

    const results = await StreamPipeline.fromIterable([1, 2, 3])
      .through(
        trigger({
          workflow: def,
          toInput: (n) => ({ value: n }),
          toWorkflowId: (n) => `wf-${n}`,
        }),
      )
      .collect();

    expect(results).toHaveLength(3);
    expect(results.every(WorkflowResult.isCompleted)).toBe(true);
    expect((results[0] as WorkflowResult.Completed<number>).result).toBe(2);
    expect((results[1] as WorkflowResult.Completed<number>).result).toBe(4);
    expect((results[2] as WorkflowResult.Completed<number>).result).toBe(6);
  });

  it("returns failed result on workflow failure", async () => {
    const storage = new InMemoryWorkflowStorage();
    const def = workflow<{ n: number }>({ name: "failing-trigger", storage })
      .step("boom", () => Pipeline.fail(new ProcessError({ message: "fail" })))
      .build();

    const results = await StreamPipeline.fromIterable([1])
      .through(
        trigger({
          workflow: def,
          toInput: (n) => ({ n }),
          toWorkflowId: (n) => `wf-fail-${n}`,
        }),
      )
      .collect();

    expect(results).toHaveLength(1);
    const r = results[0]!;
    expect(WorkflowResult.isFailed(r)).toBe(true);
    if (WorkflowResult.isFailed(r)) {
      expect(r.durationMs).toBeGreaterThanOrEqual(0);
    }
  });

  it("skips duplicates with onDuplicate=skip", async () => {
    const storage = new InMemoryWorkflowStorage();
    const def = workflow<{ n: number }>({ name: "dedup-trigger", storage })
      .step("compute", ({ input }) => Pipeline.succeed(input.n * 10))
      .build();

    // First run creates the workflow
    await def.run({ workflowId: "dedup-1", input: { n: 1 } });

    // Trigger with same ID — should skip
    const results = await StreamPipeline.fromIterable([1])
      .through(
        trigger({
          workflow: def,
          toInput: (n) => ({ n }),
          toWorkflowId: () => "dedup-1",
          onDuplicate: "skip",
        }),
      )
      .collect();

    expect(results).toHaveLength(1);
    expect(WorkflowResult.isSkipped(results[0]!)).toBe(true);
    expect((results[0] as WorkflowResult.Skipped).reason).toBe("duplicate");
  });

  it("respects concurrency", async () => {
    const storage = new InMemoryWorkflowStorage();
    let maxConcurrent = 0;
    let current = 0;

    const def = workflow<{ n: number }>({ name: "conc-trigger", storage })
      .stepAsync("slow", async ({ input }) => {
        current++;
        maxConcurrent = Math.max(maxConcurrent, current);
        await new Promise((r) => setTimeout(r, 20));
        current--;
        return input.n;
      })
      .build();

    const results = await StreamPipeline.fromIterable([1, 2, 3, 4])
      .through(
        trigger({
          workflow: def,
          toInput: (n) => ({ n }),
          toWorkflowId: (n) => `conc-${n}`,
          concurrency: 2,
        }),
      )
      .collect();

    expect(results).toHaveLength(4);
    expect(results.every(WorkflowResult.isCompleted)).toBe(true);
    expect(maxConcurrent).toBeLessThanOrEqual(2);
  });

  it("tracks durationMs", async () => {
    const storage = new InMemoryWorkflowStorage();
    const def = workflow<{}>({ name: "duration-trigger", storage })
      .stepAsync("wait", async () => {
        await new Promise((r) => setTimeout(r, 20));
        return "done";
      })
      .build();

    const results = await StreamPipeline.fromIterable([1])
      .through(
        trigger({
          workflow: def,
          toInput: () => ({}),
          toWorkflowId: (n) => `dur-${n}`,
        }),
      )
      .collect();

    const r = results[0]!;
    expect(r._tag).toBe("completed");
    if (WorkflowResult.isCompleted(r)) {
      expect(r.durationMs).toBeGreaterThanOrEqual(15);
    }
  });

  it("composes with stream operators", async () => {
    const storage = new InMemoryWorkflowStorage();
    const def = workflow<{ n: number }>({ name: "compose-trigger", storage })
      .step("double", ({ input }) => Pipeline.succeed(input.n * 2))
      .build();

    // filter + trigger + filter completed + map result
    const results = await StreamPipeline.fromIterable([1, 2, 3, 4, 5])
      .filter((n) => n % 2 === 0)
      .through(
        trigger({
          workflow: def,
          toInput: (n) => ({ n }),
          toWorkflowId: (n) => `compose-${n}`,
        }),
      )
      .filter(WorkflowResult.isCompleted)
      .map((r) => (r as WorkflowResult.Completed<number>).result)
      .collect();

    expect(results).toEqual([4, 8]); // only evens (2, 4) doubled
  });

  it("works with tick-based cron pattern", async () => {
    const storage = new InMemoryWorkflowStorage();
    let runCount = 0;

    const def = workflow<{ tick: number }>({ name: "cron-trigger", storage })
      .stepAsync("process", async ({ input }) => {
        runCount++;
        return `tick-${input.tick}`;
      })
      .build();

    await StreamPipeline.tick(10)
      .take(3)
      .through(
        trigger({
          workflow: def,
          toInput: (tick) => ({ tick }),
          toWorkflowId: (tick) => `cron-${tick}`,
        }),
      )
      .drain();

    expect(runCount).toBe(3);
  });
});
