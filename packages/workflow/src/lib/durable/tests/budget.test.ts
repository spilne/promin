// ---------------------------------------------------------------------------
// Budgets: usage reported with ctx.reportUsage is journaled (a retry or replay
// never counts it twice), summed into the status from stored step rows, and a
// run or step past its cap fails with WorkflowBudgetExceededError, which is
// never retried and rolls back like any failure.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { FakeWallClock } from "../../shared/wall-clock.ts";
import { completeDueSleeps } from "../journaled-step.ts";
import { WorkflowBudgetExceededError } from "../durable-pipeline-error.ts";
import { InMemoryWorkflowStorage } from "../in-memory-storage.ts";
import { workflow } from "../workflow-builder.ts";
import { createWorkflowRunner } from "../workflow-runner.ts";
import type { WorkflowTracer } from "../workflow-tracer.ts";

/** A journaled step that makes one "model call" per entry of `calls`, reporting each. */
function modelStep(
  calls: Array<{ inputTokens?: number; outputTokens?: number; costUsd?: number }>,
) {
  return function* (ctx: any) {
    for (const [i, usage] of calls.entries()) {
      yield* ctx.activity(`call-${i}`, async () => i);
      yield* ctx.reportUsage(usage);
    }
    return calls.length;
  };
}

describe("usage reporting", () => {
  it("sums reported usage per step and per run in the status", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const wf = workflow({ name: "usage-status" })
      .journaled("plan", modelStep([{ inputTokens: 10, outputTokens: 5, costUsd: 0.01 }]))
      .journaled(
        "write",
        modelStep([
          { inputTokens: 20, outputTokens: 10, costUsd: 0.02 },
          { inputTokens: 1, outputTokens: 1 },
        ]),
      )
      .step("plain", ({ prev }) => prev)
      .build();
    await runner.run({ workflow: wf, workflowId: "u-1", input: {} });

    const status = await runner.getStatus({ workflowId: "u-1" });
    expect(status?.usage).toEqual({
      inputTokens: 31,
      outputTokens: 16,
      totalTokens: 47,
      costUsd: 0.03,
    });
    expect(status?.steps.plan?.usage?.totalTokens).toBe(15);
    expect(status?.steps.write?.usage?.totalTokens).toBe(32);
    expect(status?.steps.plain?.usage).toBeUndefined();
  });

  it("does not double count when a step is retried over its journal", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    let modelCalls = 0;
    let attempts = 0;
    const wf = workflow({ name: "usage-retry" })
      .journaled(
        "j",
        function* (ctx: any) {
          yield* ctx.activity("model", async () => ++modelCalls);
          yield* ctx.reportUsage({ inputTokens: 40, outputTokens: 10 });
          if (++attempts < 3) throw new Error("flaky after spending");
          return "ok";
        },
        { retry: { maxRetries: 3, baseDelayMs: 1 } },
      )
      .build();
    await runner.run({ workflow: wf, workflowId: "u-2", input: {} });

    expect(modelCalls).toBe(1);
    expect(attempts).toBe(3);
    expect((await runner.getStatus({ workflowId: "u-2" }))?.usage?.totalTokens).toBe(50);
  });

  it("rejects a negative or non-finite report", async () => {
    const runner = createWorkflowRunner({ storage: new InMemoryWorkflowStorage() });
    const wf = workflow({ name: "usage-invalid" })
      .journaled("j", modelStep([{ inputTokens: -1 }]))
      .build();
    const r = await runner.runSafe({ workflow: wf, workflowId: "u-3", input: {} });
    expect(r.error).toBeInstanceOf(RangeError);
  });
});

describe("budget", () => {
  it("fails the run past maxTokens with a typed, unretried error and rolls back", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const events: string[] = [];
    let bodyRuns = 0;
    const wf = workflow({
      name: "budget-run",
      budget: { maxTokens: 100 },
      retry: { maxRetries: 3, baseDelayMs: 1 },
    })
      .step("reserve", async () => "r", {
        compensate: async () => void events.push("undo:reserve"),
      })
      .journaled(
        "llm",
        function* (ctx: any) {
          bodyRuns++;
          yield* ctx.reportUsage({ inputTokens: 60, outputTokens: 20 });
          yield* ctx.reportUsage({ inputTokens: 30 });
          events.push("unreachable");
          return 1;
        },
        { retry: { maxRetries: 3, baseDelayMs: 1 } },
      )
      .build();

    const r = await runner.runSafe({ workflow: wf, workflowId: "b-1", input: {} });
    const err = r.error as WorkflowBudgetExceededError;
    expect(err).toBeInstanceOf(WorkflowBudgetExceededError);
    expect(err).toMatchObject({ scope: "run", limit: "maxTokens", max: 100, used: 110 });
    expect(r.failure?.tag).toBe("WorkflowBudgetExceededError");
    expect(bodyRuns).toBe(1);
    expect(events).toEqual(["undo:reserve"]);

    const status = await runner.getStatus({ workflowId: "b-1" });
    expect(status?.state).toBe("failed");
    expect(status?.failure?.tag).toBe("WorkflowBudgetExceededError");
    expect(status?.usage?.totalTokens).toBe(110);
  });

  it("counts earlier steps toward the run cap", async () => {
    const runner = createWorkflowRunner({ storage: new InMemoryWorkflowStorage() });
    const wf = workflow({ name: "budget-across", budget: { maxCostUsd: 0.05 } })
      .journaled("a", modelStep([{ costUsd: 0.03 }]))
      .journaled("b", modelStep([{ costUsd: 0.03 }]))
      .journaled("c", modelStep([{ costUsd: 0.001 }]))
      .build();
    const r = await runner.runSafe({ workflow: wf, workflowId: "b-2", input: {} });
    expect(r.error).toMatchObject({ _tag: "WorkflowBudgetExceededError", limit: "maxCostUsd" });
    const status = await runner.getStatus({ workflowId: "b-2" });
    // b trips the run cap itself, mid-step, from a's stored usage.
    expect(status?.steps.b?.status).toBe("failed");
    expect(status?.steps.c).toBeUndefined();
  });

  it("applies perStep and per-step overrides", async () => {
    const runner = createWorkflowRunner({ storage: new InMemoryWorkflowStorage() });
    const build = (name: string) =>
      workflow({
        name,
        budget: { perStep: { maxTokens: 50 }, steps: { big: { maxTokens: 500 } } },
      })
        .journaled("big", modelStep([{ inputTokens: 300 }]))
        .journaled("small", modelStep([{ inputTokens: 51 }]))
        .build();
    const r = await runner.runSafe({
      workflow: build("budget-step"),
      workflowId: "b-3",
      input: {},
    });
    expect(r.error).toMatchObject({
      scope: "step",
      stepName: "small",
      limit: "maxTokens",
      max: 50,
      used: 51,
    });
    expect((await runner.getStatus({ workflowId: "b-3" }))?.steps.big?.status).toBe("completed");
  });

  it("allows usage exactly at the cap", async () => {
    const runner = createWorkflowRunner({ storage: new InMemoryWorkflowStorage() });
    const wf = workflow({ name: "budget-at-cap", budget: { maxTokens: 100 } })
      .journaled("j", modelStep([{ inputTokens: 60, outputTokens: 40 }]))
      .build();
    expect(await runner.run({ workflow: wf, workflowId: "b-4", input: {} })).toBe(1);
  });

  it("is not absorbed by onFailure: skip", async () => {
    const runner = createWorkflowRunner({ storage: new InMemoryWorkflowStorage() });
    const wf = workflow({ name: "budget-skip", budget: { maxTokens: 5 } })
      .journaled("j", modelStep([{ inputTokens: 10 }]), { onFailure: "skip" })
      .build();
    const r = await runner.runSafe({ workflow: wf, workflowId: "b-5", input: {} });
    expect(r.error).toBeInstanceOf(WorkflowBudgetExceededError);
  });

  it("stops a resumed run whose stored usage is already over the cap", async () => {
    const storage = new InMemoryWorkflowStorage();
    const clock = FakeWallClock.create("2026-01-01T00:00:00Z");
    const runner = createWorkflowRunner({ storage, clock });
    let ranPause = 0;
    const build = (budget?: { maxTokens: number }) =>
      workflow({ name: "resume-budget", ...(budget && { budget }) })
        .journaled("a", modelStep([{ inputTokens: 80 }]))
        .journaled("pause", function* (ctx: any) {
          yield* ctx.sleep(100);
          ranPause++;
          return 1;
        })
        .build();
    await expect(runner.run({ workflow: build(), workflowId: "b-6", input: {} })).rejects.toThrow(
      /sleeping until/,
    );
    clock.advance(100);
    await completeDueSleeps({ storage, now: clock.now(), limit: 10 });

    const r = await runner.runSafe({
      workflow: build({ maxTokens: 50 }),
      workflowId: "b-6",
      input: {},
    });
    expect(r.error).toBeInstanceOf(WorkflowBudgetExceededError);
    expect(ranPause).toBe(0);
  });
});

describe("tracer", () => {
  it("carries usage on the settling attempt and the run end", async () => {
    const ends: Array<Record<string, any>> = [];
    const runEnds: Array<Record<string, any>> = [];
    const tracer: WorkflowTracer = {
      stepAttemptEnd: (e) => void ends.push(e as any),
      runEnd: (e) => void runEnds.push(e as any),
    };
    const runner = createWorkflowRunner({ storage: new InMemoryWorkflowStorage(), tracer });
    const wf = workflow({ name: "budget-trace" })
      .journaled("a", modelStep([{ inputTokens: 7, outputTokens: 3, costUsd: 0.5 }]))
      .step("b", ({ prev }) => prev)
      .build();
    await runner.run({ workflow: wf, workflowId: "t-1", input: {} });

    expect(ends.find((e) => e.stepName === "a")?.usage).toEqual({
      inputTokens: 7,
      outputTokens: 3,
      totalTokens: 10,
      costUsd: 0.5,
    });
    expect(ends.find((e) => e.stepName === "b")?.usage).toBeUndefined();
    expect(runEnds[0]?.usage?.totalTokens).toBe(10);
  });
});
