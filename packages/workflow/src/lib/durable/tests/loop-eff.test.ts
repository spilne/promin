// ---------------------------------------------------------------------------
// `.dowhile()` / `.dountil()` run as one Eff: Eff bodies keep their typed
// failures, Promise bodies (`.dowhileAsync()`) fail with defects, and an
// interrupted loop (step timeout) writes no iteration rows afterwards.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { TaggedError, fail, succeed, suspend } from "@spilne/perfect-core";
import { runEffSafe } from "../../shared/eff.ts";
import { SystemWallClock } from "../../shared/wall-clock.ts";
import { InMemoryWorkflowStorage } from "../in-memory-storage.ts";
import { withStepTimeout } from "../step-policy.ts";
import { workflow } from "../workflow-builder.ts";
import { createWorkflowRunner } from "../workflow-runner.ts";

class Boom extends TaggedError("Boom")<{ readonly message: string }>() {}

const setup = () => {
  const storage = new InMemoryWorkflowStorage();
  return { storage, runner: createWorkflowRunner({ storage }) };
};

const iterRows = async (params: {
  storage: InMemoryWorkflowStorage;
  workflowId: string;
  step: string;
}): Promise<string[]> => {
  const state = await params.storage.loadWorkflow(params.workflowId);
  return Object.keys(state?.steps ?? {}).filter((n) => n.startsWith(`${params.step}.iter.`));
};

const delay = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

describe("loop bodies are Effs", () => {
  it("the condition and the next step see the Eff's value", async () => {
    const { runner } = setup();
    const seen: unknown[] = [];
    const wf = workflow<number>({ name: "eff-value" })
      .dowhile(
        "count",
        ({ prev }, iter) => suspend(() => succeed(prev + iter)),
        (n) => {
          seen.push(n);
          return n < 2;
        },
      )
      .step("next", ({ prev }) => succeed(`got:${prev}`))
      .build();

    expect(await runner.run({ workflow: wf, workflowId: "ev-1", input: 0 })).toBe("got:2");
    expect(seen).toEqual([0, 1, 2]);
  });

  it("a typed failure of the body is retried by the step retry policy", async () => {
    const { runner, storage } = setup();
    let calls = 0;
    const wf = workflow<number>({ name: "eff-retry" })
      .dowhile(
        "poll",
        () =>
          suspend(() => {
            calls++;
            return calls <= 2 ? fail(new Boom({ message: `try ${calls}` })) : succeed(calls);
          }),
        () => false,
        { retry: { maxRetries: 2, baseDelayMs: 1 } },
      )
      .build();

    expect(await runner.run({ workflow: wf, workflowId: "er-1", input: 0 })).toBe(3);
    expect(calls).toBe(3);
    const state = await storage.loadWorkflow("er-1");
    expect(state?.steps["poll.iter.0"]?.status).toBe("completed");
  });

  it("a typed failure reaches onFailure and leaves a failed iteration row", async () => {
    const { runner, storage } = setup();
    const wf = workflow<number>({ name: "eff-fallback" })
      .dowhile(
        "poll",
        (_ctx, iter) => (iter === 1 ? fail(new Boom({ message: "second pass" })) : succeed(iter)),
        () => true,
        { onFailure: { fallback: () => -1 } },
      )
      .build();

    expect(await runner.run({ workflow: wf, workflowId: "ef-1", input: 0 })).toBe(-1);
    const state = await storage.loadWorkflow("ef-1");
    expect(state?.steps["poll.iter.0"]?.status).toBe("completed");
    expect(state?.steps["poll.iter.1"]?.status).toBe("failed");
    expect(state?.steps["poll.iter.1"]?.error).toBe("second pass");
    const attempts = await storage.loadStepAttempts("ef-1", "poll.iter.1");
    expect(attempts.map((a) => a.status)).toEqual(["failed"]);
  });

  it("a non-Eff result from a .dowhile() body is a defect naming .dowhileAsync()", async () => {
    const { runner, storage } = setup();
    const wf = workflow<number>({ name: "eff-plain" })
      .dowhile(
        "bad",
        // A JS caller (or a cast) handing a plain value to `.dowhile()`.
        (() => 1) as never,
        () => false,
      )
      .build();

    const r = await runner.runSafe({ workflow: wf, workflowId: "ep-1", input: 0 });
    expect(r.data).toBeNull();
    const state = await storage.loadWorkflow("ep-1");
    expect(state?.status).toBe("failed");
    expect(state?.steps["bad.iter.0"]?.status).toBe("failed");
    expect(state?.steps["bad.iter.0"]?.error).toContain(".dowhileAsync()");
  });
});

describe("dowhileAsync / dountilAsync", () => {
  it("runs value and Promise bodies", async () => {
    const { runner } = setup();
    const wf = workflow<number>({ name: "async-bodies" })
      .dowhileAsync(
        "a",
        async (_ctx, iter) => iter,
        (n) => n < 2,
      )
      .dountilAsync(
        "b",
        ({ prev }, iter) => prev + iter,
        (n) => n >= 4,
      )
      .build();

    // a: 0, 1, 2 → 2; b: 2, 3, 4 → 4
    expect(await runner.run({ workflow: wf, workflowId: "ab-1", input: 0 })).toBe(4);
  });

  it("a rejection is a defect: no retry, failed iteration row", async () => {
    const { runner, storage } = setup();
    let calls = 0;
    const wf = workflow<number>({ name: "async-reject" })
      .dowhileAsync(
        "poll",
        async () => {
          calls++;
          throw new Boom({ message: "rejected" });
        },
        () => false,
        { retry: { maxRetries: 3, baseDelayMs: 1 } },
      )
      .build();

    const r = await runner.runSafe({ workflow: wf, workflowId: "ar-1", input: 0 });
    expect(r.data).toBeNull();
    expect(calls).toBe(1);
    const state = await storage.loadWorkflow("ar-1");
    expect(state?.status).toBe("failed");
    expect(state?.steps["poll.iter.0"]?.status).toBe("failed");
    expect(state?.steps["poll.iter.0"]?.error).toBe("rejected");
  });
});

describe("loop timeout", () => {
  it("an interrupted loop step runs no further iterations (no fence to stop it)", async () => {
    // Drive the step by hand without a fence guard, so nothing but the
    // interruption itself can stop a loop that outlives its timeout.
    const storage = new InMemoryWorkflowStorage();
    await storage.createWorkflow({ workflowId: "raw-1", workflowName: "raw", input: 0 });
    let calls = 0;
    const step = workflow<number>({ name: "raw" })
      .dowhileAsync(
        "spin",
        async (_ctx, iter) => {
          calls++;
          await delay(15);
          return iter;
        },
        () => true,
        { maxIterations: 1_000 },
      )
      .build()._definition.steps[0]!;

    const exit = await runEffSafe(
      withStepTimeout({
        eff: step.execute({
          input: 0,
          results: {},
          workflowId: "raw-1",
          storage,
          attemptRef: { current: 1 },
          metadataRef: {},
        }),
        clock: SystemWallClock,
        ms: 80,
        workflowId: "raw-1",
        stepName: "spin",
      }),
    );
    expect((exit.error as { _tag?: string } | null)?._tag).toBe("StepTimeoutError");
    const rowsAtTimeout = await iterRows({ storage, workflowId: "raw-1", step: "spin" });
    const callsAtTimeout = calls;
    expect(rowsAtTimeout.length).toBeGreaterThan(0);

    await delay(150);
    expect(calls).toBe(callsAtTimeout);
    expect(await iterRows({ storage, workflowId: "raw-1", step: "spin" })).toEqual(rowsAtTimeout);
  });

  it("stops iterating at the step timeout: no iteration rows after it", async () => {
    const { runner, storage } = setup();
    let calls = 0;
    const wf = workflow<number>({ name: "loop-timeout" })
      .dowhileAsync(
        "spin",
        async (_ctx, iter) => {
          calls++;
          await delay(15);
          return iter;
        },
        () => true,
        { timeoutMs: 80, maxIterations: 1_000 },
      )
      .build();

    const r = await runner.runSafe({ workflow: wf, workflowId: "lt-1", input: 0 });
    expect((r.error as { _tag?: string } | null)?._tag).toBe("StepTimeoutError");
    const rowsAtTimeout = await iterRows({ storage, workflowId: "lt-1", step: "spin" });
    const callsAtTimeout = calls;
    expect(rowsAtTimeout.length).toBeGreaterThan(0);
    expect(rowsAtTimeout.length).toBeLessThan(10);

    // A zombie loop would keep running bodies and writing rows here.
    await delay(150);
    expect(calls).toBe(callsAtTimeout);
    expect(await iterRows({ storage, workflowId: "lt-1", step: "spin" })).toEqual(rowsAtTimeout);
  });

  it("a retry after a timeout resumes from the completed rows without a racing loop", async () => {
    const { runner, storage } = setup();
    const running = { now: 0, max: 0 };
    let attempt = 0;
    const wf = workflow<number>({ name: "loop-timeout-retry" })
      .dowhileAsync(
        "spin",
        async (_ctx, iter) => {
          running.now++;
          running.max = Math.max(running.max, running.now);
          try {
            // The first attempt hangs on iteration 2 past the timeout.
            if (iter === 2 && attempt++ === 0) await delay(200);
            else await delay(5);
          } finally {
            running.now--;
          }
          return iter;
        },
        (n) => n < 4,
        { timeoutMs: 60, retry: { maxRetries: 1, baseDelayMs: 1 } },
      )
      .build();

    expect(await runner.run({ workflow: wf, workflowId: "ltr-1", input: 0 })).toBe(4);
    // Let the abandoned body promise settle, then check nothing was written for it.
    await delay(250);
    expect(running.max).toBe(2); // the abandoned body overlaps the retry once
    expect((await iterRows({ storage, workflowId: "ltr-1", step: "spin" })).sort()).toEqual([
      "spin.iter.0",
      "spin.iter.1",
      "spin.iter.2",
      "spin.iter.3",
      "spin.iter.4",
    ]);
    const state = await storage.loadWorkflow("ltr-1");
    expect(state?.steps["spin.iter.2"]?.status).toBe("completed");
  });
});
