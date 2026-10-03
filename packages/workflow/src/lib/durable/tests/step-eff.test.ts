// ---------------------------------------------------------------------------
// Step bodies as perfect Eff — the engine-level contract: what run()
// rejects with, how non-Eff returns are handled, per-attempt timeouts,
// retry on the runner clock, and bounded mapOver concurrency.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { TaggedError, fail, succeed, suspend, tryPromise } from "@spilne/perfect-core";
import { FakeWallClock } from "../../shared/wall-clock.ts";
import { workflow } from "../durable-pipeline.ts";
import { InMemoryWorkflowStorage } from "../in-memory-storage.ts";
import { createWorkflowRunner } from "../workflow-runner.ts";
import { StepTimeoutError } from "../durable-pipeline-error.ts";

class PaymentError extends TaggedError("PaymentError")<{ readonly message: string }>() {}

const settle = () => new Promise((r) => setTimeout(r, 0));

describe("step Eff — rejection shape", () => {
  it("run() rejects with the step's typed error itself, not a wrapper", async () => {
    const runner = createWorkflowRunner({ storage: new InMemoryWorkflowStorage() });
    const err = new PaymentError({ message: "card declined" });
    const wf = workflow<number>({ name: "typed-reject" })
      .step("charge", () => fail(err))
      .build();

    const rejection = await runner.run({ workflow: wf, workflowId: "r-1", input: 0 }).then(
      () => undefined,
      (e: unknown) => e,
    );
    expect(rejection).toBe(err);
    expect((rejection as PaymentError)._tag).toBe("PaymentError");
    expect((rejection as PaymentError).message).toContain("card declined");
  });

  it("run() rejects with a defect's thrown Error itself", async () => {
    const runner = createWorkflowRunner({ storage: new InMemoryWorkflowStorage() });
    const boom = new Error("boom");
    const wf = workflow<number>({ name: "defect-reject" })
      .step("explode", () =>
        suspend<never, never>(() => {
          throw boom;
        }),
      )
      .build();

    await expect(runner.run({ workflow: wf, workflowId: "r-2", input: 0 })).rejects.toBe(boom);
  });
});

describe("step Eff — non-Eff returns", () => {
  it("a Promise returned from .step() is awaited like .stepAsync()", async () => {
    const runner = createWorkflowRunner({ storage: new InMemoryWorkflowStorage() });
    const wf = workflow<number>({ name: "promise-step" })
      .step("double", (async ({ input }: { input: number }) => input * 2) as never)
      .build();
    expect(await runner.run({ workflow: wf, workflowId: "t-1", input: 21 })).toBe(42);
  });

  it("an async function returning an Eff runs it (Eff is thenable); its failure is a defect", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    let attempts = 0;
    const err = new PaymentError({ message: "declined" });
    const wf = workflow<number>({ name: "async-eff-step" })
      .step(
        "charge",
        (async () => {
          attempts++;
          return fail(err);
        }) as never,
        // Typed retry never sees it: the Promise already settled the Eff.
        { retry: { maxRetries: 2, baseDelayMs: 1 } },
      )
      .build();

    await expect(runner.run({ workflow: wf, workflowId: "t-2", input: 0 })).rejects.toBe(err);
    expect(attempts).toBe(1);
  });

  it("a plain value returned from .step() fails the step with a clear message", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const wf = workflow<number>({ name: "plain-value-step" })
      .step("oops", (() => 42) as never)
      .build();

    await expect(runner.run({ workflow: wf, workflowId: "t-3", input: 0 })).rejects.toThrow(
      /must return an Eff/,
    );
    const state = await storage.loadWorkflow("t-3");
    expect(state?.steps["oops"]?.status).toBe("failed");
  });
});

describe("step Eff — timeout and retry", () => {
  it("each retry attempt gets its own timeout", async () => {
    const runner = createWorkflowRunner({ storage: new InMemoryWorkflowStorage() });
    let attempts = 0;
    const wf = workflow<number>({ name: "timeout-retry" })
      .step(
        "slow-then-fast",
        () =>
          tryPromise(
            () =>
              new Promise<string>((resolve) =>
                ++attempts === 1 ? setTimeout(() => resolve("late"), 200) : resolve("fast"),
              ),
            (e) => e,
          ).orDie(),
        {
          timeoutMs: 20,
          retry: { maxRetries: 1, baseDelayMs: 1, when: (e) => e._tag === "StepTimeoutError" },
        },
      )
      .build();

    expect(await runner.run({ workflow: wf, workflowId: "to-1", input: 0 })).toBe("fast");
    expect(attempts).toBe(2);
  });

  it("a step that never settles fails with StepTimeoutError", async () => {
    const runner = createWorkflowRunner({ storage: new InMemoryWorkflowStorage() });
    const wf = workflow<number>({ name: "never" })
      .step(
        "hang",
        () =>
          tryPromise(
            () => new Promise<never>(() => {}),
            (e) => e,
          ).orDie(),
        { timeoutMs: 20 },
      )
      .build();
    const { error } = await runner.runSafe({ workflow: wf, workflowId: "to-2", input: 0 });
    expect(error).toBeInstanceOf(StepTimeoutError);
  });

  it("step retry backoff runs on the runner's WallClock", async () => {
    const clock = FakeWallClock.create(0);
    const runner = createWorkflowRunner({ storage: new InMemoryWorkflowStorage(), clock });
    let attempts = 0;
    const wf = workflow<number>({ name: "clocked-retry" })
      .step(
        "flaky",
        () =>
          suspend(() =>
            ++attempts < 3 ? fail(new PaymentError({ message: "try again" })) : succeed("paid"),
          ),
        { retry: { maxRetries: 3, baseDelayMs: 60_000 } },
      )
      .build();

    const done = runner.run({ workflow: wf, workflowId: "clk-1", input: 0 });
    for (let i = 0; i < 20 && attempts < 1; i++) await settle();
    expect(attempts).toBe(1);

    clock.advance(60_000);
    for (let i = 0; i < 20 && attempts < 2; i++) await settle();
    expect(attempts).toBe(2);

    clock.advance(120_000);
    expect(await done).toBe("paid");
    expect(attempts).toBe(3);
  });
});

describe("step Eff — mapOver concurrency", () => {
  it("never runs more than `concurrency` elements at once", async () => {
    const runner = createWorkflowRunner({ storage: new InMemoryWorkflowStorage() });
    let inFlight = 0;
    let maxInFlight = 0;
    const wf = workflow<number[]>({ name: "bounded-map" })
      .step("items", ({ input }) => succeed(input))
      .mapOver("work", { array: "items", concurrency: 2 }, (n) =>
        tryPromise(
          async () => {
            inFlight++;
            maxInFlight = Math.max(maxInFlight, inFlight);
            await new Promise((r) => setTimeout(r, 5));
            inFlight--;
            return n * 10;
          },
          (e) => e,
        ).orDie(),
      )
      .build();

    const result = await runner.run({
      workflow: wf,
      workflowId: "map-1",
      input: [1, 2, 3, 4, 5, 6],
    });
    expect(result).toEqual([10, 20, 30, 40, 50, 60]);
    expect(maxInFlight).toBe(2);
  });
});
