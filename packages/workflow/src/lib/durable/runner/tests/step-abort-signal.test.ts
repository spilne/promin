import { describe, expect, it } from "bun:test";
import { succeed } from "@spilne/perfect-core";
import { InMemoryWorkflowStorage } from "../../in-memory-storage.ts";
import { StepTimeoutError } from "../../durable-pipeline-error.ts";
import { workflow } from "../../workflow-builder.ts";
import { createWorkflowRunner } from "../../workflow-runner.ts";

/** Resolves with the signal's reason once it aborts; never settles otherwise. */
function abortReason(signal: AbortSignal): Promise<unknown> {
  return new Promise((resolve) => {
    if (signal.aborted) resolve(signal.reason);
    else signal.addEventListener("abort", () => resolve(signal.reason), { once: true });
  });
}

describe("step abort signal", () => {
  it("aborts a step body's signal when its attempt times out", async () => {
    const runner = createWorkflowRunner({ storage: new InMemoryWorkflowStorage() });
    let reason: unknown;
    const wf = workflow<number>({ name: "abort-timeout" })
      .step(
        "slow",
        async ({ signal }) => {
          reason = await abortReason(signal);
          return 1;
        },
        { timeoutMs: 20 },
      )
      .build();

    await expect(runner.run({ workflow: wf, workflowId: "t-1", input: 0 })).rejects.toThrow(
      /timed out/,
    );
    expect(reason).toBeInstanceOf(StepTimeoutError);
  });

  it("gives each retry a fresh signal and aborts the failed attempt's", async () => {
    const runner = createWorkflowRunner({ storage: new InMemoryWorkflowStorage() });
    const signals: AbortSignal[] = [];
    const wf = workflow<number>({ name: "abort-retry" })
      .step(
        "flaky",
        async ({ signal, attempt }) => {
          signals.push(signal);
          if (attempt === 1) await abortReason(signal);
          return attempt;
        },
        { timeoutMs: 20, retry: { maxRetries: 1, baseDelayMs: 1 } },
      )
      .build();

    expect(await runner.run({ workflow: wf, workflowId: "r-1", input: 0 })).toBe(2);
    expect(signals).toHaveLength(2);
    expect(signals[0]!.aborted).toBe(true);
    expect(signals[1]!.aborted).toBe(false);
  });

  it("aborts a running step's signal when the caller's run signal aborts", async () => {
    const runner = createWorkflowRunner({ storage: new InMemoryWorkflowStorage() });
    const caller = new AbortController();
    let started!: () => void;
    const running = new Promise<void>((resolve) => (started = resolve));
    let stepSignal: AbortSignal | undefined;
    const wf = workflow<number>({ name: "abort-run" })
      .step("long", async ({ signal }) => {
        stepSignal = signal;
        started();
        await abortReason(signal);
        return 1;
      })
      .build();

    const run = runner
      .run({ workflow: wf, workflowId: "c-1", input: 0, signal: caller.signal })
      .catch(() => undefined);
    await running;
    expect(stepSignal!.aborted).toBe(false);
    caller.abort(new Error("stop"));
    await run;
    expect(stepSignal!.aborted).toBe(true);
  });

  it("aborts a map element's signal when the element times out", async () => {
    const runner = createWorkflowRunner({ storage: new InMemoryWorkflowStorage() });
    const reasons: unknown[] = [];
    const wf = workflow<number[]>({ name: "abort-map" })
      .step("items", ({ input }) => succeed(input))
      .mapOver(
        "each",
        { array: "items" },
        async (_item, { signal }) => {
          reasons.push(await abortReason(signal));
          return 1;
        },
        { element: { timeoutMs: 20 } },
      )
      .build();

    await expect(runner.run({ workflow: wf, workflowId: "m-1", input: [1] })).rejects.toThrow();
    expect(reasons[0]).toBeInstanceOf(StepTimeoutError);
  });
});
