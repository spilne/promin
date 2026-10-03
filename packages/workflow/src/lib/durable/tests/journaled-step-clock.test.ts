// ---------------------------------------------------------------------------
// Journaled ctx time reads go through the injected clock — ctx.sleep wake
// times, ctx.signal timeouts and activity retry backoff all follow a
// FakeWallClock passed to runJournaledStep.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { InMemoryWorkflowStorage } from "../in-memory-storage.ts";
import { runJournaledStep } from "../journaled-step.ts";
import type { JournaledContext } from "../journaled-step.ts";
import { WorkflowSuspendedError } from "../durable-pipeline-error.ts";
import { FakeWallClock } from "../../shared/wall-clock.ts";

const T0 = "2026-01-01T00:00:00.000Z";

/** Yield to the event loop until `predicate` holds (bounded). */
async function waitFor(predicate: () => boolean): Promise<void> {
  for (let i = 0; i < 1_000 && !predicate(); i++) {
    await new Promise<void>((r) => setImmediate(r));
  }
  expect(predicate()).toBe(true);
}

describe("ctx.sleep — wake time follows the injected clock", () => {
  it("stamps wakeAt from the clock and self-completes once the clock passes it", async () => {
    const clock = FakeWallClock.create(T0);
    const storage = new InMemoryWorkflowStorage({ clock });
    const body = function* (ctx: JournaledContext<unknown, unknown>) {
      const woke = yield* ctx.sleep(60_000);
      return woke;
    };
    const run = () =>
      runJournaledStep({
        input: {},
        prev: {},
        workflowId: "jc-sleep",
        stepName: "wait",
        storage,
        clock,
        body,
      });

    await expect(run()).rejects.toBeInstanceOf(WorkflowSuspendedError);
    const journal = await storage.loadJournal("jc-sleep", "wait");
    expect(journal[0]!.wakeAt?.toISOString()).toBe("2026-01-01T00:01:00.000Z");

    clock.advance(59_999);
    await expect(run()).rejects.toBeInstanceOf(WorkflowSuspendedError);

    clock.advance(1);
    const woke = await run();
    expect(new Date(woke as unknown as string).toISOString()).toBe("2026-01-01T00:01:00.000Z");
  });
});

describe("ctx.signal({ timeout }) — deadline follows the injected clock", () => {
  it("stamps wakeAt from the clock and returns the timeout envelope once it passes", async () => {
    const clock = FakeWallClock.create(T0);
    const storage = new InMemoryWorkflowStorage({ clock });
    const body = function* (ctx: JournaledContext<unknown, unknown>) {
      return yield* ctx.signal<boolean>("approve", { timeout: 5_000 });
    };
    const run = () =>
      runJournaledStep({
        input: {},
        prev: {},
        workflowId: "jc-signal",
        stepName: "wait",
        storage,
        clock,
        body,
      });

    await expect(run()).rejects.toBeInstanceOf(WorkflowSuspendedError);
    const journal = await storage.loadJournal("jc-signal", "wait");
    expect(journal[0]!.wakeAt?.toISOString()).toBe("2026-01-01T00:00:05.000Z");

    clock.advance(4_999);
    await expect(run()).rejects.toBeInstanceOf(WorkflowSuspendedError);

    clock.advance(1);
    expect(await run()).toEqual({ ok: false, error: "timeout" });
  });
});

describe("ctx.activity retry — backoff waits on the injected clock", () => {
  it("schedules each retry delay as a clock timer and resumes only when advanced", async () => {
    const clock = FakeWallClock.create(T0);
    const storage = new InMemoryWorkflowStorage({ clock });
    let calls = 0;
    const body = function* (ctx: JournaledContext<unknown, unknown>) {
      return yield* ctx.activity(
        "flaky",
        async () => {
          calls++;
          if (calls < 3) throw new Error(`boom ${calls}`);
          return "ok";
        },
        { retry: { maxRetries: 3, baseDelayMs: 1_000 } },
      );
    };

    const done = runJournaledStep({
      input: {},
      prev: {},
      workflowId: "jc-retry",
      stepName: "work",
      storage,
      clock,
      body,
    });

    // First failure → 1s backoff timer on the fake clock.
    await waitFor(() => clock.pendingCount() === 1);
    expect(calls).toBe(1);
    clock.advance(999);
    expect(calls).toBe(1);
    clock.advance(1);

    // Second failure → 2s backoff (exponential).
    await waitFor(() => calls === 2 && clock.pendingCount() === 1);
    clock.advance(1_999);
    expect(calls).toBe(2);
    clock.advance(1);

    expect(await done).toBe("ok");
    expect(calls).toBe(3);
    expect(clock.pendingCount()).toBe(0);
  });
});
