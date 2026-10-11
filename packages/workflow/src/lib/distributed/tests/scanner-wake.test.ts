// ---------------------------------------------------------------------------
// Scanners in wake mode — a due run is handed to `wake` (with its stored
// version) instead of being resumed in this process, and resolveWorkflow
// in runner mode sees the run's version. One FakeWallClock drives storage,
// runner and scanners.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { succeed } from "@spilne/perfect-core";
import { workflow, InMemoryWorkflowStorage } from "../../../index.ts";
import { createWorkflowRunner } from "../../durable/workflow-runner.ts";
import type { WorkflowWakeup } from "../../durable/workflow-storage.ts";
import { createSleepScanner } from "../sleep-scanner.ts";
import { createSignalScanner } from "../signal-scanner.ts";
import { FakeWallClock } from "../../shared/wall-clock.ts";

const SCAN_MS = 50;

async function flush(): Promise<void> {
  for (let i = 0; i < 20; i++) await new Promise<void>((r) => setImmediate(r));
}

/** Settle, then advance the clock by `stepMs`, until `done()` holds (bounded by `maxSteps`). */
async function driveUntil(params: {
  clock: FakeWallClock;
  done: () => boolean | Promise<boolean>;
  stepMs?: number;
  maxSteps?: number;
}): Promise<void> {
  const { clock, done, stepMs = SCAN_MS, maxSteps = 400 } = params;
  for (let i = 0; i < maxSteps; i++) {
    await flush();
    if (await done()) return;
    clock.advance(stepMs);
  }
  expect(await done()).toBe(true);
}

function setup() {
  const clock = FakeWallClock.create("2026-01-01T00:00:00Z");
  const storage = new InMemoryWorkflowStorage({ clock });
  const runner = createWorkflowRunner({ storage, clock });
  return { clock, storage, runner };
}

const napper = workflow<string>({ name: "napper", version: "2" })
  .step("before", () => succeed("ok"))
  .sleep("nap", 1_000)
  .step("after", () => succeed("done"))
  .build();

const waiter = workflow<string>({ name: "waiter", version: "7" })
  .waitForSignal<{ ok: boolean }, "decision">("decision", { signalName: "go" })
  .step("after", ({ prev }) => succeed(prev.ok))
  .build();

describe("scanners in wake mode", () => {
  it("hand a due sleep to `wake` with the run's version, and keep handing it until it is resumed", async () => {
    const { clock, storage, runner } = setup();
    await runner.runSafe({ workflow: napper, workflowId: "n-1", input: "x" });
    expect((await storage.loadWorkflowStatus("n-1"))?.status).toBe("suspended");

    const woken: WorkflowWakeup[] = [];
    const scanner = createSleepScanner({
      storage,
      clock,
      scanIntervalMs: SCAN_MS,
      wake: async (run) => {
        woken.push(run);
      },
    });
    void scanner.start();

    await driveUntil({ clock, done: () => woken.length >= 2 });
    // Nothing ran here: the run is still suspended, and every scan re-finds it.
    expect((await storage.loadWorkflowStatus("n-1"))?.status).toBe("suspended");
    expect(woken[0]).toMatchObject({
      workflowId: "n-1",
      workflowName: "napper",
      version: "2",
      reason: "sleep",
      stepName: "nap",
    });

    // Whoever drives it resumes it; then scans stop finding it.
    await runner.run({ workflow: napper, workflowId: "n-1", input: "x" });
    const seen = woken.length;
    for (let i = 0; i < 5; i++) {
      clock.advance(SCAN_MS);
      await flush();
    }
    await scanner.stop();
    expect(woken.length).toBe(seen);
    expect((await storage.loadWorkflowStatus("n-1"))?.status).toBe("completed");
  });

  it("hand a run whose signal arrived to `wake`; the resumed run reads the delivered value", async () => {
    const { clock, storage, runner } = setup();
    await runner.runSafe({ workflow: waiter, workflowId: "s-1", input: "x" });
    expect((await storage.loadWorkflowStatus("s-1"))?.status).toBe("suspended");

    const woken: WorkflowWakeup[] = [];
    const scanner = createSignalScanner({
      storage,
      clock,
      scanIntervalMs: SCAN_MS,
      wake: async (run) => {
        woken.push(run);
      },
    });
    void scanner.start();

    for (let i = 0; i < 3; i++) {
      clock.advance(SCAN_MS);
      await flush();
    }
    expect(woken).toEqual([]);

    await storage.deliverSignal({ workflowId: "s-1", signalName: "go", payload: { ok: true } });
    scanner.wake();
    await driveUntil({ clock, done: () => woken.length >= 1 });
    await scanner.stop();
    expect(woken[0]).toMatchObject({
      workflowId: "s-1",
      version: "7",
      reason: "signal",
      signalName: "go",
    });

    expect(await runner.run({ workflow: waiter, workflowId: "s-1", input: "x" })).toBe(true);
  });

  it("a failing wake is reported and retried on the next scan", async () => {
    const { clock, storage, runner } = setup();
    await runner.runSafe({ workflow: napper, workflowId: "n-2", input: "x" });
    let calls = 0;
    const errors: unknown[] = [];
    const scanner = createSleepScanner({
      storage,
      clock,
      scanIntervalMs: SCAN_MS,
      wake: async () => {
        calls += 1;
        if (calls === 1) throw new Error("queue down");
      },
      onError: (_id, err) => errors.push(err),
    });
    void scanner.start();
    await driveUntil({ clock, done: () => calls >= 2 });
    await scanner.stop();
    expect(String(errors[0])).toContain("queue down");
  });

  it("rejects a config with both or neither of runner and wake", () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    expect(() =>
      createSleepScanner({
        storage,
        runner,
        resolveWorkflow: () => undefined,
        wake: async () => {},
      } as never),
    ).toThrow(/not both/);
    expect(() => createSignalScanner({ storage } as never)).toThrow(/pass `wake`/);
  });
});

describe("scanners in runner mode resolve by name and stored version", () => {
  it("resolveWorkflow receives the run's version, so a v1 run is resumed with v1 code", async () => {
    const { clock, storage, runner } = setup();
    const v1 = workflow<string>({ name: "greet", version: "1" })
      .sleep("nap", 1_000)
      .step("say", () => succeed("v1"))
      .build();
    const v2 = workflow<string>({ name: "greet", version: "2" })
      .sleep("nap", 1_000)
      .step("say", () => succeed("v2"))
      .build();
    await runner.runSafe({ workflow: v1, workflowId: "g-1", input: "x" });

    const asked: Array<{ workflowName: string; version?: string }> = [];
    const scanner = createSleepScanner({
      storage,
      runner,
      clock,
      scanIntervalMs: SCAN_MS,
      resolveWorkflow: (run) => {
        asked.push(run);
        return run.version === "1" ? v1 : v2;
      },
    });
    void scanner.start();
    await driveUntil({
      clock,
      done: async () => (await storage.loadWorkflowStatus("g-1"))?.status === "completed",
    });
    await scanner.stop();
    expect(asked[0]).toEqual({ workflowName: "greet", version: "1" });
    expect((await storage.loadWorkflow("g-1"))?.result).toBe("v1");
  });
});
