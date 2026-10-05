// ---------------------------------------------------------------------------
// SleepScanner — resumes runs whose sleep has expired. The runner, storage
// and scanner share one FakeWallClock: advancing it moves the wake
// threshold and ticks the scan loop, so nothing waits on real time.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
// Aliased: `TaggedError` is also the name of the structural `{ _tag }`
// constraint imported from `shared/tagged-error.ts` below.
import { TaggedError as PerfectTaggedError, succeed, fail } from "@spilne/perfect-core";
import type { TaggedError } from "../../shared/tagged-error.ts";
import { workflow, InMemoryWorkflowStorage } from "../../../index.ts";
import { createWorkflowRunner } from "../../durable/workflow-runner.ts";
import { createSleepScanner } from "../sleep-scanner.ts";
import { FakeWallClock } from "../../shared/wall-clock.ts";

const SCAN_MS = 50;

/** Let in-flight async work settle: a bounded number of macrotask turns. */
async function flush(): Promise<void> {
  for (let i = 0; i < 20; i++) await new Promise<void>((r) => setImmediate(r));
}

/**
 * Let work settle, then move the clock by `stepMs`, until `done()` holds.
 * Fails once `maxMs` of clock time has passed without it.
 */
async function driveUntil(params: {
  clock: FakeWallClock;
  done: () => boolean | Promise<boolean>;
  stepMs?: number;
  maxMs?: number;
}): Promise<void> {
  const { clock, done, stepMs = SCAN_MS, maxMs = 10_000 } = params;
  for (let elapsed = 0; ; elapsed += stepMs) {
    await flush();
    if (await done()) return;
    if (elapsed >= maxMs) break;
    clock.advance(stepMs);
  }
  expect(await done()).toBe(true);
}

/** Tick the clock `ticks` times by `stepMs`, settling work after each. */
async function runTicks(params: {
  clock: FakeWallClock;
  ticks: number;
  stepMs?: number;
}): Promise<void> {
  for (let i = 0; i < params.ticks; i++) {
    await flush();
    params.clock.advance(params.stepMs ?? SCAN_MS);
  }
  await flush();
}

function setup() {
  const clock = FakeWallClock.create("2026-01-01T00:00:00Z");
  const storage = new InMemoryWorkflowStorage({ clock });
  const runner = createWorkflowRunner({ storage, clock });
  return { clock, storage, runner };
}

describe("Sleep scanner — background process that wakes up sleeping workflows", () => {
  it("a 60s sleep is resumed once the clock reaches its wake time, not before", async () => {
    const { clock, storage, runner } = setup();
    const log: string[] = [];

    const wfDef = workflow<{ msg: string }>({ name: "sleepy" })
      .step("before", ({ input }) => {
        log.push("before");
        return succeed(input.msg);
      })
      .sleep("nap", 60_000)
      .step("after", ({ prev }) => {
        log.push("after");
        return succeed(`woke: ${prev}`);
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

    const resumed: string[] = [];
    const scanner = createSleepScanner({
      storage,
      runner,
      scanIntervalMs: SCAN_MS,
      clock,
      resolveWorkflow: (name) => (name === "sleepy" ? wfDef : undefined),
      onResume: (id) => resumed.push(id),
    });
    void scanner.start();

    // Scans up to 50ms before the wake time find nothing due.
    await runTicks({ clock, ticks: 20, stepMs: (60_000 - SCAN_MS) / 20 });
    expect(resumed).toEqual([]);
    expect((await storage.loadWorkflow("sleep-1"))?.status).toBe("suspended");

    // The scan at the wake time resumes it.
    await driveUntil({
      clock,
      done: async () => (await storage.loadWorkflow("sleep-1"))?.status === "completed",
    });
    await scanner.stop();

    expect(resumed).toEqual(["sleep-1"]);
    expect(log).toEqual(["before", "after"]);
    expect((await storage.loadWorkflow("sleep-1"))?.result).toBe("woke: hello");
  });

  it("workflow sleeping for 31 years is not woken up prematurely", async () => {
    const { clock, storage, runner } = setup();

    const wfDef = workflow<string>({ name: "long-sleep" })
      .step("before", () => succeed("ok"))
      .sleep("nap", 999_999_999) // ~31 years
      .step("after", () => succeed("done"))
      .build();

    await runner.runSafe({ workflow: wfDef, workflowId: "sleep-2", input: "x" });

    const resumed: string[] = [];
    const scanner = createSleepScanner({
      storage,
      runner,
      scanIntervalMs: SCAN_MS,
      clock,
      resolveWorkflow: (name) => (name === "long-sleep" ? wfDef : undefined),
      onResume: (id) => resumed.push(id),
    });

    void scanner.start();
    // Twenty scans spread over ~31 years minus a day.
    await runTicks({ clock, ticks: 20, stepMs: Math.floor((999_999_999 - 86_400_000) / 20) });
    expect(resumed).toHaveLength(0);
    expect((await storage.loadWorkflow("sleep-2"))?.status).toBe("suspended");

    // Past the wake time it is resumed.
    await driveUntil({
      clock,
      done: () => resumed.length > 0,
      stepMs: 3_600_000,
      maxMs: 2 * 86_400_000,
    });
    await scanner.stop();
    expect(resumed).toEqual(["sleep-2"]);
  });

  it("unrecognized workflow name — scanner skips it and reports it once", async () => {
    const { clock, storage, runner } = setup();

    const wfDef = workflow<string>({ name: "unknown-wf" })
      .step("before", () => succeed("ok"))
      .sleep("nap", 1_000)
      .step("after", () => succeed("done"))
      .build();

    await runner.runSafe({ workflow: wfDef, workflowId: "sleep-3", input: "x" });
    clock.advance(1_000);

    const errors: string[] = [];
    let scans = 0;
    const listDueTimers = storage.listDueTimers.bind(storage);
    storage.listDueTimers = (p) => (scans++, listDueTimers(p));
    const scanner = createSleepScanner({
      storage,
      runner,
      scanIntervalMs: SCAN_MS,
      clock,
      resolveWorkflow: () => undefined, // can't resolve
      onError: (id) => errors.push(id),
    });

    void scanner.start();
    await runTicks({ clock, ticks: 10 });
    await scanner.stop();

    // Several scans, one report for the unknown name, no resume.
    expect(scans).toBeGreaterThanOrEqual(5);
    expect(errors).toEqual(["sleep-3"]);
    expect((await storage.loadWorkflow("sleep-3"))?.status).toBe("suspended");
  });

  it("three workflows sleeping — scanner wakes all of them in one scan cycle", async () => {
    const { clock, storage, runner } = setup();

    const wfDef = workflow<string>({ name: "multi" })
      .step("before", ({ input }) => succeed(input))
      .sleep("nap", 1_000)
      .step("after", ({ prev }) => succeed(`done: ${prev}`))
      .build();

    await runner.runSafe({ workflow: wfDef, workflowId: "sleep-a", input: "a" });
    await runner.runSafe({ workflow: wfDef, workflowId: "sleep-b", input: "b" });
    await runner.runSafe({ workflow: wfDef, workflowId: "sleep-c", input: "c" });
    clock.advance(1_000);

    const resumed: string[] = [];
    const scanner = createSleepScanner({
      storage,
      runner,
      scanIntervalMs: SCAN_MS,
      clock,
      resolveWorkflow: (name) => (name === "multi" ? wfDef : undefined),
      onResume: (id) => resumed.push(id),
    });

    // The first scan runs at start, without any clock movement.
    void scanner.start();
    await flush();
    await scanner.stop();

    expect(resumed.sort()).toEqual(["sleep-a", "sleep-b", "sleep-c"]);
    for (const id of ["sleep-a", "sleep-b", "sleep-c"]) {
      expect((await storage.loadWorkflow(id))?.status).toBe("completed");
    }
  });

  it("resume fails — error callback fires but scanner keeps running", async () => {
    class ResumeFailure extends PerfectTaggedError("ResumeFailure")<{
      readonly message: string;
    }>() {}

    const { clock, storage, runner } = setup();

    // A workflow whose post-sleep step always fails — triggers the scanner's
    // onError path during resumption.
    const broken = workflow<string>({ name: "broken" })
      .step("before", () => succeed("ok"))
      .sleep("nap", 1_000)
      .step("boom", () => fail(new ResumeFailure({ message: "resume failed" }) as TaggedError))
      .build();

    // Kick it off so storage has a row sleeping on "nap"; then pass its wake time.
    await runner.runSafe({ workflow: broken, workflowId: "sleep-err", input: "x" });
    clock.advance(1_000);

    const errors: { id: string; err: unknown }[] = [];
    let scans = 0;
    const listDueTimers = storage.listDueTimers.bind(storage);
    storage.listDueTimers = (p) => (scans++, listDueTimers(p));
    const scanner = createSleepScanner({
      storage,
      runner,
      scanIntervalMs: SCAN_MS,
      clock,
      resolveWorkflow: (name) => (name === "broken" ? broken : undefined),
      onError: (id, err) => errors.push({ id, err }),
    });

    void scanner.start();
    await driveUntil({ clock, done: () => errors.length > 0 });
    // The scanner keeps scanning after the failed resume.
    const scansAfterError = scans;
    await runTicks({ clock, ticks: 3 });
    await scanner.stop();

    expect(errors[0]!.id).toBe("sleep-err");
    expect((errors[0]!.err as { _tag?: string })._tag).toBe("ResumeFailure");
    expect(scans).toBeGreaterThan(scansAfterError);
    expect((await storage.loadWorkflow("sleep-err"))?.status).toBe("failed");
  });

  it("storage RPC failures don't kill the scan loop — keeps polling, calls onError, recovers", async () => {
    // Reproduces the symptom seen against a remote storage when the
    // server briefly drops out: `listWorkflows` throws ConnectionRefused.
    // The loop catches, hands off to onError, backs off on the clock and
    // keeps going, so it picks up where it left off once the server is back.
    const clock = FakeWallClock.create("2026-01-01T00:00:00Z");
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
      clock,
      resolveWorkflow: () => undefined,
      onError: (id, err) => errors.push({ id, err }),
    });

    void scanner.start();
    // The first ticks fail with ConnectionRefused and call onError.
    await driveUntil({ clock, done: () => listCalls >= 2, stepMs: 10 });
    expect(errors.length).toBeGreaterThanOrEqual(1);
    expect(errors[0]!.id).toBe("(scan-loop)");
    expect((errors[0]!.err as { code?: string }).code).toBe("ConnectionRefused");

    // Server "comes back" — the loop is still alive and polls cleanly.
    nextThrow = null;
    const callsBefore = listCalls;
    const errorsBefore = errors.length;
    await driveUntil({ clock, done: () => listCalls >= callsBefore + 2, stepMs: 10 });
    expect(errors.length).toBe(errorsBefore);

    await scanner.stop();
  });
});
