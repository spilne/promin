// ---------------------------------------------------------------------------
// ctx.sleep + ctx.signal inside journaled steps — suspend/resume tests.
// Exercises the journal-level suspend mechanism end-to-end against the
// in-memory storage. Postgres integration is covered in the postgres package.
// ---------------------------------------------------------------------------

import { describe, it, expect, beforeEach } from "bun:test";
import { workflow } from "../workflow-builder.ts";
import { InMemoryWorkflowStorage } from "../in-memory-storage.ts";
import { createWorkflowRunner } from "../workflow-runner.ts";
import { runJournaledStep, completeSignal, completeDueSleeps } from "../journaled-step.ts";
import type { JournaledContext } from "../journaled-step.ts";
import { WorkflowSuspendedError } from "../durable-pipeline-error.ts";
import { FakeWallClock } from "../../shared/wall-clock.ts";

const T0 = "2026-01-01T00:00:00Z";

describe("ctx.sleep — durable mid-step sleep", () => {
  let storage: InMemoryWorkflowStorage;
  beforeEach(() => {
    storage = new InMemoryWorkflowStorage();
  });

  it("first run suspends with WorkflowSuspendedError and writes a pending sleep entry", async () => {
    const clock = FakeWallClock.create(T0);
    const body = function* (ctx: JournaledContext<unknown, unknown>) {
      yield* ctx.sleep(60_000); // 60s — doesn't actually block; we suspend first
      return "done";
    };

    await expect(
      runJournaledStep({
        input: {},
        prev: {},
        workflowId: "wf-sleep-1",
        stepName: "wait",
        storage,
        body,
        clock,
      }),
    ).rejects.toBeInstanceOf(WorkflowSuspendedError);

    const journal = await storage.loadJournal({ workflowId: "wf-sleep-1", stepName: "wait" });
    expect(journal).toHaveLength(1);
    expect(journal[0]!.stepType).toBe("sleep");
    expect(journal[0]!.phase).toBe("pending");
    expect(journal[0]!.wakeAt).toBeInstanceOf(Date);
    // wakeAt is exactly the clock's now + 60s.
    expect(journal[0]!.wakeAt!.getTime()).toBe(clock.currentTimeMs() + 60_000);
  });

  it("replay after completion returns wake time and continues the step", async () => {
    const clock = FakeWallClock.create(T0);
    let postSleepCalls = 0;
    const body = function* (ctx: JournaledContext<unknown, unknown>) {
      const wokeAt = yield* ctx.sleep(10_000);
      // Small cast: we care that the generator resumes, not the exact type here.
      yield* ctx.activity("post-sleep", async () => {
        postSleepCalls++;
        return "ran after sleep";
      });
      return { wokeAt, msg: "done" };
    };

    // First run — suspends at sleep.
    await expect(
      runJournaledStep({
        input: {},
        prev: {},
        workflowId: "wf-sleep-2",
        stepName: "s",
        storage,
        body,
        clock,
      }),
    ).rejects.toBeInstanceOf(WorkflowSuspendedError);
    expect(postSleepCalls).toBe(0); // post-sleep activity didn't run yet

    // The scanner fires 10s later and completes every due sleep (in
    // production the sleep scanner does this on its own clock).
    clock.advance(10_000);
    const completed = await completeDueSleeps({ storage, now: clock.now(), limit: 10 });
    expect(completed).toHaveLength(1);
    expect(completed[0]!.workflowId).toBe("wf-sleep-2");
    expect(completed[0]!.wakeAt.getTime()).toBe(clock.currentTimeMs());

    // Replay — generator fast-forwards through the completed sleep, runs
    // post-sleep activity, returns.
    const result = await runJournaledStep({
      input: {},
      prev: {},
      workflowId: "wf-sleep-2",
      stepName: "s",
      storage,
      body,
      clock,
    });
    expect(postSleepCalls).toBe(1);
    expect((result as { msg: string }).msg).toBe("done");
    expect((result as { wokeAt: Date }).wokeAt).toBeInstanceOf(Date);
  });

  it("sleep before wake time stays suspended; completeDueSleeps is a no-op", async () => {
    const clock = FakeWallClock.create(T0);
    const body = function* (ctx: JournaledContext<unknown, unknown>) {
      yield* ctx.sleep(60_000);
      return "done";
    };

    await expect(
      runJournaledStep({
        input: {},
        prev: {},
        workflowId: "wf-sleep-3",
        stepName: "s",
        storage,
        body,
        clock,
      }),
    ).rejects.toBeInstanceOf(WorkflowSuspendedError);

    // 1ms short of the wake time — the sleep isn't due yet.
    clock.advance(59_999);
    const completed = await completeDueSleeps({
      storage,
      now: clock.now(),
      limit: 10,
    });
    expect(completed).toHaveLength(0);

    // Replay — still suspends because sleep entry is still pending.
    await expect(
      runJournaledStep({
        input: {},
        prev: {},
        workflowId: "wf-sleep-3",
        stepName: "s",
        storage,
        body,
        clock,
      }),
    ).rejects.toBeInstanceOf(WorkflowSuspendedError);
  });

  it("accepts an absolute Date for sleep target", async () => {
    const wakeAt = new Date(Date.now() + 30_000);
    const body = function* (ctx: JournaledContext<unknown, unknown>) {
      yield* ctx.sleep(wakeAt);
      return "done";
    };

    await expect(
      runJournaledStep({
        input: {},
        prev: {},
        workflowId: "wf-sleep-date",
        stepName: "s",
        storage,
        body,
      }),
    ).rejects.toBeInstanceOf(WorkflowSuspendedError);

    const journal = await storage.loadJournal({ workflowId: "wf-sleep-date", stepName: "s" });
    expect(journal[0]!.wakeAt!.getTime()).toBe(wakeAt.getTime());
  });
});

describe("ctx.signal — durable mid-step signal wait", () => {
  let storage: InMemoryWorkflowStorage;
  beforeEach(() => {
    storage = new InMemoryWorkflowStorage();
  });

  it("first run suspends, pending signal entry written with the signal name", async () => {
    const body = function* (ctx: JournaledContext<unknown, unknown>) {
      const value = yield* ctx.signal<{ approved: boolean }>("approval");
      return value;
    };

    await expect(
      runJournaledStep({
        input: {},
        prev: {},
        workflowId: "wf-sig-1",
        stepName: "gate",
        storage,
        body,
      }),
    ).rejects.toBeInstanceOf(WorkflowSuspendedError);

    const journal = await storage.loadJournal({ workflowId: "wf-sig-1", stepName: "gate" });
    expect(journal).toHaveLength(1);
    expect(journal[0]!.stepType).toBe("signal");
    expect(journal[0]!.phase).toBe("pending");
    expect(journal[0]!.activityName).toBe("approval");
  });

  it("completeSignal delivers a value; replay resumes with that value", async () => {
    const body = function* (ctx: JournaledContext<unknown, unknown>) {
      const value = yield* ctx.signal<{ approved: boolean }>("approval");
      yield* ctx.activity("record", async () => ({ recorded: true }));
      return value;
    };

    // Suspend at signal.
    await expect(
      runJournaledStep({
        input: {},
        prev: {},
        workflowId: "wf-sig-2",
        stepName: "gate",
        storage,
        body,
      }),
    ).rejects.toBeInstanceOf(WorkflowSuspendedError);

    // External delivery.
    const delivered = await completeSignal({
      storage,
      workflowId: "wf-sig-2",
      stepName: "gate",
      signalName: "approval",
      value: { approved: true },
    });
    expect(delivered).toBe(true);

    // Replay returns the delivered value and continues the step.
    const result = await runJournaledStep<unknown, unknown, { approved: boolean }>({
      input: {},
      prev: {},
      workflowId: "wf-sig-2",
      stepName: "gate",
      storage,
      body: body as never,
    });
    expect(result).toEqual({ approved: true });
  });

  it("completeSignal returns false for unknown signal (idempotency-friendly)", async () => {
    const delivered = await completeSignal({
      storage,
      workflowId: "nonexistent",
      stepName: "gate",
      signalName: "approval",
      value: { any: "payload" },
    });
    expect(delivered).toBe(false);
  });

  it("double-delivery is a no-op (replay preserves the first value)", async () => {
    const body = function* (ctx: JournaledContext<unknown, unknown>) {
      const v = yield* ctx.signal<number>("count");
      return v;
    };

    await expect(
      runJournaledStep({
        input: {},
        prev: {},
        workflowId: "wf-sig-dup",
        stepName: "gate",
        storage,
        body,
      }),
    ).rejects.toBeInstanceOf(WorkflowSuspendedError);

    await completeSignal({
      storage,
      workflowId: "wf-sig-dup",
      stepName: "gate",
      signalName: "count",
      value: 1,
    });
    // Second delivery — already completed, nothing happens.
    const again = await completeSignal({
      storage,
      workflowId: "wf-sig-dup",
      stepName: "gate",
      signalName: "count",
      value: 999,
    });
    expect(again).toBe(false); // findPendingSignal won't find it (completed)

    const result = await runJournaledStep({
      input: {},
      prev: {},
      workflowId: "wf-sig-dup",
      stepName: "gate",
      storage,
      body,
    });
    expect(result).toBe(1); // first value wins
  });
});

describe("DefaultSleepScanner integration with journaled sleeps", () => {
  it("scanner finds suspended journaled-sleep workflow and re-runs; ctx.sleep auto-completes", async () => {
    const storage = new InMemoryWorkflowStorage();
    let postSleepCalls = 0;

    const buildWorkflow = () =>
      workflow<{ id: string }>({ name: "scanner-test" })
        .journaled("wait-then-do", function* (ctx) {
          // Very short sleep so the scanner integration is fast in tests.
          yield* ctx.sleep(50);
          yield* ctx.activity("post-sleep", async () => {
            postSleepCalls++;
            return "done";
          });
          return { ok: true };
        })
        .build();

    const wf = buildWorkflow();
    const clock = FakeWallClock.create(T0);
    const runner = createWorkflowRunner({ storage, clock });

    // Kick off — suspends at sleep. ctx.sleep also calls suspendWorkflow,
    // so the workflow's step.wakeAt is set for the scanner.
    await expect(
      runner.run({ workflow: wf, workflowId: "scan-1", input: { id: "a" } }),
    ).rejects.toThrow(/sleeping until/);
    expect(postSleepCalls).toBe(0);

    // Verify the workflow is marked suspended with the expected wakeAt.
    const suspendedState = storage.getWorkflow("scan-1");
    expect(suspendedState?.status).toBe("suspended");
    const sleepStep = suspendedState?.steps["wait-then-do"];
    expect(sleepStep?.status).toBe("sleeping");
    expect(sleepStep?.wakeAt?.getTime()).toBe(clock.currentTimeMs() + 50);

    // A re-run before the wake time stays suspended.
    clock.advance(49);
    await expect(
      runner.run({ workflow: buildWorkflow(), workflowId: "scan-1", input: { id: "a" } }),
    ).rejects.toThrow(/sleeping until/);
    expect(postSleepCalls).toBe(0);

    // Move past the sleep duration.
    clock.advance(1);

    // Re-run the workflow (simulating what DefaultSleepScanner does).
    // ctx.sleep auto-completes the pending journal entry since now >= wakeAt,
    // the step proceeds past sleep, and post-sleep activity fires.
    const result = await runner.run({
      workflow: buildWorkflow(),
      workflowId: "scan-1",
      input: { id: "a" },
    });
    expect(result).toEqual({ ok: true });
    expect(postSleepCalls).toBe(1);

    // Journal entry should now be completed.
    const journal = await storage.loadJournal({ workflowId: "scan-1", stepName: "wait-then-do" });
    expect(journal[0]!.stepType).toBe("sleep");
    expect(journal[0]!.phase).toBe("completed");
  });
});

describe("end-to-end workflow with suspend/resume", () => {
  it("activity → sleep → activity → signal → activity — the full approval flow", async () => {
    const storage = new InMemoryWorkflowStorage();
    const activityCalls: Record<string, number> = { create: 0, check: 0, finalize: 0 };

    const wfBuilder = workflow<{ draftId: string }>({ name: "approval-flow" })
      .journaled("body", function* (ctx, prev) {
        const created = yield* ctx.activity("create", async () => {
          activityCalls.create!++;
          return { id: prev.draftId, state: "draft" };
        });
        yield* ctx.sleep(100); // short review window
        yield* ctx.activity("check", async () => {
          activityCalls.check!++;
          return { checked: true };
        });
        const approval = yield* ctx.signal<{ approved: boolean }>("approval");
        yield* ctx.activity("finalize", async () => {
          activityCalls.finalize!++;
          return { ...created, approval, state: "final" };
        });
        return { ok: true };
      })
      .build();

    const clock = FakeWallClock.create(T0);
    const runner = createWorkflowRunner({ storage, clock });

    // Kick off — suspends at sleep. `run()` rejects with the plain
    // WorkflowSuspendedError; match on its human-readable message.
    await expect(
      runner.run({ workflow: wfBuilder, workflowId: "apr-1", input: { draftId: "d-1" } }),
    ).rejects.toThrow(/sleeping until/);
    expect(activityCalls).toEqual({ create: 1, check: 0, finalize: 0 });

    // Not due 1ms before the review window ends; due at its end.
    clock.advance(99);
    expect(await completeDueSleeps({ storage, now: clock.now(), limit: 10 })).toHaveLength(0);
    clock.advance(1);
    expect(await completeDueSleeps({ storage, now: clock.now(), limit: 10 })).toHaveLength(1);

    // Resume — suspends at signal this time.
    // A fresh builder is needed because the previous .run() returned; but the
    // WORKFLOW (journal + storage) is the same. New instance, same workflowId.
    const wf2 = workflow<{ draftId: string }>({ name: "approval-flow" })
      .journaled("body", function* (ctx, prev) {
        const created = yield* ctx.activity("create", async () => {
          activityCalls.create!++;
          return { id: prev.draftId, state: "draft" };
        });
        yield* ctx.sleep(100);
        yield* ctx.activity("check", async () => {
          activityCalls.check!++;
          return { checked: true };
        });
        const approval = yield* ctx.signal<{ approved: boolean }>("approval");
        yield* ctx.activity("finalize", async () => {
          activityCalls.finalize!++;
          return { ...created, approval, state: "final" };
        });
        return { ok: true };
      })
      .build();

    await expect(
      runner.run({ workflow: wf2, workflowId: "apr-1", input: { draftId: "d-1" } }),
    ).rejects.toThrow(/waiting for signal/);
    // create was replayed from journal, check just ran, finalize hasn't.
    expect(activityCalls).toEqual({ create: 1, check: 1, finalize: 0 });

    // Deliver approval.
    await completeSignal({
      storage,
      workflowId: "apr-1",
      stepName: "body",
      signalName: "approval",
      value: { approved: true },
    });

    // Final resume.
    const result = await runner.run({
      workflow: wf2,
      workflowId: "apr-1",
      input: { draftId: "d-1" },
    });
    expect(result).toEqual({ ok: true });
    // create + check replay, finalize ran.
    expect(activityCalls).toEqual({ create: 1, check: 1, finalize: 1 });
  });
});

describe("ctx.signal({ timeout }) — bounded suspend", () => {
  let storage: InMemoryWorkflowStorage;
  beforeEach(() => {
    storage = new InMemoryWorkflowStorage();
  });

  it("returns { ok: true, value } when delivery beats the timeout", async () => {
    type Decision = { approved: boolean };
    const body = function* (ctx: JournaledContext<unknown, unknown>) {
      const result = yield* ctx.signal<Decision>("approve", { timeout: 60_000 });
      return result;
    };

    // First run — suspends with the timeout configured.
    await expect(
      runJournaledStep({
        input: {},
        prev: {},
        workflowId: "wf-tsig-1",
        stepName: "wait",
        storage,
        body,
      }),
    ).rejects.toBeInstanceOf(WorkflowSuspendedError);

    // Journal has a pending signal entry with wakeAt set.
    const journalAfterSuspend = await storage.loadJournal({
      workflowId: "wf-tsig-1",
      stepName: "wait",
    });
    expect(journalAfterSuspend).toHaveLength(1);
    expect(journalAfterSuspend[0]!.stepType).toBe("signal");
    expect(journalAfterSuspend[0]!.wakeAt).toBeInstanceOf(Date);

    // External delivery before the timeout fires.
    const delivered = await completeSignal({
      storage,
      workflowId: "wf-tsig-1",
      stepName: "wait",
      signalName: "approve",
      value: { approved: true } satisfies Decision,
    });
    expect(delivered).toBe(true);

    // Replay returns the timed-signal envelope wrapping the bare value.
    const result = await runJournaledStep({
      input: {},
      prev: {},
      workflowId: "wf-tsig-1",
      stepName: "wait",
      storage,
      body,
    });
    expect(result).toEqual({ ok: true, value: { approved: true } });
  });

  it("self-completes with timeout outcome on replay after timeout has passed", async () => {
    type Decision = { approved: boolean };
    const body = function* (ctx: JournaledContext<unknown, unknown>) {
      const result = yield* ctx.signal<Decision>("approve", { timeout: 1_000 });
      return result;
    };
    const clock = FakeWallClock.create(T0);

    // First run suspends and writes the pending journal entry with its
    // deadline: the clock's now + 1s.
    await expect(
      runJournaledStep({
        input: {},
        prev: {},
        workflowId: "wf-tsig-2",
        stepName: "wait",
        storage,
        body,
        clock,
      }),
    ).rejects.toBeInstanceOf(WorkflowSuspendedError);
    const pending = await storage.loadJournal({ workflowId: "wf-tsig-2", stepName: "wait" });
    expect(pending[0]!.phase).toBe("pending");
    expect(pending[0]!.wakeAt!.getTime()).toBe(clock.currentTimeMs() + 1_000);

    // A re-run 1ms before the deadline still suspends.
    clock.advance(999);
    await expect(
      runJournaledStep({
        input: {},
        prev: {},
        workflowId: "wf-tsig-2",
        stepName: "wait",
        storage,
        body,
        clock,
      }),
    ).rejects.toBeInstanceOf(WorkflowSuspendedError);

    // At the deadline the re-run completes the entry with the timeout.
    clock.advance(1);
    const result = await runJournaledStep({
      input: {},
      prev: {},
      workflowId: "wf-tsig-2",
      stepName: "wait",
      storage,
      body,
      clock,
    });
    expect(result).toEqual({ ok: false, error: "timeout" });

    // Journal entry is now completed with the tagged timeout outcome.
    const journal = await storage.loadJournal({ workflowId: "wf-tsig-2", stepName: "wait" });
    expect(journal[0]!.phase).toBe("completed");
    expect(journal[0]!.exit).toEqual({
      tag: "Success",
      value: { $signal: "timeout" },
    });
  });

  it("delivery wins the race against an unexpired timeout — value, not envelope, when no opts", async () => {
    // The no-timeout overload returns the bare T.
    type Decision = { approved: boolean };
    const body = function* (ctx: JournaledContext<unknown, unknown>) {
      const result = yield* ctx.signal<Decision>("approve");
      return result;
    };

    await expect(
      runJournaledStep({
        input: {},
        prev: {},
        workflowId: "wf-tsig-3",
        stepName: "wait",
        storage,
        body,
      }),
    ).rejects.toBeInstanceOf(WorkflowSuspendedError);

    await completeSignal({
      storage,
      workflowId: "wf-tsig-3",
      stepName: "wait",
      signalName: "approve",
      value: { approved: false } satisfies Decision,
    });

    const result = await runJournaledStep({
      input: {},
      prev: {},
      workflowId: "wf-tsig-3",
      stepName: "wait",
      storage,
      body,
    });
    // Bare T.
    expect(result).toEqual({ approved: false });
  });
});
