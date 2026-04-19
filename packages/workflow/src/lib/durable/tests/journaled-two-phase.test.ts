// ---------------------------------------------------------------------------
// Two-phase activity record tests for journaled steps.
//
// Simulates a worker crash between the pending-row write and the completion
// write, then verifies replay behaviour:
//   - idempotent: false (default) → throw AmbiguousActivityOutcome
//   - idempotent: true             → re-run the activity body
// Also covers the happy path (normal completion → recorded exit on replay)
// and the legacy single-phase fallback (storages without suspend support).
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { runJournaledStep } from "../journaled-step.ts";
import { InMemoryWorkflowStorage } from "../in-memory-storage.ts";
import { AmbiguousActivityOutcome } from "../durable-pipeline-error.ts";
import type { ActivityJournalStorage, JournalEntry } from "../activity-journal.ts";

// ---------------------------------------------------------------------------
// Crash-injecting storage — stamps a `pending` row like the real engine, then
// throws a synthetic error BEFORE the engine can call completePendingEntry,
// so the next replay sees a row stuck in `pending` phase. This is the
// worker-crash window the two-phase feature exists to cover.
// ---------------------------------------------------------------------------

class CrashBetweenPhasesStorage extends InMemoryWorkflowStorage {
  private crashOnCompletion: Set<string> = new Set();

  /** Schedule one crash: the next completePendingEntry for this index throws. */
  armCrashAfterPending(stepName: string, activityIndex: number) {
    this.crashOnCompletion.add(`${stepName}\x00${activityIndex}`);
  }

  async completePendingEntry(params: {
    workflowId: string;
    stepName: string;
    activityIndex: number;
    exit: NonNullable<JournalEntry["exit"]>;
  }): Promise<void> {
    const key = `${params.stepName}\x00${params.activityIndex}`;
    if (this.crashOnCompletion.has(key)) {
      this.crashOnCompletion.delete(key);
      throw new Error("simulated worker crash between phases");
    }
    return super.completePendingEntry(params);
  }
}

// ---------------------------------------------------------------------------
// A minimal ActivityJournalStorage that doesn't implement JournaledSuspendStorage,
// used to prove the legacy single-phase fallback path still works when a
// custom storage can't do two-phase record.
// ---------------------------------------------------------------------------

class SinglePhaseOnlyStorage implements ActivityJournalStorage {
  private readonly entries = new Map<string, JournalEntry[]>();

  private key(workflowId: string, stepName: string): string {
    return `${workflowId}\x00${stepName}`;
  }

  async loadJournal(workflowId: string, stepName: string): Promise<JournalEntry[]> {
    return this.entries.get(this.key(workflowId, stepName)) ?? [];
  }

  async appendEntry(entry: {
    workflowId: string;
    stepName: string;
    activityIndex: number;
    branchPath?: string;
    activityName: string;
    exit: NonNullable<JournalEntry["exit"]>;
  }): Promise<void> {
    const key = this.key(entry.workflowId, entry.stepName);
    const list = this.entries.get(key) ?? [];
    list.push({
      activityIndex: entry.activityIndex,
      branchPath: entry.branchPath ?? "",
      activityName: entry.activityName,
      stepType: "activity",
      phase: "completed",
      exit: entry.exit,
      createdAt: new Date(),
    });
    this.entries.set(key, list);
  }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("journaled activity — two-phase record (happy path)", () => {
  it("completes pending row → replay returns recorded exit", async () => {
    const storage = new InMemoryWorkflowStorage();
    let ran = 0;

    const run = () =>
      runJournaledStep<unknown, unknown, number>({
        input: undefined,
        prev: undefined,
        workflowId: "wf-happy",
        stepName: "step",
        storage,
        body: function* (ctx) {
          const n = yield* ctx.activity("produce", async () => {
            ran++;
            return 42;
          });
          return n;
        },
      });

    const fresh = await run();
    const replay = await run();

    expect(fresh).toBe(42);
    expect(replay).toBe(42);
    expect(ran).toBe(1); // activity ran once, replay hit the journal
  });

  it("journal row is written with phase='completed' on success", async () => {
    const storage = new InMemoryWorkflowStorage();
    await runJournaledStep<unknown, unknown, number>({
      input: undefined,
      prev: undefined,
      workflowId: "wf-phase",
      stepName: "step",
      storage,
      body: function* (ctx) {
        return yield* ctx.activity("a", async () => 1);
      },
    });
    const journal = await storage.loadJournal("wf-phase", "step");
    expect(journal).toHaveLength(1);
    expect(journal[0]!.phase ?? "completed").toBe("completed");
    expect(journal[0]!.stepType).toBe("activity");
  });
});

describe("journaled activity — crash between phases (non-idempotent)", () => {
  it("leaves a pending row and throws AmbiguousActivityOutcome on replay", async () => {
    const storage = new CrashBetweenPhasesStorage();
    storage.armCrashAfterPending("step", 0);
    let ranTimes = 0;

    // First run — crashes during completePendingEntry. The side effect DID
    // run exactly once. The row stays in pending phase.
    await expect(
      runJournaledStep<unknown, unknown, number>({
        input: undefined,
        prev: undefined,
        workflowId: "wf-crash",
        stepName: "step",
        storage,
        body: function* (ctx) {
          return yield* ctx.activity("charge", async () => {
            ranTimes++;
            return 100;
          });
        },
      }),
    ).rejects.toThrow("simulated worker crash");

    // Confirm the row is pending.
    const journal = await storage.loadJournal("wf-crash", "step");
    expect(journal).toHaveLength(1);
    expect(journal[0]!.phase).toBe("pending");
    expect(ranTimes).toBe(1);

    // Second run — replay sees the pending row for a non-idempotent activity.
    // Must throw AmbiguousActivityOutcome and NOT re-run the body.
    await expect(
      runJournaledStep<unknown, unknown, number>({
        input: undefined,
        prev: undefined,
        workflowId: "wf-crash",
        stepName: "step",
        storage,
        body: function* (ctx) {
          return yield* ctx.activity("charge", async () => {
            ranTimes++;
            return 100;
          });
        },
      }),
    ).rejects.toBeInstanceOf(AmbiguousActivityOutcome);

    expect(ranTimes).toBe(1); // activity did NOT re-run
  });

  it("AmbiguousActivityOutcome carries useful diagnostic fields", async () => {
    const storage = new CrashBetweenPhasesStorage();
    storage.armCrashAfterPending("step", 0);

    await expect(
      runJournaledStep<unknown, unknown, number>({
        input: undefined,
        prev: undefined,
        workflowId: "wf-diag",
        stepName: "step",
        storage,
        body: function* (ctx) {
          return yield* ctx.activity("charge", async () => 1);
        },
      }),
    ).rejects.toThrow("simulated worker crash");

    try {
      await runJournaledStep<unknown, unknown, number>({
        input: undefined,
        prev: undefined,
        workflowId: "wf-diag",
        stepName: "step",
        storage,
        body: function* (ctx) {
          return yield* ctx.activity("charge", async () => 1);
        },
      });
      expect.unreachable("expected AmbiguousActivityOutcome");
    } catch (err) {
      expect(err).toBeInstanceOf(AmbiguousActivityOutcome);
      const e = err as AmbiguousActivityOutcome;
      expect(e.workflowId).toBe("wf-diag");
      expect(e.stepName).toBe("step");
      expect(e.activityIndex).toBe(0);
      expect(e.activityName).toBe("charge");
    }
  });
});

describe("journaled activity — crash between phases (idempotent)", () => {
  it("re-runs the activity on replay and completes the pending row", async () => {
    const storage = new CrashBetweenPhasesStorage();
    storage.armCrashAfterPending("step", 0);
    let ranTimes = 0;

    const body = function* (ctx: any) {
      return yield* ctx.activity(
        "fetch",
        async () => {
          ranTimes++;
          return ranTimes * 10;
        },
        { idempotent: true },
      );
    };

    // First run — crashes between phases.
    await expect(
      runJournaledStep<unknown, unknown, number>({
        input: undefined,
        prev: undefined,
        workflowId: "wf-idem",
        stepName: "step",
        storage,
        body,
      }),
    ).rejects.toThrow("simulated worker crash");

    expect(ranTimes).toBe(1);

    // Second run — idempotent means we re-run. Activity runs a second time,
    // the new result (20) is written as the completed exit.
    const replay = await runJournaledStep<unknown, unknown, number>({
      input: undefined,
      prev: undefined,
      workflowId: "wf-idem",
      stepName: "step",
      storage,
      body,
    });

    expect(ranTimes).toBe(2);
    expect(replay).toBe(20);

    // Journal row is now completed with the re-run result.
    const journal = await storage.loadJournal("wf-idem", "step");
    expect(journal).toHaveLength(1);
    expect(journal[0]!.phase).toBe("completed");
    expect(journal[0]!.exit).toEqual({ tag: "Success", value: 20 });
  });

  it("subsequent replay after successful re-run returns the recorded value without running", async () => {
    const storage = new CrashBetweenPhasesStorage();
    storage.armCrashAfterPending("step", 0);
    let ranTimes = 0;

    const body = function* (ctx: any) {
      return yield* ctx.activity(
        "fetch",
        async () => {
          ranTimes++;
          return 42;
        },
        { idempotent: true },
      );
    };

    await expect(
      runJournaledStep({
        input: undefined,
        prev: undefined,
        workflowId: "wf-idem-2",
        stepName: "step",
        storage,
        body,
      }),
    ).rejects.toThrow();
    await runJournaledStep({
      input: undefined,
      prev: undefined,
      workflowId: "wf-idem-2",
      stepName: "step",
      storage,
      body,
    }); // re-run completes
    const steadyState = await runJournaledStep<unknown, unknown, number>({
      input: undefined,
      prev: undefined,
      workflowId: "wf-idem-2",
      stepName: "step",
      storage,
      body,
    });
    expect(steadyState).toBe(42);
    expect(ranTimes).toBe(2); // crash + re-run = 2, steady-state replay = 0 more
  });
});

describe("journaled activity — failure during first phase", () => {
  it("failure after pending write is recorded as completed Failure", async () => {
    const storage = new InMemoryWorkflowStorage();
    await expect(
      runJournaledStep<unknown, unknown, number>({
        input: undefined,
        prev: undefined,
        workflowId: "wf-fail",
        stepName: "step",
        storage,
        body: function* (ctx) {
          return yield* ctx.activity("will-throw", async () => {
            throw new Error("business rule violated");
          });
        },
      }),
    ).rejects.toThrow("business rule violated");

    const journal = await storage.loadJournal("wf-fail", "step");
    expect(journal).toHaveLength(1);
    expect(journal[0]!.phase).toBe("completed");
    expect(journal[0]!.exit?.tag).toBe("Failure");

    // Replay rethrows the recorded failure (doesn't re-run the body).
    await expect(
      runJournaledStep<unknown, unknown, number>({
        input: undefined,
        prev: undefined,
        workflowId: "wf-fail",
        stepName: "step",
        storage,
        body: function* (ctx) {
          return yield* ctx.activity("will-throw", async () => 1); // body changed but replay ignores
        },
      }),
    ).rejects.toThrow("business rule violated");
  });
});

describe("journaled activity — legacy single-phase fallback", () => {
  it("storages without JournaledSuspendStorage still record + replay via appendEntry", async () => {
    const storage = new SinglePhaseOnlyStorage();
    let ran = 0;

    const run = () =>
      runJournaledStep<unknown, unknown, number>({
        input: undefined,
        prev: undefined,
        workflowId: "wf-legacy",
        stepName: "step",
        storage,
        body: function* (ctx) {
          return yield* ctx.activity("a", async () => {
            ran++;
            return 7;
          });
        },
      });

    const fresh = await run();
    const replay = await run();

    expect(fresh).toBe(7);
    expect(replay).toBe(7);
    expect(ran).toBe(1); // replay hit the journal even in the single-phase path
  });
});
