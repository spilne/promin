// ---------------------------------------------------------------------------
// Suspend and failure outcomes of journaled steps.
//
//   * Signal exits are tagged delivered / timeout; legacy untagged rows
//     still decode.
//   * The live run follows the exit the journal holds when another writer
//     completed the entry first, including on storages whose
//     `completePendingEntry` predates the reported result.
//   * A failure escaping the body ends the step attempt: recorded failures
//     are discarded so step-level `retry` on `.journaled` re-runs the
//     activity. Control-flow and engine-integrity exits discard nothing.
//   * Failures replay as the same kind of error.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { TaggedError } from "@spilne/perfect-core";
import { workflow } from "../durable-pipeline.ts";
import { createWorkflowRunner } from "../workflow-runner.ts";
import { InMemoryWorkflowStorage } from "../in-memory-storage.ts";
import { completeSignal, runJournaledStep } from "../journaled-step.ts";
import { TerminalError, WorkflowSuspendedError } from "../durable-pipeline-error.ts";
import type {
  CompletePendingResult,
  JournalExit,
  JournalFailureExit,
} from "../activity-journal.ts";
import {
  decodeSignalExitValue,
  deliveredSignalExitValue,
  failureExit,
  rehydrateFailure,
  timedOutSignalExitValue,
} from "../journal-exit.ts";
import { FakeWallClock } from "../../shared/wall-clock.ts";

const run = (params: {
  storage: InMemoryWorkflowStorage;
  workflowId: string;
  body: any;
  clock?: FakeWallClock;
}) =>
  runJournaledStep({
    input: undefined,
    prev: undefined,
    workflowId: params.workflowId,
    stepName: "s",
    storage: params.storage,
    body: params.body,
    ...(params.clock && { clock: params.clock }),
  });

async function capture(promise: Promise<unknown>): Promise<unknown> {
  try {
    await promise;
  } catch (err) {
    return err;
  }
  throw new Error("expected the step to throw");
}

class CardDeclined extends TaggedError("CardDeclined")<{
  readonly message: string;
  readonly code: string;
}>() {}

describe("signal exit encoding", () => {
  it("tagged values decode regardless of the payload's shape", () => {
    const payload = { ok: false, error: "timeout" };
    expect(
      decodeSignalExitValue({ stored: deliveredSignalExitValue(payload), hasTimeout: true }),
    ).toEqual({ kind: "delivered", value: payload });
    expect(decodeSignalExitValue({ stored: timedOutSignalExitValue(), hasTimeout: true })).toEqual({
      kind: "timeout",
    });
  });

  it("legacy rows: only the exact timeout shape on a timed wait is a timeout", () => {
    const legacyTimeout = { ok: false, error: "timeout" };
    expect(decodeSignalExitValue({ stored: legacyTimeout, hasTimeout: true })).toEqual({
      kind: "timeout",
    });
    expect(decodeSignalExitValue({ stored: legacyTimeout, hasTimeout: false })).toEqual({
      kind: "delivered",
      value: legacyTimeout,
    });
    const declined = { ok: false, reason: "declined" };
    expect(decodeSignalExitValue({ stored: declined, hasTimeout: true })).toEqual({
      kind: "delivered",
      value: declined,
    });
    expect(decodeSignalExitValue({ stored: "v", hasTimeout: true })).toEqual({
      kind: "delivered",
      value: "v",
    });
  });
});

describe("failure exit encoding", () => {
  it("TerminalError and RetryableError come back as their own class with fields", () => {
    const exit = failureExit(new TerminalError({ message: "no", code: 7 } as never));
    expect(exit).toMatchObject({ tag: "Failure", error: "no", errorTag: "TerminalError" });
    const back = rehydrateFailure(JSON.parse(JSON.stringify(exit)));
    expect(back).toBeInstanceOf(TerminalError);
    expect(back.message).toBe("no");
    expect((back as unknown as { code: number }).code).toBe(7);
  });

  it("a user TaggedError comes back with its _tag, name and fields", () => {
    const exit = failureExit(new CardDeclined({ message: "declined", code: "E42" }));
    const back = rehydrateFailure(JSON.parse(JSON.stringify(exit))) as Error &
      Record<string, unknown>;
    expect(back).toBeInstanceOf(Error);
    expect(back._tag).toBe("CardDeclined");
    expect(back.name).toBe("CardDeclined");
    expect(back.message).toBe("declined");
    expect(back.code).toBe("E42");
  });

  it("built-in error classes come back by name; plain errors stay plain", () => {
    expect(rehydrateFailure(failureExit(new TypeError("t")))).toBeInstanceOf(TypeError);
    const plain = failureExit(new Error("p"));
    expect(plain).toEqual({ tag: "Failure", error: "p" });
    expect(rehydrateFailure(plain).constructor).toBe(Error);
  });

  it("fields that can't be serialized are dropped, the rest kept", () => {
    const err = Object.assign(new Error("x"), { keep: new Date(0), drop: () => 1 });
    const back = rehydrateFailure(failureExit(err)) as Error & Record<string, unknown>;
    expect(back.keep).toEqual(new Date(0));
    expect(back.drop).toBeUndefined();
  });

  it("thrown non-errors and legacy rows rehydrate as a plain Error", () => {
    expect(rehydrateFailure(failureExit("boom")).message).toBe("boom");
    const legacy: JournalFailureExit = { tag: "Failure", error: "old" };
    const back = rehydrateFailure(legacy);
    expect(back.constructor).toBe(Error);
    expect(back.message).toBe("old");
  });
});

describe("live run follows the stored outcome", () => {
  /** A storage whose `completePendingEntry` predates the reported result. */
  class LegacyResultStorage extends InMemoryWorkflowStorage {
    override async completePendingEntry(
      params: Parameters<InMemoryWorkflowStorage["completePendingEntry"]>[0],
    ): Promise<CompletePendingResult> {
      await super.completePendingEntry(params);
      return undefined as unknown as CompletePendingResult;
    }
  }

  for (const [label, make] of [
    ["current storage", () => new InMemoryWorkflowStorage()],
    ["storage without a reported result", () => new LegacyResultStorage()],
  ] as const) {
    it(`a delivery landing after the journal load beats the timeout (${label})`, async () => {
      const storage = make();
      const clock = FakeWallClock.create(0);
      const body = function* (ctx: any) {
        return yield* ctx.signal("go", { timeout: 1_000 });
      };
      expect(await capture(run({ storage, workflowId: "race", body, clock }))).toBeInstanceOf(
        WorkflowSuspendedError,
      );
      clock.advance(1_000);

      const load = storage.loadJournal.bind(storage);
      let armed = true;
      storage.loadJournal = async (workflowId: string, stepName: string) => {
        const journal = await load(workflowId, stepName);
        if (armed) {
          armed = false;
          await completeSignal({ storage, workflowId, stepName, signalName: "go", value: 42 });
        }
        return journal;
      };

      expect(await run({ storage, workflowId: "race", body, clock })).toEqual({
        ok: true,
        value: 42,
      });
      expect(await run({ storage, workflowId: "race", body, clock })).toEqual({
        ok: true,
        value: 42,
      });
    });
  }

  it("completeSignal reports false when it loses the completion race", async () => {
    const storage = new InMemoryWorkflowStorage();
    await storage.appendPendingEntry({
      workflowId: "lose",
      stepName: "s",
      activityIndex: 0,
      activityName: "go",
      stepType: "signal",
    });
    // The timeout lands between completeSignal's lookup and its write.
    const find = storage.findPendingSignal.bind(storage);
    storage.findPendingSignal = async (params) => {
      const hit = await find(params);
      await storage.completePendingEntry({
        workflowId: "lose",
        stepName: "s",
        activityIndex: 0,
        exit: { tag: "Success", value: timedOutSignalExitValue() },
      });
      return hit;
    };
    expect(
      await completeSignal({
        storage,
        workflowId: "lose",
        stepName: "s",
        signalName: "go",
        value: 1,
      }),
    ).toBe(false);
  });

  it("an activity whose slot another writer completed takes the stored result", async () => {
    const storage = new InMemoryWorkflowStorage();
    const original = storage.completePendingEntry.bind(storage);
    let raced = false;
    storage.completePendingEntry = async (params) => {
      if (!raced) {
        raced = true;
        await original({ ...params, exit: { tag: "Success", value: "from-other-worker" } });
      }
      return original(params);
    };
    const body = function* (ctx: any) {
      return yield* ctx.activity("a", async () => "mine");
    };
    expect(await run({ storage, workflowId: "act-race", body })).toBe("from-other-worker");
    expect(await run({ storage, workflowId: "act-race", body })).toBe("from-other-worker");
  });

  it("a timed-out entry replayed by code waiting without a timeout is non-determinism", async () => {
    const storage = new InMemoryWorkflowStorage();
    await storage.appendPendingEntry({
      workflowId: "nd",
      stepName: "s",
      activityIndex: 0,
      activityName: "go",
      stepType: "signal",
    });
    await storage.completePendingEntry({
      workflowId: "nd",
      stepName: "s",
      activityIndex: 0,
      exit: { tag: "Success", value: timedOutSignalExitValue() },
    });
    const err = await capture(
      run({
        storage,
        workflowId: "nd",
        body: function* (ctx: any) {
          return yield* ctx.signal("go");
        },
      }),
    );
    expect((err as { _tag?: string })._tag).toBe("JournalNonDeterminismError");
  });
});

describe("failure replay and step attempts", () => {
  it("a runner retry of a .journaled step re-runs the failed activity", async () => {
    const storage = new InMemoryWorkflowStorage();
    let calls = 0;
    let before = 0;
    // Workflow-level retry re-runs the failed journaled step on the same
    // journal, like a step-level retry does.
    const wf = workflow({ name: "retry-journaled", retry: { maxRetries: 5, baseDelayMs: 1 } })
      .journaled("j", function* (ctx: any) {
        yield* ctx.activity("before", async () => ++before);
        return yield* ctx.activity("flaky", async () => {
          calls++;
          if (calls < 3) throw new Error("transient");
          return "ok";
        });
      })
      .build();
    const runner = createWorkflowRunner({ storage });
    const result = await runner.run({ workflow: wf, workflowId: "w1", input: {} });
    expect(result).toBe("ok");
    expect(calls).toBe(3);
    expect(before).toBe(1);
  });

  it("a step attempt that fails with TerminalError does not leave it to replay", async () => {
    const storage = new InMemoryWorkflowStorage();
    let calls = 0;
    const wf = workflow({ name: "terminal-journaled" })
      .journaled("j", function* (ctx: any) {
        return yield* ctx.activity("pay", async () => {
          calls++;
          throw new TerminalError({ message: "card declined" });
        });
      })
      .build();
    const runner = createWorkflowRunner({ storage });
    await expect(runner.run({ workflow: wf, workflowId: "w2", input: {} })).rejects.toThrow(
      "card declined",
    );
    expect(calls).toBe(1);
  });

  it("a recorded TerminalError replays as TerminalError (crash before the discard)", async () => {
    const storage = new (class extends InMemoryWorkflowStorage {
      override async discardJournalEntries(): Promise<void> {}
    })();
    const classes: string[] = [];
    const body = function* (ctx: any) {
      try {
        yield* ctx.activity("x", async () => {
          throw new TerminalError({ message: "no" });
        });
      } catch (e) {
        classes.push(e instanceof TerminalError ? "TerminalError" : (e as Error).constructor.name);
        throw e;
      }
    };
    await capture(run({ storage, workflowId: "p4", body }));
    await capture(run({ storage, workflowId: "p4", body }));
    expect(classes).toEqual(["TerminalError", "TerminalError"]);
  });

  it("a failed compensation is retried by the next attempt's unwind", async () => {
    const storage = new InMemoryWorkflowStorage();
    let refundTries = 0;
    let charges = 0;
    const body = function* (ctx: any) {
      yield* ctx.activity("charge", async () => ++charges, {
        compensate: () => {
          refundTries++;
          if (refundTries === 1) throw new Error("refund api down");
        },
      });
      yield* ctx.activity("ship", async () => {
        throw new Error("warehouse down");
      });
    };
    await capture(run({ storage, workflowId: "comp-retry", body }));
    expect(refundTries).toBe(1);
    // The charge was not rolled back, so it replays; its refund runs again.
    await capture(run({ storage, workflowId: "comp-retry", body }));
    expect(charges).toBe(1);
    expect(refundTries).toBe(2);
    const journal = await storage.loadJournal("comp-retry", "s");
    expect(journal).toEqual([]);
  });

  it("engine-integrity and control-flow exits leave recorded failures in place", async () => {
    const storage = new InMemoryWorkflowStorage();
    const failing = function* (ctx: any) {
      try {
        yield* ctx.activity("a", async () => {
          throw new Error("caught");
        });
      } catch {
        // handled
      }
      yield* ctx.activity("b", async () => "b");
    };
    expect(await run({ storage, workflowId: "nd-keep", body: failing })).toBeUndefined();
    const renamed = function* (ctx: any) {
      try {
        yield* ctx.activity("a", async () => "a");
      } catch {
        // handled
      }
      yield* ctx.activity("renamed", async () => "b");
    };
    const err = await capture(run({ storage, workflowId: "nd-keep", body: renamed }));
    expect((err as { _tag?: string })._tag).toBe("JournalNonDeterminismError");
    const exits = (await storage.loadJournal("nd-keep", "s")).map((e) => e.exit as JournalExit);
    expect(exits).toEqual([
      { tag: "Failure", error: "caught" },
      { tag: "Success", value: "b" },
    ]);
  });
});
