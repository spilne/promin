// ---------------------------------------------------------------------------
// Intra-step compensation unwind policy.
//
// Compensations registered via `ActivityOptions.compensate` run only when
// the body fails for a business reason. Control-flow exits (suspend,
// continue-as-new, tripwire) and engine-integrity exits (non-determinism,
// ambiguous activity outcome, lock loss) propagate with every compensation
// left unrun and the journal untouched.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { InMemoryWorkflowStorage } from "../in-memory-storage.ts";
import { JournalNonDeterminismError, runJournaledStep } from "../journaled-step.ts";
import {
  AmbiguousActivityOutcome,
  FenceTokenMismatchError,
  TerminalError,
  WorkflowContinueAsNewError,
  WorkflowLockError,
  WorkflowTripwireError,
} from "../durable-pipeline-error.ts";

const run = (params: { storage: InMemoryWorkflowStorage; workflowId: string; body: any }) =>
  runJournaledStep({
    input: undefined,
    prev: undefined,
    workflowId: params.workflowId,
    stepName: "s",
    storage: params.storage,
    body: params.body,
  });

async function capture(promise: Promise<unknown>): Promise<unknown> {
  try {
    await promise;
  } catch (err) {
    return err;
  }
  throw new Error("expected the step to throw");
}

/**
 * Body: one compensated charge, then `after` decides how the body ends —
 * either a sub-generator to delegate to, or a plain function that throws.
 */
const chargeThen =
  (rolled: string[]) => (after: (ctx: any) => Generator<any, unknown, any> | void) =>
    function* (ctx: any) {
      yield* ctx.activity("charge", async () => 1, {
        compensate: () => {
          rolled.push("refund");
        },
      });
      const next = after(ctx);
      return next ? yield* next : undefined;
    };

describe("unwind policy — control-flow exits do not compensate", () => {
  it("continueAsNew propagates without running compensations", async () => {
    const storage = new InMemoryWorkflowStorage();
    const rolled: string[] = [];
    const err = await capture(
      run({
        storage,
        workflowId: "can",
        body: chargeThen(rolled)((ctx) => {
          ctx.continueAsNew({ next: 1 });
        }),
      }),
    );
    expect(err).toBeInstanceOf(WorkflowContinueAsNewError);
    expect(rolled).toEqual([]);
    const journal = await storage.loadJournal({ workflowId: "can", stepName: "s" });
    expect(journal.some((e) => e.stepType === "compensation")).toBe(false);
  });

  it("a tripwire error propagates without running compensations", async () => {
    const storage = new InMemoryWorkflowStorage();
    const rolled: string[] = [];
    const err = await capture(
      run({
        storage,
        workflowId: "trip",
        body: chargeThen(rolled)(() => {
          throw new WorkflowTripwireError({
            workflowId: "trip",
            stepName: "s",
            reason: "stop",
            message: "tripwire",
          });
        }),
      }),
    );
    expect(err).toBeInstanceOf(WorkflowTripwireError);
    expect(rolled).toEqual([]);
  });
});

describe("unwind policy — engine-integrity exits do not compensate", () => {
  it("JournalNonDeterminismError (renamed activity) leaves completed work alone", async () => {
    const storage = new InMemoryWorkflowStorage();
    const rolled: string[] = [];
    await run({
      storage,
      workflowId: "nd",
      body: chargeThen(rolled)(function* (ctx) {
        return yield* ctx.activity("a", async () => 2);
      }),
    });

    const err = await capture(
      run({
        storage,
        workflowId: "nd",
        body: chargeThen(rolled)(function* (ctx) {
          return yield* ctx.activity("b-renamed", async () => 2);
        }),
      }),
    );
    expect(err).toBeInstanceOf(JournalNonDeterminismError);
    expect(rolled).toEqual([]);
    const journal = await storage.loadJournal({ workflowId: "nd", stepName: "s" });
    expect(journal.some((e) => e.stepType === "compensation")).toBe(false);
  });

  it("AmbiguousActivityOutcome halts without running compensations", async () => {
    const storage = new InMemoryWorkflowStorage();
    const rolled: string[] = [];
    // A previous worker wrote the pending row for "send" (slot 2: charge=0,
    // its reserved compensation slot=1) and crashed before completing it.
    await storage.appendPendingEntry({
      workflowId: "amb",
      stepName: "s",
      activityIndex: 2,
      activityName: "send",
      stepType: "activity",
    });
    let sendCalls = 0;
    const err = await capture(
      run({
        storage,
        workflowId: "amb",
        body: chargeThen(rolled)(function* (ctx) {
          return yield* ctx.activity("send", async () => ++sendCalls);
        }),
      }),
    );
    expect(err).toBeInstanceOf(AmbiguousActivityOutcome);
    expect(sendCalls).toBe(0);
    expect(rolled).toEqual([]);
  });

  it("lock loss surfacing from a journal write (FenceTokenMismatchError) does not compensate", async () => {
    class FencedStorage extends InMemoryWorkflowStorage {
      override async completePendingEntry(
        params: Parameters<InMemoryWorkflowStorage["completePendingEntry"]>[0],
      ): Promise<void> {
        if (params.activityIndex === 2) {
          throw new FenceTokenMismatchError({
            workflowId: params.workflowId,
            expected: "2",
            provided: "1",
            message: "lock lost",
          });
        }
        return super.completePendingEntry(params);
      }
    }
    const storage = new FencedStorage();
    const rolled: string[] = [];
    const err = await capture(
      run({
        storage,
        workflowId: "fence",
        body: chargeThen(rolled)(function* (ctx) {
          return yield* ctx.activity("ship", async () => 3);
        }),
      }),
    );
    expect(err).toBeInstanceOf(FenceTokenMismatchError);
    expect(rolled).toEqual([]);
  });

  it("lock loss raised inside the body (WorkflowLockError) does not compensate", async () => {
    const storage = new InMemoryWorkflowStorage();
    const rolled: string[] = [];
    const err = await capture(
      run({
        storage,
        workflowId: "lock",
        body: chargeThen(rolled)(function* (ctx) {
          return yield* ctx.activity("ship", async () => {
            throw new WorkflowLockError({ workflowId: "lock", message: "lease expired" });
          });
        }),
      }),
    );
    expect(err).toBeInstanceOf(WorkflowLockError);
    expect(rolled).toEqual([]);
  });

  it("an engine-integrity exit from inside a ctx.parallel branch does not compensate", async () => {
    const storage = new InMemoryWorkflowStorage();
    const rolled: string[] = [];
    const err = await capture(
      run({
        storage,
        workflowId: "par",
        body: chargeThen(rolled)(function* (ctx) {
          return yield* ctx.parallel([
            ctx.activity("ok", async () => 1),
            ctx.activity("lost", async () => {
              throw new WorkflowLockError({ workflowId: "par", message: "lease expired" });
            }),
          ]);
        }),
      }),
    );
    expect(err).toBeInstanceOf(WorkflowLockError);
    expect(rolled).toEqual([]);
  });
});

describe("unwind policy — genuine failures still compensate", () => {
  it("a plain Error unwinds and journals the compensation", async () => {
    // Keeps the rows the engine discards after the unwind.
    const storage = new (class extends InMemoryWorkflowStorage {
      override async discardJournalEntries(): Promise<void> {}
    })();
    const rolled: string[] = [];
    const err = await capture(
      run({
        storage,
        workflowId: "fail",
        body: chargeThen(rolled)(function* (ctx) {
          return yield* ctx.activity("ship", async () => {
            throw new Error("warehouse down");
          });
        }),
      }),
    );
    expect((err as Error).message).toBe("warehouse down");
    expect(rolled).toEqual(["refund"]);
    const journal = await storage.loadJournal({ workflowId: "fail", stepName: "s" });
    expect(journal.filter((e) => e.stepType === "compensation")).toHaveLength(1);
  });

  it("a TerminalError unwinds", async () => {
    const storage = new InMemoryWorkflowStorage();
    const rolled: string[] = [];
    const err = await capture(
      run({
        storage,
        workflowId: "term",
        body: chargeThen(rolled)(() => {
          throw new TerminalError({ message: "declined" });
        }),
      }),
    );
    expect(err).toBeInstanceOf(TerminalError);
    expect(rolled).toEqual(["refund"]);
  });

  it("a thrown non-object value unwinds", async () => {
    const storage = new InMemoryWorkflowStorage();
    const rolled: string[] = [];
    const err = await capture(
      run({
        storage,
        workflowId: "str",
        body: chargeThen(rolled)(() => {
          throw "boom";
        }),
      }),
    );
    expect(err).toBe("boom");
    expect(rolled).toEqual(["refund"]);
  });
});
