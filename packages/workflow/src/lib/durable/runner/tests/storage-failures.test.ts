import { describe, expect, it } from "bun:test";
import { TaggedError, fail, succeed } from "@spilne/perfect-core";
import { FakeWallClock } from "../../../shared/wall-clock.ts";
import { workflow } from "../../durable-pipeline.ts";
import {
  CheckpointError,
  FenceTokenMismatchError,
  WorkflowLockLostError,
} from "../../durable-pipeline-error.ts";
import { InMemoryWorkflowStorage } from "../../in-memory-storage.ts";
import { withLock } from "../../with-lock.ts";
import { createWorkflowRunner } from "../../workflow-runner.ts";

class Boom extends TaggedError("Boom")<{ readonly message: string }>() {}

const nextMacrotask = () => new Promise<void>((r) => setImmediate(r));

async function waitFor(cond: () => boolean): Promise<void> {
  for (let i = 0; i < 500 && !cond(); i++) await nextMacrotask();
  if (!cond()) throw new Error("waitFor: condition never held");
}

function deferred<T = void>(): { promise: Promise<T>; resolve: (v: T) => void } {
  let resolve!: (v: T) => void;
  const promise = new Promise<T>((r) => (resolve = r));
  return { promise, resolve };
}

type StorageMethod =
  | "checkpointStep"
  | "saveStepResult"
  | "saveStepAttempt"
  | "saveStepFailure"
  | "completeWorkflow"
  | "failWorkflow"
  | "releaseLock"
  | "heartbeat";

/** Make `method` reject `times` times (default once), then behave. Returns the call counter. */
function failing(
  storage: InMemoryWorkflowStorage,
  method: StorageMethod,
  times = 1,
): { calls: number; failures: number } {
  const counter = { calls: 0, failures: 0 };
  const original = (storage[method] as (...args: unknown[]) => Promise<unknown>).bind(storage);
  (storage as unknown as Record<string, unknown>)[method] = async (...args: unknown[]) => {
    counter.calls++;
    if (counter.failures < times) {
      counter.failures++;
      throw new Error(`${method} blip`);
    }
    return original(...args);
  };
  return counter;
}

/**
 * Settle `p`, firing each due timer other than the lock heartbeat (which
 * stays pending for the whole run) as soon as it is scheduled.
 */
async function drive<T>(clock: FakeWallClock, p: Promise<T>): Promise<T> {
  let done = false;
  const settled = p.finally(() => {
    done = true;
  });
  while (!done) {
    await waitFor(() => done || clock.pendingCount() > 1);
    if (!done) clock.advance(1_000);
  }
  return settled;
}

/**
 * A runner on in-memory storage. With `split`, the storage hides
 * `checkpointStep`, so the runner checkpoints a step with the separate
 * `saveStepResult` / `saveStepFailure` / `saveStepAttempt` writes.
 */
function setup(params?: { split?: boolean }) {
  const clock = FakeWallClock.create(0);
  const storage = new InMemoryWorkflowStorage({ clock });
  if (params?.split === true) {
    (storage as unknown as Record<string, unknown>)["checkpointStep"] = undefined;
  }
  const runner = createWorkflowRunner({ storage, clock });
  return { clock, storage, runner };
}

// ---------------------------------------------------------------------------
// One-shot storage faults are retried, not turned into step failures
// ---------------------------------------------------------------------------

describe("a storage write that fails once", () => {
  it("checkpointStep: the step is not run again and its rows are written once", async () => {
    const { clock, storage, runner } = setup();
    const writes = failing(storage, "checkpointStep");
    let charges = 0;
    const wf = workflow<number>({
      name: "blip-checkpoint",
      retry: { maxRetries: 1, baseDelayMs: 1 },
    })
      .stepAsync("charge", async ({ input }) => {
        charges++;
        return input;
      })
      .build();

    const r = await drive(clock, runner.runSafe({ workflow: wf, workflowId: "b-0", input: 1 }));
    expect(r.error).toBeNull();
    expect(charges).toBe(1);
    expect(writes.failures).toBe(1);
    const state = (await storage.loadWorkflow("b-0"))!;
    expect(state.status).toBe("completed");
    expect(state.steps["charge"]!.status).toBe("completed");
    const attempts = await storage.loadStepAttempts({ workflowId: "b-0", stepName: "charge" });
    expect(attempts.map((a) => a.status)).toEqual(["completed"]);
  });

  it("checkpointStep of a failed step: the step's own failure is what the run reports", async () => {
    const { clock, storage, runner } = setup();
    const writes = failing(storage, "checkpointStep");
    const wf = workflow<number>({ name: "blip-checkpoint-failure" })
      .step("pay", () => fail(new Boom({ message: "declined" })))
      .build();

    const r = await drive(clock, runner.runSafe({ workflow: wf, workflowId: "b-0f", input: 1 }));
    expect(r.error).toBeInstanceOf(Boom);
    expect(writes.failures).toBe(1);
    const state = (await storage.loadWorkflow("b-0f"))!;
    expect(state.status).toBe("failed");
    expect(state.steps["pay"]!.errorTag).toBe("Boom");
    const attempts = await storage.loadStepAttempts({ workflowId: "b-0f", stepName: "pay" });
    expect(attempts.map((a) => a.status)).toEqual(["failed"]);
  });

  it("saveStepResult: the step is not run again and the run completes (no double charge)", async () => {
    const { clock, storage, runner } = setup({ split: true });
    const writes = failing(storage, "saveStepResult");
    let charges = 0;
    const wf = workflow<number>({
      name: "blip-result",
      retry: { maxRetries: 1, baseDelayMs: 1 },
    })
      .stepAsync("charge", async ({ input }) => {
        charges++;
        return input;
      })
      .build();

    const r = await drive(clock, runner.runSafe({ workflow: wf, workflowId: "b-1", input: 1 }));
    expect(r.error).toBeNull();
    expect(charges).toBe(1);
    expect(writes.failures).toBe(1);
    const state = (await storage.loadWorkflow("b-1"))!;
    expect(state.status).toBe("completed");
    expect(state.steps["charge"]!.status).toBe("completed");
  });

  it("saveStepAttempt: the attempt row is written on retry", async () => {
    const { clock, storage, runner } = setup({ split: true });
    const writes = failing(storage, "saveStepAttempt");
    let charges = 0;
    const wf = workflow<number>({ name: "blip-attempt" })
      .stepAsync("charge", async ({ input }) => {
        charges++;
        return input;
      })
      .build();

    const r = await drive(clock, runner.runSafe({ workflow: wf, workflowId: "b-2", input: 1 }));
    expect(r.error).toBeNull();
    expect(charges).toBe(1);
    expect(writes.failures).toBe(1);
    const attempts = await storage.loadStepAttempts({ workflowId: "b-2", stepName: "charge" });
    expect(attempts.map((a) => a.status)).toEqual(["completed"]);
  });

  it("saveStepFailure: the step's own failure is what the run reports", async () => {
    const { clock, storage, runner } = setup({ split: true });
    const writes = failing(storage, "saveStepFailure");
    const wf = workflow<number>({ name: "blip-failure" })
      .step("pay", () => fail(new Boom({ message: "declined" })))
      .build();

    const r = await drive(clock, runner.runSafe({ workflow: wf, workflowId: "b-3", input: 1 }));
    expect(r.error).toBeInstanceOf(Boom);
    expect(writes.failures).toBe(1);
    const state = (await storage.loadWorkflow("b-3"))!;
    expect(state.status).toBe("failed");
    expect(state.steps["pay"]!.status).toBe("failed");
    expect(state.steps["pay"]!.errorTag).toBe("Boom");
  });

  it("completeWorkflow: the run completes", async () => {
    const { clock, storage, runner } = setup();
    const writes = failing(storage, "completeWorkflow");
    const wf = workflow<number>({ name: "blip-complete" })
      .step("a", ({ input }) => succeed(input + 1))
      .build();

    const r = await drive(clock, runner.runSafe({ workflow: wf, workflowId: "b-4", input: 1 }));
    expect(r.data).toBe(2);
    expect(writes.calls).toBe(2);
    expect((await storage.loadWorkflow("b-4"))!.status).toBe("completed");
  });

  it("failWorkflow: the run is still recorded failed", async () => {
    const { clock, storage, runner } = setup();
    failing(storage, "failWorkflow");
    const wf = workflow<number>({ name: "blip-fail" })
      .step("a", () => fail(new Boom({ message: "nope" })))
      .build();

    const r = await drive(clock, runner.runSafe({ workflow: wf, workflowId: "b-5", input: 1 }));
    expect(r.error).toBeInstanceOf(Boom);
    const state = (await storage.loadWorkflow("b-5"))!;
    expect(state.status).toBe("failed");
    expect(state.errorTag).toBe("Boom");
  });

  it("releaseLock: the run's result stands, and so does its error", async () => {
    const { storage, runner } = setup();
    failing(storage, "releaseLock", 2);
    const ok = workflow<number>({ name: "blip-release-ok" })
      .step("a", ({ input }) => succeed(input * 3))
      .build();
    const bad = workflow<number>({ name: "blip-release-bad" })
      .step("a", () => fail(new Boom({ message: "original" })))
      .build();

    expect(await runner.run({ workflow: ok, workflowId: "b-6", input: 2 })).toBe(6);
    const r = await runner.runSafe({ workflow: bad, workflowId: "b-7", input: 1 });
    expect(r.error).toBeInstanceOf(Boom);
    expect((r.error as Boom).message).toBe("original");
  });

  it("heartbeat: a single failed heartbeat does not abort the run", async () => {
    const { clock, storage, runner } = setup();
    const beats = failing(storage, "heartbeat");
    const started = deferred();
    const gate = deferred();
    const ran: string[] = [];
    const wf = workflow<number>({ name: "blip-heartbeat" })
      .stepAsync("long", async ({ input }) => {
        started.resolve();
        await gate.promise;
        ran.push("long");
        return input;
      })
      .stepAsync("next", async ({ prev }) => {
        ran.push("next");
        return prev;
      })
      .build();

    const run = runner.runSafe({ workflow: wf, workflowId: "b-8", input: 1 });
    await started.promise;
    clock.advance(30_000); // heartbeat fails
    await waitFor(() => beats.failures === 1);
    clock.advance(30_000); // heartbeat succeeds
    await waitFor(() => beats.calls === 2);
    gate.resolve();
    const r = await run;

    expect(r.error).toBeNull();
    expect(ran).toEqual(["long", "next"]);
  });
});

// ---------------------------------------------------------------------------
// A checkpoint that keeps failing abandons the run as it stands
// ---------------------------------------------------------------------------

describe("a checkpoint write that keeps failing", () => {
  it.each([
    { method: "checkpointStep", split: false },
    { method: "saveStepResult", split: true },
  ] as const)(
    "$method: rejects with CheckpointError: no compensation, no failWorkflow, no workflow retry",
    async ({ method, split }) => {
      const { clock, storage, runner } = setup({ split });
      const writes = failing(storage, method, 4);
      let charges = 0;
      let compensated = 0;
      const failures: string[] = [];
      const guarded = workflow<number>({
        name: "checkpoint-down-saga",
        retry: { maxRetries: 2, baseDelayMs: 1 },
        hooks: {
          onWorkflowFailure: ({ error }) => {
            failures.push(error);
          },
        },
      })
        .stepAsync("reserve", async ({ input }) => input, {
          compensate: async () => {
            compensated++;
          },
        })
        .stepAsync("charge", async ({ prev }) => {
          charges++;
          return prev as number;
        })
        .build();

      const r = await drive(
        clock,
        runner.runSafe({ workflow: guarded, workflowId: "cp-1", input: 1 }),
      );

      expect(r.error).toBeInstanceOf(CheckpointError);
      const err = r.error as CheckpointError;
      expect(err.operation).toBe(method);
      expect(err.stepName).toBe("reserve");
      expect((err.cause as Error).message).toBe(`${method} blip`);
      expect(writes.calls).toBe(4); // first try + 3 retries
      expect(compensated).toBe(0);
      expect(charges).toBe(0);
      expect(failures).toEqual([]);
      const state = (await storage.loadWorkflow("cp-1"))!;
      expect(state.status).not.toBe("failed");
      expect(state.steps["reserve"]).toBeUndefined();

      // The lock was released; once storage recovers, the run is re-driven
      // and the unsaved step runs again (at-least-once).
      const again = await runner.runSafe({ workflow: guarded, workflowId: "cp-1", input: 1 });
      expect(again.error).toBeNull();
      expect(charges).toBe(1);
      expect((await storage.loadWorkflow("cp-1"))!.status).toBe("completed");
    },
  );

  it("a completion that cannot be saved leaves the run for recovery", async () => {
    const { clock, storage, runner } = setup();
    failing(storage, "completeWorkflow", 10);
    const wf = workflow<number>({ name: "complete-down" })
      .step("a", ({ input }) => succeed(input))
      .build();

    const r = await drive(clock, runner.runSafe({ workflow: wf, workflowId: "cp-2", input: 1 }));
    expect(r.error).toBeInstanceOf(CheckpointError);
    expect((r.error as CheckpointError).operation).toBe("completeWorkflow");
    const state = (await storage.loadWorkflow("cp-2"))!;
    expect(state.status).toBe("running");
    expect(state.steps["a"]!.status).toBe("completed");
  });
});

// ---------------------------------------------------------------------------
// Lost lock
// ---------------------------------------------------------------------------

describe("lost lock", () => {
  it("the run stops at the next wave when a heartbeat finds the lock taken", async () => {
    const { clock, storage, runner } = setup();
    const started = deferred();
    const gate = deferred();
    const ran: string[] = [];
    let heartbeats = 0;
    storage.heartbeat = async ({ workflowId }) => {
      heartbeats++;
      throw new FenceTokenMismatchError({
        workflowId,
        expected: "99",
        provided: "1",
        message: "taken",
      });
    };
    const wf = workflow<number>({ name: "lock-lost" })
      .stepAsync("long", async ({ input }) => {
        started.resolve();
        await gate.promise;
        ran.push("long");
        return input;
      })
      .stepAsync("next", async ({ prev }) => {
        ran.push("next");
        return prev;
      })
      .build();

    const run = runner.runSafe({ workflow: wf, workflowId: "ll-1", input: 1 });
    await started.promise;
    clock.advance(30_000);
    await waitFor(() => heartbeats === 1);
    await nextMacrotask(); // the rejection reaches withLock's handler
    gate.resolve();
    const r = await run;

    expect(r.error).toBeInstanceOf(WorkflowLockLostError);
    expect(ran).toEqual(["long"]);
    expect((await storage.loadWorkflow("ll-1"))!.status).toBe("running");
  });
});

describe("withLock", () => {
  it("aborts its signal when a heartbeat is fenced out", async () => {
    const clock = FakeWallClock.create(0);
    const storage = new InMemoryWorkflowStorage({ clock });
    const gate = deferred();
    let signal: AbortSignal | undefined;
    const run = withLock({
      storage,
      workflowId: "wl-1",
      options: { clock, heartbeatIntervalMs: 10, lockDurationMs: 30 },
      fn: async (ctx) => {
        signal = ctx.signal;
        await gate.promise;
        return "done";
      },
    });
    await waitFor(() => signal !== undefined);
    // Another holder takes the lock under a new fence token.
    await storage.releaseLock({ workflowId: "wl-1" });
    expect((await storage.tryLock({ workflowId: "wl-1", lockDurationMs: 1_000 })).acquired).toBe(
      true,
    );
    expect(signal!.aborted).toBe(false);
    clock.advance(10);
    await waitFor(() => signal!.aborted);

    expect(signal!.reason).toBeInstanceOf(WorkflowLockLostError);
    gate.resolve();
    expect(await run).toBe("done");
  });

  it("aborts its signal once no heartbeat succeeded for a whole lock duration", async () => {
    const clock = FakeWallClock.create(0);
    const storage = new InMemoryWorkflowStorage({ clock });
    const beats = failing(storage, "heartbeat", 100);
    const gate = deferred();
    let signal: AbortSignal | undefined;
    const run = withLock({
      storage,
      workflowId: "wl-2",
      options: { clock, heartbeatIntervalMs: 10, lockDurationMs: 30 },
      fn: async (ctx) => {
        signal = ctx.signal;
        await gate.promise;
        return "done";
      },
    });
    await waitFor(() => signal !== undefined);
    clock.advance(20);
    await waitFor(() => beats.failures === 2);
    expect(signal!.aborted).toBe(false);
    clock.advance(10);
    await waitFor(() => beats.failures === 3);
    await waitFor(() => signal!.aborted);
    expect(signal!.reason).toBeInstanceOf(WorkflowLockLostError);
    gate.resolve();
    await run;
  });

  it("a failing releaseLock does not mask fn's error", async () => {
    const storage = new InMemoryWorkflowStorage();
    failing(storage, "releaseLock");
    const original = new Boom({ message: "original" });
    await expect(
      withLock({
        storage,
        workflowId: "wl-3",
        fn: async () => {
          throw original;
        },
      }),
    ).rejects.toBe(original);
  });
});
