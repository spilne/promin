import { describe, expect, it } from "bun:test";
import { TaggedError, fail, succeed } from "@spilne/perfect-core";
import { FakeWallClock } from "../../../shared/wall-clock.ts";
import { workflow } from "../../durable-pipeline.ts";
import {
  WorkflowCancelledError,
  WorkflowDeadlineError,
  WorkflowFailedError,
  WorkflowTripwireError,
} from "../../durable-pipeline-error.ts";
import { InMemoryWorkflowStorage } from "../../in-memory-storage.ts";
import { invokeQueryHandler } from "../../query-registry.ts";
import { createWorkflowRunner, type StepExecutor } from "../../workflow-runner.ts";

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

function tagOf(error: unknown): string | undefined {
  return (error as { _tag?: string } | null | undefined)?._tag;
}

// ---------------------------------------------------------------------------
// Cancellation wins over the run that was executing
// ---------------------------------------------------------------------------

describe("cancel during a run", () => {
  it("stops at the next wave: later steps never run and the run rejects as cancelled", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const started = deferred();
    const gate = deferred();
    const ran: string[] = [];
    const wf = workflow<number>({ name: "cancel-mid-run" })
      .stepAsync("slow", async ({ input }) => {
        started.resolve();
        await gate.promise;
        ran.push("slow");
        return input;
      })
      .stepAsync("after", async ({ prev }) => {
        ran.push("after");
        return (prev as number) + 1;
      })
      .build();

    const run = runner.runSafe({ workflow: wf, workflowId: "c-1", input: 1 });
    await started.promise;
    await runner.handle("c-1").cancel();
    gate.resolve();
    const r = await run;

    expect(r.error).toBeInstanceOf(WorkflowCancelledError);
    expect(ran).toEqual(["slow"]);
    const state = (await storage.loadWorkflow("c-1"))!;
    expect(state.status).toBe("failed");
    expect(state.errorTag).toBe("WorkflowCancelledError");
    expect(state.steps["after"]).toBeUndefined();
    await expect(runner.handle("c-1").result()).rejects.toBeInstanceOf(WorkflowCancelledError);
  });

  it.each([
    { mode: "checkpointStep", split: false },
    { mode: "separate step writes", split: true },
  ])(
    "$mode: a cancel seen by a step's checkpoint stops the run before the next wave",
    async ({ split }) => {
      const storage = new InMemoryWorkflowStorage();
      let statusReads = 0;
      const loadStatus = storage.loadWorkflowStatus.bind(storage);
      storage.loadWorkflowStatus = (id) => {
        statusReads++;
        return loadStatus(id);
      };
      if (split) (storage as unknown as Record<string, unknown>)["checkpointStep"] = undefined;
      const runner = createWorkflowRunner({ storage });
      const ran: string[] = [];
      const wf = workflow<number>({ name: "cancel-seen-by-checkpoint" })
        .step("a", ({ input }) => succeed(input))
        .stepAsync("cancels", async ({ prev }) => {
          ran.push("cancels");
          // Lands before this step's own checkpoint.
          await storage.cancelWorkflow("c-ck");
          return prev as number;
        })
        .stepAsync("after", async ({ prev }) => {
          ran.push("after");
          return prev as number;
        })
        .build();

      const r = await runner.runSafe({ workflow: wf, workflowId: "c-ck", input: 1 });

      expect(r.error).toBeInstanceOf(WorkflowCancelledError);
      expect(ran).toEqual(["cancels"]);
      const state = (await storage.loadWorkflow("c-ck"))!;
      expect(state.errorTag).toBe("WorkflowCancelledError");
      expect(state.steps["cancels"]!.status).toBe("completed");
      expect(state.steps["after"]).toBeUndefined();
      // With checkpointStep the status comes back with each write; the
      // separate writes need a status read before each later wave.
      expect(statusReads).toBe(split ? 2 : 0);
    },
  );

  it("a cancel between two siblings' checkpoints is seen by the later one", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const firstSaved = deferred();
    const ran: string[] = [];
    const wf = workflow<number>({ name: "cancel-between-siblings" })
      .stepAsync("fast", async ({ input }) => input)
      .stepAsync(
        "slow",
        async ({ input }) => {
          await firstSaved.promise;
          await storage.cancelWorkflow("c-sib");
          return input;
        },
        { dependsOn: [] },
      )
      .stepAsync(
        "after",
        async ({ input }) => {
          ran.push("after");
          return input;
        },
        { dependsOn: ["fast", "slow"] },
      )
      .build();

    const run = runner.runSafe({ workflow: wf, workflowId: "c-sib", input: 1 });
    await waitFor(() => storage.getWorkflow("c-sib")?.steps["fast"]?.status === "completed");
    firstSaved.resolve();
    const r = await run;

    expect(r.error).toBeInstanceOf(WorkflowCancelledError);
    expect(ran).toEqual([]);
  });

  it("a cancel that lands during the last wave wins over the completion", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const started = deferred();
    const gate = deferred();
    const wf = workflow<number>({ name: "cancel-last-wave" })
      .stepAsync("only", async ({ input }) => {
        started.resolve();
        await gate.promise;
        return input;
      })
      .build();

    const run = runner.runSafe({ workflow: wf, workflowId: "c-2", input: 1 });
    await started.promise;
    await runner.handle("c-2").cancel();
    gate.resolve();
    const r = await run;

    expect(r.error).toBeInstanceOf(WorkflowCancelledError);
    expect((await storage.loadWorkflow("c-2"))!.status).toBe("failed");
  });

  it("a cancel during a failing wave skips the workflow retry and compensation", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const started = deferred();
    const gate = deferred();
    let attempts = 0;
    let compensated = 0;
    const wf = workflow<number>({ name: "cancel-failing", retry: { maxRetries: 3 } })
      .step("a", ({ input }) => succeed(input), {
        compensate: async () => {
          compensated++;
        },
      })
      .stepAsync("b", async () => {
        attempts++;
        started.resolve();
        await gate.promise;
        throw new Error("down");
      })
      .build();

    const run = runner.runSafe({ workflow: wf, workflowId: "c-3", input: 1 });
    await started.promise;
    await runner.handle("c-3").cancel();
    gate.resolve();
    const r = await run;

    expect(r.error).toBeInstanceOf(WorkflowCancelledError);
    expect(attempts).toBe(1);
    expect(compensated).toBe(0);
  });

  it("a run cancelled while sleeping does not resume", async () => {
    const clock = FakeWallClock.create(0);
    const storage = new InMemoryWorkflowStorage({ clock });
    const runner = createWorkflowRunner({ storage, clock });
    const ran: string[] = [];
    const wf = workflow<number>({ name: "cancel-sleep" })
      .step("a", ({ input }) => succeed(input))
      .sleep("nap", 1_000)
      .stepAsync("b", async ({ prev }) => {
        ran.push("b");
        return prev;
      })
      .build();

    const first = await runner.runSafe({ workflow: wf, workflowId: "c-4", input: 1 });
    expect(tagOf(first.error)).toBe("WorkflowSuspendedError");
    await runner.handle("c-4").cancel();
    clock.advance(2_000);
    const resumed = await runner.runSafe({ workflow: wf, workflowId: "c-4", input: 1 });

    expect(resumed.error).toBeInstanceOf(WorkflowCancelledError);
    expect(ran).toEqual([]);
  });
});

// ---------------------------------------------------------------------------
// Entry gate: an ended run is not executed again
// ---------------------------------------------------------------------------

describe("re-running an ended run", () => {
  it("a tripwired run rethrows its tripwire and does not run the steps after it", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const ran: string[] = [];
    const wf = workflow<number>({ name: "tw" })
      .step("a", ({ input }) => succeed(input))
      .tripwire("guard", { when: (n: number) => n > 0, reason: () => "stop" })
      .step("charge", ({ prev }) => {
        ran.push("charge");
        return succeed(prev);
      })
      .build();

    const r1 = await runner.runSafe({ workflow: wf, workflowId: "tw-1", input: 1 });
    expect(r1.error).toBeInstanceOf(WorkflowTripwireError);
    const r2 = await runner.runSafe({ workflow: wf, workflowId: "tw-1", input: 1 });

    expect(r2.error).toBeInstanceOf(WorkflowTripwireError);
    expect((r2.error as WorkflowTripwireError).stepName).toBe("guard");
    expect((r2.error as WorkflowTripwireError).reason).toBe("stop");
    expect(ran).toEqual([]);
    expect((await storage.loadWorkflow("tw-1"))!.status).toBe("tripwire");
  });

  it("a cancelled run rejects as cancelled without executing", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    let runs = 0;
    const wf = workflow<number>({ name: "cancelled-rerun" })
      .stepAsync("a", async ({ input }) => {
        runs++;
        return input;
      })
      .build();
    await storage.createWorkflow({ workflowId: "cr-1", workflowName: wf.name, input: 1 });
    await storage.cancelWorkflow("cr-1");

    const r = await runner.runSafe({ workflow: wf, workflowId: "cr-1", input: 1 });
    expect(r.error).toBeInstanceOf(WorkflowCancelledError);
    expect(runs).toBe(0);
  });

  it("a completed run answers with its result without executing", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    let runs = 0;
    const wf = workflow<number>({ name: "completed-rerun" })
      .stepAsync("a", async ({ input }) => {
        runs++;
        return input * 2;
      })
      .build();

    expect(await runner.run({ workflow: wf, workflowId: "cd-1", input: 21 })).toBe(42);
    expect(await runner.run({ workflow: wf, workflowId: "cd-1", input: 21 })).toBe(42);
    expect(runs).toBe(1);
  });

  it("a failed run rejects with its stored failure and tag; force starts a fresh run", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    let runs = 0;
    const wf = workflow<number>({ name: "failed-rerun" })
      .step("a", ({ input }) => {
        runs++;
        return runs === 1 ? fail(new Boom({ message: "card declined" })) : succeed(input);
      })
      .build();

    const r1 = await runner.runSafe({ workflow: wf, workflowId: "fr-1", input: 7 });
    expect(r1.error).toBeInstanceOf(Boom);

    const r2 = await runner.runSafe({ workflow: wf, workflowId: "fr-1", input: 7 });
    expect(r2.error).toBeInstanceOf(WorkflowFailedError);
    expect((r2.error as WorkflowFailedError).errorTag).toBe("Boom");
    expect((r2.error as WorkflowFailedError).stepName).toBe("a");
    expect(runs).toBe(1);

    const forced = await runner.runSafe({
      workflow: wf,
      workflowId: "fr-1",
      input: 7,
      force: true,
    });
    expect(forced.error).toBeNull();
    expect(forced.data).toBe(7);
    expect(runs).toBe(2);
    const state = (await storage.loadWorkflow("fr-1"))!;
    expect(state.status).toBe("completed");
    expect(state.run).toBe(2);
  });
});

// ---------------------------------------------------------------------------
// Error tags survive storage
// ---------------------------------------------------------------------------

describe("stored error tags", () => {
  it("handle.result() and status keep the failing error's tag", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const wf = workflow<number>({ name: "tagged-failure" })
      .step("pay", () => fail(new Boom({ message: "card declined" })))
      .build();

    await runner.runSafe({ workflow: wf, workflowId: "tag-1", input: 1 });

    const state = (await storage.loadWorkflow("tag-1"))!;
    expect(state.errorTag).toBe("Boom");
    expect(state.steps["pay"]!.errorTag).toBe("Boom");
    expect((await runner.handle("tag-1").status())!.errorTag).toBe("Boom");
    const err = await runner
      .handle("tag-1")
      .result()
      .catch((e: unknown) => e);
    expect(err).toBeInstanceOf(WorkflowFailedError);
    expect((err as WorkflowFailedError).errorTag).toBe("Boom");
    expect((err as WorkflowFailedError).message).toBe("card declined");
  });

  it("an executor's reported tag is stored, not StepError's", async () => {
    const storage = new InMemoryWorkflowStorage();
    const stepExecutor: StepExecutor = {
      executeStep: async () => ({
        ok: false,
        kind: "failed",
        error: "card declined",
        errorTag: "PaymentDeclined",
      }),
    };
    const runner = createWorkflowRunner({ storage, stepExecutor });
    const wf = workflow<number>({ name: "remote-tagged-failure" })
      .step("pay", ({ input }) => succeed(input))
      .build();

    const r = await runner.runSafe({ workflow: wf, workflowId: "tag-3", input: 1 });
    expect(tagOf(r.error)).toBe("StepError");
    const state = (await storage.loadWorkflow("tag-3"))!;
    expect(state.steps["pay"]!.errorTag).toBe("PaymentDeclined");
    expect(state.errorTag).toBe("PaymentDeclined");
    const err = await runner
      .handle("tag-3")
      .result()
      .catch((e: unknown) => e);
    expect((err as WorkflowFailedError).errorTag).toBe("PaymentDeclined");
  });

  it("a defect is stored without a tag", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const wf = workflow<number>({ name: "defect-failure" })
      .stepAsync("pay", async () => {
        throw new Error("socket hang up");
      })
      .build();

    await runner.runSafe({ workflow: wf, workflowId: "tag-2", input: 1 });
    const state = (await storage.loadWorkflow("tag-2"))!;
    expect(state.error).toBe("socket hang up");
    expect(state.errorTag).toBeUndefined();
    expect(state.steps["pay"]!.errorTag).toBeUndefined();
  });
});

// ---------------------------------------------------------------------------
// Lifecycle hooks are isolated
// ---------------------------------------------------------------------------

describe("lifecycle hooks", () => {
  it("a throwing onStepComplete is reported and the run completes", async () => {
    const storage = new InMemoryWorkflowStorage();
    const reported: Array<{ hook: string; message: string }> = [];
    const runner = createWorkflowRunner({
      storage,
      hooks: {
        onStepComplete: () => {
          throw new Error("hook exploded");
        },
        onHookError: ({ hook, error }) => {
          reported.push({ hook, message: (error as Error).message });
        },
      },
    });
    const wf = workflow<number>({ name: "hook-throw" })
      .step("a", ({ input }) => succeed(input))
      .step("b", ({ prev }) => succeed((prev as number) + 1))
      .build();

    const r = await runner.runSafe({ workflow: wf, workflowId: "h-1", input: 1 });

    expect(r.error).toBeNull();
    expect(r.data).toBe(2);
    const state = (await storage.loadWorkflow("h-1"))!;
    expect(state.status).toBe("completed");
    expect(Object.keys(state.steps).sort()).toEqual(["a", "b"]);
    expect(reported).toEqual([
      { hook: "onStepComplete", message: "hook exploded" },
      { hook: "onStepComplete", message: "hook exploded" },
    ]);
  });

  it("a rejecting onWorkflowComplete does not turn a completed run into a failure", async () => {
    const storage = new InMemoryWorkflowStorage();
    const reported: string[] = [];
    const runner = createWorkflowRunner({
      storage,
      hooks: {
        onWorkflowComplete: async () => {
          throw new Error("observer down");
        },
        onHookError: ({ hook }) => {
          reported.push(hook);
        },
      },
    });
    const wf = workflow<number>({ name: "hook-reject" })
      .step("a", ({ input }) => succeed(input))
      .build();

    expect(await runner.run({ workflow: wf, workflowId: "h-2", input: 5 })).toBe(5);
    expect(reported).toEqual(["onWorkflowComplete"]);
  });

  it("a throwing onStepFailure / onWorkflowFailure leaves the failure path intact", async () => {
    const storage = new InMemoryWorkflowStorage();
    let compensated = 0;
    const runner = createWorkflowRunner({
      storage,
      hooks: {
        onStepFailure: () => {
          throw new Error("x");
        },
        onWorkflowFailure: () => {
          throw new Error("y");
        },
        onHookError: () => {
          throw new Error("the reporter itself fails");
        },
      },
    });
    const wf = workflow<number>({ name: "hook-failure-path" })
      .step("a", ({ input }) => succeed(input), {
        compensate: async () => {
          compensated++;
        },
      })
      .step("b", () => fail(new Boom({ message: "bad" })))
      .build();

    const r = await runner.runSafe({ workflow: wf, workflowId: "h-3", input: 1 });
    expect(r.error).toBeInstanceOf(Boom);
    expect(compensated).toBe(1);
    expect((await storage.loadWorkflow("h-3"))!.status).toBe("failed");
  });
});

// ---------------------------------------------------------------------------
// Deadline from the persisted start
// ---------------------------------------------------------------------------

describe("workflow deadline", () => {
  it("a resume after a signal wait keeps the original deadline", async () => {
    const clock = FakeWallClock.create(0);
    const storage = new InMemoryWorkflowStorage({ clock });
    const runner = createWorkflowRunner({ storage, clock });
    const ran: string[] = [];
    const wf = workflow<number>({ name: "deadline", timeoutMs: 1_000 })
      .step("a", ({ input }) => succeed(input))
      .waitForSignal("w", { signalName: "go" })
      .step("b", ({ prev }) => {
        ran.push("b");
        return succeed(prev);
      })
      .build();

    const r1 = await runner.runSafe({ workflow: wf, workflowId: "d-1", input: 1 });
    expect(tagOf(r1.error)).toBe("WorkflowSuspendedError");
    clock.advance(60_000);
    await storage.deliverSignal("d-1", "go", 42);
    const r2 = await runner.runSafe({ workflow: wf, workflowId: "d-1", input: 1 });

    expect(r2.error).toBeInstanceOf(WorkflowDeadlineError);
    expect(ran).toEqual([]);
    expect((await storage.loadWorkflow("d-1"))!.status).toBe("failed");
  });

  it("a resume inside the deadline completes", async () => {
    const clock = FakeWallClock.create(0);
    const storage = new InMemoryWorkflowStorage({ clock });
    const runner = createWorkflowRunner({ storage, clock });
    const wf = workflow<number>({ name: "deadline-ok", timeoutMs: 1_000 })
      .step("a", ({ input }) => succeed(input))
      .waitForSignal("w", { signalName: "go" })
      .build();

    await runner.runSafe({ workflow: wf, workflowId: "d-2", input: 1 });
    clock.advance(500);
    await storage.deliverSignal("d-2", "go", 42);
    const r2 = await runner.runSafe({ workflow: wf, workflowId: "d-2", input: 1 });
    expect(r2.error).toBeNull();
    expect(r2.data).toBe(42);
  });
});

// ---------------------------------------------------------------------------
// Query handlers are cleared only by the run that owns the lock
// ---------------------------------------------------------------------------

describe("query handler ownership", () => {
  it("a duplicate run rejected with WorkflowLockError keeps the live run's handlers", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const started = deferred();
    const gate = deferred();
    const wf = workflow<number>({ name: "q" })
      .journaled("body", function* (ctx) {
        ctx.setQueryHandler("status", () => "busy");
        yield* ctx.activity("wait", async () => {
          started.resolve();
          await gate.promise;
          return 1;
        });
        return 1;
      })
      .build();

    const live = runner.runSafe({ workflow: wf, workflowId: "q-1", input: 1 });
    await started.promise;
    expect(await invokeQueryHandler("q-1", "status")).toBe("busy");

    const dup = await runner.runSafe({ workflow: wf, workflowId: "q-1", input: 1 });
    expect(tagOf(dup.error)).toBe("WorkflowLockError");
    expect(await invokeQueryHandler("q-1", "status")).toBe("busy");

    gate.resolve();
    expect((await live).error).toBeNull();
    await expect(invokeQueryHandler("q-1", "status")).rejects.toThrow();
  });
});

// ---------------------------------------------------------------------------
// Continue-as-new archives under the lock
// ---------------------------------------------------------------------------

describe("continue-as-new under the lock", () => {
  it("no other worker can take the lock while the run is archived", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const peerAttempts: boolean[] = [];
    const startFreshRun = storage.startFreshRun.bind(storage);
    storage.startFreshRun = async (workflowId: string) => {
      peerAttempts.push((await storage.tryLock(workflowId, 30_000)).acquired);
      return startFreshRun(workflowId);
    };
    const wf = workflow<{ n: number }>({ name: "can-lock" })
      .journaled("loop", function* (ctx) {
        yield* ctx.activity("tick", async () => ctx.input.n);
        if (ctx.input.n >= 2) return ctx.input.n;
        ctx.continueAsNew({ n: ctx.input.n + 1 });
      })
      .build();

    expect(await runner.run({ workflow: wf, workflowId: "can-1", input: { n: 0 } })).toBe(2);
    expect(peerAttempts).toEqual([false, false]);
    // The lock is released once the chain ends.
    expect((await storage.tryLock("can-1", 30_000)).acquired).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// Workflow-level retry semantics
// ---------------------------------------------------------------------------

describe("workflow-level retry", () => {
  /** Run `p` to completion, firing every timer but the lock heartbeat as it is scheduled. */
  async function driveRetries<T>(clock: FakeWallClock, p: Promise<T>, delays: number[]) {
    let done = false;
    const settled = p.finally(() => {
      done = true;
    });
    for (const delay of delays) {
      await waitFor(() => done || clock.pendingCount() > 1);
      if (done) break;
      clock.advance(delay);
    }
    return settled;
  }

  it("retries typed failures with the shared defaults (3 retries, 250ms doubling)", async () => {
    const clock = FakeWallClock.create(0);
    const storage = new InMemoryWorkflowStorage({ clock });
    const runner = createWorkflowRunner({ storage, clock });
    const at: number[] = [];
    const wf = workflow<number>({ name: "wf-retry-defaults", retry: {} })
      .step("flaky", ({ input }) => {
        at.push(clock.currentTimeMs());
        return at.length < 4 ? fail(new Boom({ message: "again" })) : succeed(input);
      })
      .build();

    const r = await driveRetries(
      clock,
      runner.runSafe({ workflow: wf, workflowId: "wr-1", input: 3 }),
      [250, 500, 1_000],
    );
    expect(r.error).toBeNull();
    expect(at).toEqual([0, 250, 750, 1_750]);
  });

  it("does not retry a defect unless retryDefects is set", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    let calls = 0;
    const body = async (): Promise<number> => {
      calls++;
      if (calls === 1) throw new Error("socket hang up");
      return 1;
    };
    const plain = workflow<number>({ name: "wf-defect", retry: { maxRetries: 2, baseDelayMs: 1 } })
      .stepAsync("a", body)
      .build();
    const r1 = await runner.runSafe({ workflow: plain, workflowId: "wd-1", input: 1 });
    expect((r1.error as Error).message).toBe("socket hang up");
    expect(calls).toBe(1);

    calls = 0;
    const optedIn = workflow<number>({
      name: "wf-defect-opt-in",
      retry: { maxRetries: 2, baseDelayMs: 1, retryDefects: true },
    })
      .stepAsync("a", body)
      .build();
    const r2 = await runner.runSafe({ workflow: optedIn, workflowId: "wd-2", input: 1 });
    expect(r2.error).toBeNull();
    expect(calls).toBe(2);
  });

  it("does not retry a spent deadline", async () => {
    const clock = FakeWallClock.create(0);
    const storage = new InMemoryWorkflowStorage({ clock });
    const runner = createWorkflowRunner({ storage, clock });
    let calls = 0;
    const wf = workflow<number>({
      name: "wf-deadline-retry",
      timeoutMs: 100,
      retry: { maxRetries: 3, baseDelayMs: 1 },
    })
      .step("slow", ({ input }) => {
        calls++;
        clock.advance(200);
        return succeed(input);
      })
      .step("next", ({ prev }) => succeed(prev))
      .build();

    const r = await runner.runSafe({ workflow: wf, workflowId: "wdl-1", input: 1 });
    expect(r.error).toBeInstanceOf(WorkflowDeadlineError);
    expect(calls).toBe(1);
  });
});
