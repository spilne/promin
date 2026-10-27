// ---------------------------------------------------------------------------
// One orchestration context for runs, version drains and child workflows:
// every run started by a runner (directly, through a drain, or as a child)
// sees the runner's clock, step executor and executor id; step bodies get
// the run's fence guard, version and patches; step timeouts run on the
// injected WallClock.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { succeed, tryPromise } from "@spilne/perfect-core";
import { workflow, type Workflow } from "../../durable-pipeline.ts";
import { InMemoryWorkflowStorage } from "../../in-memory-storage.ts";
import type { SuspendWorkflowParams } from "../../workflow-storage.ts";
import { FakeWallClock } from "../../../shared/wall-clock.ts";
import {
  createWorkflowRunner,
  InProcessStepExecutor,
  type StepExecutionRequest,
  type StepExecutionResult,
  type StepExecutor,
} from "../../workflow-runner.ts";

const T0 = "2026-01-01T00:00:00.000Z";

/** Records every request, then delegates; rebinds along with its inner executor. */
class RecordingExecutor implements StepExecutor {
  constructor(
    private readonly inner: StepExecutor,
    readonly seen: StepExecutionRequest[],
  ) {}

  executeStep(req: StepExecutionRequest): Promise<StepExecutionResult> {
    this.seen.push(req);
    return this.inner.executeStep(req);
  }

  forWorkflow(wf: Workflow<unknown, unknown>): StepExecutor {
    return new RecordingExecutor(this.inner.forWorkflow?.(wf) ?? this.inner, this.seen);
  }
}

function setup(rootWorkflow: Workflow<unknown, unknown>) {
  const clock = FakeWallClock.create(T0);
  const storage = new InMemoryWorkflowStorage({ clock });
  const seen: StepExecutionRequest[] = [];
  const stepExecutor = new RecordingExecutor(
    new InProcessStepExecutor(rootWorkflow, { storage, clock }),
    seen,
  );
  const runner = createWorkflowRunner({ storage, clock, stepExecutor, executorId: "exec-1" });
  return { clock, storage, runner, seen };
}

/** Let a fire-and-forget run reach its next await on the fake clock. */
async function settle(): Promise<void> {
  for (let i = 0; i < 20; i++) await Promise.resolve();
  await new Promise((r) => setImmediate(r));
}

describe("version drain — keeps the runner's executor and clock", () => {
  it("drives the stored version's steps through the executor on the fake clock", async () => {
    const ran: string[] = [];
    const v1 = workflow<number>({ name: "drain-ctx", version: "1" })
      .step("a", () => succeed("v1-a"))
      .sleep("nap", 60_000)
      .step("b", () => {
        ran.push("v1-b");
        return succeed("v1-done");
      })
      .build();
    const v2 = workflow<number>({
      name: "drain-ctx",
      version: "2",
      onVersionMismatch: "drain",
      previousVersions: [v1 as Workflow<unknown, unknown>],
    })
      .step("a", () => succeed("v2-a"))
      .sleep("nap", 60_000)
      .step("b", () => {
        ran.push("v2-b");
        return succeed("v2-done");
      })
      .build();

    const { clock, storage, runner, seen } = setup(v1 as Workflow<unknown, unknown>);
    const first = await runner.runSafe({ workflow: v1, workflowId: "d-1", input: 1 });
    expect(first.error?._tag).toBe("WorkflowSuspendedError");
    const asleep = await storage.loadWorkflow("d-1");
    expect(asleep?.steps["nap"]?.wakeAt?.toISOString()).toBe("2026-01-01T00:01:00.000Z");

    seen.length = 0;
    clock.advance(60_000);
    // Resume with the v2 definition: the run drains on v1.
    const done = await runner.runSafe({ workflow: v2, workflowId: "d-1", input: 1 });
    expect(done.error).toBeNull();
    expect(done.data).toBe("v1-done");
    expect(ran).toEqual(["v1-b"]);
    expect(seen.map((r) => `${r.version}:${r.stepName}`)).toEqual(["1:nap", "1:b"]);

    const state = await storage.loadWorkflow("d-1");
    expect(state?.steps["b"]?.completedAt?.toISOString()).toBe("2026-01-01T00:01:00.000Z");
    const attempts = await storage.loadStepAttempts({ workflowId: "d-1" });
    expect(attempts.find((a) => a.stepName === "b")?.executorId).toBe("exec-1");
  });
});

describe("child workflows — inherit the parent's clock and executor", () => {
  const child = workflow<{ n: number }>({ name: "child-ctx", version: "c1" })
    .sleep("child-nap", 30_000)
    .step("double", ({ input }) => succeed(input.n * 2))
    .build();

  it(".subworkflow() runs the child through the executor on the fake clock", async () => {
    const parent = workflow<number>({ name: "parent-sub", version: "p1" })
      .subworkflow("spawn", child, {
        input: (n) => ({ n }),
        workflowId: () => "sub-child-1",
      })
      .build();
    const { clock, storage, runner, seen } = setup(parent as Workflow<unknown, unknown>);

    const first = await runner.runSafe({ workflow: parent, workflowId: "sub-1", input: 21 });
    expect(first.error).not.toBeNull();
    const childState = await storage.loadWorkflow("sub-child-1");
    expect(childState?.parentWorkflowId).toBe("sub-1");
    expect(childState?.version).toBe("c1");
    expect(childState?.steps["child-nap"]?.wakeAt?.toISOString()).toBe("2026-01-01T00:00:30.000Z");
    expect(seen.map((r) => `${r.version}:${r.stepName}`)).toEqual(["p1:spawn", "c1:child-nap"]);

    clock.advance(30_000);
    const done = await runner.runSafe({ workflow: parent, workflowId: "sub-1", input: 21 });
    expect(done.error).toBeNull();
    expect(done.data).toBe(42);
    const attempts = await storage.loadStepAttempts({ workflowId: "sub-child-1" });
    expect(attempts.find((a) => a.stepName === "double")?.executorId).toBe("exec-1");
  });

  it("journaled ctx.child runs the child through the executor on the fake clock", async () => {
    const quickChild = workflow<{ n: number }>({ name: "quick-child", version: "c2" })
      .step("double", ({ input }) => succeed(input.n * 2))
      .build();
    const parent = workflow<number>({ name: "parent-journaled", version: "p1" })
      .journaled("spawn", function* (ctx, n) {
        return yield* ctx.child(quickChild, { input: { n }, workflowId: "j-child-1" });
      })
      .build();
    const { clock, storage, runner, seen } = setup(parent as Workflow<unknown, unknown>);
    clock.advance(5_000);

    const done = await runner.runSafe({ workflow: parent, workflowId: "j-1", input: 5 });
    expect(done.error).toBeNull();
    expect(done.data).toBe(10);

    const childState = await storage.loadWorkflow("j-child-1");
    expect(childState?.parentWorkflowId).toBe("j-1");
    expect(childState?.steps["double"]?.completedAt?.toISOString()).toBe(
      "2026-01-01T00:00:05.000Z",
    );
    expect(seen.map((r) => `${r.version}:${r.stepName}`)).toEqual(["p1:spawn", "c2:double"]);
    const attempts = await storage.loadStepAttempts({ workflowId: "j-child-1" });
    expect(attempts.find((a) => a.stepName === "double")?.executorId).toBe("exec-1");
  });

  it("journaled ctx.child surfaces a child create failure instead of swallowing it", async () => {
    class FailingChildCreate extends InMemoryWorkflowStorage {
      override async createWorkflow(
        params: Parameters<InMemoryWorkflowStorage["createWorkflow"]>[0],
      ): ReturnType<InMemoryWorkflowStorage["createWorkflow"]> {
        if (params.parentWorkflowId !== undefined) throw new Error("child create: disk full");
        return super.createWorkflow(params);
      }
    }
    let childRuns = 0;
    const counted = workflow<number>({ name: "counted-child" })
      .step("count", () => {
        childRuns++;
        return succeed(1);
      })
      .build();
    const parent = workflow<number>({ name: "parent-create-fail" })
      .journaled("spawn", function* (ctx, n) {
        return yield* ctx.child(counted, { input: n, workflowId: "cf-child-1" });
      })
      .build();
    const storage = new FailingChildCreate();
    const runner = createWorkflowRunner({ storage });

    const res = await runner.runSafe({ workflow: parent, workflowId: "cf-1", input: 1 });
    expect(res.error).not.toBeNull();
    expect(String((res.error as Error).message)).toContain("child create: disk full");
    expect(childRuns).toBe(0);
    expect(await storage.loadWorkflow("cf-child-1")).toBeNull();
  });
});

describe("step timeouts — run on the injected WallClock", () => {
  it("fails a hung step with StepTimeoutError when the fake clock passes timeoutMs", async () => {
    const clock = FakeWallClock.create(T0);
    const storage = new InMemoryWorkflowStorage({ clock });
    const runner = createWorkflowRunner({ storage, clock });
    let started = false;
    const wf = workflow<number>({ name: "timeout-fake" })
      .step(
        "hang",
        () =>
          tryPromise(
            () =>
              new Promise<number>(() => {
                started = true;
              }),
            (e) => e,
          ).orDie(),
        { timeoutMs: 10_000 },
      )
      .build();

    const run = runner.runSafe({ workflow: wf, workflowId: "to-1", input: 1 });
    while (!started) await settle();
    await settle();
    clock.advance(9_999);
    await settle();
    clock.advance(1);
    const res = await run;
    expect(res.error?._tag).toBe("StepTimeoutError");
  });

  it("a step that settles first clears its timeout timer", async () => {
    const clock = FakeWallClock.create(T0);
    const storage = new InMemoryWorkflowStorage({ clock });
    const runner = createWorkflowRunner({ storage, clock });
    const wf = workflow<number>({ name: "timeout-fast" })
      .step("fast", ({ input }) => succeed(input + 1), { timeoutMs: 10_000 })
      .build();

    const res = await runner.runSafe({ workflow: wf, workflowId: "tf-1", input: 1 });
    expect(res.data).toBe(2);
    expect(clock.pendingCount()).toBe(0);
  });

  it("InProcessStepExecutor times out on its clock too", async () => {
    const clock = FakeWallClock.create(T0);
    const storage = new InMemoryWorkflowStorage({ clock });
    let started = false;
    const wf = workflow<number>({ name: "timeout-exec" })
      .step(
        "hang",
        () =>
          tryPromise(
            () =>
              new Promise<number>(() => {
                started = true;
              }),
            (e) => e,
          ).orDie(),
        { timeoutMs: 5_000 },
      )
      .build();
    const executor = new InProcessStepExecutor(wf as Workflow<unknown, unknown>, {
      storage,
      clock,
    });

    const pending = executor.executeStep({
      workflowId: "te-1",
      stepName: "hang",
      input: 1,
      prevResults: {},
      attempt: 1,
    });
    while (!started) await settle();
    clock.advance(5_000);
    const res = await pending;
    expect(res.ok).toBe(false);
    expect(res.ok === false && res.error).toContain("timed out after 5000ms");
  });
});

describe("InProcessStepExecutor without a runner-supplied runtime", () => {
  it("hands journaled bodies the bound definition's version and patches", async () => {
    const storage = new InMemoryWorkflowStorage();
    let seen: { version?: string; patched?: boolean } = {};
    const wf = workflow<number>({ name: "bare-exec", version: "7", patches: ["fix-1"] })
      .journaled("j", function* (ctx) {
        seen = { version: ctx.workflowVersion, patched: ctx.patched("fix-1") };
        return yield* ctx.activity("one", async () => 1);
      })
      .build();
    const executor = new InProcessStepExecutor(wf as Workflow<unknown, unknown>, { storage });

    const res = await executor.executeStep({
      workflowId: "be-1",
      stepName: "j",
      input: 1,
      prevResults: {},
      attempt: 1,
    });
    expect(res.ok).toBe(true);
    expect(seen).toEqual({ version: "7", patched: true });
  });
});

describe("fenced step-side writes — a stale lease cannot suspend the run", () => {
  /**
   * Loses the lease right before every suspension write: the running
   * orchestration's lock is released and another holder takes it, so the
   * write arrives with a stale fence token.
   */
  class LeaseLostBeforeSuspend extends InMemoryWorkflowStorage {
    override async suspendWorkflow(params: SuspendWorkflowParams): Promise<void> {
      const { workflowId } = params;
      await this.releaseLock({ workflowId });
      const taken = await this.tryLock({ workflowId, lockDurationMs: 60_000 });
      if (!taken.acquired) throw new Error("could not take the lock");
      return super.suspendWorkflow(params);
    }
  }

  async function runStale(wf: Workflow<number, unknown>, workflowId: string) {
    const storage = new LeaseLostBeforeSuspend();
    const runner = createWorkflowRunner({ storage });
    const res = await runner.runSafe({ workflow: wf, workflowId, input: 1 });
    return { res, state: await storage.loadWorkflow(workflowId) };
  }

  it(".sleep() suspension write is rejected", async () => {
    const wf = workflow<number>({ name: "fence-sleep" })
      .step("first", () => succeed(0))
      .sleep("nap", 60_000)
      .build();

    const { res, state } = await runStale(wf, "fs-1");
    expect(res.error?._tag).toBe("FenceTokenMismatchError");
    expect(state?.status).not.toBe("suspended");
    expect(state?.steps["nap"]?.status).not.toBe("sleeping");
  });

  it(".waitForSignal() suspension write is rejected", async () => {
    const wf = workflow<number>({ name: "fence-signal" })
      .step("first", () => succeed(0))
      .waitForSignal<string>("approval", { signalName: "approve" })
      .build();

    const { res, state } = await runStale(wf, "fsig-1");
    expect(res.error?._tag).toBe("FenceTokenMismatchError");
    expect(state?.status).not.toBe("suspended");
    expect(state?.steps["approval"]?.status).not.toBe("waiting_for_signal");
  });

  it("journaled ctx.sleep suspension write is rejected", async () => {
    const wf = workflow<number>({ name: "fence-journaled" })
      .journaled("wait", function* (ctx) {
        yield* ctx.sleep(60_000);
        return 1;
      })
      .build();

    const { res, state } = await runStale(wf, "fj-1");
    expect(res.error).not.toBeNull();
    expect(res.error?._tag).not.toBe("WorkflowSuspendedError");
    expect(state?.status).not.toBe("suspended");
    expect(state?.steps["wait"]?.status).not.toBe("sleeping");
  });

  it("a run that keeps its lease suspends normally", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const wf = workflow<number>({ name: "fence-ok" })
      .step("first", () => succeed(0))
      .sleep("nap", 60_000)
      .build();

    const res = await runner.runSafe({ workflow: wf, workflowId: "fo-1", input: 1 });
    expect(res.error?._tag).toBe("WorkflowSuspendedError");
    expect((await storage.loadWorkflow("fo-1"))?.steps["nap"]?.status).toBe("sleeping");
  });
});
