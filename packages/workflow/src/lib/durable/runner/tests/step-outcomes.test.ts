import { describe, expect, it } from "bun:test";
import { TaggedError, fail, succeed } from "@spilne/perfect-core";
import { FakeWallClock } from "../../../shared/wall-clock.ts";
import { workflow } from "../../durable-pipeline.ts";
import { StepError, WorkflowFailedError } from "../../durable-pipeline-error.ts";
import { InMemoryWorkflowStorage } from "../../in-memory-storage.ts";
import {
  InProcessStepExecutor,
  createWorkflowRunner,
  type StepExecutionRequest,
  type StepExecutionResult,
  type StepExecutor,
} from "../../workflow-runner.ts";

class Boom extends TaggedError("Boom")<{ readonly message: string }>() {}

const nextMacrotask = () => new Promise<void>((r) => setImmediate(r));

async function waitFor(cond: () => boolean): Promise<void> {
  for (let i = 0; i < 500 && !cond(); i++) await nextMacrotask();
  if (!cond()) throw new Error("waitFor: condition never held");
}

function deferred<T>(): { promise: Promise<T>; resolve: (v: T) => void } {
  let resolve!: (v: T) => void;
  const promise = new Promise<T>((r) => (resolve = r));
  return { promise, resolve };
}

function statuses(steps: Record<string, { status: string; error?: string }>) {
  return Object.fromEntries(
    Object.entries(steps).map(([k, v]) => [k, v.status + (v.error ? `(${v.error})` : "")]),
  );
}

// ---------------------------------------------------------------------------
// Failure attribution in parallel waves
// ---------------------------------------------------------------------------

describe("parallel wave failure attribution", () => {
  it("records the failure on the step that failed and keeps its sibling completed", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const wf = workflow<number>({ name: "par-fail" })
      .step("root", ({ input }) => succeed(input))
      .parallelSteps("fork", {
        ok: ({ prev }) => succeed((prev as number) * 10),
        bad: () => fail(new Boom({ message: "bad branch" })),
      })
      .build();

    const { error } = await runner.runSafe({ workflow: wf, workflowId: "pf-1", input: 1 });

    expect(error).toBeInstanceOf(Boom);
    const st = await storage.loadWorkflow("pf-1");
    expect(statuses(st!.steps)).toMatchObject({
      root: "completed",
      "fork.ok": "completed",
      "fork.bad": "failed(bad branch)",
    });
  });

  it("attributes a defect (rejected stepAsync) to its own step", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const wf = workflow<number>({ name: "par-defect" })
      .step("root", ({ input }) => succeed(input))
      .step("slowok", { dependsOn: ["root"] }, () => succeed(1))
      .stepAsync("boom", { dependsOn: ["root"] }, () => Promise.reject(new Error("defect in boom")))
      .build();

    await runner.runSafe({ workflow: wf, workflowId: "pd-1", input: 1 });

    const st = await storage.loadWorkflow("pd-1");
    expect(st!.steps["slowok"]!.status).toBe("completed");
    expect(st!.steps["boom"]!.status).toBe("failed");
    expect(st!.steps["boom"]!.error).toBe("defect in boom");
  });

  it("compensates the completed sibling of a failed step", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const compensated: string[] = [];
    const wf = workflow<number>({ name: "par-comp" })
      .step("root", ({ input }) => succeed(input))
      .step("ok", { dependsOn: ["root"] }, () => succeed("charged"), {
        compensate: ({ result }) => {
          compensated.push(`ok:${result}`);
          return succeed(undefined);
        },
      })
      .step("bad", { dependsOn: ["root"] }, () => fail(new Boom({ message: "nope" })))
      .build();

    await runner.runSafe({ workflow: wf, workflowId: "pc-1", input: 1 });

    expect(compensated).toEqual(["ok:charged"]);
  });

  it("does not re-run a completed sibling on workflow retry", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    let okRuns = 0;
    let badRuns = 0;
    const wf = workflow<number>({ name: "par-retry", retry: { maxRetries: 1, baseDelayMs: 1 } })
      .step("root", ({ input }) => succeed(input))
      .step("ok", { dependsOn: ["root"] }, () => succeed(++okRuns))
      .step("bad", { dependsOn: ["root"] }, () => {
        badRuns++;
        return fail(new Boom({ message: "nope" }));
      })
      .build();

    await runner.runSafe({ workflow: wf, workflowId: "pr-1", input: 1 });

    expect(okRuns).toBe(1);
    expect(badRuns).toBe(2);
  });

  it("records a row for every failed step of a wave and fails with the first", async () => {
    const storage = new InMemoryWorkflowStorage();
    const failures: string[] = [];
    const runner = createWorkflowRunner({
      storage,
      hooks: { onStepFailure: ({ stepName }) => void failures.push(stepName) },
    });
    const wf = workflow<number>({ name: "par-both" })
      .step("root", ({ input }) => succeed(input))
      .step("a", { dependsOn: ["root"] }, () => fail(new Boom({ message: "a" })))
      .step("b", { dependsOn: ["root"] }, () => fail(new Boom({ message: "b" })))
      .build();

    const { error } = await runner.runSafe({ workflow: wf, workflowId: "pb-1", input: 1 });

    expect((error as Boom).message).toBe("a");
    const st = await storage.loadWorkflow("pb-1");
    expect(st!.steps["a"]!.status).toBe("failed");
    expect(st!.steps["b"]!.status).toBe("failed");
    expect(failures.sort()).toEqual(["a", "b"]);
  });

  it("stores the real start and duration of a failed step", async () => {
    const clock = FakeWallClock.create(1_000);
    const storage = new InMemoryWorkflowStorage({ clock });
    const hookDurations: number[] = [];
    const runner = createWorkflowRunner({
      storage,
      clock,
      hooks: { onStepFailure: ({ durationMs }) => void hookDurations.push(durationMs) },
    });
    let armed = false;
    const wf = workflow<number>({ name: "fail-timing" })
      .stepAsync("slow-fail", async () => {
        armed = true;
        await new Promise<void>((r) => clock.setTimeout(() => r(), 500));
        throw new Error("late failure");
      })
      .build();

    const run = runner.runSafe({ workflow: wf, workflowId: "ft-1", input: 1 });
    await waitFor(() => armed);
    clock.advance(500);
    await run;

    const step = (await storage.loadWorkflow("ft-1"))!.steps["slow-fail"]!;
    expect(step.status).toBe("failed");
    expect(step.startedAt!.getTime()).toBe(1_000);
    expect(step.durationMs).toBe(500);
    expect(hookDurations).toEqual([500]);
    const [attempt] = await storage.loadStepAttempts({ workflowId: "ft-1", stepName: "slow-fail" });
    expect(attempt!.startedAt.getTime()).toBe(1_000);
    expect(attempt!.durationMs).toBe(500);
  });
});

// ---------------------------------------------------------------------------
// Attempt rows
// ---------------------------------------------------------------------------

describe("one attempt row per step attempt", () => {
  const flaky = (counter: { n: number }) =>
    workflow<number>({ name: "att" })
      .step(
        "flaky",
        () => (++counter.n < 3 ? fail(new Boom({ message: `x${counter.n}` })) : succeed(counter.n)),
        { retry: { maxRetries: 5, baseDelayMs: 1 } },
      )
      .build();

  it("inline: records each failed retry attempt and the final success", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });

    await runner.run({ workflow: flaky({ n: 0 }), workflowId: "att-1", input: 1 });

    const rows = await storage.loadStepAttempts({ workflowId: "att-1", stepName: "flaky" });
    expect(rows.map((r) => `${r.attempt}:${r.status}:${r.error ?? ""}`)).toEqual([
      "1:failed:x1",
      "2:failed:x2",
      "3:completed:",
    ]);
  });

  it("executor path: records the same rows", async () => {
    const storage = new InMemoryWorkflowStorage();
    const wf = flaky({ n: 0 });
    const runner = createWorkflowRunner({
      storage,
      stepExecutor: new InProcessStepExecutor(wf, { storage }),
    });

    await runner.run({ workflow: wf, workflowId: "att-2", input: 1 });

    const rows = await storage.loadStepAttempts({ workflowId: "att-2", stepName: "flaky" });
    expect(rows.map((r) => `${r.attempt}:${r.status}`)).toEqual([
      "1:failed",
      "2:failed",
      "3:completed",
    ]);
  });

  it("an attempt absorbed by onFailure keeps its failed row; the step completes", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const wf = workflow<number>({ name: "att-fallback" })
      .step("s", () => fail(new Boom({ message: "nope" })), {
        onFailure: { fallback: () => 0 },
      })
      .build();

    await runner.run({ workflow: wf, workflowId: "att-3", input: 1 });

    const rows = await storage.loadStepAttempts({ workflowId: "att-3", stepName: "s" });
    expect(rows.map((r) => `${r.attempt}:${r.status}`)).toEqual(["1:failed"]);
    expect((await storage.loadWorkflow("att-3"))!.steps["s"]!.status).toBe("completed");
  });

  it("numbers attempts on across workflow retries without duplicating the last one", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const wf = workflow<number>({ name: "att-wf", retry: { maxRetries: 1, baseDelayMs: 1 } })
      .step("s", () => fail(new Boom({ message: "nope" })), {
        retry: { maxRetries: 1, baseDelayMs: 1 },
      })
      .build();

    await runner.runSafe({ workflow: wf, workflowId: "att-4", input: 1 });

    const rows = await storage.loadStepAttempts({ workflowId: "att-4", stepName: "s" });
    expect(rows.map((r) => `${r.attempt}:${r.status}`)).toEqual([
      "1:failed",
      "2:failed",
      "3:failed",
      "4:failed",
    ]);
  });
});

// ---------------------------------------------------------------------------
// Executor path parity
// ---------------------------------------------------------------------------

describe("executor path", () => {
  it("waits for every sibling before failing, so a workflow retry never overlaps it", async () => {
    const storage = new InMemoryWorkflowStorage();
    let slowRuns = 0;
    let inflight = 0;
    let maxInflight = 0;
    let slowSawAbort = false;
    const gate = deferred<void>();
    const executor: StepExecutor = {
      async executeStep(req: StepExecutionRequest): Promise<StepExecutionResult> {
        if (req.stepName === "fork.bad") return { ok: false, error: "bad" };
        if (req.stepName === "fork.slow") {
          slowRuns++;
          inflight++;
          maxInflight = Math.max(maxInflight, inflight);
          await gate.promise;
          slowSawAbort = req.signal?.aborted === true;
          inflight--;
          return { ok: true, result: 1 };
        }
        return { ok: true, result: req.input };
      },
    };
    const runner = createWorkflowRunner({ storage, stepExecutor: executor });
    const wf = workflow<number>({ name: "exec-par", retry: { maxRetries: 1, baseDelayMs: 1 } })
      .step("root", ({ input }) => succeed(input))
      .parallelSteps("fork", { slow: () => succeed(1), bad: () => succeed(2) })
      .build();

    let settled = false;
    const run = runner.runSafe({ workflow: wf, workflowId: "e-1", input: 1 });
    void run.then(() => (settled = true));
    await waitFor(() => slowRuns === 1);
    for (let i = 0; i < 20; i++) await nextMacrotask();
    expect(settled).toBe(false);
    gate.resolve();
    const { error } = await run;

    expect(error).toBeInstanceOf(StepError);
    expect((error as StepError).stepName).toBe("fork.bad");
    expect(slowRuns).toBe(1);
    expect(maxInflight).toBe(1);
    expect(slowSawAbort).toBe(true);
    const st = await storage.loadWorkflow("e-1");
    expect(st!.steps["fork.slow"]!.status).toBe("completed");
    expect(st!.steps["fork.bad"]!.status).toBe("failed");
  });

  it("continue-as-new through InProcessStepExecutor chains a fresh run", async () => {
    const storage = new InMemoryWorkflowStorage();
    const wf = workflow<{ count: number }>({ name: "counter" })
      // oxlint-disable-next-line require-yield -- continue-as-new needs no activity
      .journaled("loop", function* (ctx) {
        if (ctx.input.count >= 2) return { final: ctx.input.count };
        ctx.continueAsNew({ count: ctx.input.count + 1 });
      })
      .build();
    const runner = createWorkflowRunner({
      storage,
      stepExecutor: new InProcessStepExecutor(wf, { storage }),
    });

    const result = await runner.run({ workflow: wf, workflowId: "can-1", input: { count: 0 } });

    expect(result).toEqual({ final: 2 });
  });

  it("InProcessStepExecutor reports continue-as-new and defects without throwing", async () => {
    const storage = new InMemoryWorkflowStorage();
    const wf = workflow<{ count: number }>({ name: "counter-direct" })
      // oxlint-disable-next-line require-yield -- continue-as-new needs no activity
      .journaled("loop", function* (ctx) {
        ctx.continueAsNew({ count: ctx.input.count + 1 });
      })
      .stepAsync("crash", () => Promise.reject(new Error("crashed")))
      .build();
    const executor = new InProcessStepExecutor(wf, { storage });
    await storage.createWorkflow({ workflowId: "d-1", workflowName: wf.name, input: { count: 0 } });

    const can = await executor.executeStep({
      workflowId: "d-1",
      stepName: "loop",
      input: { count: 0 },
      prevResults: {},
      attempt: 1,
    });
    expect(can.ok).toBe(false);
    expect(can.ok === false && can.kind).toBe("continue-as-new");
    if (!can.ok && can.kind === "continue-as-new") expect(can.nextInput).toEqual({ count: 1 });

    const crash = await executor.executeStep({
      workflowId: "d-1",
      stepName: "crash",
      input: { count: 0 },
      prevResults: {},
      attempt: 1,
    });
    expect(crash.ok === false && crash.kind).toBe("failed");
    if (!crash.ok && crash.kind === "failed") {
      expect(crash.error).toBe("crashed");
      expect(crash.cause).toBeInstanceOf(Error);
    }
  });

  it("keeps the step's typed error, so workflow retry `when` sees its tag", async () => {
    const storage = new InMemoryWorkflowStorage();
    let runs = 0;
    const wf = workflow<number>({
      name: "exec-typed",
      retry: { maxRetries: 1, baseDelayMs: 1, when: (e) => e._tag === "Boom" },
    })
      .step("s", () => {
        runs++;
        return fail(new Boom({ message: "typed" }));
      })
      .build();
    const runner = createWorkflowRunner({
      storage,
      stepExecutor: new InProcessStepExecutor(wf, { storage }),
    });

    const { error } = await runner.runSafe({ workflow: wf, workflowId: "t-1", input: 1 });

    expect(error).toBeInstanceOf(Boom);
    expect(runs).toBe(2);
  });

  it("a remote failure report becomes a StepError carrying its errorTag", async () => {
    const storage = new InMemoryWorkflowStorage();
    const executor: StepExecutor = {
      executeStep: async () => ({ ok: false, kind: "failed", error: "remote", errorTag: "Boom" }),
    };
    const runner = createWorkflowRunner({ storage, stepExecutor: executor });
    const wf = workflow<number>({ name: "exec-remote" })
      .step("s", () => succeed(1))
      .build();

    const { error } = await runner.runSafe({ workflow: wf, workflowId: "r-1", input: 1 });

    expect(error).toBeInstanceOf(StepError);
    expect((error as StepError).errorTag).toBe("Boom");
    expect((error as StepError).message).toBe("remote");
  });

  it("a suspension report suspends the run", async () => {
    const storage = new InMemoryWorkflowStorage();
    const executor: StepExecutor = {
      executeStep: async () => ({ ok: false, kind: "suspended", reason: "signal" }),
    };
    const runner = createWorkflowRunner({ storage, stepExecutor: executor });
    const wf = workflow<number>({ name: "exec-suspend" })
      .step("s", () => succeed(1))
      .build();

    const { error } = await runner.runSafe({ workflow: wf, workflowId: "s-1", input: 1 });

    expect((error as { _tag?: string })._tag).toBe("WorkflowSuspendedError");
    expect((await storage.loadWorkflow("s-1"))!.steps["s"]?.status).not.toBe("failed");
  });
});

// ---------------------------------------------------------------------------
// handle.result()
// ---------------------------------------------------------------------------

describe("WorkflowHandle.result()", () => {
  it("rejects a failed run with WorkflowFailedError naming the failed step", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const wf = workflow<number>({ name: "hr" })
      .step("x", () => fail(new Boom({ message: "typed" })))
      .build();

    const handle = await runner.start({ workflow: wf, workflowId: "hr-1", input: 1 });
    const error = await handle.result({ intervalMs: 5, timeoutMs: 2_000 }).catch((e) => e);

    expect(error).toBeInstanceOf(WorkflowFailedError);
    expect((error as WorkflowFailedError).stepName).toBe("x");
    expect((error as WorkflowFailedError).message).toBe("typed");
  });
});
